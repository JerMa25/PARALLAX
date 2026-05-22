/*
 * orchestrator.c
 *
 * Implémentation du moteur principal de l'orchestrateur.
 *
 * Architecture interne :
 *   - 4 sous-modules synchronisés (worker_table, job_table, scheduler,
 *     queue d'actions).
 *   - Un dispatcher dans handle_event() qui route vers le bon handler.
 *   - Un try_schedule() interne appelé après tout événement qui peut
 *     ouvrir une opportunité d'attribution.
 *
 * Convention sur la cohérence des structures :
 *   - worker_table : vue du cluster (qui est UP, état)
 *   - scheduler : registre des workers utilisables (avec scores)
 *   - Ces deux structures peuvent diverger transitoirement (un worker
 *     enregistré dans scheduler mais déjà parti de worker_table par ex.)
 *     Le scheduler est tolérant à ce désync (cf. test_unregistered_*).
 */

#include "orchestrator.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ==================================================================== */
/* Configuration                                                         */
/* ==================================================================== */

orchestrator_config_t orchestrator_default_config(void) {
    orchestrator_config_t c;
    c.heartbeat_timeout_ms = 10000;   /* 10s - cf. discussion protocole */
    c.max_task_retries = 3;
    c.max_actions_per_event = 64;
    return c;
}

/* ==================================================================== */
/* Queue d'actions interne                                               */
/* ==================================================================== */

/*
 * Implémentation simple : array circulaire de capacité fixe initiale,
 * avec realloc en cas de besoin. Les actions sont stockées par valeur
 * (les buffers payload sont possédés par chaque action).
 *
 * On utilise un buffer linéaire avec head/count plutôt qu'un anneau,
 * pour faciliter le drain par batch (memcpy).
 */
typedef struct {
    orchestrator_action_t *buf;
    size_t count;
    size_t capacity;
} action_queue_t;

static int aq_init(action_queue_t *q, size_t initial) {
    if (initial == 0) initial = 16;
    q->buf = calloc(initial, sizeof(orchestrator_action_t));
    if (!q->buf) return -1;
    q->count = 0;
    q->capacity = initial;
    return 0;
}

static void aq_destroy(action_queue_t *q) {
    if (!q || !q->buf) return;
    /* Libérer les payloads des actions encore dans la queue */
    for (size_t i = 0; i < q->count; i++) {
        orchestrator_action_free_payload(&q->buf[i]);
    }
    free(q->buf);
    q->buf = NULL;
    q->count = 0;
    q->capacity = 0;
}

static int aq_push(action_queue_t *q, const orchestrator_action_t *a) {
    if (q->count >= q->capacity) {
        size_t nc = q->capacity * 2;
        orchestrator_action_t *nb = realloc(q->buf,
                                              nc * sizeof(*nb));
        if (!nb) return -1;
        memset(&nb[q->capacity], 0,
               (nc - q->capacity) * sizeof(*nb));
        q->buf = nb;
        q->capacity = nc;
    }
    q->buf[q->count++] = *a;
    return 0;
}

/*
 * Drain : copie jusqu'à max actions dans out, et compacte la queue.
 * Renvoie le nombre copié.
 */
static size_t aq_drain(action_queue_t *q, orchestrator_action_t *out,
                        size_t max) {
    size_t n = (q->count < max) ? q->count : max;
    if (n == 0) return 0;
    memcpy(out, q->buf, n * sizeof(*out));
    /* Compacter le reste vers le début */
    if (n < q->count) {
        memmove(q->buf, &q->buf[n],
                (q->count - n) * sizeof(*q->buf));
    }
    q->count -= n;
    /* Zero la zone libérée pour pas garder de pointeurs orphelins
     * (sécurité en cas de bug ailleurs) */
    memset(&q->buf[q->count], 0, n * sizeof(*q->buf));
    return n;
}

/* ==================================================================== */
/* Structure principale                                                  */
/* ==================================================================== */

struct orchestrator_s {
    orchestrator_config_t  config;
    worker_table_t        *workers;
    job_table_t           *jobs;
    scheduler_t           *sched;
    action_queue_t         actions;
};

/* ==================================================================== */
/* Cycle de vie                                                          */
/* ==================================================================== */

orchestrator_t *orchestrator_create(const orchestrator_config_t *config) {
    orchestrator_t *o = calloc(1, sizeof(*o));
    if (!o) return NULL;

    if (config) {
        o->config = *config;
    } else {
        o->config = orchestrator_default_config();
    }

    o->workers = worker_table_create(8);
    o->jobs    = job_table_create(8);
    o->sched   = scheduler_create(8);
    if (!o->workers || !o->jobs || !o->sched) goto fail;

    if (aq_init(&o->actions, 32) != 0) goto fail;

    return o;

fail:
    if (o->workers) worker_table_destroy(o->workers);
    if (o->jobs)    job_table_destroy(o->jobs);
    if (o->sched)   scheduler_destroy(o->sched);
    free(o);
    return NULL;
}

void orchestrator_destroy(orchestrator_t *o) {
    if (!o) return;
    aq_destroy(&o->actions);
    scheduler_destroy(o->sched);
    job_table_destroy(o->jobs);
    worker_table_destroy(o->workers);
    free(o);
}

/* ==================================================================== */
/* Helpers : production d'actions                                        */
/* ==================================================================== */

/*
 * Produit une action ACT_DISPATCH_TASK pour la couche réseau.
 * Copie le payload (le caller pourra libérer la tâche d'origine).
 * Renvoie 0 ou -1 si OOM.
 */
static int emit_dispatch(orchestrator_t *o, uint64_t now_ms,
                          const char *worker_id, uint64_t job_id,
                          uint32_t task_id,
                          const uint8_t *payload, size_t payload_size) {
    orchestrator_action_t a;
    memset(&a, 0, sizeof(a));
    a.type = ACT_DISPATCH_TASK;
    a.timestamp_ms = now_ms;
    strncpy(a.data.dispatch_task.worker_id, worker_id,
            PARALLAX_UUID_LEN - 1);
    a.data.dispatch_task.job_id = job_id;
    a.data.dispatch_task.task_id = task_id;
    if (payload && payload_size > 0) {
        a.data.dispatch_task.payload = malloc(payload_size);
        if (!a.data.dispatch_task.payload) return -1;
        memcpy(a.data.dispatch_task.payload, payload, payload_size);
        a.data.dispatch_task.payload_size = payload_size;
    }
    if (aq_push(&o->actions, &a) != 0) {
        free(a.data.dispatch_task.payload);
        return -1;
    }
    return 0;
}

static void emit_log(orchestrator_t *o, uint64_t now_ms, int severity,
                      const char *fmt, ...) {
    orchestrator_action_t a;
    memset(&a, 0, sizeof(a));
    a.type = ACT_LOG;
    a.timestamp_ms = now_ms;
    a.data.log.severity = severity;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(a.data.log.message, sizeof(a.data.log.message), fmt, ap);
    va_end(ap);
    aq_push(&o->actions, &a);  /* on accepte la perte si OOM ici */
}

static int emit_job_done(orchestrator_t *o, uint64_t now_ms,
                          uint64_t job_id) {
    job_info_t info;
    if (job_table_get_info(o->jobs, job_id, &info) != 0) return -1;

    orchestrator_action_t a;
    memset(&a, 0, sizeof(a));
    a.type = ACT_NOTIFY_JOB_DONE;
    a.timestamp_ms = now_ms;
    a.data.notify_job_done.job_id = info.job_id;
    strncpy(a.data.notify_job_done.client_id, info.client_id,
            PARALLAX_CLIENT_ID_LEN - 1);
    a.data.notify_job_done.final_state = info.state;
    a.data.notify_job_done.stats = info.stats;
    return aq_push(&o->actions, &a);
}

/* ==================================================================== */
/* Helpers : try_schedule                                                */
/* ==================================================================== */

/*
 * Tente d'attribuer des tâches en suspens aux workers disponibles.
 *
 * Stratégie : on parcourt tous les jobs en état READY ou RUNNING,
 * on demande au scheduler d'attribuer un lot de tâches pour chaque,
 * borné par max_actions_per_event au total.
 *
 * Pour chaque attribution réussie, on émet une action DISPATCH_TASK
 * avec le payload de la tâche.
 */
static void try_schedule(orchestrator_t *o, uint64_t now_ms) {
    /* Liste des jobs schedulables : READY ou RUNNING */
    size_t total_jobs = job_table_count(o->jobs);
    if (total_jobs == 0) return;

    job_info_t *jobs_buf = calloc(total_jobs, sizeof(job_info_t));
    if (!jobs_buf) return;
    size_t got = job_table_snapshot_all(o->jobs, jobs_buf, total_jobs);

    size_t budget = o->config.max_actions_per_event;
    /* Réserver une marge pour les éventuels logs et notifications.
     * On n'utilise que la moitié pour les dispatches. */
    if (budget > 4) budget /= 2;

    for (size_t i = 0; i < got && budget > 0; i++) {
        if (jobs_buf[i].state != JOB_READY &&
            jobs_buf[i].state != JOB_RUNNING) continue;

        task_pool_t *pool = job_table_get_pool(o->jobs,
                                                 jobs_buf[i].job_id);
        if (!pool) continue;

        /* Borne par l'espace restant */
        schedule_action_t sched_buf[32];
        size_t k = (budget < 32) ? budget : 32;
        size_t n = scheduler_schedule(o->sched, o->workers, pool,
                                        jobs_buf[i].job_id, now_ms,
                                        sched_buf, k);
        if (n == 0) continue;

        /* Pour chaque attribution, émettre un dispatch */
        for (size_t j = 0; j < n; j++) {
            task_t *t = task_pool_find(pool, sched_buf[j].task_id);
            if (!t) continue;  /* ne devrait pas arriver */
            int rc = emit_dispatch(o, now_ms,
                                    sched_buf[j].worker_id,
                                    sched_buf[j].job_id,
                                    sched_buf[j].task_id,
                                    t->payload, t->payload_size);
            if (rc != 0) {
                emit_log(o, now_ms, 2,
                         "OOM emitting dispatch for task %u",
                         sched_buf[j].task_id);
            }
            budget--;
        }

        /* Mettre à jour l'état du job (probablement passe en RUNNING) */
        job_table_reevaluate(o->jobs, jobs_buf[i].job_id, now_ms);
    }

    free(jobs_buf);
}

/* ==================================================================== */
/* Helpers : finalisation d'un job                                       */
/* ==================================================================== */

/*
 * Vérifie si un job vient de basculer en état terminal et émet une
 * notification + supprime le job (libère sa pool).
 *
 * À appeler après chaque modification d'état d'une tâche.
 */
static void check_job_completion(orchestrator_t *o, uint64_t job_id,
                                   uint64_t now_ms) {
    job_state_t s = job_table_reevaluate(o->jobs, job_id, now_ms);
    if (s == JOB_COMPLETED || s == JOB_COMPLETED_PARTIAL ||
        s == JOB_FAILED) {
        emit_job_done(o, now_ms, job_id);
        emit_log(o, now_ms, 0,
                 "Job %lu finished in state %d",
                 (unsigned long)job_id, (int)s);
        /*
         * Note : on NE supprime PAS le job ici. Pourquoi : des résultats
         * tardifs (workers zombies) peuvent encore arriver. Le job
         * reste dans la table en lecture seule pour les ignorer
         * proprement. Une politique de garbage collection (par ex. tick
         * périodique qui supprime les jobs terminés depuis > N minutes)
         * sera ajoutée plus tard.
         */
    }
}

/* ==================================================================== */
/* Handlers d'événements                                                 */
/* ==================================================================== */

static int handle_job_submitted(orchestrator_t *o,
                                  const orchestrator_event_t *e) {
    const evt_job_submitted_t *d = &e->data.job_submitted;
    if (!d->tasks || d->n_tasks == 0) {
        emit_log(o, e->timestamp_ms, 1,
                 "JOB_SUBMITTED with 0 tasks, ignored");
        return 0;
    }

    /* Créer le job */
    uint64_t job_id = job_table_add_job(o->jobs, d->client_id,
                                          e->timestamp_ms);
    if (job_id == 0) {
        emit_log(o, e->timestamp_ms, 2,
                 "OOM creating job for client %s", d->client_id);
        return -2;
    }

    /* Remplir la pool. Les payloads sont copiés par task_pool_add. */
    task_pool_t *pool = job_table_get_pool(o->jobs, job_id);
    for (size_t i = 0; i < d->n_tasks; i++) {
        task_t t = d->tasks[i];
        /* Forcer la cohérence : les retries max viennent de la config
         * de l'orchestrator, pas du caller (qui pourrait passer 0). */
        t.max_retries = o->config.max_task_retries;
        if (task_pool_add(pool, &t) != 0) {
            emit_log(o, e->timestamp_ms, 2,
                     "Failed to add task %u to job %lu",
                     t.task_id, (unsigned long)job_id);
        }
    }

    /* Marquer prêt à scheduler */
    job_table_mark_ready(o->jobs, job_id);

    emit_log(o, e->timestamp_ms, 0,
             "Job %lu submitted by %s with %zu tasks",
             (unsigned long)job_id, d->client_id, d->n_tasks);

    /* Tenter une distribution immédiate */
    try_schedule(o, e->timestamp_ms);
    return 0;
}

static int handle_task_result(orchestrator_t *o,
                                const orchestrator_event_t *e) {
    const evt_task_result_t *d = &e->data.task_result;

    task_pool_t *pool = job_table_get_pool(o->jobs, d->job_id);
    if (!pool) {
        /* Résultat orphelin : job supprimé. Pas une erreur. */
        emit_log(o, e->timestamp_ms, 0,
                 "Result for unknown job %lu task %u, ignored",
                 (unsigned long)d->job_id, d->result.task_id);
        return 0;
    }

    /* Trouver le worker pour le repasser AVAILABLE */
    const char *worker_id = d->result.worker_id;

    /* Tenter la complétion */
    int rc = task_pool_mark_completed(pool, d->result.task_id,
                                        &d->result, e->timestamp_ms);
    if (rc == -1) {
        emit_log(o, e->timestamp_ms, 1,
                 "Result for unknown task %u in job %lu",
                 d->result.task_id, (unsigned long)d->job_id);
    } else if (rc == -2) {
        /* État incompatible : tâche déjà COMPLETED ou FAILED.
         * C'est le cas du résultat tardif d'un worker zombie. */
        emit_log(o, e->timestamp_ms, 0,
                 "Late result for task %u (state mismatch), ignored",
                 d->result.task_id);
    } else if (rc == -3) {
        /* Worker mismatch : tâche réassignée à un autre worker */
        emit_log(o, e->timestamp_ms, 0,
                 "Result from %s for task %u reassigned to other "
                 "worker, ignored", worker_id, d->result.task_id);
    } else if (rc == -4) {
        emit_log(o, e->timestamp_ms, 2,
                 "OOM storing result for task %u",
                 d->result.task_id);
        return -2;
    } else {
        /* Succès. Libérer le worker. */
        if (worker_table_find(o->workers, worker_id)) {
            worker_table_set_state(o->workers, worker_id, WORKER_AVAILABLE);
        }
        check_job_completion(o, d->job_id, e->timestamp_ms);
        try_schedule(o, e->timestamp_ms);
    }
    return 0;
}

static int handle_worker_joined(orchestrator_t *o,
                                  const orchestrator_event_t *e) {
    const evt_worker_joined_t *d = &e->data.worker_joined;
    if (d->node_id[0] == '\0') return -1;

    /*
     * 1. Ajouter / mettre à jour dans worker_table.
     *    add_or_update préserve l'état historique si le worker
     *    existait déjà (cas d'une re-jointure rapide).
     */
    int rc = worker_table_add_or_update(o->workers, d->node_id,
                                          d->node_name, &d->caps);
    if (rc != 0) {
        emit_log(o, e->timestamp_ms, 2,
                 "Failed to register worker %s in table (rc=%d)",
                 d->node_id, rc);
        return -2;
    }

    /*
     * 2. Initialiser son heartbeat. Sans cela, le tick suivant le
     *    déclarerait immédiatement SUSPECT (last_heartbeat_ms=0).
     */
    worker_table_touch_heartbeat(o->workers, d->node_id, e->timestamp_ms);

    /*
     * 3. Calculer le score et enregistrer dans le scheduler.
     *    register_worker s'occupe de l'init de received_count pour
     *    préserver l'équité asymptotique (cf. test late_joining).
     */
    uint64_t score = scheduler_compute_score(&d->caps);
    if (score == 0) {
        emit_log(o, e->timestamp_ms, 1,
                 "Worker %s has score 0 (caps=ram:%u cpu:%u), ignored",
                 d->node_id, d->caps.ram_mb, d->caps.cpu_count);
        /* On garde le worker dans worker_table mais pas dans le scheduler.
         * Il ne recevra pas de tâche. */
        return 0;
    }
    if (scheduler_register_worker(o->sched, d->node_id, score) != 0) {
        emit_log(o, e->timestamp_ms, 2,
                 "Failed to register worker %s in scheduler", d->node_id);
        return -2;
    }

    emit_log(o, e->timestamp_ms, 0,
             "Worker %s joined (ram=%u cpu=%u score=%lu)",
             d->node_id, d->caps.ram_mb, d->caps.cpu_count,
             (unsigned long)score);

    /* 4. Tenter d'attribuer des tâches qui attendaient un worker */
    try_schedule(o, e->timestamp_ms);
    return 0;
}
/*
 * kick_worker : retire un worker du système et requeue ses tâches.
 *
 * Utilisé par WORKER_LEFT (départ propre) et WORKER_FAILED (mort
 * détectée). Le comportement est identique ; seul le message de log
 * diffère.
 *
 * Étapes :
 *   1. Pour chaque job actif, requeue les tâches assignées à ce worker.
 *      task_pool_requeue_worker_tasks gère les retries et le passage
 *      en FAILED définitif si max_retries dépassé.
 *   2. Réévaluer chaque job touché (peut basculer en COMPLETED_PARTIAL).
 *   3. Marquer le worker comme FAILED dans worker_table.
 *   4. Désenregistrer du scheduler.
 *   5. Tenter une re-attribution.
 */
static void kick_worker(orchestrator_t *o, const char *worker_id,
                         const char *reason, uint64_t now_ms) {
    /* Snapshot des jobs actuels pour parcourir leurs pools */
    size_t njobs = job_table_count(o->jobs);
    if (njobs == 0) goto cleanup_state;

    job_info_t *jobs = calloc(njobs, sizeof(job_info_t));
    if (!jobs) {
        emit_log(o, now_ms, 2, "OOM in kick_worker for %s", worker_id);
        goto cleanup_state;
    }
    size_t got = job_table_snapshot_all(o->jobs, jobs, njobs);

    size_t total_requeued = 0;
    size_t total_failed = 0;

    for (size_t i = 0; i < got; i++) {
        /* Skip les jobs déjà terminés : leurs tâches sont COMPLETED
         * ou FAILED, le requeue n'aurait aucun effet mais on évite
         * un parcours inutile. */
        if (jobs[i].state == JOB_COMPLETED ||
            jobs[i].state == JOB_COMPLETED_PARTIAL ||
            jobs[i].state == JOB_FAILED ||
            jobs[i].state == JOB_CANCELLED) continue;

        task_pool_t *pool = job_table_get_pool(o->jobs, jobs[i].job_id);
        if (!pool) continue;

        uint32_t requeued_ids[64], failed_ids[64];
        size_t affected = task_pool_requeue_worker_tasks(
            pool, worker_id, now_ms,
            requeued_ids, 64, failed_ids, 64);

        if (affected > 0) {
            /* On ne sait pas exactement combien dans chaque catégorie
             * si on a dépassé la capacité, mais on a une borne.
             * Pour le log, on rapporte le nombre total touché. */
            emit_log(o, now_ms, 1,
                     "Worker %s left (%s): %zu tasks affected in job %lu",
                     worker_id, reason, affected,
                     (unsigned long)jobs[i].job_id);
            total_requeued += affected;
        }

        /* Réévaluer le job : il peut basculer en COMPLETED_PARTIAL
         * si certaines tâches viennent de passer en FAILED définitif. */
        check_job_completion(o, jobs[i].job_id, now_ms);
    }

    free(jobs);
    (void)total_failed;

cleanup_state:
    /* Marquer le worker FAILED dans worker_table.
     * On ne le RETIRE pas immédiatement : un worker FAILED reste
     * dans la table en lecture pour que les résultats tardifs
     * puissent être routés et rejetés proprement.
     * Le retrait définitif se fera par TICK ou par admin. */
    worker_table_set_state(o->workers, worker_id, WORKER_FAILED);

    /* Désenregistrer du scheduler : il ne recevra plus de tâche.
     * Note : ses contributions historiques au total_assignments sont
     * préservées (cf. scheduler_unregister_worker, comportement voulu). */
    scheduler_unregister_worker(o->sched, worker_id);

    /* Tenter une re-attribution maintenant que des tâches sont
     * REQUEUED. */
    try_schedule(o, now_ms);
    (void)total_requeued;
}

static int handle_worker_left(orchestrator_t *o,
                                const orchestrator_event_t *e) {
    const evt_worker_left_t *d = &e->data.worker_left;
    if (d->node_id[0] == '\0') return -1;
    emit_log(o, e->timestamp_ms, 0, "Worker %s leaving gracefully",
             d->node_id);
    kick_worker(o, d->node_id, "graceful", e->timestamp_ms);
    return 0;
}

static int handle_worker_failed(orchestrator_t *o,
                                  const orchestrator_event_t *e) {
    const evt_worker_failed_t *d = &e->data.worker_failed;
    if (d->node_id[0] == '\0') return -1;
    emit_log(o, e->timestamp_ms, 1, "Worker %s failed: %s",
             d->node_id, d->reason);
    kick_worker(o, d->node_id, d->reason, e->timestamp_ms);
    return 0;
}
static int handle_worker_heartbeat(orchestrator_t *o,
                                     const orchestrator_event_t *e) {
    const evt_worker_heartbeat_t *d = &e->data.worker_heartbeat;
    if (d->node_id[0] == '\0') return -1;

    /* Le worker doit être connu. Sinon, c'est un heartbeat orphelin :
     * un worker qui envoie des heartbeats sans avoir fait HELLO d'abord.
     * On loggue mais on ignore (sécurité). */
    worker_t *w = worker_table_find(o->workers, d->node_id);
    if (!w) {
        emit_log(o, e->timestamp_ms, 1,
                 "Heartbeat from unknown worker %s, ignored",
                 d->node_id);
        return 0;
    }

    /* Mise à jour du timestamp */
    worker_table_touch_heartbeat(o->workers, d->node_id, e->timestamp_ms);

    /*
     * Résurrection : si le worker était SUSPECT, il revient à la vie.
     * On le passe AVAILABLE.
     * Si il était FAILED, on ne fait rien (il est définitivement out :
     * le Controller doit envoyer un nouveau JOINED si on veut le ré-utiliser).
     */
    if (w->state == WORKER_SUSPECT) {
        worker_table_set_state(o->workers, d->node_id, WORKER_AVAILABLE);
        emit_log(o, e->timestamp_ms, 0,
                 "Worker %s recovered from SUSPECT", d->node_id);
        try_schedule(o, e->timestamp_ms);
    }

    return 0;
}

static int handle_tick(orchestrator_t *o,
                        const orchestrator_event_t *e) {
    /*
     * 1. Détection de timeouts : workers dont le dernier heartbeat
     *    est trop vieux passent en SUSPECT.
     *
     *    NOTE : on ne déclenche PAS de requeue/failure ici. SUSPECT
     *    est un état d'attente. La transition SUSPECT → FAILED est
     *    décidée par le Controller (qui a une vue plus complète) et
     *    arrive via WORKER_FAILED. C'est une dette technique consciente :
     *    si le Controller est lui-même mort, on a un problème (un
     *    worker peut rester SUSPECT indéfiniment).
     */
    if (o->config.heartbeat_timeout_ms > 0) {
        char suspects[16][PARALLAX_UUID_LEN];
        size_t flagged = worker_table_detect_timeouts(
            o->workers, e->timestamp_ms,
            o->config.heartbeat_timeout_ms,
            suspects, 16);
        for (size_t i = 0; i < flagged && i < 16; i++) {
            emit_log(o, e->timestamp_ms, 1,
                     "Worker %s flagged SUSPECT (timeout)", suspects[i]);
        }
    }

    /* 2. Tentative de scheduling proactif : au cas où une tâche aurait
     *    raté son train à un événement précédent. */
    try_schedule(o, e->timestamp_ms);
    return 0;
}

/* ==================================================================== */
/* API publique : handle_event                                           */
/* ==================================================================== */

int orchestrator_handle_event(orchestrator_t *o,
                                const orchestrator_event_t *event) {
    if (!o || !event) return -1;
    switch (event->type) {
        case EVT_JOB_SUBMITTED:    return handle_job_submitted(o, event);
        case EVT_TASK_RESULT:      return handle_task_result(o, event);
        case EVT_WORKER_JOINED:    return handle_worker_joined(o, event);
        case EVT_WORKER_LEFT:      return handle_worker_left(o, event);
        case EVT_WORKER_HEARTBEAT: return handle_worker_heartbeat(o, event);
        case EVT_WORKER_FAILED:    return handle_worker_failed(o, event);
        case EVT_TICK:             return handle_tick(o, event);
        default: return -1;
    }
}

/* ==================================================================== */
/* API publique : drain_outgoing                                         */
/* ==================================================================== */

size_t orchestrator_drain_outgoing(orchestrator_t *o,
                                    orchestrator_action_t *out,
                                    size_t max) {
    if (!o || !out || max == 0) return 0;
    return aq_drain(&o->actions, out, max);
}

void orchestrator_action_free_payload(orchestrator_action_t *a) {
    if (!a) return;
    if (a->type == ACT_DISPATCH_TASK) {
        free(a->data.dispatch_task.payload);
        a->data.dispatch_task.payload = NULL;
        a->data.dispatch_task.payload_size = 0;
    }
    /* Autres types : pas d'allocation à libérer (les structures sont
     * stockées par valeur). */
}

size_t orchestrator_pending_actions(const orchestrator_t *o) {
    return o ? o->actions.count : 0;
}

/* ==================================================================== */
/* Introspection                                                         */
/* ==================================================================== */

const worker_table_t *orchestrator_get_worker_table(const orchestrator_t *o) {
    return o ? o->workers : NULL;
}
const job_table_t *orchestrator_get_job_table(const orchestrator_t *o) {
    return o ? o->jobs : NULL;
}
const scheduler_t *orchestrator_get_scheduler(const orchestrator_t *o) {
    return o ? o->sched : NULL;
}
