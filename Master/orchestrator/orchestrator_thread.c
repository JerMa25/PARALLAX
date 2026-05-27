/*
 * orchestrator_thread.c
 *
 * Pont entre l'API event-driven de l'orchestrator (handle_event / drain)
 * et le contrat thread imposé par Agent_Init (Tchami) + le contrat IPC
 * imposé par l'Execution Master (em_ipc).
 *
 * Boucle principale du thread :
 *
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │  while (running) {                                            │
 *   │     msgrcv(EM_IPC_KEY_SUBMIT, IPC_NOWAIT)                     │
 *   │       └─▶ convertit en EVT_JOB_SUBMITTED                      │
 *   │            └─▶ orchestrator_handle_event()                    │
 *   │     drain_actions()                                           │
 *   │       └─▶ ACT_DISPATCH_TASK    : (loopback mode) → execute    │
 *   │       └─▶ ACT_NOTIFY_JOB_DONE  : envoie em_result_msg_t       │
 *   │       └─▶ ACT_LOG              : printf                       │
 *   │     EVT_TICK toutes les TICK_INTERVAL_MS                      │
 *   │     usleep(POLL_INTERVAL_US)                                  │
 *   │  }                                                            │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * Mode loopback (LOOPBACK_WORKERS=1, défaut) :
 *   En l'absence d'un Network Thread effectivement connecté aux workers
 *   distants, ce thread "simule" les workers en exécutant directement
 *   le calcul chunk_value × multiplier à la réception d'un
 *   ACT_DISPATCH_TASK, puis en réinjectant un EVT_TASK_RESULT.
 *
 *   Cela permet de valider la chaîne complète :
 *     Execution Master → IPC → Orchestrator → (simulé) → IPC → EM
 *   sans dépendre de la couche réseau.
 *
 *   Pour désactiver (vrai réseau) : compiler avec -DLOOPBACK_WORKERS=0
 *   et ajouter l'envoi des dispatches via la network layer.
 */

#include "orchestrator.h"
#include "em_ipc.h"
#include "em_types.h"

#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/msg.h>

/* ====================================================================
 * Paramètres de la boucle
 * ==================================================================== */

#ifndef LOOPBACK_WORKERS
#define LOOPBACK_WORKERS 1   /* 1 = simule les workers localement */
#endif

#define POLL_INTERVAL_US     10000   /* 10ms entre deux polls msgrcv */
#define TICK_INTERVAL_MS     1000    /* tick interne toutes les secondes */
#define MAX_ACTIONS_PER_LOOP 64

/* ====================================================================
 * État global du thread
 * ==================================================================== */

static volatile int    g_running       = 0;
static orchestrator_t *g_orch          = NULL;
static int             g_submit_qid    = -1;  /* msgget EM_IPC_KEY_SUBMIT */
static int             g_result_qid    = -1;  /* msgget EM_IPC_KEY_RESULT */

/* Mapping em_job_id ↔ orch_job_id et collecte des résultats partiels.
 *
 * L'orchestrator assigne son propre job_id interne, différent de celui
 * envoyé par l'EM. On garde la correspondance.
 */
#define MAX_TRACKED_JOBS 32
typedef struct {
    uint64_t          em_job_id;     /* tel qu'envoyé par l'EM */
    uint64_t          orch_job_id;   /* assigné en interne par l'orchestrator */
    bool              in_use;
    bool              orch_id_known; /* false jusqu'au premier dispatch */
    size_t            n_results;
    em_task_result_t  results[EM_MAX_TASKS_PER_JOB];
} job_track_t;
static job_track_t g_tracked[MAX_TRACKED_JOBS];

/* ====================================================================
 * Helpers : logging horodaté
 * ==================================================================== */

static void orch_log(const char *level, const char *fmt, ...) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    printf("[%02d:%02d:%02d] [ORCHESTRATOR] [%s] %s\n",
           t->tm_hour, t->tm_min, t->tm_sec, level, buf);
    fflush(stdout);
}

/* ====================================================================
 * Helpers : tracking par em_job_id et par orch_job_id
 * ==================================================================== */

static job_track_t *track_create(uint64_t em_job_id) {
    for (int i = 0; i < MAX_TRACKED_JOBS; i++) {
        if (!g_tracked[i].in_use) {
            memset(&g_tracked[i], 0, sizeof(job_track_t));
            g_tracked[i].in_use = true;
            g_tracked[i].em_job_id = em_job_id;
            g_tracked[i].orch_id_known = false;
            return &g_tracked[i];
        }
    }
    return NULL;
}

static job_track_t *track_find_by_orch(uint64_t orch_job_id) {
    for (int i = 0; i < MAX_TRACKED_JOBS; i++) {
        if (g_tracked[i].in_use && g_tracked[i].orch_id_known &&
            g_tracked[i].orch_job_id == orch_job_id)
            return &g_tracked[i];
    }
    return NULL;
}

static void track_release(job_track_t *trk) {
    if (trk) trk->in_use = false;
}

/* ====================================================================
 * Conversion em_task_t  ↔  task_t (payload binaire)
 * ====================================================================
 *
 * Payload format (32 bytes, little-endian) :
 *   [0..7]   : chunk_value (int64)
 *   [8..15]  : multiplier  (int64)
 *   [16..23] : position    (int64, signed)
 *   [24..31] : task_id     (uint32 + padding)
 */

#define PAYLOAD_SIZE 32

static void pack_em_task(uint8_t *buf, const em_task_t *t) {
    int64_t cv = (int64_t)t->chunk_value;
    int64_t mu = (int64_t)t->multiplier;
    int64_t po = (int64_t)t->position;
    memcpy(&buf[0],  &cv, 8);
    memcpy(&buf[8],  &mu, 8);
    memcpy(&buf[16], &po, 8);
    memcpy(&buf[24], &t->task_id, 4);
    memset(&buf[28], 0, 4);
}

static void unpack_em_task(const uint8_t *buf, int64_t *chunk_value,
                            int64_t *multiplier, int64_t *position) {
    memcpy(chunk_value, &buf[0],  8);
    memcpy(multiplier,  &buf[8],  8);
    memcpy(position,    &buf[16], 8);
}

/* ====================================================================
 * Envoi d'un résultat final vers l'Execution Master
 * ==================================================================== */

static void send_result_to_em(job_track_t *trk, bool success) {
    em_result_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.mtype     = EM_MSG_JOB_RESULT;
    msg.job_id    = trk->em_job_id;   /* IMPORTANT : le job_id de l'EM */
    msg.success   = success;
    msg.n_results = trk->n_results;
    memcpy(msg.results, trk->results,
           trk->n_results * sizeof(em_task_result_t));

    if (g_result_qid < 0) {
        g_result_qid = msgget(EM_IPC_KEY_RESULT, 0666 | IPC_CREAT);
        if (g_result_qid < 0) {
            orch_log("ERROR", "msgget(RESULT): %s", strerror(errno));
            return;
        }
    }

    /* Taille du payload : on n'envoie QUE les n_results premiers résultats,
     * pas le tableau de taille max. Sinon le message dépasse MSGMAX (8 Ko
     * sur la plupart des systèmes). */
    size_t header_size = sizeof(em_result_msg_t)
                         - sizeof(em_task_result_t) * EM_MAX_TASKS_PER_JOB
                         - sizeof(long);
    size_t payload_size = header_size
                          + trk->n_results * sizeof(em_task_result_t);

    if (msgsnd(g_result_qid, &msg, payload_size, IPC_NOWAIT) != 0) {
        orch_log("ERROR", "msgsnd(RESULT em_job=%lu): %s",
                 (unsigned long)trk->em_job_id, strerror(errno));
        return;
    }
    orch_log("INFO",
        "Result sent to EM (em_job=%lu n_results=%zu success=%d)",
        (unsigned long)trk->em_job_id, trk->n_results, (int)success);
}

/* ====================================================================
 * Mode loopback : simule l'exécution d'une tâche par un worker
 * ==================================================================== */

#if LOOPBACK_WORKERS
static void loopback_execute_and_inject(const act_dispatch_task_t *dt,
                                          uint64_t now_ms) {
    int64_t cv, mu, po;
    unpack_em_task(dt->payload, &cv, &mu, &po);

    /* Le calcul "worker" : chunk × multiplier */
    long partial = (long)(cv * mu);

    /* Construire un EVT_TASK_RESULT et le réinjecter dans l'orchestrator */
    orchestrator_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.type = EVT_TASK_RESULT;
    evt.timestamp_ms = now_ms;
    evt.data.task_result.job_id = dt->job_id;
    evt.data.task_result.result.task_id = dt->task_id;
    strncpy(evt.data.task_result.result.worker_id, dt->worker_id,
            PARALLAX_UUID_LEN - 1);
    evt.data.task_result.result.success = true;
    evt.data.task_result.result.exit_code = 0;
    evt.data.task_result.result.execution_ms = 1;

    /* Encoder le résultat partiel dans 'output' (16 bytes : partial + position) */
    uint8_t obuf[16];
    int64_t partial64 = (int64_t)partial;
    memcpy(&obuf[0], &partial64, 8);
    memcpy(&obuf[8], &po, 8);
    evt.data.task_result.result.output = obuf;
    evt.data.task_result.result.output_size = sizeof(obuf);

    orchestrator_handle_event(g_orch, &evt);

    /* Aussi enregistrer dans le tracker pour reconstruire le résultat
     * final vers l'EM. dt->job_id est le orch_job_id. */
    job_track_t *trk = track_find_by_orch(dt->job_id);
    if (trk && trk->n_results < EM_MAX_TASKS_PER_JOB) {
        em_task_result_t *r = &trk->results[trk->n_results++];
        r->task_id = dt->task_id;
        r->job_id  = trk->em_job_id;
        r->success = true;
        r->partial_result = partial;
        r->position = (int)po;
        r->error[0] = '\0';
    }
}
#endif /* LOOPBACK_WORKERS */

/* ====================================================================
 * Helpers : injection initiale de workers virtuels
 * ====================================================================
 *
 * En mode loopback, on simule 3 workers virtuels au démarrage. Sans eux,
 * le scheduler ne pourrait dispatcher aucune tâche.
 */

#if LOOPBACK_WORKERS
static void inject_loopback_workers(uint64_t now_ms) {
    const char *ids[] = { "loopback-1", "loopback-2", "loopback-3" };
    for (int i = 0; i < 3; i++) {
        orchestrator_event_t evt;
        memset(&evt, 0, sizeof(evt));
        evt.type = EVT_WORKER_JOINED;
        evt.timestamp_ms = now_ms;
        strncpy(evt.data.worker_joined.node_id, ids[i],
                PARALLAX_UUID_LEN - 1);
        strncpy(evt.data.worker_joined.node_name, ids[i],
                PARALLAX_NODE_NAME_MAX - 1);
        evt.data.worker_joined.caps.ram_mb = 1024;
        evt.data.worker_joined.caps.cpu_count = 1;
        evt.data.worker_joined.caps.cpu_mhz = 2000;
        orchestrator_handle_event(g_orch, &evt);
    }
}
#endif

/* ====================================================================
 * Drain des actions et routing
 * ==================================================================== */

static void process_actions(uint64_t now_ms) {
    orchestrator_action_t actions[MAX_ACTIONS_PER_LOOP];
    size_t n = orchestrator_drain_outgoing(g_orch, actions,
                                              MAX_ACTIONS_PER_LOOP);
    for (size_t i = 0; i < n; i++) {
        switch (actions[i].type) {
        case ACT_DISPATCH_TASK:
#if LOOPBACK_WORKERS
            loopback_execute_and_inject(&actions[i].data.dispatch_task,
                                          now_ms);
#else
            /* TODO : envoyer via la couche réseau (Ngonga) */
            orch_log("WARN",
                "DISPATCH task=%u (LOOPBACK off, no network layer)",
                actions[i].data.dispatch_task.task_id);
#endif
            break;

        case ACT_NOTIFY_JOB_DONE: {
            uint64_t orch_jid = actions[i].data.notify_job_done.job_id;
            job_track_t *trk = track_find_by_orch(orch_jid);
            bool success = (actions[i].data.notify_job_done.final_state
                            == JOB_COMPLETED);
            if (trk) {
                send_result_to_em(trk, success);
                track_release(trk);
            } else {
                orch_log("WARN",
                    "NOTIFY_JOB_DONE for unknown orch_job=%lu",
                    (unsigned long)orch_jid);
            }
            break;
        }

        case ACT_LOG: {
            const char *lvls[] = {"INFO", "WARN", "ERROR"};
            int sv = actions[i].data.log.severity;
            if (sv < 0 || sv > 2) sv = 0;
            orch_log(lvls[sv], "%s", actions[i].data.log.message);
            break;
        }
        }
        orchestrator_action_free_payload(&actions[i]);
    }
}

/* ====================================================================
 * Réception d'une soumission EM → événement JOB_SUBMITTED
 * ==================================================================== */

static void poll_submissions(uint64_t now_ms) {
    em_submit_msg_t msg;
    size_t payload_size = sizeof(em_submit_msg_t) - sizeof(long);

    /* IPC_NOWAIT : non bloquant, on sort si rien à lire */
    ssize_t r = msgrcv(g_submit_qid, &msg, payload_size,
                        EM_MSG_JOB_SUBMIT, IPC_NOWAIT);
    if (r < 0) {
        if (errno != ENOMSG && errno != EAGAIN) {
            orch_log("ERROR", "msgrcv(SUBMIT): %s", strerror(errno));
        }
        return;
    }

    orch_log("INFO", "Job %lu received from EM (n_tasks=%zu, client=%s)",
             (unsigned long)msg.job_id, msg.n_tasks, msg.client_id);

    /* Convertir em_task_t[] en task_t[] avec payload binaire */
    if (msg.n_tasks == 0 || msg.n_tasks > EM_MAX_TASKS_PER_JOB) {
        orch_log("ERROR", "Invalid n_tasks=%zu for job %lu",
                 msg.n_tasks, (unsigned long)msg.job_id);
        return;
    }

    task_t *tasks = calloc(msg.n_tasks, sizeof(task_t));
    if (!tasks) {
        orch_log("ERROR", "OOM allocating %zu tasks for job %lu",
                 msg.n_tasks, (unsigned long)msg.job_id);
        return;
    }
    uint8_t *payload_buffers = calloc(msg.n_tasks, PAYLOAD_SIZE);
    if (!payload_buffers) {
        free(tasks);
        orch_log("ERROR", "OOM allocating payloads");
        return;
    }

    for (size_t i = 0; i < msg.n_tasks; i++) {
        tasks[i].task_id = msg.tasks[i].task_id;
        tasks[i].state = TASK_PENDING;
        tasks[i].max_retries = 3;
        tasks[i].payload = &payload_buffers[i * PAYLOAD_SIZE];
        tasks[i].payload_size = PAYLOAD_SIZE;
        pack_em_task(tasks[i].payload, &msg.tasks[i]);
    }

    /* Construire l'événement JOB_SUBMITTED. Note : l'orchestrator copie
     * le payload en interne, on peut donc libérer les buffers locaux
     * après l'appel. */
    orchestrator_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.type = EVT_JOB_SUBMITTED;
    evt.timestamp_ms = now_ms;
    strncpy(evt.data.job_submitted.client_id, msg.client_id,
            PARALLAX_CLIENT_ID_LEN - 1);
    evt.data.job_submitted.tasks = tasks;
    evt.data.job_submitted.n_tasks = msg.n_tasks;

    /* Réserver le slot de tracking pour cet em_job_id.
     * L'orch_job_id sera connu après l'appel à handle_event. */
    job_track_t *trk = track_create(msg.job_id);
    if (!trk) {
        free(payload_buffers); free(tasks);
        orch_log("ERROR", "tracker full, dropping job %lu",
                 (unsigned long)msg.job_id);
        return;
    }

    /* Snapshot du nombre de jobs AVANT pour identifier le nouveau */
    size_t jobs_before = job_table_count(orchestrator_get_job_table(g_orch));

    orchestrator_handle_event(g_orch, &evt);

    /* Résoudre l'orch_job_id du job qu'on vient de créer.
     * job_table_count a augmenté de 1. Le job le plus récent est
     * celui qu'on cherche. Comme l'orchestrator utilise un compteur
     * monotone, c'est le job avec l'id maximal. */
    {
        const job_table_t *jt = orchestrator_get_job_table(g_orch);
        size_t after = job_table_count(jt);
        (void)jobs_before;
        if (after > 0) {
            job_info_t *all = calloc(after, sizeof(job_info_t));
            if (all) {
                size_t got = job_table_snapshot_all(jt, all, after);
                uint64_t max_id = 0;
                for (size_t k = 0; k < got; k++) {
                    if (all[k].job_id > max_id) max_id = all[k].job_id;
                }
                trk->orch_job_id = max_id;
                trk->orch_id_known = true;
                orch_log("INFO",
                    "Mapping em_job=%lu -> orch_job=%lu",
                    (unsigned long)msg.job_id, (unsigned long)max_id);
                free(all);
            }
        }
    }

    free(payload_buffers);
    free(tasks);
}

/* ====================================================================
 * Tick périodique
 * ==================================================================== */

static uint64_t now_ms_real(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void emit_tick(uint64_t now_ms) {
    orchestrator_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.type = EVT_TICK;
    evt.timestamp_ms = now_ms;
    orchestrator_handle_event(g_orch, &evt);
}

/* ====================================================================
 * API publique : run / stop
 * ==================================================================== */

void *orchestrator_thread_run(void *arg) {
    (void)arg;

    /* 1. Création de l'instance d'orchestrator */
    g_orch = orchestrator_create(NULL);
    if (!g_orch) {
        orch_log("ERROR", "orchestrator_create failed");
        return NULL;
    }
    memset(g_tracked, 0, sizeof(g_tracked));

    /* 2. Ouverture (ou création) de la queue de soumission de l'EM.
     *    L'EM la crée normalement le premier ; on utilise IPC_CREAT
     *    par défense (race possible au démarrage). */
    g_submit_qid = msgget(EM_IPC_KEY_SUBMIT, 0666 | IPC_CREAT);
    if (g_submit_qid < 0) {
        orch_log("ERROR", "msgget(SUBMIT): %s", strerror(errno));
        orchestrator_destroy(g_orch);
        g_orch = NULL;
        return NULL;
    }
    g_result_qid = msgget(EM_IPC_KEY_RESULT, 0666 | IPC_CREAT);
    if (g_result_qid < 0) {
        orch_log("WARN", "msgget(RESULT): %s (will retry on send)",
                 strerror(errno));
    }

    orch_log("INFO", "Orchestrator thread started (loopback=%d)",
             LOOPBACK_WORKERS);

#if LOOPBACK_WORKERS
    inject_loopback_workers(now_ms_real());
    process_actions(now_ms_real());  /* drainer les logs de jointure */
#endif

    g_running = 1;
    uint64_t last_tick = now_ms_real();

    /* 3. Boucle principale */
    while (g_running) {
        uint64_t now = now_ms_real();

        poll_submissions(now);
        process_actions(now);

        if (now - last_tick >= TICK_INTERVAL_MS) {
            emit_tick(now);
            process_actions(now);
            last_tick = now;
        }

        usleep(POLL_INTERVAL_US);
    }

    orch_log("INFO", "Orchestrator thread stopping...");

    /* 4. Cleanup */
    orchestrator_destroy(g_orch);
    g_orch = NULL;
    /* On ne supprime PAS les queues IPC : l'EM peut encore en avoir besoin
     * (et les supprimer ici causerait des erreurs côté EM). */
    return NULL;
}

void orchestrator_stop(void) {
    g_running = 0;
    /* Le thread sort de sa boucle dès le prochain poll (~10ms max). */
}
