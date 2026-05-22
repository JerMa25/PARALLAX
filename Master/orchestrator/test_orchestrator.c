/*
 * test_orchestrator.c
 *
 * Tests d'intégration de l'orchestrator.
 *
 * On teste le module dans son ensemble : injection d'événements,
 * drain d'actions, et observation de l'état interne. C'est différent
 * des tests unitaires précédents : on vérifie le COMPORTEMENT global,
 * pas l'API d'un module isolé.
 *
 * Couverture étape A :
 *   - Création / destruction
 *   - Job submitted → dispatches émis
 *   - Task results → job se termine → notification émise
 *   - Late results (worker zombie) → ignorés sans erreur
 *   - Job sans worker → reste en attente sans actions
 *   - Multiples jobs en parallèle
 */

#include "test_framework.h"
#include "orchestrator.h"

#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

/* Crée un orchestrator par défaut, plus quelques workers prêts. */
typedef struct {
    orchestrator_t *o;
} orch_ctx_t;

static orch_ctx_t make_orch(void) {
    orch_ctx_t c;
    c.o = orchestrator_create(NULL);
    return c;
}

static void destroy_orch(orch_ctx_t *c) {
    /* Drain et libération des actions restantes */
    orchestrator_action_t actions[64];
    size_t n;
    while ((n = orchestrator_drain_outgoing(c->o, actions, 64)) > 0) {
        for (size_t i = 0; i < n; i++) {
            orchestrator_action_free_payload(&actions[i]);
        }
    }
    orchestrator_destroy(c->o);
}

/*
 * Pour l'étape A, les workers sont injectés directement dans les
 * sous-tables internes via des accès en cast (les handlers
 * WORKER_JOINED ne sont pas encore implémentés). Quand l'étape B
 * sera faite, on remplacera par des événements WORKER_JOINED.
 */
static void inject_worker(orchestrator_t *o, const char *id, uint64_t score) {
    /* Hack temporaire : on accède à l'état interne. C'est OK pour les
     * tests d'étape A. Le test sera refait via WORKER_JOINED en étape B. */
    worker_table_t *wt = (worker_table_t *)orchestrator_get_worker_table(o);
    scheduler_t    *sc = (scheduler_t *)orchestrator_get_scheduler(o);
    worker_capabilities_t caps = { .ram_mb = 1024, .cpu_count = 1 };
    worker_table_add_or_update(wt, id, "test-node", &caps);
    scheduler_register_worker(sc, id, score);
}

/* Construit un événement JOB_SUBMITTED avec n tâches simples.
 * Les tasks et leurs payloads sont alloués localement et libérés
 * par le caller après l'appel à handle_event. */
static void build_job_event(orchestrator_event_t *evt, task_t *tasks_buf,
                              size_t n, const char *client,
                              uint64_t now_ms) {
    memset(evt, 0, sizeof(*evt));
    evt->type = EVT_JOB_SUBMITTED;
    evt->timestamp_ms = now_ms;
    strncpy(evt->data.job_submitted.client_id, client,
            PARALLAX_CLIENT_ID_LEN - 1);
    for (size_t i = 0; i < n; i++) {
        memset(&tasks_buf[i], 0, sizeof(task_t));
        tasks_buf[i].task_id = (uint32_t)(i + 1);
        tasks_buf[i].state = TASK_PENDING;
        tasks_buf[i].max_retries = 3;
        /* Payload : 4 octets contenant l'id, pour vérifier la copie */
        tasks_buf[i].payload = malloc(4);
        memcpy(tasks_buf[i].payload, &tasks_buf[i].task_id, 4);
        tasks_buf[i].payload_size = 4;
    }
    evt->data.job_submitted.tasks = tasks_buf;
    evt->data.job_submitted.n_tasks = n;
}

static void free_tasks_buf(task_t *tasks, size_t n) {
    for (size_t i = 0; i < n; i++) free(tasks[i].payload);
}

/* Construit un événement TASK_RESULT.
 * Le caller fournit l'output (peut être NULL). */
static void build_result_event(orchestrator_event_t *evt,
                                 uint64_t job_id, uint32_t task_id,
                                 const char *worker_id, bool success,
                                 const char *output, uint64_t now_ms) {
    memset(evt, 0, sizeof(*evt));
    evt->type = EVT_TASK_RESULT;
    evt->timestamp_ms = now_ms;
    evt->data.task_result.job_id = job_id;
    evt->data.task_result.result.task_id = task_id;
    strncpy(evt->data.task_result.result.worker_id, worker_id,
            PARALLAX_UUID_LEN - 1);
    evt->data.task_result.result.success = success;
    evt->data.task_result.result.exit_code = success ? 0 : 1;
    if (output) {
        size_t n = strlen(output);
        /* On utilise un buffer statique pour simplifier ; pour les
         * tests, c'est OK car on n'a qu'un évent à la fois.
         * Note : task_pool_mark_completed copie le buffer, donc même
         * si on libère/réutilise, c'est safe. */
        static uint8_t buf[256];
        size_t cp = (n < sizeof(buf)) ? n : sizeof(buf);
        memcpy(buf, output, cp);
        evt->data.task_result.result.output = buf;
        evt->data.task_result.result.output_size = cp;
    }
}

/* Drain et libère toutes les actions actuellement en queue. */
static void drain_and_free(orchestrator_t *o) {
    orchestrator_action_t actions[64];
    size_t n;
    while ((n = orchestrator_drain_outgoing(o, actions, 64)) > 0) {
        for (size_t i = 0; i < n; i++) {
            orchestrator_action_free_payload(&actions[i]);
        }
    }
}

/* Compte les actions d'un type donné dans un tableau. */
static size_t count_actions(const orchestrator_action_t *actions, size_t n,
                              action_type_t type) {
    size_t k = 0;
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == type) k++;
    }
    return k;
}

/* ------------------------------------------------------------------ */
/* Tests                                                               */
/* ------------------------------------------------------------------ */

static void test_create_and_destroy(void) {
    TEST_BEGIN("create_and_destroy");
    orchestrator_t *o = orchestrator_create(NULL);
    ASSERT_TRUE(o != NULL);
    ASSERT_EQ_INT(orchestrator_pending_actions(o), 0);
    orchestrator_destroy(o);
    orchestrator_destroy(NULL);
    TEST_END_PASS();
}

static void test_default_config(void) {
    TEST_BEGIN("default_config");
    orchestrator_config_t c = orchestrator_default_config();
    ASSERT_TRUE(c.heartbeat_timeout_ms > 0);
    ASSERT_TRUE(c.max_task_retries > 0);
    ASSERT_TRUE(c.max_actions_per_event > 0);
    TEST_END_PASS();
}

static void test_invalid_event(void) {
    TEST_BEGIN("invalid_event");
    orch_ctx_t c = make_orch();
    int rc = orchestrator_handle_event(c.o, NULL);
    ASSERT_EQ_INT(rc, -1);
    rc = orchestrator_handle_event(NULL, NULL);
    ASSERT_EQ_INT(rc, -1);

    orchestrator_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.type = 99999;  /* type inconnu */
    rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, -1);
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Test fondamental : un job arrive, des tâches sont distribuées.
 * On doit voir des actions DISPATCH_TASK égales au min(workers, tasks).
 */
static void test_job_submitted_distributes_tasks(void) {
    TEST_BEGIN("job_submitted_distributes_tasks");
    orch_ctx_t c = make_orch();
    inject_worker(c.o, "wA", 100);
    inject_worker(c.o, "wB", 100);

    task_t tasks[5];
    orchestrator_event_t evt;
    build_job_event(&evt, tasks, 5, "alice", 1000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);
    free_tasks_buf(tasks, 5);

    /* Drain et compter */
    orchestrator_action_t actions[64];
    size_t n = orchestrator_drain_outgoing(c.o, actions, 64);

    size_t dispatches = count_actions(actions, n, ACT_DISPATCH_TASK);
    /* 2 workers disponibles, 5 tâches : on doit dispatcher 2 */
    ASSERT_EQ_INT(dispatches, 2);

    /* Vérifier que chaque dispatch a un payload non-nul */
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_DISPATCH_TASK) {
            ASSERT_TRUE(actions[i].data.dispatch_task.payload != NULL);
            ASSERT_EQ_INT(actions[i].data.dispatch_task.payload_size, 4);
        }
    }

    /* Libérer les payloads */
    for (size_t i = 0; i < n; i++) {
        orchestrator_action_free_payload(&actions[i]);
    }

    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Test du cycle complet happy path : job arrive, dispatched, results
 * reviennent, job complété, notification émise.
 */
static void test_full_cycle_happy_path(void) {
    TEST_BEGIN("full_cycle_happy_path");
    orch_ctx_t c = make_orch();
    inject_worker(c.o, "wA", 100);

    task_t tasks[3];
    orchestrator_event_t evt;
    build_job_event(&evt, tasks, 3, "alice", 1000);
    orchestrator_handle_event(c.o, &evt);
    free_tasks_buf(tasks, 3);

    /* Drain première vague : 1 dispatch (un seul worker pour 3 tâches) */
    orchestrator_action_t actions[64];
    size_t n = orchestrator_drain_outgoing(c.o, actions, 64);
    size_t d = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(d, 1);
    uint32_t first_task = 0;
    uint64_t first_job = 0;
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_DISPATCH_TASK) {
            first_task = actions[i].data.dispatch_task.task_id;
            first_job = actions[i].data.dispatch_task.job_id;
        }
        orchestrator_action_free_payload(&actions[i]);
    }

    /* Renvoyer le résultat */
    build_result_event(&evt, first_job, first_task, "wA", true,
                        "OK", 2000);
    orchestrator_handle_event(c.o, &evt);

    /* Devrait dispatcher la 2e tâche maintenant */
    n = orchestrator_drain_outgoing(c.o, actions, 64);
    d = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(d, 1);
    uint32_t second_task = 0;
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_DISPATCH_TASK) {
            second_task = actions[i].data.dispatch_task.task_id;
        }
        orchestrator_action_free_payload(&actions[i]);
    }
    ASSERT_TRUE(second_task != first_task);

    /* Renvoyer ce résultat */
    build_result_event(&evt, first_job, second_task, "wA", true,
                        "OK", 3000);
    orchestrator_handle_event(c.o, &evt);

    /* 3e dispatch */
    n = orchestrator_drain_outgoing(c.o, actions, 64);
    d = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(d, 1);
    uint32_t third_task = 0;
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_DISPATCH_TASK) {
            third_task = actions[i].data.dispatch_task.task_id;
        }
        orchestrator_action_free_payload(&actions[i]);
    }

    /* Renvoyer le 3e résultat */
    build_result_event(&evt, first_job, third_task, "wA", true,
                        "OK", 4000);
    orchestrator_handle_event(c.o, &evt);

    /* Maintenant on doit avoir une notification de fin de job */
    n = orchestrator_drain_outgoing(c.o, actions, 64);
    size_t notifs = count_actions(actions, n, ACT_NOTIFY_JOB_DONE);
    ASSERT_EQ_INT(notifs, 1);

    /* Vérifier le contenu de la notification */
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_NOTIFY_JOB_DONE) {
            ASSERT_EQ_INT(actions[i].data.notify_job_done.job_id, first_job);
            ASSERT_EQ_STR(actions[i].data.notify_job_done.client_id, "alice");
            ASSERT_EQ_INT(actions[i].data.notify_job_done.final_state,
                          JOB_COMPLETED);
            ASSERT_EQ_INT(actions[i].data.notify_job_done.stats.completed, 3);
            ASSERT_EQ_INT(actions[i].data.notify_job_done.stats.failed, 0);
        }
        orchestrator_action_free_payload(&actions[i]);
    }

    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Pas de worker disponible : aucune tâche n'est dispatched.
 * Les actions ne contiennent que des logs, pas de DISPATCH_TASK.
 */
static void test_job_with_no_workers(void) {
    TEST_BEGIN("job_with_no_workers");
    orch_ctx_t c = make_orch();
    /* Pas d'inject_worker */

    task_t tasks[3];
    orchestrator_event_t evt;
    build_job_event(&evt, tasks, 3, "alice", 1000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);
    free_tasks_buf(tasks, 3);

    orchestrator_action_t actions[64];
    size_t n = orchestrator_drain_outgoing(c.o, actions, 64);
    size_t d = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(d, 0);  /* aucun dispatch */
    /* Les 3 tâches restent PENDING dans la pool, prêtes à être
     * dispatched dès qu'un worker arrivera. */

    for (size_t i = 0; i < n; i++) {
        orchestrator_action_free_payload(&actions[i]);
    }
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Résultat tardif d'un worker zombie : doit être ignoré sans erreur,
 * sans changement d'état.
 */
static void test_late_result_ignored(void) {
    TEST_BEGIN("late_result_ignored");
    orch_ctx_t c = make_orch();
    inject_worker(c.o, "wA", 100);

    task_t tasks[1];
    orchestrator_event_t evt;
    build_job_event(&evt, tasks, 1, "alice", 1000);
    orchestrator_handle_event(c.o, &evt);
    free_tasks_buf(tasks, 1);

    orchestrator_action_t actions[64];
    orchestrator_drain_outgoing(c.o, actions, 64);
    uint32_t task_id = actions[0].data.dispatch_task.task_id;
    uint64_t job_id  = actions[0].data.dispatch_task.job_id;
    for (size_t i = 0; i < 64; i++) {
        if (actions[i].type == ACT_DISPATCH_TASK)
            orchestrator_action_free_payload(&actions[i]);
    }

    /* Premier résultat : OK */
    build_result_event(&evt, job_id, task_id, "wA", true, "OK", 2000);
    orchestrator_handle_event(c.o, &evt);
    drain_and_free(c.o);

    /* Résultat dupliqué (zombie ou retransmission) : doit être ignoré */
    build_result_event(&evt, job_id, task_id, "wA", true, "DUP", 3000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);

    /* Pas de nouveau dispatch ni de double notification */
    size_t n = orchestrator_drain_outgoing(c.o, actions, 64);
    size_t notifs = count_actions(actions, n, ACT_NOTIFY_JOB_DONE);
    size_t dispatches = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(notifs, 0);
    ASSERT_EQ_INT(dispatches, 0);
    /* Mais probablement un log d'avertissement */

    for (size_t i = 0; i < n; i++) {
        orchestrator_action_free_payload(&actions[i]);
    }
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Résultat pour un job inconnu : ignoré sans erreur.
 */
static void test_result_for_unknown_job(void) {
    TEST_BEGIN("result_for_unknown_job");
    orch_ctx_t c = make_orch();
    inject_worker(c.o, "wA", 100);

    orchestrator_event_t evt;
    build_result_event(&evt, 999, 1, "wA", true, "OK", 1000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);  /* pas d'erreur */

    drain_and_free(c.o);
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Plusieurs jobs en parallèle : chacun progresse indépendamment.
 */
static void test_multiple_jobs(void) {
    TEST_BEGIN("multiple_jobs");
    orch_ctx_t c = make_orch();
    inject_worker(c.o, "wA", 100);
    inject_worker(c.o, "wB", 100);
    inject_worker(c.o, "wC", 100);

    task_t tasks1[2], tasks2[2];
    orchestrator_event_t evt;

    /* Soumettre 2 jobs */
    build_job_event(&evt, tasks1, 2, "alice", 1000);
    orchestrator_handle_event(c.o, &evt);
    free_tasks_buf(tasks1, 2);

    build_job_event(&evt, tasks2, 2, "bob", 1100);
    orchestrator_handle_event(c.o, &evt);
    free_tasks_buf(tasks2, 2);

    /* On a 4 tâches, 3 workers : 3 dispatches au moins */
    orchestrator_action_t actions[64];
    size_t n = orchestrator_drain_outgoing(c.o, actions, 64);
    size_t dispatches = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(dispatches, 3);

    /* Vérifier qu'on a bien 2 jobs distincts dans les dispatches */
    uint64_t found_job_a = 0, found_job_b = 0;
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_DISPATCH_TASK) {
            uint64_t jid = actions[i].data.dispatch_task.job_id;
            if (jid == 1) found_job_a = 1;
            if (jid == 2) found_job_b = 1;
        }
        orchestrator_action_free_payload(&actions[i]);
    }
    ASSERT_TRUE(found_job_a && found_job_b);

    destroy_orch(&c);
    TEST_END_PASS();
}

/* ------------------------------------------------------------------ */
/* Helpers étape B                                                     */
/* ------------------------------------------------------------------ */

static void build_worker_joined_event(orchestrator_event_t *evt,
                                        const char *id, uint32_t ram,
                                        uint16_t cpu, uint64_t now_ms) {
    memset(evt, 0, sizeof(*evt));
    evt->type = EVT_WORKER_JOINED;
    evt->timestamp_ms = now_ms;
    strncpy(evt->data.worker_joined.node_id, id, PARALLAX_UUID_LEN - 1);
    strncpy(evt->data.worker_joined.node_name, "test-node",
            PARALLAX_NODE_NAME_MAX - 1);
    evt->data.worker_joined.caps.ram_mb = ram;
    evt->data.worker_joined.caps.cpu_count = cpu;
    evt->data.worker_joined.caps.cpu_mhz = 2400;
}

static void build_worker_failed_event(orchestrator_event_t *evt,
                                        const char *id,
                                        const char *reason,
                                        uint64_t now_ms) {
    memset(evt, 0, sizeof(*evt));
    evt->type = EVT_WORKER_FAILED;
    evt->timestamp_ms = now_ms;
    strncpy(evt->data.worker_failed.node_id, id, PARALLAX_UUID_LEN - 1);
    strncpy(evt->data.worker_failed.reason, reason, 63);
}

static void build_worker_left_event(orchestrator_event_t *evt,
                                      const char *id, uint64_t now_ms) {
    memset(evt, 0, sizeof(*evt));
    evt->type = EVT_WORKER_LEFT;
    evt->timestamp_ms = now_ms;
    strncpy(evt->data.worker_left.node_id, id, PARALLAX_UUID_LEN - 1);
}

static void build_heartbeat_event(orchestrator_event_t *evt,
                                    const char *id, uint64_t now_ms) {
    memset(evt, 0, sizeof(*evt));
    evt->type = EVT_WORKER_HEARTBEAT;
    evt->timestamp_ms = now_ms;
    strncpy(evt->data.worker_heartbeat.node_id, id, PARALLAX_UUID_LEN - 1);
}

static void build_tick_event(orchestrator_event_t *evt, uint64_t now_ms) {
    memset(evt, 0, sizeof(*evt));
    evt->type = EVT_TICK;
    evt->timestamp_ms = now_ms;
}

/* ------------------------------------------------------------------ */
/* Tests étape B                                                       */
/* ------------------------------------------------------------------ */

/*
 * Test fondamental : remplacer le hack inject_worker par WORKER_JOINED.
 * Vérifie que JOINED dispatch les tâches en attente.
 */
static void test_worker_joined_triggers_pending_dispatch(void) {
    TEST_BEGIN("worker_joined_triggers_pending_dispatch");
    orch_ctx_t c = make_orch();

    /* Soumettre un job AVANT d'avoir un worker */
    task_t tasks[3];
    orchestrator_event_t evt;
    build_job_event(&evt, tasks, 3, "alice", 1000);
    orchestrator_handle_event(c.o, &evt);
    free_tasks_buf(tasks, 3);

    /* Aucun dispatch attendu */
    orchestrator_action_t actions[64];
    size_t n = orchestrator_drain_outgoing(c.o, actions, 64);
    ASSERT_EQ_INT(count_actions(actions, n, ACT_DISPATCH_TASK), 0);
    for (size_t i = 0; i < n; i++)
        orchestrator_action_free_payload(&actions[i]);

    /* Worker arrive : dispatch immédiat */
    build_worker_joined_event(&evt, "wA", 4096, 4, 2000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);

    n = orchestrator_drain_outgoing(c.o, actions, 64);
    size_t d = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(d, 1);  /* 1 worker, 1 dispatch */

    for (size_t i = 0; i < n; i++)
        orchestrator_action_free_payload(&actions[i]);
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * WORKER_FAILED en milieu d'exécution : ses tâches sont remises en queue
 * et redistribuées si d'autres workers sont disponibles.
 */
static void test_worker_failed_requeues_tasks(void) {
    TEST_BEGIN("worker_failed_requeues_tasks");
    orch_ctx_t c = make_orch();
    orchestrator_event_t evt;

    build_worker_joined_event(&evt, "wA", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    build_worker_joined_event(&evt, "wB", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    drain_and_free(c.o);

    /* 4 tâches, 2 workers : 2 dispatches initialement */
    task_t tasks[4];
    build_job_event(&evt, tasks, 4, "alice", 2000);
    orchestrator_handle_event(c.o, &evt);
    free_tasks_buf(tasks, 4);

    /* Capturer les dispatches initiaux pour savoir qui a quoi */
    orchestrator_action_t actions[64];
    size_t n = orchestrator_drain_outgoing(c.o, actions, 64);
    ASSERT_EQ_INT(count_actions(actions, n, ACT_DISPATCH_TASK), 2);
    /* Identifier la tâche envoyée à wA */
    uint32_t task_for_wA = 0;
    uint64_t job_id = 0;
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_DISPATCH_TASK) {
            if (strcmp(actions[i].data.dispatch_task.worker_id, "wA") == 0) {
                task_for_wA = actions[i].data.dispatch_task.task_id;
                job_id = actions[i].data.dispatch_task.job_id;
            }
            orchestrator_action_free_payload(&actions[i]);
        }
    }
    ASSERT_TRUE(task_for_wA != 0);

    /* wA tombe ! */
    build_worker_failed_event(&evt, "wA", "timeout", 3000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);

    /* La tâche de wA doit être réattribuée. Comme wB est déjà BUSY,
     * la tâche reste REQUEUED jusqu'à ce que wB se libère.
     * On vérifie qu'elle est bien REQUEUED dans la pool. */
    const job_table_t *jt = orchestrator_get_job_table(c.o);
    job_info_t info;
    job_table_get_info(jt, job_id, &info);
    /* total = 4, assigned = 1 (wB), requeued = 1, pending = 2 */
    ASSERT_EQ_INT(info.stats.assigned, 1);
    ASSERT_EQ_INT(info.stats.requeued, 1);
    ASSERT_EQ_INT(info.stats.pending, 2);

    /* Pas de nouveau dispatch : wB est encore BUSY, et wA n'existe plus */
    n = orchestrator_drain_outgoing(c.o, actions, 64);
    size_t d = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(d, 0);
    for (size_t i = 0; i < n; i++)
        orchestrator_action_free_payload(&actions[i]);

    /* wB termine sa tâche → reprend la REQUEUED */
    /* On lit le state du worker pour trouver sa task_id en cours */
    const worker_table_t *wt = orchestrator_get_worker_table(c.o);
    /* On va prendre un raccourci : le test est qu'après un résultat
     * de wB sur n'importe quelle tâche, le scheduler doit rebondir
     * sur wB pour la REQUEUED. */
    /* Trouvons la tâche assignée à wB */
    task_pool_t *pool = job_table_get_pool((job_table_t *)jt, job_id);
    uint32_t task_for_wB = 0;
    for (uint32_t tid = 1; tid <= 4; tid++) {
        task_t *t = task_pool_find(pool, tid);
        if (t && t->state == TASK_ASSIGNED &&
            strcmp(t->assigned_worker, "wB") == 0) {
            task_for_wB = tid;
            break;
        }
    }
    ASSERT_TRUE(task_for_wB != 0);
    (void)wt;

    build_result_event(&evt, job_id, task_for_wB, "wB", true, "OK", 4000);
    orchestrator_handle_event(c.o, &evt);

    /* Maintenant wB doit avoir un nouveau dispatch (avec une REQUEUED) */
    n = orchestrator_drain_outgoing(c.o, actions, 64);
    d = count_actions(actions, n, ACT_DISPATCH_TASK);
    ASSERT_EQ_INT(d, 1);
    for (size_t i = 0; i < n; i++)
        orchestrator_action_free_payload(&actions[i]);

    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * WORKER_LEFT (départ propre) : même comportement que FAILED côté
 * tâches, mais log différent.
 */
static void test_worker_left_requeues_tasks(void) {
    TEST_BEGIN("worker_left_requeues_tasks");
    orch_ctx_t c = make_orch();
    orchestrator_event_t evt;

    build_worker_joined_event(&evt, "wA", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    drain_and_free(c.o);

    task_t tasks[2];
    build_job_event(&evt, tasks, 2, "alice", 2000);
    orchestrator_handle_event(c.o, &evt);
    free_tasks_buf(tasks, 2);
    drain_and_free(c.o);

    /* wA part. Sa tâche en cours doit être REQUEUED. */
    build_worker_left_event(&evt, "wA", 3000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);

    /* Vérifier l'état des tâches */
    const job_table_t *jt = orchestrator_get_job_table(c.o);
    job_info_t info;
    job_table_get_info(jt, 1, &info);
    /* total=2, requeued=1, pending=1 */
    ASSERT_EQ_INT(info.stats.requeued, 1);
    ASSERT_EQ_INT(info.stats.pending, 1);
    ASSERT_EQ_INT(info.stats.assigned, 0);

    drain_and_free(c.o);
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Heartbeat met à jour le timestamp mais ne change pas l'état d'un
 * worker AVAILABLE.
 */
static void test_heartbeat_updates_timestamp(void) {
    TEST_BEGIN("heartbeat_updates_timestamp");
    orch_ctx_t c = make_orch();
    orchestrator_event_t evt;

    build_worker_joined_event(&evt, "wA", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    drain_and_free(c.o);

    /* Heartbeat à t=5000 */
    build_heartbeat_event(&evt, "wA", 5000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);

    /* Vérifier */
    const worker_table_t *wt = orchestrator_get_worker_table(c.o);
    worker_t *w = worker_table_find((worker_table_t *)wt, "wA");
    ASSERT_TRUE(w != NULL);
    ASSERT_EQ_INT(w->last_heartbeat_ms, 5000);
    ASSERT_EQ_INT(w->state, WORKER_AVAILABLE);

    drain_and_free(c.o);
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Heartbeat d'un worker inconnu : ignoré sans erreur.
 */
static void test_heartbeat_unknown_worker(void) {
    TEST_BEGIN("heartbeat_unknown_worker");
    orch_ctx_t c = make_orch();
    orchestrator_event_t evt;
    build_heartbeat_event(&evt, "ghost", 1000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);  /* pas d'erreur */
    drain_and_free(c.o);
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * TICK détecte les workers timeout et les passe en SUSPECT.
 */
static void test_tick_detects_timeout(void) {
    TEST_BEGIN("tick_detects_timeout");
    orch_ctx_t c = make_orch();
    orchestrator_event_t evt;

    build_worker_joined_event(&evt, "wA", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    drain_and_free(c.o);

    /* Timeout par défaut = 10000 ms. Tick à t=20000 sans heartbeat
     * → wA doit passer SUSPECT. */
    build_tick_event(&evt, 20000);
    int rc = orchestrator_handle_event(c.o, &evt);
    ASSERT_EQ_INT(rc, 0);

    const worker_table_t *wt = orchestrator_get_worker_table(c.o);
    worker_t *w = worker_table_find((worker_table_t *)wt, "wA");
    ASSERT_EQ_INT(w->state, WORKER_SUSPECT);

    drain_and_free(c.o);
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Heartbeat ressuscite un worker SUSPECT.
 */
static void test_heartbeat_recovers_suspect(void) {
    TEST_BEGIN("heartbeat_recovers_suspect");
    orch_ctx_t c = make_orch();
    orchestrator_event_t evt;

    build_worker_joined_event(&evt, "wA", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    drain_and_free(c.o);

    /* Forcer SUSPECT via tick */
    build_tick_event(&evt, 20000);
    orchestrator_handle_event(c.o, &evt);
    drain_and_free(c.o);

    const worker_table_t *wt = orchestrator_get_worker_table(c.o);
    worker_t *w = worker_table_find((worker_table_t *)wt, "wA");
    ASSERT_EQ_INT(w->state, WORKER_SUSPECT);

    /* Heartbeat → résurrection */
    build_heartbeat_event(&evt, "wA", 21000);
    orchestrator_handle_event(c.o, &evt);
    w = worker_table_find((worker_table_t *)wt, "wA");
    ASSERT_EQ_INT(w->state, WORKER_AVAILABLE);

    drain_and_free(c.o);
    destroy_orch(&c);
    TEST_END_PASS();
}

/*
 * Failed worker en cours d'une dernière tâche d'un job : le job
 * devient COMPLETED_PARTIAL si max_retries dépassé sur cette tâche.
 *
 * Plus simple : on vérifie qu'avec max_retries=0 et un seul worker
 * qui meurt, le job passe en FAILED.
 */
static void test_worker_failure_completes_job_partial(void) {
    TEST_BEGIN("worker_failure_completes_job_partial");
    /* Config custom : max_retries = 0 (pas de retry) */
    orchestrator_config_t cfg = orchestrator_default_config();
    cfg.max_task_retries = 0;
    orchestrator_t *o = orchestrator_create(&cfg);

    orchestrator_event_t evt;
    build_worker_joined_event(&evt, "wA", 1024, 1, 1000);
    orchestrator_handle_event(o, &evt);

    task_t tasks[1];
    build_job_event(&evt, tasks, 1, "alice", 2000);
    orchestrator_handle_event(o, &evt);
    free_tasks_buf(tasks, 1);

    /* Vider la queue : on a 1 dispatch + logs */
    orchestrator_action_t actions[64];
    size_t n;
    while ((n = orchestrator_drain_outgoing(o, actions, 64)) > 0)
        for (size_t i = 0; i < n; i++)
            orchestrator_action_free_payload(&actions[i]);

    /* wA meurt : sa tâche essaie un retry mais max=0, donc FAILED */
    build_worker_failed_event(&evt, "wA", "crash", 3000);
    orchestrator_handle_event(o, &evt);

    /* On devrait avoir une notification JOB_DONE en état FAILED */
    n = orchestrator_drain_outgoing(o, actions, 64);
    size_t notifs = count_actions(actions, n, ACT_NOTIFY_JOB_DONE);
    ASSERT_EQ_INT(notifs, 1);
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_NOTIFY_JOB_DONE) {
            ASSERT_EQ_INT(actions[i].data.notify_job_done.final_state,
                          JOB_FAILED);
        }
        orchestrator_action_free_payload(&actions[i]);
    }

    /* Cleanup */
    while ((n = orchestrator_drain_outgoing(o, actions, 64)) > 0)
        for (size_t i = 0; i < n; i++)
            orchestrator_action_free_payload(&actions[i]);
    orchestrator_destroy(o);
    TEST_END_PASS();
}

/*
 * Scénario complet de résilience : 3 workers, 6 tâches, le worker A
 * meurt en milieu d'exécution, le job se termine quand même grâce
 * aux retries.
 */
static void test_resilience_full_scenario(void) {
    TEST_BEGIN("resilience_full_scenario");
    orch_ctx_t c = make_orch();
    orchestrator_event_t evt;

    /* 3 workers identiques */
    build_worker_joined_event(&evt, "wA", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    build_worker_joined_event(&evt, "wB", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    build_worker_joined_event(&evt, "wC", 1024, 1, 1000);
    orchestrator_handle_event(c.o, &evt);
    drain_and_free(c.o);

    /* 6 tâches */
    task_t tasks[6];
    build_job_event(&evt, tasks, 6, "alice", 2000);
    orchestrator_handle_event(c.o, &evt);
    free_tasks_buf(tasks, 6);

    orchestrator_action_t actions[64];
    /* Drain les 3 dispatches initiaux */
    size_t n = orchestrator_drain_outgoing(c.o, actions, 64);
    /* Au moins 3 dispatches (3 workers) */
    ASSERT_TRUE(count_actions(actions, n, ACT_DISPATCH_TASK) == 3);
    /* Récupérer le job_id */
    uint64_t jid = 0;
    for (size_t i = 0; i < n; i++) {
        if (actions[i].type == ACT_DISPATCH_TASK)
            jid = actions[i].data.dispatch_task.job_id;
        orchestrator_action_free_payload(&actions[i]);
    }

    /* Boucle : compléter chaque tâche assignée, faire avancer le job.
     * On simule l'arrivée des résultats. À mi-parcours, wA meurt. */
    int wA_killed = 0;
    int safety = 0;
    while (safety++ < 100) {
        /* Trouver une tâche ASSIGNED à compléter */
        task_pool_t *pool = job_table_get_pool(
            (job_table_t *)orchestrator_get_job_table(c.o), jid);
        if (!pool) break;
        task_pool_stats_t s;
        task_pool_get_stats(pool, &s);
        if (s.completed + s.failed == s.total) break;  /* fini */

        /* Trouver une tâche assignée */
        uint32_t to_complete = 0;
        char w_id[PARALLAX_UUID_LEN] = {0};
        for (uint32_t tid = 1; tid <= 6; tid++) {
            task_t *t = task_pool_find(pool, tid);
            if (t && t->state == TASK_ASSIGNED) {
                to_complete = tid;
                strncpy(w_id, t->assigned_worker, PARALLAX_UUID_LEN - 1);
                break;
            }
        }
        if (to_complete == 0) break;

        /* Si on a complété 2 tâches et wA pas encore mort, le tuer */
        if (s.completed >= 2 && !wA_killed) {
            build_worker_failed_event(&evt, "wA", "test_kill", 3000);
            orchestrator_handle_event(c.o, &evt);
            wA_killed = 1;
            drain_and_free(c.o);
            continue;
        }

        /* Si wA est mort et c'est sa tâche, skip (elle est REQUEUED) */
        if (wA_killed && strcmp(w_id, "wA") == 0) continue;

        /* Compléter la tâche */
        build_result_event(&evt, jid, to_complete, w_id, true, "OK", 5000);
        orchestrator_handle_event(c.o, &evt);
        drain_and_free(c.o);
    }

    /* Vérifier : le job doit être COMPLETED (pas PARTIAL) parce que les
     * tâches de wA ont été redistribuées avec succès */
    job_info_t info;
    job_table_get_info(orchestrator_get_job_table(c.o), jid, &info);
    ASSERT_EQ_INT(info.state, JOB_COMPLETED);
    ASSERT_EQ_INT(info.stats.completed, 6);
    ASSERT_EQ_INT(info.stats.failed, 0);
    ASSERT_TRUE(wA_killed);

    destroy_orch(&c);
    TEST_END_PASS();
}

int main(void) {
    printf("Running orchestrator tests...\n\n");
    /* Étape A */
    test_create_and_destroy();
    test_default_config();
    test_invalid_event();
    test_job_submitted_distributes_tasks();
    test_full_cycle_happy_path();
    test_job_with_no_workers();
    test_late_result_ignored();
    test_result_for_unknown_job();
    test_multiple_jobs();
    /* Étape B */
    test_worker_joined_triggers_pending_dispatch();
    test_worker_failed_requeues_tasks();
    test_worker_left_requeues_tasks();
    test_heartbeat_updates_timestamp();
    test_heartbeat_unknown_worker();
    test_tick_detects_timeout();
    test_heartbeat_recovers_suspect();
    test_worker_failure_completes_job_partial();
    test_resilience_full_scenario();
    TEST_REPORT();
    return TEST_EXIT_CODE();
}
