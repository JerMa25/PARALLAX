/*
 * demo_main.c
 *
 * Simulation visuelle de l'orchestrator sans couche réseau.
 * Montre le cycle complet : workers join, jobs soumis, distribution,
 * pannes, et complétion finale.
 *
 * Compilation : make demo
 * Exécution   : ./demo
 */

#include "orchestrator.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *evt_name(event_type_t t) {
    switch (t) {
        case EVT_JOB_SUBMITTED:    return "JOB_SUBMITTED";
        case EVT_TASK_RESULT:      return "TASK_RESULT";
        case EVT_WORKER_JOINED:    return "WORKER_JOINED";
        case EVT_WORKER_LEFT:      return "WORKER_LEFT";
        case EVT_WORKER_HEARTBEAT: return "WORKER_HEARTBEAT";
        case EVT_WORKER_FAILED:    return "WORKER_FAILED";
        case EVT_TICK:             return "TICK";
        default: return "?";
    }
}

static const char *job_state_name(job_state_t s) {
    switch (s) {
        case JOB_SUBMITTED:         return "SUBMITTED";
        case JOB_READY:             return "READY";
        case JOB_RUNNING:           return "RUNNING";
        case JOB_COMPLETED:         return "COMPLETED";
        case JOB_COMPLETED_PARTIAL: return "COMPLETED_PARTIAL";
        case JOB_FAILED:            return "FAILED";
        case JOB_CANCELLED:         return "CANCELLED";
        default: return "?";
    }
}

static void drain_and_print(orchestrator_t *o) {
    orchestrator_action_t actions[64];
    size_t n = orchestrator_drain_outgoing(o, actions, 64);
    for (size_t i = 0; i < n; i++) {
        switch (actions[i].type) {
            case ACT_DISPATCH_TASK:
                printf("    -> DISPATCH task=%u to worker=%s (job=%lu)\n",
                       actions[i].data.dispatch_task.task_id,
                       actions[i].data.dispatch_task.worker_id,
                       (unsigned long)actions[i].data.dispatch_task.job_id);
                break;
            case ACT_NOTIFY_JOB_DONE:
                printf("    -> NOTIFY_JOB_DONE job=%lu state=%s "
                       "(completed=%zu failed=%zu)\n",
                       (unsigned long)actions[i].data.notify_job_done.job_id,
                       job_state_name(actions[i].data.notify_job_done.final_state),
                       actions[i].data.notify_job_done.stats.completed,
                       actions[i].data.notify_job_done.stats.failed);
                break;
            case ACT_LOG: {
                const char *lvl[] = {"INFO", "WARN", "ERROR"};
                int sv = actions[i].data.log.severity;
                if (sv < 0 || sv > 2) sv = 0;
                printf("    [%s] %s\n", lvl[sv], actions[i].data.log.message);
                break;
            }
        }
        orchestrator_action_free_payload(&actions[i]);
    }
}

static void send_event(orchestrator_t *o, const orchestrator_event_t *e) {
    printf("\n[t=%lu] %s\n", (unsigned long)e->timestamp_ms, evt_name(e->type));
    orchestrator_handle_event(o, e);
    drain_and_print(o);
}

int main(void) {
    printf("=== PARALLAX Orchestrator demo ===\n");

    orchestrator_t *o = orchestrator_create(NULL);
    orchestrator_event_t e;

    /* 3 workers heterogenes */
    memset(&e, 0, sizeof(e));
    e.type = EVT_WORKER_JOINED; e.timestamp_ms = 1000;
    strcpy(e.data.worker_joined.node_id, "core-i5");
    strcpy(e.data.worker_joined.node_name, "lab-pc-01");
    e.data.worker_joined.caps.ram_mb = 8192;
    e.data.worker_joined.caps.cpu_count = 4;
    send_event(o, &e);

    e.timestamp_ms = 1100;
    strcpy(e.data.worker_joined.node_id, "pentium-1");
    strcpy(e.data.worker_joined.node_name, "lab-pc-02");
    e.data.worker_joined.caps.ram_mb = 1024;
    e.data.worker_joined.caps.cpu_count = 2;
    send_event(o, &e);

    e.timestamp_ms = 1200;
    strcpy(e.data.worker_joined.node_id, "pentium-2");
    strcpy(e.data.worker_joined.node_name, "lab-pc-03");
    e.data.worker_joined.caps.ram_mb = 1024;
    e.data.worker_joined.caps.cpu_count = 2;
    send_event(o, &e);

    /* Soumission d'un job de 6 taches */
    task_t tasks[6];
    memset(tasks, 0, sizeof(tasks));
    for (uint32_t i = 0; i < 6; i++) {
        tasks[i].task_id = i + 1;
        tasks[i].state = TASK_PENDING;
        tasks[i].max_retries = 3;
        char buf[32];
        snprintf(buf, sizeof(buf), "task_%u_payload", i + 1);
        size_t plen = strlen(buf);
        tasks[i].payload = malloc(plen);
        memcpy(tasks[i].payload, buf, plen);
        tasks[i].payload_size = plen;
    }
    memset(&e, 0, sizeof(e));
    e.type = EVT_JOB_SUBMITTED; e.timestamp_ms = 2000;
    strcpy(e.data.job_submitted.client_id, "researcher-alice");
    e.data.job_submitted.tasks = tasks;
    e.data.job_submitted.n_tasks = 6;
    send_event(o, &e);
    for (size_t i = 0; i < 6; i++) free(tasks[i].payload);

    /* Resultats arrivent : core-i5 finit ses taches plus vite */
    uint32_t completed_ids[6] = {0};
    size_t completed_n = 0;
    /* Simule: core-i5 traite la tache assignee, puis recoit la suivante */
    const char *workers_seq[] = {
        "core-i5", "pentium-1", "pentium-2",
        "core-i5", "core-i5", "pentium-1"
    };
    uint64_t t = 3000;
    for (size_t round = 0; round < 6; round++) {
        const job_table_t *jt = orchestrator_get_job_table(o);
        task_pool_t *pool = job_table_get_pool((job_table_t *)jt, 1);
        uint32_t to_complete = 0;
        char wid[PARALLAX_UUID_LEN] = {0};
        for (uint32_t tid = 1; tid <= 6; tid++) {
            task_t *tp = task_pool_find(pool, tid);
            if (tp && tp->state == TASK_ASSIGNED) {
                int already = 0;
                for (size_t k = 0; k < completed_n; k++)
                    if (completed_ids[k] == tid) { already = 1; break; }
                if (already) continue;
                to_complete = tid;
                strncpy(wid, tp->assigned_worker, PARALLAX_UUID_LEN - 1);
                break;
            }
        }
        if (to_complete == 0) break;
        (void)workers_seq;

        memset(&e, 0, sizeof(e));
        e.type = EVT_TASK_RESULT; e.timestamp_ms = t;
        e.data.task_result.job_id = 1;
        e.data.task_result.result.task_id = to_complete;
        strncpy(e.data.task_result.result.worker_id, wid, PARALLAX_UUID_LEN - 1);
        e.data.task_result.result.success = true;
        e.data.task_result.result.exit_code = 0;
        send_event(o, &e);
        completed_ids[completed_n++] = to_complete;
        t += 500;
    }

    /* Cleanup */
    orchestrator_destroy(o);
    printf("\n=== End of demo ===\n");
    return 0;
}
