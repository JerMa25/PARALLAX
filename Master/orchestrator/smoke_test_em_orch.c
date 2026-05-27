/*
 * smoke_test_em_orch.c
 *
 * Test d'intégration : simule un Execution Master qui :
 *   1. lance le thread orchestrator
 *   2. envoie un em_submit_msg_t (calcul 123 × 4)
 *   3. attend la réponse em_result_msg_t
 *   4. vérifie le résultat
 *   5. arrête proprement
 *
 * Sert à valider le contrat IPC sans avoir besoin du vrai EM compilé.
 *
 * Compilation : voir Makefile (cible smoke_test)
 */

#include "orchestrator.h"
#include "em_ipc.h"
#include "em_types.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/msg.h>

int main(void) {
    /* Nettoyer d'éventuelles queues résiduelles (test isolé) */
    int qs = msgget(EM_IPC_KEY_SUBMIT, 0666);
    if (qs >= 0) msgctl(qs, IPC_RMID, NULL);
    int qr = msgget(EM_IPC_KEY_RESULT, 0666);
    if (qr >= 0) msgctl(qr, IPC_RMID, NULL);

    /* Lancer le thread orchestrator */
    pthread_t tid;
    pthread_create(&tid, NULL, orchestrator_thread_run, NULL);

    /* Laisser le thread initialiser les queues */
    usleep(200000);

    /* Récupérer les handles côté EM */
    int submit_qid = msgget(EM_IPC_KEY_SUBMIT, 0666);
    int result_qid = msgget(EM_IPC_KEY_RESULT, 0666);
    if (submit_qid < 0 || result_qid < 0) {
        fprintf(stderr, "Failed to obtain IPC handles\n");
        return 1;
    }

    /* Construire un job : 123 × 4 = 492.
     * Décomposition chunk_digits=1 :
     *   chunk_value=3, multiplier=4, position=0 → 3×4×10^0 = 12
     *   chunk_value=2, multiplier=4, position=1 → 2×4×10^1 = 80
     *   chunk_value=1, multiplier=4, position=2 → 1×4×10^2 = 400
     *   Σ = 492
     */
    em_submit_msg_t sub;
    memset(&sub, 0, sizeof(sub));
    sub.mtype   = EM_MSG_JOB_SUBMIT;
    sub.job_id  = 42;
    strncpy(sub.client_id, "smoke-test", EM_CLIENT_ID_LEN - 1);
    sub.n_tasks = 3;
    sub.tasks[0].task_id = 1; sub.tasks[0].job_id = 42;
    sub.tasks[0].chunk_value = 3; sub.tasks[0].multiplier = 4;
    sub.tasks[0].position = 0;
    sub.tasks[1].task_id = 2; sub.tasks[1].job_id = 42;
    sub.tasks[1].chunk_value = 2; sub.tasks[1].multiplier = 4;
    sub.tasks[1].position = 1;
    sub.tasks[2].task_id = 3; sub.tasks[2].job_id = 42;
    sub.tasks[2].chunk_value = 1; sub.tasks[2].multiplier = 4;
    sub.tasks[2].position = 2;

    size_t pl = sizeof(em_submit_msg_t) - sizeof(long);
    if (msgsnd(submit_qid, &sub, pl, 0) != 0) {
        perror("msgsnd SUBMIT");
        return 1;
    }
    printf("[TEST] Job 42 sent to orchestrator (123 × 4 expected = 492)\n");

    /* Attendre le résultat (jusqu'à 5 secondes) */
    em_result_msg_t res;
    size_t pl_res = sizeof(em_result_msg_t) - sizeof(long);
    ssize_t r = -1;
    for (int i = 0; i < 50; i++) {
        r = msgrcv(result_qid, &res, pl_res, EM_MSG_JOB_RESULT, IPC_NOWAIT);
        if (r > 0) break;
        usleep(100000);
    }
    if (r < 0) {
        fprintf(stderr, "[TEST] No result received within 5s\n");
        orchestrator_stop();
        pthread_join(tid, NULL);
        return 1;
    }

    printf("[TEST] Result received: job_id=%lu success=%d n_results=%zu\n",
           (unsigned long)res.job_id, (int)res.success, res.n_results);

    /* Recombiner */
    long total = 0;
    long pow10[] = {1, 10, 100, 1000, 10000};
    for (size_t i = 0; i < res.n_results; i++) {
        printf("[TEST]   result[%zu]: task=%u partial=%ld position=%d success=%d\n",
               i, res.results[i].task_id, res.results[i].partial_result,
               res.results[i].position, (int)res.results[i].success);
        if (res.results[i].position >= 0 && res.results[i].position < 5)
            total += res.results[i].partial_result * pow10[res.results[i].position];
    }
    printf("[TEST] Recomposed = %ld (expected 492)  ==>  %s\n",
           total, total == 492 ? "OK" : "FAIL");

    orchestrator_stop();
    pthread_join(tid, NULL);

    /* Cleanup queues */
    msgctl(submit_qid, IPC_RMID, NULL);
    msgctl(result_qid, IPC_RMID, NULL);

    return total == 492 ? 0 : 1;
}
