#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>

// ======================================================
// CONFIGURATION
// ======================================================

#define NUM_TASKS 4
#define MAX_RETRIES 2
#define TASK_TIMEOUT 4
#define LASTCHANCE_TIMEOUT 6
#define POLL_INTERVAL 1

// ======================================================
// LOGGING
// ======================================================

void log_msg(const char* who, const char* msg) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);

    printf("[%02d:%02d:%02d] [%s] %s\n",
           t->tm_hour,
           t->tm_min,
           t->tm_sec,
           who,
           msg);
}

// ======================================================
// STRUCTURES
// ======================================================

typedef struct {
    int task_id;

    long chunk_value;
    long multiplier;

    int position;

    char node_id[32];

    int retries;
    int is_last_chance;

} Task;

typedef struct {
    int task_id;

    int success;

    long partial_result;

    int position;

    char error[128];

} TaskResult;

typedef struct {

    int failed_tasks[32];
    int failed_count;

} Orchestrator;

typedef struct {

    pid_t pid;

    int pipe_fd;

    int active;

    time_t start_time;

    Task task;

} WorkerProcess;

// ======================================================
// ORCHESTRATOR
// ======================================================

void notify_failure(
    Orchestrator* orch,
    int task_id,
    const char* node_id,
    const char* reason
) {
    orch->failed_tasks[orch->failed_count++] = task_id;

    char msg[256];

    snprintf(msg,
             sizeof(msg),
             "ECHEC task=%d node=%s reason=%s",
             task_id,
             node_id,
             reason);

    log_msg("ORCHESTRATOR", msg);
}

// ======================================================
// WORKER EXECUTION
// ======================================================

TaskResult execute_task(Task task) {

    TaskResult res;

    res.task_id = task.task_id;
    res.position = task.position;

    // ==========================================
    // Simulation erreur applicative
    // ==========================================

    if (task.task_id == 1 && !task.is_last_chance) {

        strcpy(res.error, "erreur applicative simulee");

        res.success = 0;

        return res;
    }

    // ==========================================
    // Simulation timeout permanent
    // ==========================================

    if (task.task_id == 3) {

        log_msg("WORKER", "blocage permanent simule");

        sleep(60);
    }

    // ==========================================
    // Calcul normal
    // ==========================================

    char msg[256];

    snprintf(msg,
             sizeof(msg),
             "calcul task=%d : %ld x %ld (%s)",
             task.task_id,
             task.chunk_value,
             task.multiplier,
             task.node_id);

    log_msg("WORKER", msg);

    sleep(1 + (task.chunk_value % 3));

    long partial = task.chunk_value * task.multiplier;

    snprintf(msg,
             sizeof(msg),
             "resultat task=%d = %ld",
             task.task_id,
             partial);

    log_msg("WORKER", msg);

    res.success = 1;
    res.partial_result = partial;

    strcpy(res.error, "ok");

    return res;
}

// ======================================================
// ORCHESTRATOR DISPATCH
// ======================================================

void dispatch_task(
    Orchestrator* orch,
    WorkerProcess* worker,
    Task task
) {

    char msg[256];

    snprintf(msg,
             sizeof(msg),
             "dispatch task %d vers %s",
             task.task_id,
             task.node_id);

    log_msg("ORCHESTRATOR", msg);

    int fd[2];

    pipe(fd);

    pid_t pid = fork();

    // ==========================================
    // CHILD = WORKER
    // ==========================================

    if (pid == 0) {

        close(fd[0]);

        TaskResult result = execute_task(task);

        write(fd[1], &result, sizeof(TaskResult));

        close(fd[1]);

        exit(0);
    }

    // ==========================================
    // PARENT = MASTER
    // ==========================================

    close(fd[1]);

    worker->pid = pid;
    worker->pipe_fd = fd[0];
    worker->active = 1;
    worker->start_time = time(NULL);
    worker->task = task;
}

// ======================================================
// RETRY
// ======================================================

void retry_task(
    Orchestrator* orch,
    WorkerProcess* worker
) {

    worker->task.retries++;

    char msg[256];

    snprintf(msg,
             sizeof(msg),
             "retry task %d (%d/%d)",
             worker->task.task_id,
             worker->task.retries,
             MAX_RETRIES);

    log_msg("MASTER", msg);

    dispatch_task(
        orch,
        worker,
        worker->task
    );
}

// ======================================================
// LAST CHANCE
// ======================================================

int last_chance(
    Task* task,
    TaskResult* result
) {

    log_msg("LASTCHANCE", "relance last chance");

    task->is_last_chance = 1;

    int fd[2];

    pipe(fd);

    pid_t pid = fork();

    if (pid == 0) {

        close(fd[0]);

        TaskResult res = execute_task(*task);

        write(fd[1], &res, sizeof(TaskResult));

        close(fd[1]);

        exit(0);
    }

    close(fd[1]);

    time_t start = time(NULL);

    while (1) {

        if (waitpid(pid, NULL, WNOHANG) == pid)
            break;

        if (time(NULL) - start > LASTCHANCE_TIMEOUT) {

            kill(pid, SIGKILL);

            log_msg("LASTCHANCE", "timeout final");

            close(fd[0]);

            return 0;
        }

        sleep(1);
    }

    read(fd[0], result, sizeof(TaskResult));

    close(fd[0]);

    return result->success;
}

// ======================================================
// COMBINER
// ======================================================

long power10(int p) {

    long r = 1;

    for (int i = 0; i < p; i++)
        r *= 10;

    return r;
}

long combine(
    TaskResult results[],
    int n
) {

    log_msg("COMBINER", "recombinaison");

    long total = 0;

    for (int i = 0; i < n; i++) {

        if (results[i].success) {

            long contribution =
                results[i].partial_result *
                power10(results[i].position);

            printf("  %ld x 10^%d = %ld\n",
                   results[i].partial_result,
                   results[i].position,
                   contribution);

            total += contribution;
        }
    }

    return total;
}

// ======================================================
// MAIN
// ======================================================

int main() {

    long A = 987654321;
    long B = 56789;

    log_msg("MAIN", "debut execution distribuee");

    // ==================================================
    // EXECUTION MASTER
    // ==================================================

    Orchestrator orch;

    orch.failed_count = 0;

    // ==================================================
    // PARSER (simulation)
    // ==================================================

    long chunks[NUM_TASKS] = {98, 76, 54, 321};

    int positions[NUM_TASKS] = {6, 4, 2, 0};

    Task tasks[NUM_TASKS];

    TaskResult results[NUM_TASKS];

    WorkerProcess workers[NUM_TASKS];

    // ==================================================
    // GENERATION DES WORKLOADS
    // ==================================================

    for (int i = 0; i < NUM_TASKS; i++) {

        tasks[i].task_id = i;

        tasks[i].chunk_value = chunks[i];

        tasks[i].multiplier = B;

        tasks[i].position = positions[i];

        tasks[i].retries = 0;

        tasks[i].is_last_chance = 0;

        sprintf(tasks[i].node_id,
                "node-%02d",
                i);

        results[i].success = 0;

        workers[i].active = 0;
    }

    // ==================================================
    // EXECUTION MASTER -> ORCHESTRATOR
    // ==================================================

    log_msg("MASTER",
            "envoi des workloads a l'orchestrateur");

    // ==================================================
    // ORCHESTRATOR -> WORKERS
    // ==================================================

    for (int i = 0; i < NUM_TASKS; i++) {

        dispatch_task(
            &orch,
            &workers[i],
            tasks[i]
        );
    }

    // ==================================================
    // SUPERVISION COLLECTIVE
    // ==================================================

    int remaining = NUM_TASKS;

    while (remaining > 0) {

        for (int i = 0; i < NUM_TASKS; i++) {

            if (!workers[i].active)
                continue;

            pid_t status =
                waitpid(
                    workers[i].pid,
                    NULL,
                    WNOHANG
                );

            // ==========================================
            // RESULTAT DISPONIBLE
            // ==========================================

            if (status == workers[i].pid) {

                read(workers[i].pipe_fd,
                     &results[i],
                     sizeof(TaskResult));

                close(workers[i].pipe_fd);

                workers[i].active = 0;

                remaining--;

                // ======================================
                // SUCCES
                // ======================================

                if (results[i].success) {

                    char msg[128];

                    snprintf(msg,
                             sizeof(msg),
                             "task %d collectee",
                             i);

                    log_msg("MASTER", msg);
                }

                // ======================================
                // ECHEC
                // ======================================

                else {

                    if (workers[i].task.retries
                        < MAX_RETRIES) {

                        retry_task(
                            &orch,
                            &workers[i]
                        );

                        workers[i].active = 1;

                        remaining++;
                    }

                    else {

                        log_msg("MASTER",
                                "echec definitif");
                    }
                }
            }

            // ==========================================
            // TIMEOUT
            // ==========================================

            else {

                if (time(NULL)
                    - workers[i].start_time
                    > TASK_TIMEOUT) {

                    kill(workers[i].pid,
                         SIGKILL);

                    log_msg("MASTER",
                            "timeout -> kill");

                    close(workers[i].pipe_fd);

                    workers[i].active = 0;

                    remaining--;

                    if (workers[i].task.retries
                        < MAX_RETRIES) {

                        retry_task(
                            &orch,
                            &workers[i]
                        );

                        workers[i].active = 1;

                        remaining++;
                    }

                    else {

                        strcpy(results[i].error,
                               "timeout final");

                        results[i].success = 0;

                        results[i].task_id = i;
                    }
                }
            }
        }

        sleep(POLL_INTERVAL);
    }

    // ==================================================
    // LAST CHANCE
    // ==================================================

    for (int i = 0; i < NUM_TASKS; i++) {

        if (!results[i].success) {

            int success =
                last_chance(
                    &tasks[i],
                    &results[i]
                );

            if (!success) {

                notify_failure(
                    &orch,
                    tasks[i].task_id,
                    tasks[i].node_id,
                    "timeout final"
                );
            }
        }
    }

    // ==================================================
    // VERIFICATION
    // ==================================================

    int all_ok = 1;

    for (int i = 0; i < NUM_TASKS; i++) {

        if (!results[i].success)
            all_ok = 0;
    }

    // ==================================================
    // SUCCESS
    // ==================================================

    if (all_ok) {

        long final =
            combine(results,
                    NUM_TASKS);

        printf("\nRESULTAT FINAL = %ld\n",
               final);

        printf("VERIFICATION    = %ld\n",
               A * B);
    }

    // ==================================================
    // FAILURE
    // ==================================================

    else {

        log_msg("MAIN",
                "recombinaison annulee");

        printf("ECHECS : ");

        for (int i = 0;
             i < orch.failed_count;
             i++) {

            printf("%d ",
                   orch.failed_tasks[i]);
        }

        printf("\n");
    }

    return 0;
}
