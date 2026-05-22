#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>
#include <math.h>

// ================= CONFIG =================

#define MAX_RETRIES 2
#define TASK_TIMEOUT 4
#define LASTCHANCE_TIMEOUT 6
#define POLL_INTERVAL 1

// ================= LOG =================

void log_msg(const char* who, const char* msg) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    printf("[%02d:%02d:%02d] [%s] %s\n",
           t->tm_hour, t->tm_min, t->tm_sec, who, msg);
}

// ================= STRUCTURES =================

typedef struct {
    int task_id;
    char code[64];
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

// ================= ORCHESTRATEUR =================

typedef struct {
    int failed_tasks[20];
    int failed_count;
} Orchestrator;

void notify_failure(Orchestrator* orch, int task_id, const char* node_id, const char* reason) {
    orch->failed_tasks[orch->failed_count++] = task_id;

    printf("[ORCHESTRATOR] FAIL task=%d node=%s reason=%s\n",
           task_id, node_id, reason);
}

// ================= WORKER =================

TaskResult execute_task(Task task) {
    TaskResult res;
    res.task_id = task.task_id;
    res.position = task.position;

    // simulation echec task 1
    if (task.task_id == 1 && !task.is_last_chance) {
        strcpy(res.error, "erreur applicative simulee");
        res.success = 0;
        return res;
    }

    // simulation blocage task 3
    if (task.task_id == 3) {
        sleep(60);
    }

    char buf[128];
    snprintf(buf, sizeof(buf),
             "calcul : %ld x %ld (node %s)",
             task.chunk_value, task.multiplier, task.node_id);
    log_msg("WORKER", buf);

    sleep(1 + (task.chunk_value % 3));

    res.partial_result = task.chunk_value * task.multiplier;
    res.success = 1;
    strcpy(res.error, "ok");

    snprintf(buf, sizeof(buf),
             "resultat partiel : %ld", res.partial_result);
    log_msg("WORKER", buf);

    return res;
}

// ================= PIPE EXECUTION =================

int run_task(Task task, TaskResult* result) {
    int fd[2];
    pipe(fd);

    pid_t pid = fork();

    if (pid == 0) {
        close(fd[0]);

        TaskResult res = execute_task(task);
        write(fd[1], &res, sizeof(TaskResult));

        close(fd[1]);
        exit(0);
    }

    close(fd[1]);

    time_t start = time(NULL);

    while (1) {
        if (waitpid(pid, NULL, WNOHANG) == pid) break;

        if (time(NULL) - start > TASK_TIMEOUT) {
            kill(pid, SIGKILL);
            log_msg("MASTER", "timeout -> kill task");
            close(fd[0]);
            return 0;
        }

        sleep(POLL_INTERVAL);
    }

    read(fd[0], result, sizeof(TaskResult));
    close(fd[0]);

    return result->success;
}

// ================= RETRY =================

int retry_task(Task* task, TaskResult* result) {
    task->retries++;

    char msg[128];
    snprintf(msg, sizeof(msg),
             "retry task %d (%d/%d)",
             task->task_id, task->retries, MAX_RETRIES);

    log_msg("MASTER", msg);

    return run_task(*task, result);
}

// ================= LAST CHANCE =================

int last_chance(Task* task, TaskResult* result) {
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
        if (waitpid(pid, NULL, WNOHANG) == pid) break;

        if (time(NULL) - start > LASTCHANCE_TIMEOUT) {
            kill(pid, SIGKILL);
            log_msg("LASTCHANCE", "timeout final");
            return 0;
        }

        sleep(1);
    }

    read(fd[0], result, sizeof(TaskResult));
    close(fd[0]);

    return result->success;
}

// ================= COMBINER =================

long power10(int p) {
    long r = 1;
    for (int i = 0; i < p; i++) r *= 10;
    return r;
}

long combine(TaskResult results[], int n) {
    long total = 0;

    log_msg("COMBINER", "recombinaison");

    for (int i = 0; i < n; i++) {
        if (results[i].success) {
            long contrib = results[i].partial_result *
                           power10(results[i].position);

            printf("  %ld x 10^%d = %ld\n",
                   results[i].partial_result,
                   results[i].position,
                   contrib);

            total += contrib;
        }
    }

    return total;
}

// ================= MAIN =================

int main() {
    long A = 987654321;
    long B = 56789;

    log_msg("MAIN", "debut execution distribuee");

    Orchestrator orch;
    orch.failed_count = 0;

    Task tasks[4];
    TaskResult results[4];

    long chunks[4] = {98, 76, 54, 321};
    int positions[4] = {6, 4, 2, 0};

    for (int i = 0; i < 4; i++) {
        tasks[i].task_id = i;
        tasks[i].chunk_value = chunks[i];
        tasks[i].multiplier = B;
        tasks[i].position = positions[i];
        tasks[i].retries = 0;
        tasks[i].is_last_chance = 0;

        sprintf(tasks[i].node_id, "node-%02d", i);
    }

    // ================= EXECUTION + RETRY =================

    for (int i = 0; i < 4; i++) {

        int success = run_task(tasks[i], &results[i]);

        if (!success) {

            if (tasks[i].retries < MAX_RETRIES) {
                retry_task(&tasks[i], &results[i]);
                i--;
                continue;
            }

            log_msg("MASTER", "task en echec definitif");
        }
    }

    // ================= LAST CHANCE =================

    for (int i = 0; i < 4; i++) {
        if (!results[i].success) {

            last_chance(&tasks[i], &results[i]);

            if (!results[i].success) {
                notify_failure(&orch,
                               tasks[i].task_id,
                               tasks[i].node_id,
                               "echec final");
            }
        }
    }

    // ================= CHECK =================

    int all_ok = 1;
    for (int i = 0; i < 4; i++) {
        if (!results[i].success) all_ok = 0;
    }

    if (all_ok) {
        long final = combine(results, 4);

        printf("\nRESULTAT FINAL = %ld\n", final);
        printf("VERIFICATION = %ld\n", A * B);
    } else {
        log_msg("MAIN", "echec global -> recombinaison annulée");
        printf("sous-taches reussies : ");
        for (int i = 0; i < orch.failed_count; i++) {
            printf("%d ", orch.failed_tasks[i]);
        }
        printf("\n");
    }

    return 0;
}