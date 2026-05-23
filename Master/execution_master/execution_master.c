/*
 * execution_master.c — Implémentation de l'Execution Master
 */

#include "execution_master.h"
#include "em_parser.h"
#include "em_combiner.h"
#include "em_ipc.h"

#include <stdio.h>
#include <string.h>
#include <stdatomic.h>
#include <time.h>

/* ── État interne ─────────────────────────────────────────────────────────── */

static em_config_t           g_config;
static atomic_uint_fast64_t  g_job_counter = 0;
static int                   g_initialized  = 0;

/* ── Logging interne ──────────────────────────────────────────────────────── */

static void em_log(const char *level, const char *msg)
{
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    printf("[%02d:%02d:%02d] [EXEC_MASTER] [%s] %s\n",
           t->tm_hour, t->tm_min, t->tm_sec, level, msg);
}

/* ── Configuration ────────────────────────────────────────────────────────── */

em_config_t em_default_config(void)
{
    em_config_t c;
    c.chunk_digits  = 3;
    c.job_timeout_s = 60;
    return c;
}

/* ── Utilitaires ──────────────────────────────────────────────────────────── */

uint64_t em_generate_job_id(void)
{
    /* Combine timestamp (secondes) et compteur pour garantir l'unicité
     * même en cas d'appels multiples dans la même seconde. */
    uint64_t ts  = (uint64_t)time(NULL);
    uint64_t seq = atomic_fetch_add(&g_job_counter, 1);
    return (ts << 20) | (seq & 0xFFFFF);
}

/* ── Cycle de vie ─────────────────────────────────────────────────────────── */

int execution_master_thread_run(const em_config_t *config)
{
    if (g_initialized) {
        em_log("WARN", "execution_master_thread_run() appelé deux fois, ignoré.");
        return 0;
    }

    g_config = config ? *config : em_default_config();

    if (em_ipc_init() != 0) {
        em_log("ERROR", "Échec de l'initialisation IPC.");
        return -1;
    }

    g_initialized = 1;
    em_log("INFO", "Execution Master initialisé.");
    return 0;
}

void execution_master_stop(void)
{
    if (!g_initialized)
        return;

    em_ipc_destroy();
    g_initialized = 0;
    em_log("INFO", "Execution Master arrêté.");
}

/* ── Affichage du résultat ────────────────────────────────────────────────── */

void em_print_result(const em_job_result_t *result, long expected)
{
    printf("\n╔══════════════════════════════════════╗\n");
    printf("║        RÉSULTAT JOB %-16lu║\n", (unsigned long)result->job_id);
    printf("╠══════════════════════════════════════╣\n");
    printf("║  Tâches totales   : %-16d║\n", result->n_tasks);
    printf("║  Tâches réussies  : %-16d║\n", result->n_succeeded);
    printf("║  Tâches échouées  : %-16d║\n", result->n_failed);
    printf("╠══════════════════════════════════════╣\n");

    if (result->success) {
        printf("║  Résultat calculé : %-16ld║\n", result->final_result);
        printf("║  Résultat attendu : %-16ld║\n", expected);
        if (result->final_result == expected)
            printf("║  Vérification     : OK ✓              ║\n");
        else
            printf("║  Vérification     : ERREUR ✗           ║\n");
    } else {
        printf("║  STATUT : ÉCHEC PARTIEL               ║\n");
        printf("║  Résultat partiel : %-16ld║\n", result->final_result);
    }

    printf("╚══════════════════════════════════════╝\n\n");
}

/* ── Moteur principal ─────────────────────────────────────────────────────── */

int em_run_job(long             operand_A,
               long             operand_B,
               const char      *client_id,
               em_job_result_t *result)
{
    if (!g_initialized) {
        em_log("ERROR", "em_run_job() appelé sans em_init().");
        return -1;
    }

    char buf[128];

    /* ── Étape 1 : Construction du descripteur de job ─────────────────────── */

    em_job_t job;
    memset(&job, 0, sizeof(job));
    job.job_id       = em_generate_job_id();
    job.operand_A    = operand_A;
    job.operand_B    = operand_B;
    job.chunk_digits = g_config.chunk_digits;
    job.state        = EM_JOB_CREATED;
    strncpy(job.client_id, client_id ? client_id : "anonymous",
            EM_CLIENT_ID_LEN - 1);

    snprintf(buf, sizeof(buf),
             "Nouveau job %lu : %ld × %ld (client=%s)",
             (unsigned long)job.job_id, operand_A, operand_B, job.client_id);
    em_log("INFO", buf);

    /* ── Étape 2 : Décomposition (Parser) ────────────────────────────────── */

    em_task_t tasks[EM_MAX_TASKS_PER_JOB];
    size_t    n_tasks = 0;

    em_log("INFO", "Décomposition du job en tâches (Parser)...");

    int rc = em_parser_decompose(&job, tasks, &n_tasks);
    if (rc != 0) {
        snprintf(buf, sizeof(buf),
                 "Parser échoué (code=%d) pour A=%ld", rc, operand_A);
        em_log("ERROR", buf);
        return -1;
    }

    if (!em_parser_verify(tasks, n_tasks, operand_A)) {
        em_log("ERROR", "Intégrité parser : somme des chunks ≠ A.");
        return -1;
    }

    snprintf(buf, sizeof(buf),
             "Parser OK : %zu tâches créées (chunk_digits=%d).",
             n_tasks, g_config.chunk_digits);
    em_log("INFO", buf);

    for (size_t i = 0; i < n_tasks; i++) {
        snprintf(buf, sizeof(buf),
                 "  tâche[%zu] chunk=%ld × %ld  position=10^%d",
                 i, tasks[i].chunk_value, tasks[i].multiplier, tasks[i].position);
        em_log("INFO", buf);
    }

    /* ── Étape 3 : Soumission à l'Orchestrateur (IPC) ───────────────────── */

    em_log("INFO", "Soumission du job à l'Orchestrateur...");
    job.state = EM_JOB_SUBMITTED;

    if (em_ipc_submit_job(&job, tasks, n_tasks) != 0) {
        em_log("ERROR", "Échec de la soumission IPC.");
        return -2;
    }

    /* ── Étape 4 : Attente du résultat ───────────────────────────────────── */

    snprintf(buf, sizeof(buf),
             "En attente du résultat (timeout=%ds)...", g_config.job_timeout_s);
    em_log("INFO", buf);

    em_result_msg_t result_msg;
    rc = em_ipc_wait_result(job.job_id, &result_msg, g_config.job_timeout_s);

    if (rc == -2) {
        em_log("ERROR", "Timeout : l'Orchestrateur n'a pas répondu.");
        return -3;
    }
    if (rc != 0) {
        em_log("ERROR", "Erreur IPC en attente du résultat.");
        return -2;
    }

    /* ── Étape 5 : Recombinaison (Combiner) ─────────────────────────────── */

    em_log("INFO", "Recombinaison des résultats partiels (Combiner)...");

    rc = em_combiner_reconstruct(result_msg.results,
                                  result_msg.n_results,
                                  result);
    result->job_id = job.job_id;

    if (rc == -2) {
        snprintf(buf, sizeof(buf),
                 "Recombinaison partielle : %d tâche(s) en échec.",
                 result->n_failed);
        em_log("WARN", buf);
        return -4;
    }

    snprintf(buf, sizeof(buf),
             "Job %lu terminé. Résultat = %ld",
             (unsigned long)job.job_id, result->final_result);
    em_log("INFO", buf);
    return 0;
}
