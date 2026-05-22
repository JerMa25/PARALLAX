/*
 * em_combiner.c — Implémentation du Combiner de l'Execution Master
 */

#include "em_combiner.h"

#include <string.h>

/* ── Helper interne ───────────────────────────────────────────────────────── */

static long power10(int exp)
{
    long r = 1;
    for (int i = 0; i < exp; i++)
        r *= 10;
    return r;
}

/* ── Recombinaison ────────────────────────────────────────────────────────── */

int em_combiner_reconstruct(const em_task_result_t *results,
                             size_t                  n_results,
                             em_job_result_t        *job_result)
{
    if (!results || !job_result || n_results == 0)
        return -1;

    memset(job_result, 0, sizeof(*job_result));
    job_result->job_id  = results[0].job_id;
    job_result->n_tasks = (int)n_results;

    long total       = 0;
    int  n_succeeded = 0;
    int  n_failed    = 0;

    for (size_t i = 0; i < n_results; i++) {
        if (results[i].success) {
            total       += results[i].partial_result * power10(results[i].position);
            n_succeeded++;
        } else {
            n_failed++;
        }
    }

    job_result->final_result = total;
    job_result->n_succeeded  = n_succeeded;
    job_result->n_failed     = n_failed;
    job_result->success      = (n_failed == 0);

    return (n_failed == 0) ? 0 : -2;
}

/* ── Vérification ─────────────────────────────────────────────────────────── */

bool em_combiner_verify(const em_job_result_t *job_result, long expected)
{
    if (!job_result || !job_result->success)
        return false;
    return (job_result->final_result == expected);
}
