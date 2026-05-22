/*
 * em_parser.c — Implémentation du Parser de l'Execution Master
 */

#include "em_parser.h"

#include <stdio.h>
#include <string.h>

/* ── Helper interne ───────────────────────────────────────────────────────── */

static long power10(int exp)
{
    long r = 1;
    for (int i = 0; i < exp; i++)
        r *= 10;
    return r;
}

/* ── Décomposition positionnelle ─────────────────────────────────────────── */

int em_parser_decompose(const em_job_t *job,
                         em_task_t      *tasks,
                         size_t         *n_tasks)
{
    if (!job || !tasks || !n_tasks)
        return -1;
    if (job->chunk_digits <= 0)
        return -1;

    long divisor  = power10(job->chunk_digits);
    long remaining = job->operand_A;
    int  position  = 0;
    size_t count   = 0;

    /* Cas particulier : A == 0 → une seule tâche triviale. */
    if (remaining == 0) {
        tasks[0].task_id     = 0;
        tasks[0].job_id      = job->job_id;
        tasks[0].chunk_value = 0;
        tasks[0].multiplier  = job->operand_B;
        tasks[0].position    = 0;
        tasks[0].state       = EM_TASK_PENDING;
        *n_tasks = 1;
        return 0;
    }

    /*
     * Décomposition positionnelle de droite à gauche.
     *
     * A = 987654321, chunk_digits=3, divisor=1000 :
     *   iter 0 : chunk=321, remaining=987654, position=0
     *   iter 1 : chunk=654, remaining=987,    position=3
     *   iter 2 : chunk=987, remaining=0,      position=6
     */
    while (remaining > 0) {
        if (count >= EM_MAX_TASKS_PER_JOB)
            return -2;

        long chunk = remaining % divisor;
        remaining  = remaining / divisor;

        tasks[count].task_id     = (uint32_t)count;
        tasks[count].job_id      = job->job_id;
        tasks[count].chunk_value = chunk;
        tasks[count].multiplier  = job->operand_B;
        tasks[count].position    = position;
        tasks[count].state       = EM_TASK_PENDING;

        position += job->chunk_digits;
        count++;
    }

    *n_tasks = count;
    return 0;
}
