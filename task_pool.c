/*
 * task_pool.c
 *
 * Implémentation du pool de tâches.
 *
 * Représentation interne :
 *   - Array dynamique de task_t (les tâches elles-mêmes)
 *   - Pour chaque tâche, payload alloué séparément (taille variable)
 *   - Pour les COMPLETED, un task_result_t alloué séparément
 *
 * Recherche par task_id : O(N) linéaire. À optimiser si on dépasse 10K.
 */

#include "task_pool.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define DEFAULT_INITIAL_CAPACITY 32

/*
 * On stocke en parallèle au tableau des task_t un tableau de
 * pointeurs vers les résultats. Justification : task_result_t contient
 * un buffer alloué (output), on ne veut pas l'embarquer en valeur dans
 * task_t sinon le snapshot/copy se complique. Le pointeur est NULL tant
 * que la tâche n'est pas COMPLETED.
 */
struct task_pool_s {
    task_t          *tasks;       /* tableau parallèle aux résultats */
    task_result_t  **results;     /* results[i] correspond à tasks[i] */
    size_t           count;
    size_t           capacity;
};

/* ------------------------------------------------------------------ */
/* Helpers internes                                                    */
/* ------------------------------------------------------------------ */

static size_t find_index_by_id(const task_pool_t *p, uint32_t task_id) {
    for (size_t i = 0; i < p->count; i++) {
        if (p->tasks[i].task_id == task_id) return i;
    }
    return SIZE_MAX;
}

static int ensure_capacity(task_pool_t *p, size_t needed) {
    if (p->capacity >= needed) return 0;
    size_t new_cap = p->capacity ? p->capacity * 2 : DEFAULT_INITIAL_CAPACITY;
    while (new_cap < needed) new_cap *= 2;

    task_t *new_tasks = realloc(p->tasks, new_cap * sizeof(task_t));
    if (!new_tasks) return -1;
    p->tasks = new_tasks;

    task_result_t **new_results = realloc(p->results,
                                           new_cap * sizeof(task_result_t *));
    if (!new_results) return -1;
    p->results = new_results;

    /* Initialiser les nouvelles cases */
    memset(&p->tasks[p->capacity], 0,
           (new_cap - p->capacity) * sizeof(task_t));
    memset(&p->results[p->capacity], 0,
           (new_cap - p->capacity) * sizeof(task_result_t *));

    p->capacity = new_cap;
    return 0;
}

/* Libère le payload d'une tâche (mais pas la struct elle-même). */
static void free_task_payload(task_t *t) {
    if (!t) return;
    free(t->payload);
    t->payload = NULL;
    t->payload_size = 0;
}

/* Libère un task_result_t complet (struct + output). */
static void free_result(task_result_t *r) {
    if (!r) return;
    free(r->output);
    free(r);
}

/* Copie profonde d'un task_t vers une cible. La cible doit être zero'ée. */
static int deep_copy_task(task_t *dst, const task_t *src) {
    *dst = *src;
    dst->payload = NULL;
    dst->payload_size = 0;
    if (src->payload && src->payload_size > 0) {
        dst->payload = malloc(src->payload_size);
        if (!dst->payload) return -1;
        memcpy(dst->payload, src->payload, src->payload_size);
        dst->payload_size = src->payload_size;
    }
    return 0;
}

/* Copie profonde d'un task_result_t. Renvoie NULL si OOM. */
static task_result_t *deep_copy_result(const task_result_t *src) {
    task_result_t *r = calloc(1, sizeof(*r));
    if (!r) return NULL;
    *r = *src;
    r->output = NULL;
    r->output_size = 0;
    if (src->output && src->output_size > 0) {
        r->output = malloc(src->output_size);
        if (!r->output) { free(r); return NULL; }
        memcpy(r->output, src->output, src->output_size);
        r->output_size = src->output_size;
    }
    return r;
}

/* ------------------------------------------------------------------ */
/* Cycle de vie                                                        */
/* ------------------------------------------------------------------ */

task_pool_t *task_pool_create(size_t initial_capacity) {
    task_pool_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;
    if (initial_capacity == 0) initial_capacity = DEFAULT_INITIAL_CAPACITY;
    p->tasks = calloc(initial_capacity, sizeof(task_t));
    p->results = calloc(initial_capacity, sizeof(task_result_t *));
    if (!p->tasks || !p->results) {
        free(p->tasks); free(p->results); free(p);
        return NULL;
    }
    p->capacity = initial_capacity;
    return p;
}

void task_pool_destroy(task_pool_t *p) {
    if (!p) return;
    for (size_t i = 0; i < p->count; i++) {
        free_task_payload(&p->tasks[i]);
        free_result(p->results[i]);
    }
    free(p->tasks);
    free(p->results);
    free(p);
}

/* ------------------------------------------------------------------ */
/* Insertion                                                           */
/* ------------------------------------------------------------------ */

int task_pool_add(task_pool_t *p, const task_t *task) {
    if (!p || !task) return -3;
    if (find_index_by_id(p, task->task_id) != SIZE_MAX) return -2;
    if (ensure_capacity(p, p->count + 1) != 0) return -1;

    task_t *slot = &p->tasks[p->count];
    if (deep_copy_task(slot, task) != 0) {
        /* Rollback en cas de OOM sur le payload */
        memset(slot, 0, sizeof(task_t));
        return -1;
    }
    /* Force l'état initial à PENDING */
    slot->state = TASK_PENDING;
    slot->assigned_worker[0] = '\0';
    slot->assigned_ms = 0;
    slot->completed_ms = 0;
    /* retry_count et max_retries sont pris de src (déjà copiés) */

    p->results[p->count] = NULL;
    p->count++;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Peek FIFO                                                           */
/* ------------------------------------------------------------------ */

task_t *task_pool_peek_pending(task_pool_t *p) {
    if (!p) return NULL;
    /* FIFO : on parcourt dans l'ordre d'insertion. PENDING et REQUEUED
     * sont équivalents pour la sélection. */
    for (size_t i = 0; i < p->count; i++) {
        if (p->tasks[i].state == TASK_PENDING ||
            p->tasks[i].state == TASK_REQUEUED) {
            return &p->tasks[i];
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Transitions d'état                                                  */
/* ------------------------------------------------------------------ */

int task_pool_mark_assigned(task_pool_t *p, uint32_t task_id,
                             const char *worker_id, uint64_t now_ms) {
    if (!p || !worker_id) return -2;
    size_t idx = find_index_by_id(p, task_id);
    if (idx == SIZE_MAX) return -1;
    task_t *t = &p->tasks[idx];
    if (t->state != TASK_PENDING && t->state != TASK_REQUEUED) return -2;

    strncpy(t->assigned_worker, worker_id, PARALLAX_UUID_LEN - 1);
    t->assigned_worker[PARALLAX_UUID_LEN - 1] = '\0';
    t->state = TASK_ASSIGNED;
    t->assigned_ms = now_ms;
    return 0;
}

int task_pool_mark_completed(task_pool_t *p, uint32_t task_id,
                              const task_result_t *result, uint64_t now_ms) {
    if (!p || !result) return -2;
    size_t idx = find_index_by_id(p, task_id);
    if (idx == SIZE_MAX) return -1;
    task_t *t = &p->tasks[idx];

    /* Filtrer les résultats tardifs / inappropriés */
    if (t->state != TASK_ASSIGNED) return -2;
    if (strncmp(t->assigned_worker, result->worker_id,
                PARALLAX_UUID_LEN) != 0) return -3;

    /* Stocker le résultat (copie profonde) */
    task_result_t *copy = deep_copy_result(result);
    if (!copy) return -4;

    /* Si on avait déjà un résultat (ne devrait pas arriver mais on est
     * défensif), on le remplace proprement. */
    free_result(p->results[idx]);
    p->results[idx] = copy;

    t->state = TASK_COMPLETED;
    t->completed_ms = now_ms;

    /* Le payload n'est plus utile. Libération immédiate pour l'empreinte
     * mémoire du pool : sur 10K tâches avec payload de 100 Ko chacune,
     * ça représente 1 Go de RAM économisée à la fin du job. */
    free_task_payload(t);

    return 0;
}

int task_pool_mark_failed(task_pool_t *p, uint32_t task_id,
                           const char *reason, uint64_t now_ms) {
    (void)reason; /* TODO: stocker dans un champ dédié si besoin */
    if (!p) return -2;
    size_t idx = find_index_by_id(p, task_id);
    if (idx == SIZE_MAX) return -1;
    task_t *t = &p->tasks[idx];
    t->state = TASK_FAILED;
    t->completed_ms = now_ms;
    free_task_payload(t);
    return 0;
}

int task_pool_requeue(task_pool_t *p, uint32_t task_id, uint64_t now_ms) {
    if (!p) return -2;
    size_t idx = find_index_by_id(p, task_id);
    if (idx == SIZE_MAX) return -1;
    task_t *t = &p->tasks[idx];
    if (t->state != TASK_ASSIGNED && t->state != TASK_PENDING &&
        t->state != TASK_REQUEUED) return -2;

    t->retry_count++;
    if (t->retry_count > t->max_retries) {
        t->state = TASK_FAILED;
        t->completed_ms = now_ms;
        free_task_payload(t);
        return 1;  /* indique passage en FAILED */
    }
    t->state = TASK_REQUEUED;
    t->assigned_worker[0] = '\0';
    t->assigned_ms = 0;
    return 0;
}

size_t task_pool_requeue_worker_tasks(task_pool_t *p, const char *worker_id,
                                       uint64_t now_ms,
                                       uint32_t *out_requeued,
                                       size_t out_requeued_cap,
                                       uint32_t *out_failed,
                                       size_t out_failed_cap) {
    if (!p || !worker_id) return 0;
    size_t affected = 0;
    size_t requeued_n = 0, failed_n = 0;

    for (size_t i = 0; i < p->count; i++) {
        task_t *t = &p->tasks[i];
        if (t->state != TASK_ASSIGNED) continue;
        if (strncmp(t->assigned_worker, worker_id,
                    PARALLAX_UUID_LEN) != 0) continue;

        uint32_t tid = t->task_id;
        int rc = task_pool_requeue(p, tid, now_ms);
        if (rc == 0) {
            if (out_requeued && requeued_n < out_requeued_cap) {
                out_requeued[requeued_n++] = tid;
            }
            affected++;
        } else if (rc == 1) {
            if (out_failed && failed_n < out_failed_cap) {
                out_failed[failed_n++] = tid;
            }
            affected++;
        }
    }
    return affected;
}

/* ------------------------------------------------------------------ */
/* Lecture                                                             */
/* ------------------------------------------------------------------ */

task_t *task_pool_find(task_pool_t *p, uint32_t task_id) {
    if (!p) return NULL;
    size_t idx = find_index_by_id(p, task_id);
    return (idx == SIZE_MAX) ? NULL : &p->tasks[idx];
}

const task_result_t *task_pool_get_result(const task_pool_t *p,
                                            uint32_t task_id) {
    if (!p) return NULL;
    size_t idx = find_index_by_id(p, task_id);
    if (idx == SIZE_MAX) return NULL;
    if (p->tasks[idx].state != TASK_COMPLETED) return NULL;
    return p->results[idx];
}

size_t task_pool_count(const task_pool_t *p) {
    return p ? p->count : 0;
}

size_t task_pool_count_in_state(const task_pool_t *p, task_state_t state) {
    if (!p) return 0;
    size_t n = 0;
    for (size_t i = 0; i < p->count; i++) {
        if (p->tasks[i].state == state) n++;
    }
    return n;
}

void task_pool_get_stats(const task_pool_t *p, task_pool_stats_t *out) {
    if (!out) return;
    memset(out, 0, sizeof(*out));
    if (!p) return;
    for (size_t i = 0; i < p->count; i++) {
        switch (p->tasks[i].state) {
            case TASK_PENDING:   out->pending++;   break;
            case TASK_ASSIGNED:  out->assigned++;  break;
            case TASK_COMPLETED: out->completed++; break;
            case TASK_FAILED:    out->failed++;    break;
            case TASK_REQUEUED:  out->requeued++;  break;
        }
    }
    out->total = p->count;
}
