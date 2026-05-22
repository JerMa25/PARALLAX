/*
 * job_table.c
 *
 * Implémentation du registre de jobs.
 *
 * Stratégie : array dynamique d'entrées job_entry_t.
 * Recherche linéaire par job_id.
 *
 * Justification : on s'attend à << 100 jobs concurrents dans le système
 * (un chercheur soumet rarement plus de 5-10 jobs à la fois). La
 * complexité linéaire est négligeable.
 */

#include "job_table.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_INITIAL_CAPACITY 8

typedef struct {
    uint64_t      job_id;
    char          client_id[PARALLAX_CLIENT_ID_LEN];
    job_state_t   state;
    uint64_t      submitted_ms;
    uint64_t      started_ms;
    uint64_t      completed_ms;
    task_pool_t  *pool;          /* possédée par cette entrée */
} job_entry_t;

struct job_table_s {
    job_entry_t *entries;
    size_t       count;
    size_t       capacity;
    uint64_t     next_job_id;    /* compteur monotone, jamais réutilisé */
};

/* ------------------------------------------------------------------ */
/* Helpers internes                                                    */
/* ------------------------------------------------------------------ */

static size_t find_index(const job_table_t *t, uint64_t job_id) {
    if (!t || job_id == 0) return SIZE_MAX;
    for (size_t i = 0; i < t->count; i++) {
        if (t->entries[i].job_id == job_id) return i;
    }
    return SIZE_MAX;
}

static int ensure_capacity(job_table_t *t, size_t needed) {
    if (t->capacity >= needed) return 0;
    size_t new_cap = t->capacity ? t->capacity * 2 : DEFAULT_INITIAL_CAPACITY;
    while (new_cap < needed) new_cap *= 2;
    job_entry_t *new_buf = realloc(t->entries, new_cap * sizeof(job_entry_t));
    if (!new_buf) return -1;
    memset(&new_buf[t->capacity], 0,
           (new_cap - t->capacity) * sizeof(job_entry_t));
    t->entries = new_buf;
    t->capacity = new_cap;
    return 0;
}

static void fill_info(const job_entry_t *e, job_info_t *out) {
    memset(out, 0, sizeof(*out));
    out->job_id = e->job_id;
    strncpy(out->client_id, e->client_id, PARALLAX_CLIENT_ID_LEN - 1);
    out->state = e->state;
    out->submitted_ms = e->submitted_ms;
    out->started_ms = e->started_ms;
    out->completed_ms = e->completed_ms;
    task_pool_get_stats(e->pool, &out->stats);
}

/* États terminaux (le job ne peut plus changer d'état) */
static int is_terminal(job_state_t s) {
    return s == JOB_COMPLETED || s == JOB_COMPLETED_PARTIAL ||
           s == JOB_FAILED || s == JOB_CANCELLED;
}

/* ------------------------------------------------------------------ */
/* Cycle de vie                                                        */
/* ------------------------------------------------------------------ */

job_table_t *job_table_create(size_t initial_capacity) {
    job_table_t *t = calloc(1, sizeof(*t));
    if (!t) return NULL;
    if (initial_capacity == 0) initial_capacity = DEFAULT_INITIAL_CAPACITY;
    t->entries = calloc(initial_capacity, sizeof(job_entry_t));
    if (!t->entries) { free(t); return NULL; }
    t->capacity = initial_capacity;
    t->next_job_id = 1;
    return t;
}

void job_table_destroy(job_table_t *t) {
    if (!t) return;
    for (size_t i = 0; i < t->count; i++) {
        task_pool_destroy(t->entries[i].pool);
    }
    free(t->entries);
    free(t);
}

/* ------------------------------------------------------------------ */
/* Ajout                                                               */
/* ------------------------------------------------------------------ */

uint64_t job_table_add_job(job_table_t *t, const char *client_id,
                            uint64_t now_ms) {
    if (!t) return 0;
    if (ensure_capacity(t, t->count + 1) != 0) return 0;

    task_pool_t *pool = task_pool_create(0);
    if (!pool) return 0;

    job_entry_t *e = &t->entries[t->count];
    memset(e, 0, sizeof(*e));
    e->job_id = t->next_job_id++;
    if (client_id) {
        strncpy(e->client_id, client_id, PARALLAX_CLIENT_ID_LEN - 1);
        e->client_id[PARALLAX_CLIENT_ID_LEN - 1] = '\0';
    }
    e->state = JOB_SUBMITTED;
    e->submitted_ms = now_ms;
    e->pool = pool;

    t->count++;
    return e->job_id;
}

task_pool_t *job_table_get_pool(job_table_t *t, uint64_t job_id) {
    size_t idx = find_index(t, job_id);
    return (idx == SIZE_MAX) ? NULL : t->entries[idx].pool;
}

/* ------------------------------------------------------------------ */
/* Transitions                                                         */
/* ------------------------------------------------------------------ */

int job_table_mark_ready(job_table_t *t, uint64_t job_id) {
    size_t idx = find_index(t, job_id);
    if (idx == SIZE_MAX) return -1;
    job_entry_t *e = &t->entries[idx];
    if (e->state != JOB_SUBMITTED) return -2;
    e->state = JOB_READY;
    return 0;
}

int job_table_cancel(job_table_t *t, uint64_t job_id, uint64_t now_ms) {
    size_t idx = find_index(t, job_id);
    if (idx == SIZE_MAX) return -1;
    job_entry_t *e = &t->entries[idx];
    if (is_terminal(e->state)) return -2;
    e->state = JOB_CANCELLED;
    e->completed_ms = now_ms;
    return 0;
}

job_state_t job_table_reevaluate(job_table_t *t, uint64_t job_id,
                                  uint64_t now_ms) {
    size_t idx = find_index(t, job_id);
    if (idx == SIZE_MAX) return JOB_FAILED; /* convention : id invalide */
    job_entry_t *e = &t->entries[idx];

    /* CANCELLED est définitif, on ne le change pas */
    if (e->state == JOB_CANCELLED) return e->state;

    task_pool_stats_t s;
    task_pool_get_stats(e->pool, &s);

    /* Pas de tâches : reste dans l'état actuel
     * (le job vient peut-être d'être créé) */
    if (s.total == 0) return e->state;

    /* Cas terminaux d'abord */
    int all_terminal = (s.completed + s.failed) == s.total;
    if (all_terminal) {
        job_state_t old = e->state;
        if (s.failed == 0) {
            e->state = JOB_COMPLETED;
        } else if (s.completed == 0) {
            e->state = JOB_FAILED;
        } else {
            e->state = JOB_COMPLETED_PARTIAL;
        }
        if (!is_terminal(old)) {
            e->completed_ms = now_ms;
        }
        return e->state;
    }

    /* Au moins une tâche est ASSIGNED ou en cours : on est RUNNING */
    if (s.assigned > 0) {
        if (e->state == JOB_READY || e->state == JOB_SUBMITTED) {
            e->state = JOB_RUNNING;
            if (e->started_ms == 0) e->started_ms = now_ms;
        }
    }
    /* Sinon pas de changement (encore PENDING/READY) */
    return e->state;
}

/* ------------------------------------------------------------------ */
/* Lecture                                                             */
/* ------------------------------------------------------------------ */

int job_table_get_info(const job_table_t *t, uint64_t job_id,
                        job_info_t *out) {
    if (!out) return -1;
    size_t idx = find_index(t, job_id);
    if (idx == SIZE_MAX) return -1;
    fill_info(&t->entries[idx], out);
    return 0;
}

size_t job_table_snapshot_all(const job_table_t *t, job_info_t *out_array,
                               size_t out_capacity) {
    if (!t || !out_array || out_capacity == 0) return 0;
    size_t n = (t->count < out_capacity) ? t->count : out_capacity;
    for (size_t i = 0; i < n; i++) {
        fill_info(&t->entries[i], &out_array[i]);
    }
    return n;
}

size_t job_table_snapshot_by_state(const job_table_t *t, job_state_t state,
                                    job_info_t *out_array,
                                    size_t out_capacity) {
    if (!t || !out_array || out_capacity == 0) return 0;
    size_t n = 0;
    for (size_t i = 0; i < t->count && n < out_capacity; i++) {
        if (t->entries[i].state == state) {
            fill_info(&t->entries[i], &out_array[n++]);
        }
    }
    return n;
}

size_t job_table_count(const job_table_t *t) {
    return t ? t->count : 0;
}

size_t job_table_count_in_state(const job_table_t *t, job_state_t state) {
    if (!t) return 0;
    size_t n = 0;
    for (size_t i = 0; i < t->count; i++) {
        if (t->entries[i].state == state) n++;
    }
    return n;
}

/* ------------------------------------------------------------------ */
/* Suppression                                                         */
/* ------------------------------------------------------------------ */

int job_table_remove_job(job_table_t *t, uint64_t job_id) {
    size_t idx = find_index(t, job_id);
    if (idx == SIZE_MAX) return -1;
    task_pool_destroy(t->entries[idx].pool);
    /* Swap with last */
    if (idx != t->count - 1) {
        t->entries[idx] = t->entries[t->count - 1];
    }
    memset(&t->entries[t->count - 1], 0, sizeof(job_entry_t));
    t->count--;
    return 0;
}
