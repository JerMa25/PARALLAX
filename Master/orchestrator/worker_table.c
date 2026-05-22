/*
 * worker_table.c
 *
 * Implémentation de la table de workers.
 * Stratégie : array dynamique, recherche linéaire.
 */

#include "worker_table.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

struct worker_table_s {
    worker_t *workers;
    size_t    count;
    size_t    capacity;
};

/* ------------------------------------------------------------------ */
/* Helpers internes                                                    */
/* ------------------------------------------------------------------ */

/* Recherche linéaire. Renvoie l'index ou SIZE_MAX si non trouvé. */
static size_t find_index(const worker_table_t *t, const char *node_id) {
    if (!t || !node_id) return SIZE_MAX;
    for (size_t i = 0; i < t->count; i++) {
        if (strncmp(t->workers[i].node_id, node_id, PARALLAX_UUID_LEN) == 0) {
            return i;
        }
    }
    return SIZE_MAX;
}

/* Garantit que la capacité est >= needed. Renvoie 0 ou -1 si OOM. */
static int ensure_capacity(worker_table_t *t, size_t needed) {
    if (t->capacity >= needed) return 0;
    size_t new_cap = t->capacity ? t->capacity * 2 : 8;
    while (new_cap < needed) new_cap *= 2;
    worker_t *new_buf = realloc(t->workers, new_cap * sizeof(worker_t));
    if (!new_buf) return -1;
    /* Zero les nouvelles cases pour éviter des données indéterminées */
    memset(&new_buf[t->capacity], 0,
           (new_cap - t->capacity) * sizeof(worker_t));
    t->workers = new_buf;
    t->capacity = new_cap;
    return 0;
}

/* ------------------------------------------------------------------ */
/* API publique                                                        */
/* ------------------------------------------------------------------ */

worker_table_t *worker_table_create(size_t initial_capacity) {
    worker_table_t *t = calloc(1, sizeof(*t));
    if (!t) return NULL;
    if (initial_capacity > 0) {
        t->workers = calloc(initial_capacity, sizeof(worker_t));
        if (!t->workers) { free(t); return NULL; }
        t->capacity = initial_capacity;
    }
    return t;
}

void worker_table_destroy(worker_table_t *t) {
    if (!t) return;
    free(t->workers);
    free(t);
}

int worker_table_add_or_update(worker_table_t *t,
                                const char *node_id,
                                const char *node_name,
                                const worker_capabilities_t *caps) {
    if (!t || !node_id || node_id[0] == '\0') return -2;

    size_t idx = find_index(t, node_id);
    if (idx != SIZE_MAX) {
        /* Mise à jour : on ne touche QUE aux infos statiques */
        if (node_name) {
            strncpy(t->workers[idx].node_name, node_name,
                    PARALLAX_NODE_NAME_MAX - 1);
            t->workers[idx].node_name[PARALLAX_NODE_NAME_MAX - 1] = '\0';
        }
        if (caps) {
            t->workers[idx].caps = *caps;
        }
        return 0;
    }

    /* Nouveau worker */
    if (ensure_capacity(t, t->count + 1) != 0) return -1;
    worker_t *w = &t->workers[t->count];
    memset(w, 0, sizeof(*w));
    strncpy(w->node_id, node_id, PARALLAX_UUID_LEN - 1);
    w->node_id[PARALLAX_UUID_LEN - 1] = '\0';
    if (node_name) {
        strncpy(w->node_name, node_name, PARALLAX_NODE_NAME_MAX - 1);
        w->node_name[PARALLAX_NODE_NAME_MAX - 1] = '\0';
    }
    if (caps) w->caps = *caps;
    w->state = WORKER_AVAILABLE;  /* nouveau venu = disponible par défaut */
    t->count++;
    return 0;
}

int worker_table_remove(worker_table_t *t, const char *node_id) {
    if (!t || !node_id) return -1;
    size_t idx = find_index(t, node_id);
    if (idx == SIZE_MAX) return -1;
    /* Remplace par le dernier (ordre non préservé, on s'en fiche) */
    if (idx != t->count - 1) {
        t->workers[idx] = t->workers[t->count - 1];
    }
    memset(&t->workers[t->count - 1], 0, sizeof(worker_t));
    t->count--;
    return 0;
}

int worker_table_set_state(worker_table_t *t,
                            const char *node_id,
                            worker_state_t new_state) {
    if (!t || !node_id) return -1;
    size_t idx = find_index(t, node_id);
    if (idx == SIZE_MAX) return -1;
    t->workers[idx].state = new_state;
    return 0;
}

int worker_table_touch_heartbeat(worker_table_t *t,
                                  const char *node_id,
                                  uint64_t timestamp_ms) {
    if (!t || !node_id) return -1;
    size_t idx = find_index(t, node_id);
    if (idx == SIZE_MAX) return -1;
    t->workers[idx].last_heartbeat_ms = timestamp_ms;
    /* Si le worker était SUSPECT et qu'il revient, le repasser AVAILABLE
     * est tentant mais ce n'est PAS la responsabilité de cette fonction.
     * C'est l'orchestrateur qui décide après vérification.
     * Voir détection de fausse panne dans orchestrator.c (à venir). */
    return 0;
}

worker_t *worker_table_find(worker_table_t *t, const char *node_id) {
    if (!t || !node_id) return NULL;
    size_t idx = find_index(t, node_id);
    return (idx == SIZE_MAX) ? NULL : &t->workers[idx];
}

size_t worker_table_count(const worker_table_t *t) {
    return t ? t->count : 0;
}

size_t worker_table_count_in_state(const worker_table_t *t,
                                    worker_state_t state) {
    if (!t) return 0;
    size_t n = 0;
    for (size_t i = 0; i < t->count; i++) {
        if (t->workers[i].state == state) n++;
    }
    return n;
}

size_t worker_table_snapshot_by_state(const worker_table_t *t,
                                       worker_state_t state,
                                       worker_t *out_array,
                                       size_t out_capacity) {
    if (!t || !out_array || out_capacity == 0) return 0;
    size_t n = 0;
    for (size_t i = 0; i < t->count && n < out_capacity; i++) {
        if (t->workers[i].state == state) {
            out_array[n++] = t->workers[i];  /* copie */
        }
    }
    return n;
}

size_t worker_table_detect_timeouts(worker_table_t *t,
                                     uint64_t now_ms,
                                     uint64_t timeout_ms,
                                     char (*out_suspects)[PARALLAX_UUID_LEN],
                                     size_t out_capacity) {
    if (!t) return 0;
    size_t flagged = 0;
    for (size_t i = 0; i < t->count; i++) {
        worker_t *w = &t->workers[i];
        if (w->state == WORKER_SUSPECT || w->state == WORKER_FAILED) continue;
        /* Si jamais reçu de heartbeat, ne pas timeout
         * (le worker vient peut-être d'arriver) */
        if (w->last_heartbeat_ms == 0) continue;
        if (now_ms < w->last_heartbeat_ms) continue; /* horloge bizarre */
        if (now_ms - w->last_heartbeat_ms > timeout_ms) {
            w->state = WORKER_SUSPECT;
            if (out_suspects && flagged < out_capacity) {
                memcpy(out_suspects[flagged], w->node_id, PARALLAX_UUID_LEN);
            }
            flagged++;
        }
    }
    return flagged;
}
