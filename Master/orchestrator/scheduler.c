/*
 * scheduler.c
 *
 * Implémentation du scheduler Deficit Round Robin pondéré.
 *
 * Représentation de la dette en arithmétique entière :
 *
 *   dette_réelle(w) = total_assignments * S_w / sum_S - received(w)
 *
 *   On multiplie par sum_S pour rester en entier :
 *   weighted_debt(w) = total_assignments * S_w - received(w) * sum_S
 *
 *   Comme on compare les dettes entre workers à un instant donné
 *   (donc avec le même total_assignments et le même sum_S), la
 *   comparaison sur weighted_debt est strictement équivalente à la
 *   comparaison sur dette_réelle. Aucune perte de précision.
 *
 *   Risque d'overflow : weighted_debt utilise int64_t.
 *   - max(received) ~ 2^32 (UINT32_MAX assignations par worker)
 *   - max(sum_S) borné par sum des scores. Si on prend des scores en
 *     uint64_t avec max raisonnable de ~10^6 (RAM en Mo + 1024*CPU),
 *     pour 50 workers max -> sum_S ~ 5*10^7
 *   - donc received * sum_S au pire ~ 2*10^17, sous 2^63 = ~9.2*10^18.
 *   On a une marge confortable de 4 ordres de grandeur.
 */

#include "scheduler.h"

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#define DEFAULT_INITIAL_CAPACITY 8

typedef struct {
    char     worker_id[PARALLAX_UUID_LEN];
    uint64_t score;
    uint64_t received_count;
} sched_worker_t;

struct scheduler_s {
    sched_worker_t *workers;
    size_t          count;
    size_t          capacity;
    uint64_t        sum_scores;        /* somme des scores des workers actifs */
    uint64_t        total_assignments; /* compteur global cumulé */
};

/* ------------------------------------------------------------------ */
/* Helpers internes                                                    */
/* ------------------------------------------------------------------ */

static size_t find_idx(const scheduler_t *s, const char *worker_id) {
    if (!s || !worker_id) return SIZE_MAX;
    for (size_t i = 0; i < s->count; i++) {
        if (strncmp(s->workers[i].worker_id, worker_id,
                    PARALLAX_UUID_LEN) == 0) return i;
    }
    return SIZE_MAX;
}

static int ensure_cap(scheduler_t *s, size_t needed) {
    if (s->capacity >= needed) return 0;
    size_t new_cap = s->capacity ? s->capacity * 2 : DEFAULT_INITIAL_CAPACITY;
    while (new_cap < needed) new_cap *= 2;
    sched_worker_t *nb = realloc(s->workers, new_cap * sizeof(sched_worker_t));
    if (!nb) return -1;
    memset(&nb[s->capacity], 0,
           (new_cap - s->capacity) * sizeof(sched_worker_t));
    s->workers = nb;
    s->capacity = new_cap;
    return 0;
}

/*
 * Calcule la dette pondérée d'un worker à l'instant courant.
 *
 *   weighted_debt = total_assignments * score - received_count * sum_scores
 *
 * Plus c'est grand (positif), plus on doit donner à ce worker.
 */
static int64_t compute_weighted_debt(const scheduler_t *s,
                                      const sched_worker_t *w) {
    /* Cast prudent : on passe en int64_t avant les multiplications.
     * Les opérandes sont uint64_t mais bornées, voir analyse en
     * en-tête du fichier. */
    int64_t a = (int64_t)s->total_assignments * (int64_t)w->score;
    int64_t b = (int64_t)w->received_count * (int64_t)s->sum_scores;
    return a - b;
}

/* ------------------------------------------------------------------ */
/* Cycle de vie                                                        */
/* ------------------------------------------------------------------ */

scheduler_t *scheduler_create(size_t initial_capacity) {
    scheduler_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    if (initial_capacity == 0) initial_capacity = DEFAULT_INITIAL_CAPACITY;
    s->workers = calloc(initial_capacity, sizeof(sched_worker_t));
    if (!s->workers) { free(s); return NULL; }
    s->capacity = initial_capacity;
    return s;
}

void scheduler_destroy(scheduler_t *s) {
    if (!s) return;
    free(s->workers);
    free(s);
}

/* ------------------------------------------------------------------ */
/* Gestion des workers connus                                          */
/* ------------------------------------------------------------------ */

int scheduler_register_worker(scheduler_t *s, const char *worker_id,
                                uint64_t score) {
    if (!s || !worker_id || worker_id[0] == '\0' || score == 0) return -2;
    size_t idx = find_idx(s, worker_id);
    if (idx != SIZE_MAX) {
        /* Mise à jour du score : ajuster sum_scores */
        s->sum_scores = s->sum_scores - s->workers[idx].score + score;
        s->workers[idx].score = score;
        return 0;
    }
    if (ensure_cap(s, s->count + 1) != 0) return -1;
    sched_worker_t *w = &s->workers[s->count];
    memset(w, 0, sizeof(*w));
    strncpy(w->worker_id, worker_id, PARALLAX_UUID_LEN - 1);
    w->worker_id[PARALLAX_UUID_LEN - 1] = '\0';
    w->score = score;
    /*
     * Subtilité d'équité à la jointure : un nouveau worker arrive
     * alors que d'autres ont déjà reçu beaucoup de tâches. Si on
     * laissait received_count à 0, le nouveau worker hériterait d'une
     * dette énorme et monopoliserait toutes les attributions suivantes
     * jusqu'à rattraper le quota théorique.
     *
     * Solution : on initialise received_count à son quota théorique
     * actuel, c'est-à-dire :
     *     received = total_assignments * score / new_sum_S
     * où new_sum_S = sum_scores + score. On part avec une dette de 0
     * (équilibre). Le nouveau worker prendra sa part future, sans
     * monopoliser.
     */
    uint64_t new_sum_S = s->sum_scores + score;
    if (new_sum_S > 0 && s->total_assignments > 0) {
        w->received_count =
            (s->total_assignments * score) / new_sum_S;
    } else {
        w->received_count = 0;
    }
    s->sum_scores = new_sum_S;
    s->count++;
    return 0;
}

int scheduler_unregister_worker(scheduler_t *s, const char *worker_id) {
    if (!s || !worker_id) return -1;
    size_t idx = find_idx(s, worker_id);
    if (idx == SIZE_MAX) return -1;
    s->sum_scores -= s->workers[idx].score;
    /*
     * On NE soustrait PAS s->workers[idx].received_count à
     * total_assignments : l'historique global est préservé.
     * Cela maintient les dettes des autres workers cohérentes
     * (ils ont reçu réellement N tâches sur les total_assignments
     * cumulées, peu importe que certains workers soient partis).
     */
    if (idx != s->count - 1) {
        s->workers[idx] = s->workers[s->count - 1];
    }
    memset(&s->workers[s->count - 1], 0, sizeof(sched_worker_t));
    s->count--;
    return 0;
}

uint64_t scheduler_compute_score(const worker_capabilities_t *caps) {
    if (!caps) return 0;
    /* Formule documentée dans scheduler.h. À calibrer après benchmarks. */
    return (uint64_t)caps->ram_mb + (uint64_t)caps->cpu_count * 1024;
}

/* ------------------------------------------------------------------ */
/* Cœur : scheduling                                                   */
/* ------------------------------------------------------------------ */

/*
 * Sélectionne le worker parmi `candidates_idx` (indices dans s->workers)
 * ayant la plus grande dette pondérée. En cas d'égalité, prend le plus
 * petit index (déterministe).
 *
 * Renvoie SIZE_MAX si aucun candidat.
 */
static size_t pick_best_worker(const scheduler_t *s,
                                const size_t *candidates_idx,
                                size_t n_candidates) {
    if (n_candidates == 0) return SIZE_MAX;
    size_t best = candidates_idx[0];
    int64_t best_debt = compute_weighted_debt(s, &s->workers[best]);
    for (size_t i = 1; i < n_candidates; i++) {
        size_t idx = candidates_idx[i];
        int64_t d = compute_weighted_debt(s, &s->workers[idx]);
        if (d > best_debt) {
            best = idx;
            best_debt = d;
        }
    }
    return best;
}

size_t scheduler_schedule(scheduler_t *s, worker_table_t *workers,
                           task_pool_t *pool, uint64_t job_id,
                           uint64_t now_ms,
                           schedule_action_t *actions, size_t max_actions) {
    if (!s || !workers || !pool || !actions || max_actions == 0) return 0;

    /*
     * Construire la liste des indices de workers candidats : ceux qui
     * sont AVAILABLE dans worker_table ET enregistrés dans le scheduler.
     *
     * Approche : snapshot des AVAILABLE, puis on filtre par présence
     * dans s->workers. Le scheduler doit être tolérant aux desync
     * entre les deux structures (ex : worker juste arrivé, pas encore
     * enregistré).
     */
    size_t n_available = worker_table_count_in_state(workers, WORKER_AVAILABLE);
    if (n_available == 0) return 0;

    worker_t *avail_snap = calloc(n_available, sizeof(worker_t));
    if (!avail_snap) return 0;
    size_t got = worker_table_snapshot_by_state(workers, WORKER_AVAILABLE,
                                                  avail_snap, n_available);

    /* Indices dans s->workers correspondant aux disponibles connus */
    size_t *candidates = calloc(got, sizeof(size_t));
    if (!candidates) { free(avail_snap); return 0; }
    size_t n_candidates = 0;
    for (size_t i = 0; i < got; i++) {
        size_t sidx = find_idx(s, avail_snap[i].node_id);
        if (sidx != SIZE_MAX) {
            candidates[n_candidates++] = sidx;
        }
    }

    size_t produced = 0;
    while (produced < max_actions && n_candidates > 0) {
        /* Y a-t-il une tâche à attribuer ? */
        task_t *t = task_pool_peek_pending(pool);
        if (!t) break;

        /* Choisir le meilleur candidat */
        size_t best = pick_best_worker(s, candidates, n_candidates);
        if (best == SIZE_MAX) break;
        sched_worker_t *bw = &s->workers[best];

        /* Effectuer l'attribution */
        if (task_pool_mark_assigned(pool, t->task_id, bw->worker_id,
                                     now_ms) != 0) {
            /* Échec inattendu : on sort en silence */
            break;
        }
        if (worker_table_set_state(workers, bw->worker_id,
                                    WORKER_BUSY) != 0) {
            /* Idem, désynchronisation : on annule l'assignation pool
             * et on sort. C'est conservateur. */
            break;
        }

        /* Enregistrer dans actions[] */
        memset(&actions[produced], 0, sizeof(actions[produced]));
        strncpy(actions[produced].worker_id, bw->worker_id,
                PARALLAX_UUID_LEN - 1);
        actions[produced].job_id = job_id;
        actions[produced].task_id = t->task_id;
        produced++;

        /* Mise à jour des compteurs scheduler */
        bw->received_count++;
        s->total_assignments++;

        /* Retirer ce worker des candidats (k=1 par tour de scheduling) */
        for (size_t i = 0; i < n_candidates; i++) {
            if (candidates[i] == best) {
                candidates[i] = candidates[n_candidates - 1];
                n_candidates--;
                break;
            }
        }
    }

    free(avail_snap);
    free(candidates);
    return produced;
}

/* ------------------------------------------------------------------ */
/* Introspection                                                       */
/* ------------------------------------------------------------------ */

size_t scheduler_worker_count(const scheduler_t *s) {
    return s ? s->count : 0;
}

uint64_t scheduler_total_assignments(const scheduler_t *s) {
    return s ? s->total_assignments : 0;
}

size_t scheduler_snapshot_stats(const scheduler_t *s,
                                 scheduler_worker_stat_t *out,
                                 size_t out_capacity) {
    if (!s || !out || out_capacity == 0) return 0;
    size_t n = (s->count < out_capacity) ? s->count : out_capacity;
    for (size_t i = 0; i < n; i++) {
        memset(&out[i], 0, sizeof(out[i]));
        strncpy(out[i].worker_id, s->workers[i].worker_id,
                PARALLAX_UUID_LEN - 1);
        out[i].score = s->workers[i].score;
        out[i].received_count = s->workers[i].received_count;
        out[i].weighted_debt = compute_weighted_debt(s, &s->workers[i]);
    }
    return n;
}
