/*
 * job_table.h
 *
 * Registre des jobs soumis à l'orchestrateur.
 *
 * Un "job" est l'unité de soumission par un chercheur : un programme
 * annoté qui sera décomposé en N tâches indépendantes (bag-of-tasks).
 *
 * Architecture :
 *   - Une task_pool dédiée par job (isolation complète).
 *   - Le job_table possède les pools : il les crée à add_job() et les
 *     détruit à remove_job() ou destroy().
 *   - L'orchestrator accède aux pools via job_table_get_pool() pour
 *     éviter les wrappers inutiles dans les boucles chaudes.
 *
 * Cycle de vie d'un job :
 *
 *   SUBMITTED ──parser──▶ READY ──first_assign──▶ RUNNING
 *                                                   │
 *                       ┌─────── COMPLETED ◀────────┤  (toutes OK)
 *                       │                            │
 *                       ├─── COMPLETED_PARTIAL ◀────┤  (certaines FAILED)
 *                       │                            │
 *                       ├──────── FAILED ◀──────────┤  (toutes FAILED)
 *                       │
 *                       └──────── CANCELLED         (annulé par client)
 *
 * Thread-safety : NON. L'orchestrateur synchronise globalement.
 */

#ifndef PARALLAX_JOB_TABLE_H
#define PARALLAX_JOB_TABLE_H

#include "parallax_types.h"
#include "task_pool.h"

#define PARALLAX_CLIENT_ID_LEN 64

/* ----- États possibles d'un job ----- */
typedef enum {
    JOB_SUBMITTED         = 0,  /* reçu, pas encore parsé en tâches */
    JOB_READY             = 1,  /* tâches générées, en attente de scheduling */
    JOB_RUNNING           = 2,  /* au moins une tâche assignée */
    JOB_COMPLETED         = 3,  /* toutes les tâches en COMPLETED */
    JOB_COMPLETED_PARTIAL = 4,  /* certaines OK, d'autres FAILED définitif */
    JOB_FAILED            = 5,  /* toutes les tâches en FAILED */
    JOB_CANCELLED         = 6   /* annulé par le client */
} job_state_t;

typedef struct job_table_s job_table_t;

/* ----- Cycle de vie ----- */

/* Crée une job_table vide. */
job_table_t *job_table_create(size_t initial_capacity);

/* Détruit la table et TOUS les jobs (et donc toutes leurs task_pools). */
void job_table_destroy(job_table_t *table);

/* ----- Ajout d'un job ----- */

/*
 * Crée un nouveau job. Renvoie son job_id (>= 1) ou 0 en cas d'erreur.
 *
 * Le job est créé en état JOB_SUBMITTED avec une task_pool vide.
 * À l'appelant (Execution Master via Parser) de remplir la pool ensuite
 * via job_table_get_pool().
 *
 * Quand toutes les tâches ont été ajoutées, l'appelant doit appeler
 * job_table_mark_ready() pour passer le job en JOB_READY (= prêt à
 * scheduler).
 */
uint64_t job_table_add_job(job_table_t *table,
                            const char *client_id,
                            uint64_t now_ms);

/*
 * Récupère un POINTEUR vers la task_pool d'un job. NULL si introuvable.
 *
 * Le pointeur est valide tant que le job n'est pas supprimé (remove_job
 * ou destroy). L'orchestrator doit redemander un pointeur après chaque
 * suppression.
 */
task_pool_t *job_table_get_pool(job_table_t *table, uint64_t job_id);

/* ----- Transitions d'état ----- */

/*
 * Marque un job comme JOB_READY. À appeler après que le Parser a fini
 * de remplir la pool de tâches. Renvoie 0 si OK, -1 si introuvable,
 * -2 si l'état actuel n'autorise pas la transition.
 */
int job_table_mark_ready(job_table_t *table, uint64_t job_id);

/*
 * Marque un job comme CANCELLED. Toutes ses tâches PENDING et REQUEUED
 * doivent être nettoyées par l'appelant (ou ignorées par le scheduler).
 *
 * Renvoie 0 si annulation effective, -1 si introuvable, -2 si déjà
 * dans un état terminal (déjà COMPLETED/FAILED/CANCELLED).
 */
int job_table_cancel(job_table_t *table, uint64_t job_id, uint64_t now_ms);

/*
 * Réévalue l'état d'un job en fonction des stats de sa pool.
 *
 * Logique :
 *   - Si toutes les tâches sont COMPLETED : JOB_COMPLETED
 *   - Si toutes les tâches sont FAILED : JOB_FAILED
 *   - Si pending=0 ET assigned=0 ET (completed > 0) ET (failed > 0) :
 *     JOB_COMPLETED_PARTIAL
 *   - Sinon, si au moins une tâche est ASSIGNED : JOB_RUNNING
 *   - Sinon : pas de changement d'état
 *
 * Ne change PAS l'état si le job est déjà CANCELLED.
 *
 * Renvoie le nouvel état (peut être identique à l'ancien).
 * Si transition vers un état terminal, completed_ms est mis à jour.
 *
 * Cette fonction est appelée par l'orchestrator après chaque transition
 * de tâche.
 */
job_state_t job_table_reevaluate(job_table_t *table,
                                  uint64_t job_id,
                                  uint64_t now_ms);

/* ----- Lecture ----- */

/* Vue en lecture d'un job (snapshot). */
typedef struct {
    uint64_t    job_id;
    char        client_id[PARALLAX_CLIENT_ID_LEN];
    job_state_t state;
    uint64_t    submitted_ms;
    uint64_t    started_ms;     /* 0 si pas encore RUNNING */
    uint64_t    completed_ms;   /* 0 si pas encore terminé */
    task_pool_stats_t stats;
} job_info_t;

/*
 * Récupère un snapshot du job. Renvoie 0 si OK, -1 si introuvable.
 */
int job_table_get_info(const job_table_t *table,
                        uint64_t job_id,
                        job_info_t *out);

/*
 * Récupère un snapshot de tous les jobs. out_array doit avoir une
 * capacité d'au moins job_table_count(). Renvoie le nombre de jobs
 * copiés.
 */
size_t job_table_snapshot_all(const job_table_t *table,
                               job_info_t *out_array,
                               size_t out_capacity);

/* Filtrage par état */
size_t job_table_snapshot_by_state(const job_table_t *table,
                                    job_state_t state,
                                    job_info_t *out_array,
                                    size_t out_capacity);

size_t job_table_count(const job_table_t *table);
size_t job_table_count_in_state(const job_table_t *table, job_state_t state);

/* ----- Suppression ----- */

/*
 * Supprime un job de la table. Détruit sa task_pool.
 * Recommandé seulement pour les jobs en état terminal
 * (COMPLETED, COMPLETED_PARTIAL, FAILED, CANCELLED).
 *
 * Renvoie 0 si OK, -1 si introuvable.
 *
 * NOTE : si le job était RUNNING ou READY et qu'on le supprime, les
 * éventuels résultats tardifs des workers seront orphelins. C'est
 * voulu (et logué côté orchestrator). Pour un kill propre, faire
 * cancel() puis attendre, puis remove_job().
 */
int job_table_remove_job(job_table_t *table, uint64_t job_id);

#endif /* PARALLAX_JOB_TABLE_H */
