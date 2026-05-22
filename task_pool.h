/*
 * task_pool.h
 *
 * Pool de tâches pour l'orchestrateur PARALLAX.
 *
 * Modèle : bag-of-tasks.
 *   - Toutes les tâches d'un job sont stockées dans le pool dès création.
 *   - On dépile les PENDING dans l'ordre FIFO pour les assigner.
 *   - On peut rechercher une tâche par task_id (pour traiter les résultats).
 *   - Les tâches COMPLETED sont conservées (avec leur résultat) jusqu'à
 *     destruction du pool, pour permettre l'agrégation du job.
 *
 * Thread-safety : NON. Synchronisation au niveau orchestrator.
 *
 * Justification du choix d'implémentation :
 *   - Array dynamique d'indices avec recherche linéaire.
 *   - N <= 10000 tâches/job dans notre périmètre. Linéaire = OK.
 *   - Si on observe un goulot, on ajoutera un hash task_id -> index.
 */

#ifndef PARALLAX_TASK_POOL_H
#define PARALLAX_TASK_POOL_H

#include "parallax_types.h"

typedef struct task_pool_s task_pool_t;

/* ----- Cycle de vie ----- */

/* Crée un pool vide. initial_capacity = 0 => valeur par défaut. */
task_pool_t *task_pool_create(size_t initial_capacity);

/* Libère TOUT (tâches, payloads, résultats). Safe sur NULL. */
void task_pool_destroy(task_pool_t *pool);

/* ----- Insertion ----- */

/*
 * Ajoute une tâche au pool. La tâche est copiée (struct + payload).
 * Le payload du caller peut être libéré après l'appel.
 *
 * task->task_id doit être unique dans le pool.
 * task->state est forcé à TASK_PENDING (peu importe la valeur passée).
 *
 * Renvoie 0 en succès, -1 si OOM, -2 si task_id déjà présent,
 * -3 si arguments invalides.
 */
int task_pool_add(task_pool_t *pool, const task_t *task);

/* ----- Pop d'une tâche pending ----- */

/*
 * Récupère un POINTEUR vers la prochaine tâche PENDING (FIFO).
 * Ne change PAS son état. C'est l'appelant qui doit appeler
 * task_pool_mark_assigned() après avoir effectivement envoyé la tâche.
 *
 * Renvoie NULL si aucune tâche PENDING.
 *
 * ATTENTION : le pointeur est invalidé par toute opération qui peut
 * réallouer le buffer interne (add). Usage atomique uniquement.
 */
task_t *task_pool_peek_pending(task_pool_t *pool);

/* ----- Transitions d'état ----- */

/*
 * Marque une tâche comme ASSIGNED à un worker.
 * Vérifie que la tâche existe et est dans l'état PENDING ou REQUEUED.
 * Renvoie 0 si OK, -1 si introuvable, -2 si état incompatible.
 */
int task_pool_mark_assigned(task_pool_t *pool,
                             uint32_t task_id,
                             const char *worker_id,
                             uint64_t now_ms);

/*
 * Marque une tâche comme COMPLETED et y attache le résultat.
 * Le résultat est COPIÉ (struct + output buffer).
 *
 * Vérifie :
 *   - tâche existe
 *   - tâche est dans l'état ASSIGNED
 *   - result->worker_id correspond à task->assigned_worker
 *
 * Renvoie 0 si OK, -1 introuvable, -2 état incompatible,
 *         -3 worker mismatch (résultat tardif d'un autre worker),
 *         -4 OOM lors de la copie du résultat.
 *
 * Si l'orchestrateur reçoit un résultat tardif (voir scénario t=25
 * dans la doc d'architecture), cette fonction retourne -2 ou -3
 * et l'appelant DOIT logger et ignorer.
 */
int task_pool_mark_completed(task_pool_t *pool,
                              uint32_t task_id,
                              const task_result_t *result,
                              uint64_t now_ms);

/*
 * Marque une tâche comme FAILED définitivement.
 * Utilisé quand max_retries est atteint.
 */
int task_pool_mark_failed(task_pool_t *pool,
                           uint32_t task_id,
                           const char *reason,
                           uint64_t now_ms);

/*
 * Remet une tâche en PENDING (REQUEUED) suite à panne du worker.
 * Incrémente retry_count. Si retry_count > max_retries, marque
 * automatiquement comme FAILED.
 *
 * Renvoie 0 si remise en queue, 1 si convertie en FAILED (limite
 * atteinte), -1 introuvable, -2 état incompatible.
 */
int task_pool_requeue(task_pool_t *pool,
                       uint32_t task_id,
                       uint64_t now_ms);

/*
 * Remet en queue toutes les tâches actuellement assignées au worker
 * indiqué (utilisé quand on déclare un worker FAILED).
 *
 * out_requeued (optionnel) reçoit la liste des task_ids remis en queue.
 * out_failed (optionnel) reçoit la liste des task_ids passés en FAILED
 * définitif (limite de retries dépassée).
 *
 * Renvoie le nombre total de tâches affectées (requeued + failed).
 */
size_t task_pool_requeue_worker_tasks(task_pool_t *pool,
                                       const char *worker_id,
                                       uint64_t now_ms,
                                       uint32_t *out_requeued,
                                       size_t out_requeued_cap,
                                       uint32_t *out_failed,
                                       size_t out_failed_cap);

/* ----- Lecture ----- */

/*
 * Récupère un pointeur vers une tâche par son ID. NULL si introuvable.
 * Pointeur invalidé par add().
 */
task_t *task_pool_find(task_pool_t *pool, uint32_t task_id);

/*
 * Récupère le résultat d'une tâche COMPLETED.
 * NULL si tâche introuvable ou pas COMPLETED.
 */
const task_result_t *task_pool_get_result(const task_pool_t *pool,
                                            uint32_t task_id);

/* Comptage par état */
size_t task_pool_count(const task_pool_t *pool);
size_t task_pool_count_in_state(const task_pool_t *pool, task_state_t state);

/*
 * Résumé : combien de tâches dans chaque état.
 * Pratique pour reporter au job_table sans plusieurs appels.
 */
typedef struct {
    size_t pending;
    size_t assigned;
    size_t completed;
    size_t failed;
    size_t requeued;
    size_t total;
} task_pool_stats_t;

void task_pool_get_stats(const task_pool_t *pool, task_pool_stats_t *out);

#endif /* PARALLAX_TASK_POOL_H */
