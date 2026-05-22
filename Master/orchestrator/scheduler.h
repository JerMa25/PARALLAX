/*
 * scheduler.h
 *
 * Scheduler du orchestrator PARALLAX.
 *
 * Algorithme : Deficit Round Robin pondéré par capacité.
 *
 * Référence académique :
 *   Shreedhar & Varghese (1996), "Efficient Fair Queueing using
 *   Deficit Round Robin", ACM SIGCOMM CCR. Algorithme à l'origine
 *   conçu pour le scheduling de paquets QoS, étendu ici au scheduling
 *   de tâches sur ressources hétérogènes.
 *
 * Principe :
 *   Chaque worker w a un score de capacité S_w. Le quota théorique
 *   d'attributions au worker w après N assignations totales est :
 *
 *       quota(w) = N * S_w / sum(S_i)
 *
 *   La "dette" envers le worker w est :
 *
 *       dette(w) = quota(w) - received(w)
 *
 *   Le scheduler attribue chaque tâche au worker ayant la plus grande
 *   dette (positive). Cela garantit la convergence exacte des ratios
 *   d'attribution vers les ratios de scores.
 *
 * Implémentation : pour éviter le flottant, on stocke
 *
 *       weighted_debt(w) = N * S_w  -  received(w) * sum_S
 *
 *   qui est mathématiquement équivalent à dette(w) * sum_S, et reste
 *   entier. La comparaison entre workers reste valide (toutes les
 *   dettes sont multipliées par la même constante positive sum_S).
 *
 * Thread-safety : NON. Synchronisation au niveau orchestrator.
 */

#ifndef PARALLAX_SCHEDULER_H
#define PARALLAX_SCHEDULER_H

#include "parallax_types.h"
#include "worker_table.h"
#include "task_pool.h"

/* ----- Action de scheduling produite par schedule() ----- */
typedef struct {
    char     worker_id[PARALLAX_UUID_LEN];
    uint64_t job_id;
    uint32_t task_id;
} schedule_action_t;

typedef struct scheduler_s scheduler_t;

/* ----- Cycle de vie ----- */

scheduler_t *scheduler_create(size_t initial_capacity);
void         scheduler_destroy(scheduler_t *sched);

/* ----- Gestion des workers connus ----- */

/*
 * Enregistre (ou met à jour) un worker dans le scheduler avec son score.
 *
 * Le score est typiquement calculé comme :
 *     score = ram_mb + cpu_count * 1024
 * mais l'orchestrator est libre de fournir n'importe quelle métrique
 * positive.
 *
 * Si le worker existait déjà, son score est mis à jour. Son compteur
 * received_count n'est PAS réinitialisé (équité préservée).
 *
 * Renvoie 0 si OK, -1 si OOM, -2 si arguments invalides ou score=0.
 */
int scheduler_register_worker(scheduler_t *sched,
                                const char *worker_id,
                                uint64_t score);

/*
 * Retire un worker du scheduler. Ses contributions au total des
 * scores et au total des attributions sont conservées dans les
 * compteurs globaux pour préserver la cohérence des dettes des
 * autres workers.
 *
 * Renvoie 0 si OK, -1 si introuvable.
 */
int scheduler_unregister_worker(scheduler_t *sched, const char *worker_id);

/* ----- Calcul de score à partir des capacités matérielles ----- */

/*
 * Helper standard : calcule un score à partir des capacités.
 *     score = ram_mb + cpu_count * 1024
 *
 * cpu_mhz n'est pas utilisé pour l'instant (info brute peu fiable
 * en pratique : les Pentium D 3 GHz sont très inférieurs aux Core i5
 * 2.4 GHz). À ajuster après benchmarks réels.
 */
uint64_t scheduler_compute_score(const worker_capabilities_t *caps);

/* ----- Cœur : décider des attributions ----- */

/*
 * Décide jusqu'à max_actions attributions (worker, tâche).
 *
 * Source des candidats :
 *   - Workers : les workers AVAILABLE de worker_table, dont le
 *     scheduler connaît le score (via register_worker précédent).
 *     Un worker AVAILABLE non enregistré dans le scheduler est ignoré.
 *   - Tâches : les tâches PENDING ou REQUEUED de pool, dans l'ordre
 *     FIFO.
 *
 * Effets :
 *   - Pour chaque attribution décidée :
 *       * actions[i] est rempli avec (worker_id, task_id, job_id)
 *       * task_pool_mark_assigned() est appelé sur la tâche
 *       * Le worker est marqué BUSY dans worker_table
 *       * Le compteur received_count du worker est incrémenté
 *   - L'orchestrator doit ensuite envoyer ces actions au réseau
 *     (via la couche de Ngonga).
 *
 * Renvoie le nombre d'attributions effectuées (0 si rien à faire).
 *
 * IMPORTANT : un worker enregistré dans le scheduler mais ABSENT
 * de worker_table (parce qu'il a été supprimé) est ignoré pour cette
 * itération mais conservé en interne. C'est à l'orchestrator d'appeler
 * scheduler_unregister_worker() lors d'une suppression.
 */
size_t scheduler_schedule(scheduler_t *sched,
                          worker_table_t *workers,
                          task_pool_t *pool,
                          uint64_t job_id,
                          uint64_t now_ms,
                          schedule_action_t *actions,
                          size_t max_actions);

/* ----- Introspection / tests ----- */

/* Nombre de workers connus du scheduler. */
size_t scheduler_worker_count(const scheduler_t *sched);

/* Total cumulé des attributions effectuées par ce scheduler. */
uint64_t scheduler_total_assignments(const scheduler_t *sched);

/*
 * Snapshot des stats par worker. Utile pour le rapport scientifique
 * (montrer la convergence des ratios vers les scores).
 */
typedef struct {
    char     worker_id[PARALLAX_UUID_LEN];
    uint64_t score;
    uint64_t received_count;
    /* dette pondérée actuelle, peut être négative */
    int64_t  weighted_debt;
} scheduler_worker_stat_t;

size_t scheduler_snapshot_stats(const scheduler_t *sched,
                                 scheduler_worker_stat_t *out,
                                 size_t out_capacity);

#endif /* PARALLAX_SCHEDULER_H */
