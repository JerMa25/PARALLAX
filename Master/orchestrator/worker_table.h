/*
 * worker_table.h
 *
 * Table des workers connus de l'orchestrateur.
 *
 * Choix d'implémentation : array dynamique avec recherche linéaire.
 * Justification : on vise N <= 50 nœuds (objectif 10 dans le rapport).
 * Une hashtable serait du sur-engineering ; le cache hit rate d'un
 * array contigu battra une hashtable jusqu'à plusieurs centaines
 * d'entrées.
 *
 * Thread-safety : NON. L'appelant doit fournir la synchronisation.
 * On gardera un mutex unique au niveau de l'orchestrateur (voir
 * orchestrator.c) plutôt que de mettre des verrous dans chaque module.
 */

#ifndef PARALLAX_WORKER_TABLE_H
#define PARALLAX_WORKER_TABLE_H

#include "parallax_types.h"

typedef struct worker_table_s worker_table_t;

/* ----- Cycle de vie ----- */

/* Crée une table vide avec capacité initiale. Renvoie NULL si OOM. */
worker_table_t *worker_table_create(size_t initial_capacity);

/* Libère toute la mémoire. Safe sur NULL. */
void worker_table_destroy(worker_table_t *table);

/* ----- Modification ----- */

/*
 * Ajoute un worker à la table. Si node_id existe déjà, met à jour
 * les champs (caps, name) sans toucher à state ni aux compteurs.
 * Renvoie 0 en succès, -1 si OOM, -2 si node_id invalide.
 */
int worker_table_add_or_update(worker_table_t *table,
                                const char *node_id,
                                const char *node_name,
                                const worker_capabilities_t *caps);

/* Retire un worker. Renvoie 0 si trouvé et retiré, -1 sinon. */
int worker_table_remove(worker_table_t *table, const char *node_id);

/*
 * Met à jour l'état d'un worker. Renvoie 0 si OK, -1 si introuvable.
 * N'effectue PAS de validation des transitions d'état : c'est au
 * scheduler/orchestrator d'appliquer la machine à états.
 */
int worker_table_set_state(worker_table_t *table,
                            const char *node_id,
                            worker_state_t new_state);

/* Met à jour le timestamp du dernier heartbeat. */
int worker_table_touch_heartbeat(worker_table_t *table,
                                  const char *node_id,
                                  uint64_t timestamp_ms);

/* ----- Lecture ----- */

/*
 * Récupère un pointeur vers le worker. Renvoie NULL si non trouvé.
 * ATTENTION : le pointeur est invalidé par toute opération qui peut
 * réallouer (add, remove). À utiliser uniquement de manière atomique.
 */
worker_t *worker_table_find(worker_table_t *table, const char *node_id);

/* Nombre total de workers dans la table, tous états confondus. */
size_t worker_table_count(const worker_table_t *table);

/* Nombre de workers dans un état donné. */
size_t worker_table_count_in_state(const worker_table_t *table,
                                    worker_state_t state);

/*
 * Récupère un snapshot des workers dans l'état donné.
 *
 * out_array doit être pré-alloué avec une capacité d'au moins
 * worker_table_count_in_state(table, state) éléments. Copie les
 * structures (pas de pointeurs internes), donc safe à utiliser après
 * libération du verrou.
 *
 * Renvoie le nombre de workers copiés.
 */
size_t worker_table_snapshot_by_state(const worker_table_t *table,
                                       worker_state_t state,
                                       worker_t *out_array,
                                       size_t out_capacity);

/* ----- Détection de pannes ----- */

/*
 * Marque comme SUSPECT tous les workers dont last_heartbeat_ms est
 * antérieur à (now_ms - timeout_ms) et qui ne sont pas déjà SUSPECT
 * ou FAILED. Renvoie le nombre de transitions effectuées.
 *
 * out_suspects (optionnel, peut être NULL) reçoit les node_ids des
 * workers passés en SUSPECT, jusqu'à out_capacity.
 */
size_t worker_table_detect_timeouts(worker_table_t *table,
                                     uint64_t now_ms,
                                     uint64_t timeout_ms,
                                     char (*out_suspects)[PARALLAX_UUID_LEN],
                                     size_t out_capacity);

#endif /* PARALLAX_WORKER_TABLE_H */
