/*
 * execution_master.h — Interface publique de l'Execution Master
 *
 * L'Execution Master est le point d'entrée côté MASTER pour tout
 * job de calcul distribué. Il orchestre les étapes suivantes sans
 * jamais exécuter lui-même le moindre calcul :
 *
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │                    EXECUTION MASTER                          │
 *   │                                                              │
 *   │  Job reçu                                                    │
 *   │     │                                                        │
 *   │     ▼                                                        │
 *   │  [Parser]   ─── décompose A en chunks ──→  em_task_t[]      │
 *   │     │                                                        │
 *   │     ▼                                                        │
 *   │  [IPC]      ─── EVT_JOB_SUBMITTED ─────→  Orchestrateur     │
 *   │     │                                          │             │
 *   │     │        ←─── ACT_NOTIFY_JOB_DONE ─────────             │
 *   │     ▼                                                        │
 *   │  [Combiner] ── Σ (partial × 10^pos) ───→  Résultat final    │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * Ce que l'Execution Master NE fait PAS :
 *   - Exécuter des calculs          → rôle des Workers
 *   - Gérer les retries de tâches   → rôle de l'Orchestrateur
 *   - Superviser les timeouts       → rôle du Watchdog (state-receiver)
 *   - Connaître l'état du cluster   → rôle du Controller
 */

#ifndef EXECUTION_MASTER_H
#define EXECUTION_MASTER_H

#include "em_types.h"

/* ── Configuration ────────────────────────────────────────────────────────── */

typedef struct {
    int chunk_digits;    /* chiffres décimaux par chunk (défaut: 3)    */
    int job_timeout_s;   /* timeout max en attente de résultat (60s)   */
} em_config_t;

/* Retourne une configuration par défaut raisonnable. */
em_config_t em_default_config(void);

/* ── Cycle de vie ─────────────────────────────────────────────────────────── */

/*
 * em_init() — Initialise l'Execution Master (ouvre les canaux IPC).
 * Retour : 0 si OK, -1 si erreur IPC.
 */
int execution_master_thread_run(const em_config_t *config);

/*
 * em_shutdown() — Libère les ressources et ferme les canaux IPC.
 */
void execution_master_stop(void);

/* ── API principale ───────────────────────────────────────────────────────── */

/*
 * em_run_job() — Exécute un job de bout en bout (bloquant).
 *
 * Retour :
 *    0  : succès.
 *   -1  : erreur de décomposition (parser).
 *   -2  : erreur IPC.
 *   -3  : timeout expiré.
 *   -4  : au moins une tâche en échec définitif.
 */
int em_run_job(long             operand_A,
               long             operand_B,
               const char      *client_id,
               em_job_result_t *result);

/* ── Utilitaires ──────────────────────────────────────────────────────────── */

/* Génère un job_id unique (timestamp + compteur atomique). */
uint64_t em_generate_job_id(void);

/* Affiche le résultat d'un job de façon lisible sur stdout. */
void em_print_result(const em_job_result_t *result, long expected);

#endif /* EXECUTION_MASTER_H */
