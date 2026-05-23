/*
 * fault_tolerance.h
 * Module de gestion des pannes — PARALLAX
 *
 * Rôles couverts :
 *   - Nœud Maître (primaire)  : surveillance workers, migration de tâches,
 *                                élection d'un nouveau secondaire
 *   - Nœud Secondaire         : synchronisation d'état, promotion en maître
 *                                sur panne du maître primaire
 *
 * Intégration dans l'architecture PARALLAX :
 *   - Lit l'état des nœuds depuis NodeTable  (state_receiver/node.h)
 *   - Lit les métriques via NetworkMessage   (state_receiver/message.h)
 *   - Notifie l'orchestrateur via EVT_WORKER_FAILED / EVT_WORKER_HEARTBEAT
 *     (Master/orchestrator/parallax_types.h  +  RFC-001-protocol.md)
 *   - Score de meilleur nœud : score = (1 - cpu_usage) * (1 - ram_usage)
 *     aligné sur master_score_worker() de l'ancien fault_master.c
 *     et sur scheduler_compute_score() du scheduler
 *
 * Timings synchronisés avec state_receiver/node.h :
 *   T_HEARTBEAT_SEC = 2 s   → période normale des heartbeats
 *   T_SUSPECT_SEC   = 4 s   → silence → nœud SUSPECT
 *   T_FAILED_SEC    = 8 s   → silence confirmé → nœud EN_PANNE
 *
 * Conventions de nommage (imposées par le projet) :
 *   Lancement du thread : <nom>_thread_run
 *   Arrêt   du thread   : <nom>_stop
 *
 * Thread-safety : le module protège ses propres structures internes.
 *   L'accès à NodeTable reste sous la responsabilité de l'appelant
 *   (cf. node.h : "appelé sous mutex par l'appelant").
 */

#ifndef FAULT_TOLERANCE_H
#define FAULT_TOLERANCE_H

#define _POSIX_C_SOURCE 200809L

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>

/* ── Inclusions de l'architecture PARALLAX ──────────────────────────── */
#include "../../Controller/state_receiver/node.h"      /* NodeTable, NodeInfo, NodeStatus */
#include "../../Controller/state_receiver/message.h"   /* NetworkMessage, MSG_* */

/* ─────────────────────────────────────────────────────────────────────
 * CONSTANTES  (calées sur node.h et sur monitoring/Monitoring.h)
 * ───────────────────────────────────────────────────────────────────── */

/* Timings heartbeat — identiques à node.h pour cohérence */
#define FM_HEARTBEAT_SEC       2      /* période d'émission heartbeat        */
#define FM_SUSPECT_SEC         4      /* silence → SUSPECT                   */
#define FM_FAILED_SEC          8      /* silence confirmé → EN_PANNE         */

/* Seuils de surcharge — identiques à Monitoring.h */
#define FM_CPU_OVERLOAD        0.85f  /* 85 % CPU                            */
#define FM_RAM_OVERLOAD        0.85f  /* 85 % RAM                            */

/* Délai de confirmation avant promotion du secondaire (ms) */
#define FM_ELECTION_TIMEOUT_MS 5000

/* Nombre max de workers suivis par le fault_manager */
#define FM_MAX_WORKERS         64

/* Longueur d'un UUID PARALLAX (36 chars + '\0') */
#define FM_UUID_LEN            64     /* même que node.h : char uuid[64]     */

/* Taille des messages de log internes */
#define FM_LOG_SIZE            256

/* ─────────────────────────────────────────────────────────────────────
 * RÔLE DU NŒUD LOCAL
 * ───────────────────────────────────────────────────────────────────── */

typedef enum {
    FM_ROLE_MASTER    = 0,
    FM_ROLE_SECONDARY = 1,
    FM_ROLE_WORKER    = 2   /* non géré par ce module, défini pour exhaustivité */
} fm_role_t;

/* ─────────────────────────────────────────────────────────────────────
 * TYPE DE PANNE
 * ───────────────────────────────────────────────────────────────────── */

typedef enum {
    FM_FAULT_NONE             = 0,
    FM_FAULT_HEARTBEAT_LOSS   = 1,  /* silence > FM_FAILED_SEC              */
    FM_FAULT_CPU_OVERLOAD     = 2,  /* cpu_usage > FM_CPU_OVERLOAD          */
    FM_FAULT_RAM_OVERLOAD     = 3,  /* ram_usage > FM_RAM_OVERLOAD          */
    FM_FAULT_MASTER_DOWN      = 4,  /* maître primaire injoignable          */
    FM_FAULT_WORKER_CRASH     = 5   /* worker disparu en cours d'exécution  */
} fm_fault_type_t;

/* ─────────────────────────────────────────────────────────────────────
 * ÉVÉNEMENT DE PANNE  (produit par le fault_manager, consommé par
 *                       l'orchestrateur via le callback on_fault)
 * ───────────────────────────────────────────────────────────────────── */

typedef struct {
    fm_fault_type_t  type;
    char             node_uuid[FM_UUID_LEN];
    time_t           timestamp;
    char             detail[FM_LOG_SIZE];
} fm_fault_event_t;

/* ─────────────────────────────────────────────────────────────────────
 * SNAPSHOT D'UN WORKER  (copie locale, indépendante de NodeTable)
 *
 * Le fault_manager prend des snapshots atomiques de NodeInfo pour
 * éviter de maintenir un verrou long sur la NodeTable partagée.
 * ───────────────────────────────────────────────────────────────────── */

typedef struct {
    char        uuid[FM_UUID_LEN];
    char        ip[16];
    int         port;
    NodeStatus  status;
    time_t      last_heartbeat;
    float       cpu_usage;   /* de NodeMetrics.cpu_usage                   */
    float       ram_usage;   /* de NodeMetrics.ram_usage                   */
    float       score;       /* de NodeMetrics.score (pré-calculé worker)  */
} fm_worker_snapshot_t;

/* ─────────────────────────────────────────────────────────────────────
 * CONTEXTE PRINCIPAL DU FAULT MANAGER
 *
 * Une seule instance par processus (maître ou secondaire).
 * Initialisée par fm_init(), détruite par fm_destroy().
 * ───────────────────────────────────────────────────────────────────── */

typedef struct {
    /* ── Identité du nœud local ─────────────────────────────────────── */
    char       self_uuid[FM_UUID_LEN];
    char       self_ip[16];
    fm_role_t  self_role;

    /* ── Référence vers la table partagée (non possédée) ────────────── */
    NodeTable *node_table;          /* fournie à fm_init(), lue sous lock  */

    /* ── Cache interne : maître primaire connu ──────────────────────── */
    char       master_uuid[FM_UUID_LEN];
    char       master_ip[16];
    int        master_port;
    time_t     master_last_seen;    /* mis à jour à chaque heartbeat reçu  */

    /* ── Contrôle des threads ───────────────────────────────────────── */
    atomic_int running;             /* 1 = actif, 0 = arrêt demandé        */
    pthread_t  watchdog_tid;        /* thread principal de surveillance     */
    pthread_t  promotion_tid;       /* thread de promotion (secondaire)     */

    /* ── Mutex de protection du contexte ───────────────────────────── */
    pthread_mutex_t lock;

    /* ── Callbacks vers l'orchestrateur (RFC-001) ───────────────────── */
    /*
     * on_fault  : appelé dès qu'une panne est confirmée.
     *             Le fault_manager remplit fm_fault_event_t.
     *             L'orchestrateur doit envoyer EVT_WORKER_FAILED.
     */
    void (*on_fault)(const fm_fault_event_t *evt, void *user_data);

    /*
     * on_master_promoted : appelé quand CE nœud secondaire se promeut
     *                      maître. L'appelant doit reconfigurer ses
     *                      sockets et broadcaster MASTER_CHANGED.
     */
    void (*on_master_promoted)(const char *new_master_uuid,
                               const char *new_master_ip,
                               int new_master_port,
                               void *user_data);

    /*
     * best_worker_for_migration : callback injecté par l'orchestrateur.
     *   Entrée  : snapshot des workers disponibles + leur nombre.
     *   Sortie  : UUID du worker cible, ou chaîne vide si aucun.
     *   Si NULL → fm_default_best_worker() est utilisé.
     */
    void (*best_worker_for_migration)(const fm_worker_snapshot_t *candidates,
                                      int n_candidates,
                                      char *out_uuid,          /* FM_UUID_LEN */
                                      void *user_data);

    /* Donnée opaque passée à tous les callbacks */
    void *user_data;

} fm_context_t;

/* ─────────────────────────────────────────────────────────────────────
 * API PUBLIQUE
 * ───────────────────────────────────────────────────────────────────── */

/*
 * fm_init
 * Initialise le contexte du fault_manager.
 *
 * Paramètres :
 *   ctx        : contexte à initialiser (alloué par l'appelant)
 *   role       : FM_ROLE_MASTER ou FM_ROLE_SECONDARY
 *   self_uuid  : UUID du nœud local
 *   self_ip    : IP du nœud local
 *   node_table : table NodeTable partagée avec state_receiver
 *
 * Retour : 0 si OK, -1 si erreur
 */
int fm_init(fm_context_t  *ctx,
            fm_role_t      role,
            const char    *self_uuid,
            const char    *self_ip,
            NodeTable     *node_table);

/*
 * fm_destroy
 * Arrête les threads et libère les ressources internes.
 * Appeler fm_stop() avant fm_destroy() ou laisser fm_destroy()
 * l'appeler lui-même.
 */
void fm_destroy(fm_context_t *ctx);

/* ── Gestion des threads ─────────────────────────────────────────── */

/*
 * fault_manager_thread_run
 * Lance le thread de surveillance (watchdog) du fault_manager.
 * Selon le rôle :
 *   FM_ROLE_MASTER    → surveille les workers ET le secondaire
 *   FM_ROLE_SECONDARY → surveille le maître primaire
 *
 * Convention de nommage projet : <nom>_thread_run / <nom>_stop
 */
void *fault_manager_thread_run(void *ctx_arg);

/*
 * fault_manager_stop
 * Demande l'arrêt propre du thread de surveillance.
 * Bloquant : attend la fin du thread avant de retourner.
 */
void fault_manager_stop(fm_context_t *ctx);

/* ── Notification de heartbeat reçu ─────────────────────────────── */

/*
 * fm_on_heartbeat_received
 * Appelé par le state_receiver (ou la couche réseau) dès qu'un
 * heartbeat NetworkMessage est reçu.
 *
 * Met à jour last_heartbeat et les métriques dans NodeTable,
 * puis recalcule le score du nœud pour la politique de migration.
 *
 * Thread-safe : protégé par node_table->lock.
 */
void fm_on_heartbeat_received(fm_context_t     *ctx,
                               const NetworkMessage *msg);

/* ── Utilitaires exposés ─────────────────────────────────────────── */

/*
 * fm_default_best_worker
 * Politique par défaut de sélection du meilleur nœud pour migration.
 *
 * Score = (1 - cpu_usage) * (1 - ram_usage)
 *   → aligné sur master_score_worker() et scheduler_compute_score()
 *
 * Retourne l'UUID du meilleur candidat dans out_uuid.
 * Si aucun candidat valide : out_uuid[0] = '\0'.
 */
void fm_default_best_worker(const fm_worker_snapshot_t *candidates,
                             int n_candidates,
                             char *out_uuid,
                             void *user_data);

/*
 * fm_elapsed_sec
 * Nombre de secondes écoulées depuis un timestamp time_t.
 * Utilise time(NULL) (pas CLOCK_MONOTONIC car NodeTable stocke time_t).
 */
double fm_elapsed_sec(time_t since);

/* Interne — exposée pour les tests */
void fm_secondary_promote_to_master(fm_context_t *ctx);

/* Interne — exposée pour le watchdog secondaire */
void fm_master_watchdog_loop(fm_context_t *ctx);
void fm_master_trigger_election(fm_context_t *ctx);
void fm_master_handle_worker_failure(fm_context_t *ctx, const char *worker_uuid);

#endif /* FAULT_TOLERANCE_H */
