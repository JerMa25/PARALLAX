/*
 * orchestrator.h
 *
 * Module principal de l'orchestrateur PARALLAX.
 *
 * Modèle : event-driven, single-threaded, passif.
 *
 * L'orchestrator :
 *   - Reçoit des événements via orchestrator_handle_event()
 *   - Met à jour son état interne (worker_table, job_table, scheduler)
 *   - Pousse des actions sortantes dans une queue interne
 *   - L'appelant draine les actions via orchestrator_drain_outgoing()
 *
 * Le module ne fait JAMAIS d'I/O direct : il est totalement testable
 * en isolation. La couche réseau (Ngonga) sera un module séparé qui
 * convertira des paquets UDP/TCP en orchestrator_event_t et qui
 * convertira les orchestrator_action_t en messages réseau.
 *
 * Thread-safety : NON. Conçu pour un seul thread d'event loop.
 */

#ifndef PARALLAX_ORCHESTRATOR_H
#define PARALLAX_ORCHESTRATOR_H

#include "parallax_types.h"
#include "worker_table.h"
#include "task_pool.h"
#include "job_table.h"
#include "scheduler.h"

/* ====================================================================
 * Événements entrants
 * ==================================================================== */

typedef enum {
    EVT_JOB_SUBMITTED      = 1,
    EVT_TASK_RESULT        = 2,
    EVT_WORKER_JOINED      = 3,
    EVT_WORKER_LEFT        = 4,
    EVT_WORKER_HEARTBEAT   = 5,
    EVT_WORKER_FAILED      = 6,
    EVT_TICK               = 7
} event_type_t;

/* ----- Données spécifiques par type d'événement ----- */

/* EVT_JOB_SUBMITTED : un nouveau job arrive de l'Execution Master.
 *
 * IMPORTANT : les tâches sont fournies dans `tasks[]`. Le caller reste
 * propriétaire des structures task_t (ainsi que des payloads). Mais
 * l'orchestrator fait une COPIE PROFONDE en interne via task_pool_add.
 * Le caller peut donc libérer après l'appel à handle_event.
 */
typedef struct {
    char     client_id[PARALLAX_CLIENT_ID_LEN];
    /* Référence aux tâches (lecture seule, copies internes faites) */
    const task_t *tasks;
    size_t        n_tasks;
} evt_job_submitted_t;

/* EVT_TASK_RESULT : un worker rapporte le résultat d'une tâche. */
typedef struct {
    uint64_t      job_id;
    task_result_t result;  /* output buffer copié en interne */
} evt_task_result_t;

/* EVT_WORKER_JOINED : nouveau worker dans le cluster (push du
 * Controller).
 */
typedef struct {
    char     node_id[PARALLAX_UUID_LEN];
    char     node_name[PARALLAX_NODE_NAME_MAX];
    worker_capabilities_t caps;
} evt_worker_joined_t;

/* EVT_WORKER_LEFT : départ propre du worker (graceful shutdown).
 * Différent de WORKER_FAILED qui est une mort suspecte.
 */
typedef struct {
    char node_id[PARALLAX_UUID_LEN];
} evt_worker_left_t;

/* EVT_WORKER_HEARTBEAT : mise à jour de vivacité. */
typedef struct {
    char node_id[PARALLAX_UUID_LEN];
} evt_worker_heartbeat_t;

/* EVT_WORKER_FAILED : déclaration de mort par le Controller.
 * Toutes ses tâches en cours seront remises en queue.
 */
typedef struct {
    char node_id[PARALLAX_UUID_LEN];
    char reason[64];   /* "timeout", "crash", "overload", ... */
} evt_worker_failed_t;

/* EVT_TICK : ne contient aucune donnée additionnelle. */

/* ----- Union taguée ----- */

typedef struct {
    event_type_t type;
    uint64_t     timestamp_ms;
    union {
        evt_job_submitted_t    job_submitted;
        evt_task_result_t      task_result;
        evt_worker_joined_t    worker_joined;
        evt_worker_left_t      worker_left;
        evt_worker_heartbeat_t worker_heartbeat;
        evt_worker_failed_t    worker_failed;
        /* tick : pas de data */
    } data;
} orchestrator_event_t;

/* ====================================================================
 * Actions sortantes
 * ==================================================================== */

typedef enum {
    ACT_DISPATCH_TASK     = 1,  /* envoyer une tâche à un worker */
    ACT_NOTIFY_JOB_DONE   = 2,  /* informer Execution Master qu'un job
                                 * est terminé (succès, partiel, ou échec) */
    ACT_LOG               = 3   /* log d'information / erreur */
} action_type_t;

/* ACT_DISPATCH_TASK : la couche réseau doit envoyer cette tâche.
 *
 * NOTE sur la durée de vie : le payload pointé est copié en interne
 * dans la queue d'actions. Il sera libéré quand l'action est drain.
 */
typedef struct {
    char     worker_id[PARALLAX_UUID_LEN];
    uint64_t job_id;
    uint32_t task_id;
    uint8_t *payload;
    size_t   payload_size;
} act_dispatch_task_t;

/* ACT_NOTIFY_JOB_DONE : le job est dans un état terminal.
 * L'Execution Master doit notifier le client.
 */
typedef struct {
    uint64_t job_id;
    char     client_id[PARALLAX_CLIENT_ID_LEN];
    job_state_t final_state;
    task_pool_stats_t stats;
} act_notify_job_done_t;

/* ACT_LOG : message à journaliser (pour audit / debug). */
typedef struct {
    char message[256];
    int  severity;   /* 0=info, 1=warn, 2=error */
} act_log_t;

typedef struct {
    action_type_t type;
    uint64_t      timestamp_ms;
    union {
        act_dispatch_task_t   dispatch_task;
        act_notify_job_done_t notify_job_done;
        act_log_t             log;
    } data;
} orchestrator_action_t;

/* ====================================================================
 * Configuration
 * ==================================================================== */

typedef struct {
    /* Timeout (en ms) au-delà duquel un worker sans heartbeat est
     * considéré comme SUSPECT par le tick. 0 = désactivé. */
    uint64_t heartbeat_timeout_ms;

    /* Nombre maximum de retries par tâche avant FAILED définitif. */
    uint8_t  max_task_retries;

    /* Nombre maximum d'actions à produire par appel à handle_event.
     * Sécurité contre les boucles d'attribution explosives. */
    size_t   max_actions_per_event;
} orchestrator_config_t;

/* Configuration par défaut raisonnable. */
orchestrator_config_t orchestrator_default_config(void);

/* ====================================================================
 * Type principal et cycle de vie
 * ==================================================================== */

typedef struct orchestrator_s orchestrator_t;

/*
 * Crée un orchestrator. Renvoie NULL si OOM.
 * Le config est COPIÉ (le caller peut libérer après).
 */
orchestrator_t *orchestrator_create(const orchestrator_config_t *config);

/* Détruit l'orchestrator et toutes ses ressources internes. */
void orchestrator_destroy(orchestrator_t *orch);

/* ====================================================================
 * API principale
 * ==================================================================== */

/*
 * Traite un événement entrant.
 *
 * Effets :
 *   - Met à jour les structures internes (worker_table, job_table,
 *     scheduler).
 *   - Peut produire des actions sortantes (à drainer ensuite).
 *
 * Renvoie :
 *    0 : OK, événement traité.
 *   -1 : événement invalide (type inconnu, pointeurs NULL, etc.)
 *   -2 : OOM lors du traitement (l'état peut être partiellement modifié,
 *        mais reste cohérent).
 *
 * Cette fonction NE bloque JAMAIS et NE fait AUCUN I/O.
 */
int orchestrator_handle_event(orchestrator_t *orch,
                                const orchestrator_event_t *event);

/*
 * Drain la queue d'actions sortantes : copie jusqu'à `max` actions
 * dans `out` et les retire de la queue interne.
 *
 * Le caller devient propriétaire des payload buffers dans les actions
 * (à libérer via orchestrator_action_free_payload).
 *
 * Renvoie le nombre d'actions copiées (0 si queue vide).
 */
size_t orchestrator_drain_outgoing(orchestrator_t *orch,
                                    orchestrator_action_t *out,
                                    size_t max);

/*
 * Libère les ressources allouées DANS une action (ex: payload de
 * dispatch_task). Le caller est responsable d'appeler ceci sur chaque
 * action drainée.
 */
void orchestrator_action_free_payload(orchestrator_action_t *action);

/* Nombre d'actions actuellement dans la queue (sans drain). */
size_t orchestrator_pending_actions(const orchestrator_t *orch);

/* ====================================================================
 * Introspection (pour tests, monitoring, debug)
 * ==================================================================== */

/* Accès lecture aux sous-modules internes. NE PAS modifier directement.
 * Ces accesseurs existent pour les tests et l'observabilité. */
const worker_table_t *orchestrator_get_worker_table(const orchestrator_t *orch);
const job_table_t    *orchestrator_get_job_table(const orchestrator_t *orch);
const scheduler_t    *orchestrator_get_scheduler(const orchestrator_t *orch);

#endif /* PARALLAX_ORCHESTRATOR_H */
