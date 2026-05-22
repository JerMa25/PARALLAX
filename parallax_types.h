/*
 * parallax_types.h
 *
 * Types fondamentaux partagés par l'orchestrateur.
 *
 * Conventions :
 *   - Les UUID sont stockés en string (37 octets : 36 + '\0')
 *     pour faciliter le debug. On peut passer en binaire (16 octets)
 *     plus tard si on mesure un coût significatif.
 *   - Toutes les tailles en octets sont des size_t.
 *   - Les timestamps sont en millisecondes depuis epoch (uint64_t).
 *   - Pas d'allocation dynamique dans les structures de base
 *     sauf payload des tâches (taille variable par nature).
 */

#ifndef PARALLAX_TYPES_H
#define PARALLAX_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define PARALLAX_UUID_LEN 37  /* 36 chars + null terminator */
#define PARALLAX_NODE_NAME_MAX 64

/* ========================================================================
 * État d'un worker tel que l'orchestrateur le voit.
 *
 * La transition suit ce diagramme :
 *
 *   UNKNOWN ──hello──▶ AVAILABLE ──assign──▶ BUSY
 *                          ▲                    │
 *                          └────done────────────┘
 *
 *   any state ──timeout──▶ SUSPECT ──confirm──▶ FAILED
 *                              │
 *                              └─heartbeat─▶ previous state
 * ======================================================================== */
typedef enum {
    WORKER_UNKNOWN   = 0,  /* jamais vu */
    WORKER_AVAILABLE = 1,  /* prêt à recevoir une tâche */
    WORKER_BUSY      = 2,  /* exécute une tâche */
    WORKER_SUSPECT   = 3,  /* heartbeat manqué, en cours de vérification */
    WORKER_FAILED    = 4   /* déclaré mort, ses tâches sont à redistribuer */
} worker_state_t;

/* ========================================================================
 * Capacités d'un worker.
 *
 * Le score de capacité est utilisé par le scheduler pour pondérer
 * l'attribution des tâches. Formule (à ajuster après benchmarks) :
 *
 *   score = ram_mb + (cpu_count * 1024)
 *
 * Justification : 1 cœur ≈ 1 Go de RAM en termes de "valeur" pour du
 * calcul CPU-bound. À recalibrer après mesures réelles.
 * ======================================================================== */
typedef struct {
    uint32_t ram_mb;       /* mémoire totale en Mo */
    uint16_t cpu_count;    /* nombre de cœurs logiques */
    uint32_t cpu_mhz;      /* fréquence approximative, info seulement */
} worker_capabilities_t;

/* ========================================================================
 * Représentation d'un worker dans la table de l'orchestrateur.
 * ======================================================================== */
typedef struct {
    char     node_id[PARALLAX_UUID_LEN];        /* identifiant stable */
    char     node_name[PARALLAX_NODE_NAME_MAX]; /* hostname, debug */
    worker_capabilities_t caps;
    worker_state_t state;
    uint64_t last_heartbeat_ms;   /* dernier heartbeat reçu */
    uint32_t tasks_assigned;      /* total cumulé, pour métriques */
    uint32_t tasks_completed;
    uint32_t tasks_failed;
    uint32_t current_task_id;     /* 0 si aucune, sinon id en cours */
} worker_t;

/* ========================================================================
 * Une tâche dans le bag-of-tasks.
 *
 * Le payload est opaque pour l'orchestrateur : c'est l'Execution Master
 * qui produit le contenu (segment de code à exécuter + données d'entrée).
 * L'orchestrateur ne fait que router.
 * ======================================================================== */
typedef enum {
    TASK_PENDING    = 0,  /* dans la queue, pas encore attribuée */
    TASK_ASSIGNED   = 1,  /* envoyée à un worker, en attente de résultat */
    TASK_COMPLETED  = 2,  /* résultat reçu avec succès */
    TASK_FAILED     = 3,  /* échec définitif après retries */
    TASK_REQUEUED   = 4   /* worker mort, tâche remise en queue */
} task_state_t;

typedef struct {
    uint32_t task_id;             /* unique dans une session */
    task_state_t state;
    char     assigned_worker[PARALLAX_UUID_LEN]; /* "" si pending */
    uint64_t created_ms;
    uint64_t assigned_ms;
    uint64_t completed_ms;
    uint8_t  retry_count;         /* combien de fois redistribuée */
    uint8_t  max_retries;         /* limite avant échec définitif */

    /* Payload : possédé par la tâche, libéré quand la tâche est détruite */
    uint8_t *payload;
    size_t   payload_size;
} task_t;

/* ========================================================================
 * Résultat d'exécution d'une tâche, tel que rapporté par un worker.
 * ======================================================================== */
typedef struct {
    uint32_t task_id;
    char     worker_id[PARALLAX_UUID_LEN];
    bool     success;
    int32_t  exit_code;       /* code de retour si applicable */
    uint64_t execution_ms;    /* durée d'exécution côté worker */
    uint8_t *output;          /* résultat brut */
    size_t   output_size;
    char     error_msg[256];  /* vide si success */
} task_result_t;

#endif /* PARALLAX_TYPES_H */
