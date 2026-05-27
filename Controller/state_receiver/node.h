#ifndef NODE_H
#define NODE_H

#include <time.h>
#include <pthread.h>

// ─── Constantes officielles du projet ────────────────────────────────────────
#define T_HEARTBEAT_SEC     2    // période normale d'émission des heartbeats
#define T_SUSPECT_SEC       4    // silence > 4s → nœud SUSPECT
#define T_FAILED_SEC        8    // silence > 8s → nœud EN_PANNE confirmé

// ─── États d'un nœud ─────────────────────────────────────────────────────────
typedef enum {
    NODE_ACTIF,
    NODE_SUSPECT,
    NODE_EN_PANNE,
    NODE_SURCHARGE,
    NODE_EN_MAINTENANCE
} NodeStatus;

// ─── Caractéristiques INTRINSÈQUES d'un nœud ─────────────────────────────────
// Envoyées UNE SEULE FOIS lors du premier heartbeat (HEARTBEAT_INIT)
typedef struct {
    int   cpu_cores;
    int   cpu_threads_per_core;
    float cpu_freq_mhz;
    char  cpu_model[128];

    long  ram_total_mb;

    long  disk_total_gb;
    char  disk_mount[32];

    char  network_iface[16];

    int   initialized;   // 0 = pas encore reçu, 1 = déjà rempli
} NodeHardware;

// ─── Métriques DYNAMIQUES d'un nœud ──────────────────────────────────────────
// Mises à jour à chaque heartbeat, bufferisées en RAM avant flush disque
typedef struct {
    float cpu_usage;
    float ram_usage;
    long  ram_used_mb;
    float disk_usage;
    long  disk_used_mb;
    int   queue_len;
    float score;
    float load_avg;
} NodeMetrics;

// ─── Représentation complète d'un nœud ───────────────────────────────────────
typedef struct NodeInfo {
    char         uuid[64];
    char         ip[16];
    int          port;

    NodeStatus   status;
    time_t       last_heartbeat;

    NodeHardware hardware;
    NodeMetrics  metrics;

    struct NodeInfo* next;   // chaînage vers le nœud suivant
} NodeInfo;

// ─── Table des nœuds (liste chaînée) ─────────────────────────────────────────
// head → premier nœud, count → nombre total de nœuds connus
typedef struct {
    NodeInfo*       head;
    int             count;
    pthread_mutex_t lock;
} NodeTable;

// ─── Cycle de vie de la table ─────────────────────────────────────────────────
void      node_table_init(NodeTable* table);
void      node_table_destroy(NodeTable* table);  // libère tous les nœuds

// ─── Lookup ───────────────────────────────────────────────────────────────────
// Retourne le pointeur vers le nœud ou NULL — appelé sous mutex par l'appelant
NodeInfo* node_table_find(NodeTable* table, const char* uuid);

// ─── Insertion ────────────────────────────────────────────────────────────────
// Alloue un nouveau nœud et l'insère en tête — retourne le pointeur ou NULL
NodeInfo* node_table_add(NodeTable* table, const char* uuid,
                          const char* ip, int port);

#endif /* NODE_H */