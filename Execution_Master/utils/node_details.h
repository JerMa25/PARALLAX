#ifndef NODE_DETAILS_H
#define NODE_DETAILS_H


typedef enum {
    NODE_ACTIF,
    NODE_SUSPECT,
    NODE_EN_PANNE,
    NODE_SURCHARGE,
    NODE_EN_MAINTENANCE
} NodeStatus;

// ─── Caractéristiques INTRINSÈQUES d'un nœud ─────────────────────────────────
// Ce que le nœud EST (hardware fixe)
// Envoyées UNE SEULE FOIS lors du premier heartbeat
// Correspondent à ce que collecte le Monitoring Thread (lscpu, free, df)

typedef struct {
    // CPU
    int   cpu_cores;          // nombre de cœurs logiques (lscpu → CPU(s))
    int   cpu_threads_per_core; // threads par cœur (lscpu → Thread(s) per core)
    float cpu_freq_mhz;       // fréquence en MHz (lscpu → CPU MHz)
    char  cpu_model[128];     // modèle (lscpu → Model name)

    // RAM
    long  ram_total_mb;       // RAM totale en Mo (free -m → total)

    // Disque
    long  disk_total_gb;      // espace disque total en Go (df -h)
    char  disk_mount[32];     // point de montage principal (ex: "/")

    // Réseau
    char  network_iface[16];  // interface réseau (ex: "eth0", "ens3")

    int   initialized;        // 0 = pas encore reçu, 1 = déjà rempli
} NodeHardware;

// ─── Métriques DYNAMIQUES d'un nœud ──────────────────────────────────────────
// Ce que le nœud CONSOMME (change à chaque heartbeat)
typedef struct {
    float cpu_usage;      // fraction CPU utilisée (0.0 à 1.0)
    float ram_usage;      // fraction RAM utilisée (0.0 à 1.0)
    long  ram_used_mb;    // RAM utilisée en Mo
    float disk_usage;     // fraction disque utilisée (0.0 à 1.0)
    long  disk_used_gb;   // disque utilisé en Go
    int   queue_len;      // nombre de tâches en attente
    float score;          // score de capacité (formule section 2.3.3)
    float load_avg;       // charge système (uptime → load average 1min)
} NodeMetrics;

// ─── Représentation complète d'un nœud ───────────────────────────────────────
typedef struct {
    char         uuid[64];        // identifiant unique (GENERATE_UUID)
    char         ip[16];          // adresse IP
    int          port;            // port d'écoute

    NodeStatus   status;          // état courant
 

    NodeHardware hardware;        // caractéristiques fixes (1er heartbeat)
    NodeMetrics  metrics;         // métriques dynamiques (chaque heartbeat)
} NodeInfo;
#endif