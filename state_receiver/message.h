// message.h
#ifndef MESSAGE_H
#define MESSAGE_H

#define MSG_HELLO           1
#define MSG_HEARTBEAT       2
#define MSG_HEARTBEAT_INIT  3   // premier heartbeat : contient aussi le hardware

typedef struct {
    long  msg_type;       // requis par les message queues POSIX

    int   type;           // MSG_HELLO, MSG_HEARTBEAT ou MSG_HEARTBEAT_INIT
    char  uuid[64];
    char  ip[16];
    int   port;

    // ── Métriques dynamiques (tous les heartbeats) ──
    float cpu_usage;
    float ram_usage;
    long  ram_used_mb;
    float disk_usage;
    long  disk_used_gb;
    int   queue_len;
    float score;
    float load_avg;

    // ── Hardware fixe (uniquement MSG_HEARTBEAT_INIT) ──
    // Ces champs sont ignorés pour les heartbeats normaux
    int   cpu_cores;
    int   cpu_threads_per_core;
    float cpu_freq_mhz;
    char  cpu_model[128];
    long  ram_total_mb;
    long  disk_total_gb;
    char  disk_mount[32];
    char  network_iface[16];

} NetworkMessage;

#endif
