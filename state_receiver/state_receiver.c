#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/msg.h>
#include <pthread.h>
#include <stdatomic.h>    // pour atomic_int → arrêt propre sans mutex
#include "node.h"
#include "message.h"
#include "state_receiver.h"

// ─── Flags d'arrêt ───────────────────────────────────────────────────────────
// atomic_int : garanti thread-safe en lecture/écriture sans mutex
// Le thread principal met le flag à 1, le thread le lit et sort de sa boucle
static atomic_int receiver_running = 0;
static atomic_int watchdog_running = 0;

// ─── Handles des threads ─────────────────────────────────────────────────────
static pthread_t receiver_tid;
static pthread_t watchdog_tid;

// ════════════════════════════════════════════════════════════════════════════
//  FONCTIONS INTERNES (inchangées)
// ════════════════════════════════════════════════════════════════════════════

static int find_node(NodeTable* table, const char* uuid) {
    for (int i = 0; i < table->count; i++) {
        if (strcmp(table->nodes[i].uuid, uuid) == 0)
            return i;
    }
    return -1;
}

static void register_node(NodeTable* table, NetworkMessage* msg) {
    pthread_mutex_lock(&table->lock);

    if (find_node(table, msg->uuid) >= 0) {
        printf("[StateReceiver] Noeud %s déjà enregistré, HELLO ignoré\n", msg->uuid);
        pthread_mutex_unlock(&table->lock);
        return;
    }
    if (table->count >= MAX_NODES) {
        printf("[StateReceiver] ⚠ Table pleine\n");
        pthread_mutex_unlock(&table->lock);
        return;
    }

    NodeInfo* node        = &table->nodes[table->count++];
    strncpy(node->uuid, msg->uuid, sizeof(node->uuid) - 1);
    strncpy(node->ip,   msg->ip,   sizeof(node->ip)   - 1);
    node->port            = msg->port;
    node->status          = NODE_ACTIF;
    node->last_heartbeat  = time(NULL);
    node->hardware.initialized = 0;
    memset(&node->metrics, 0, sizeof(NodeMetrics));

    printf("[StateReceiver] ✓ Nouveau nœud : uuid=%s ip=%s\n", node->uuid, node->ip);
    pthread_mutex_unlock(&table->lock);
}

static void update_heartbeat(NodeTable* table, NetworkMessage* msg) {
    pthread_mutex_lock(&table->lock);

    int idx = find_node(table, msg->uuid);
    if (idx < 0) {
        printf("[StateReceiver] ⚠ Heartbeat nœud inconnu %s\n", msg->uuid);
        pthread_mutex_unlock(&table->lock);
        return;
    }

    NodeInfo* node           = &table->nodes[idx];
    node->last_heartbeat     = time(NULL);
    node->metrics.cpu_usage  = msg->cpu_usage;
    node->metrics.ram_usage  = msg->ram_usage;
    node->metrics.ram_used_mb  = msg->ram_used_mb;
    node->metrics.disk_usage   = msg->disk_usage;
    node->metrics.disk_used_gb = msg->disk_used_gb;
    node->metrics.queue_len  = msg->queue_len;
    node->metrics.score      = msg->score;
    node->metrics.load_avg   = msg->load_avg;

    if (msg->cpu_usage > 0.85f || msg->ram_usage > 0.85f) {
        node->status = NODE_SURCHARGE;
        printf("[StateReceiver] ⚡ Nœud %s SURCHARGE\n", node->uuid);
    } else {
        if (node->status != NODE_ACTIF)
            printf("[StateReceiver] ↑ Nœud %s revient à ACTIF\n", node->uuid);
        node->status = NODE_ACTIF;
    }

    pthread_mutex_unlock(&table->lock);
}

static void init_heartbeat(NodeTable* table, NetworkMessage* msg) {
    pthread_mutex_lock(&table->lock);

    int idx = find_node(table, msg->uuid);
    if (idx < 0) {
        printf("[StateReceiver] ⚠ HEARTBEAT_INIT nœud inconnu %s\n", msg->uuid);
        pthread_mutex_unlock(&table->lock);
        return;
    }

    NodeInfo* node = &table->nodes[idx];

    if (!node->hardware.initialized) {
        node->hardware.cpu_cores            = msg->cpu_cores;
        node->hardware.cpu_threads_per_core = msg->cpu_threads_per_core;
        node->hardware.cpu_freq_mhz         = msg->cpu_freq_mhz;
        node->hardware.ram_total_mb         = msg->ram_total_mb;
        node->hardware.disk_total_gb        = msg->disk_total_gb;
        strncpy(node->hardware.cpu_model,     msg->cpu_model,     sizeof(node->hardware.cpu_model)     - 1);
        strncpy(node->hardware.disk_mount,    msg->disk_mount,    sizeof(node->hardware.disk_mount)    - 1);
        strncpy(node->hardware.network_iface, msg->network_iface, sizeof(node->hardware.network_iface) - 1);
        node->hardware.initialized = 1;

        printf("[StateReceiver] ✓ Hardware nœud %s : %d cœurs %.0fMHz %ldMo RAM\n",
               node->uuid, node->hardware.cpu_cores,
               node->hardware.cpu_freq_mhz, node->hardware.ram_total_mb);
    }

    node->last_heartbeat      = time(NULL);
    node->metrics.cpu_usage   = msg->cpu_usage;
    node->metrics.ram_usage   = msg->ram_usage;
    node->metrics.queue_len   = msg->queue_len;
    node->metrics.score       = msg->score;
    node->status              = NODE_ACTIF;

    pthread_mutex_unlock(&table->lock);
}

// ════════════════════════════════════════════════════════════════════════════
//  LOGIQUE INTERNE DES THREADS
// ════════════════════════════════════════════════════════════════════════════

// Fonction interne du thread receiver (passée à pthread_create)
static void* _receiver_loop(void* arg) {
    NodeTable* table = (NodeTable*)arg;

    key_t key = ftok("/tmp/parallax_queue", 42);
    int msqid  = msgget(key, 0666 | IPC_CREAT);
    if (msqid == -1) {
        perror("[StateReceiver] msgget() échoué");
        return NULL;
    }

    printf("[StateReceiver] Démarré (msqid=%d)\n", msqid);

    NetworkMessage msg;

    // receiver_running == 1 tant que stop() n'a pas été appelé
    while (atomic_load(&receiver_running)) {

        // msgrcv avec IPC_NOWAIT : non bloquant
        // Si pas de message → ENOMSG → on reboucle et on vérifie le flag
        // C'est ce qui permet à stop() d'arrêter proprement le thread
        ssize_t ret = msgrcv(msqid, &msg, sizeof(msg) - sizeof(long), 0, IPC_NOWAIT);

        if (ret == -1) {
            // Pas de message disponible → on attend un peu et on reboucle
            usleep(100000); // 100ms
            continue;
        }

        switch (msg.type) {
            case MSG_HELLO:          register_node(table, &msg);   break;
            case MSG_HEARTBEAT:      update_heartbeat(table, &msg); break;
            case MSG_HEARTBEAT_INIT: init_heartbeat(table, &msg);  break;
            default:
                printf("[StateReceiver] ⚠ Type inconnu : %d\n", msg.type);
        }
    }

    printf("[StateReceiver] Arrêté proprement\n");
    return NULL;
}

// Fonction interne du thread watchdog
static void* _watchdog_loop(void* arg) {
    NodeTable* table = (NodeTable*)arg;

    printf("[Watchdog] Démarré\n");

    while (atomic_load(&watchdog_running)) {
        sleep(T_HEARTBEAT_SEC);

        pthread_mutex_lock(&table->lock);
        time_t now = time(NULL);

        for (int i = 0; i < table->count; i++) {
            NodeInfo* node = &table->nodes[i];
            if (node->status == NODE_EN_MAINTENANCE) continue;

            double delta = difftime(now, node->last_heartbeat);

            if (delta >= T_FAILED_SEC && node->status != NODE_EN_PANNE) {
                node->status = NODE_EN_PANNE;
                printf("[Watchdog] ✗ PANNE : nœud %s (silence %.0fs)\n",
                       node->uuid, delta);
                // TODO : notifier Panne Detection (Kouam/Tagne)

            } else if (delta >= T_SUSPECT_SEC &&
                       (node->status == NODE_ACTIF || node->status == NODE_SURCHARGE)) {
                node->status = NODE_SUSPECT;
                printf("[Watchdog] ? SUSPECT : nœud %s (silence %.0fs)\n",
                       node->uuid, delta);
                // TODO : déclencher gossip_query
            }
        }

        pthread_mutex_unlock(&table->lock);
    }

    printf("[Watchdog] Arrêté proprement\n");
    return NULL;
}

// ════════════════════════════════════════════════════════════════════════════
//  FONCTIONS PUBLIQUES — interface pour Tchami (init)
// ════════════════════════════════════════════════════════════════════════════

void state_receiver_thread_run(NodeTable* table) {
    atomic_store(&receiver_running, 1);
    if (pthread_create(&receiver_tid, NULL, _receiver_loop, table) != 0) {
        perror("[StateReceiver] pthread_create() échoué");
        atomic_store(&receiver_running, 0);
    }
}

void state_receiver_stop(void) {
    atomic_store(&receiver_running, 0);   // signal d'arrêt au thread
    pthread_join(receiver_tid, NULL);     // on attend qu'il termine proprement
}

void watchdog_thread_run(NodeTable* table) {
    atomic_store(&watchdog_running, 1);
    if (pthread_create(&watchdog_tid, NULL, _watchdog_loop, table) != 0) {
        perror("[Watchdog] pthread_create() échoué");
        atomic_store(&watchdog_running, 0);
    }
}

void watchdog_stop(void) {
    atomic_store(&watchdog_running, 0);   // signal d'arrêt au thread
    pthread_join(watchdog_tid, NULL);     // on attend qu'il termine proprement
}