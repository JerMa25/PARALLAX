#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/msg.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdatomic.h>
#include "node.h"
#include "message.h"
#include "state_receiver.h"

// ════════════════════════════════════════════════════════════════════════════
//  PERSISTANCE DISQUE — cache RAM + flush périodique
// ════════════════════════════════════════════════════════════════════════════

// Répertoire de stockage des métriques et des snapshots nœuds
#define PERSIST_DIR          "/var/lib/parallax"
#define METRICS_FLUSH_SEC    30     // flush du cache métriques toutes les 30s
#define METRICS_BUFFER_MAX   256    // entrées max dans le buffer avant flush forcé

// Une entrée du buffer RAM : horodatage + métriques d'un nœud
typedef struct {
    char        uuid[64];
    time_t      ts;
    NodeMetrics metrics;
} MetricEntry;

// Buffer RAM partagé entre le receiver et le flusher
static MetricEntry  metrics_buf[METRICS_BUFFER_MAX];
static int          metrics_buf_len = 0;
static pthread_mutex_t metrics_buf_lock = PTHREAD_MUTEX_INITIALIZER;

// Thread de flush disque
static pthread_t    flusher_tid;
static atomic_int   flusher_running = 0;

// ─── Écriture d'un snapshot du nœud (hardware + état) en JSON ────────────────
// Fichier : PERSIST_DIR/nodes/<uuid>.json
// Écrasé à chaque mise à jour : on veut l'état courant, pas l'historique
static void persist_node_snapshot(const NodeInfo* node) {
    char path[256];
    snprintf(path, sizeof(path), PERSIST_DIR "/nodes/%s.json", node->uuid);

    FILE* f = fopen(path, "w");
    if (!f) {
        // Le répertoire n'existe peut-être pas encore ; on tente de le créer
        mkdir(PERSIST_DIR "/nodes", 0755);
        f = fopen(path, "w");
        if (!f) {
            perror("[Persist] fopen snapshot");
            return;
        }
    }

    fprintf(f,
        "{\n"
        "  \"uuid\": \"%s\",\n"
        "  \"ip\": \"%s\",\n"
        "  \"port\": %d,\n"
        "  \"status\": %d,\n"
        "  \"last_heartbeat\": %ld,\n"
        "  \"hardware\": {\n"
        "    \"cpu_cores\": %d,\n"
        "    \"cpu_threads_per_core\": %d,\n"
        "    \"cpu_freq_mhz\": %.1f,\n"
        "    \"cpu_model\": \"%s\",\n"
        "    \"ram_total_mb\": %ld,\n"
        "    \"disk_total_gb\": %ld,\n"
        "    \"disk_mount\": \"%s\",\n"
        "    \"network_iface\": \"%s\"\n"
        "  }\n"
        "}\n",
        node->uuid, node->ip, node->port,
        (int)node->status, (long)node->last_heartbeat,
        node->hardware.cpu_cores,
        node->hardware.cpu_threads_per_core,
        node->hardware.cpu_freq_mhz,
        node->hardware.cpu_model,
        node->hardware.ram_total_mb,
        node->hardware.disk_total_gb,
        node->hardware.disk_mount,
        node->hardware.network_iface);

    fclose(f);
}

// ─── Ajout d'une entrée métriques dans le buffer RAM ─────────────────────────
// Appelé à chaque heartbeat ; si le buffer est plein → flush immédiat
static void metrics_buf_push(const char* uuid, time_t ts, const NodeMetrics* m) {
    pthread_mutex_lock(&metrics_buf_lock);

    if (metrics_buf_len >= METRICS_BUFFER_MAX) {
        // Buffer plein : on force le flush dans ce même appel
        // (le thread flusher n'a pas encore tourné)
        // On libère le mutex le temps d'écrire, puis on reprend
        pthread_mutex_unlock(&metrics_buf_lock);

        // Appel direct — pas de récursivité, juste une écriture bloquante
        // On relock ensuite pour vider le compteur
        extern void metrics_flush_now(void);   // déclaré plus bas
        metrics_flush_now();

        pthread_mutex_lock(&metrics_buf_lock);
        metrics_buf_len = 0;   // garanti vidé par flush_now
    }

    MetricEntry* e = &metrics_buf[metrics_buf_len++];
    strncpy(e->uuid, uuid, sizeof(e->uuid) - 1);
    e->ts      = ts;
    e->metrics = *m;

    pthread_mutex_unlock(&metrics_buf_lock);
}

// ─── Flush effectif : écrit le buffer dans PERSIST_DIR/metrics/<uuid>.csv ────
// Format CSV : timestamp,cpu,ram,ram_mb,disk,disk_gb,queue,score,load
// Chaque nœud a son propre fichier, ouvert en mode "append".
void metrics_flush_now(void) {
    pthread_mutex_lock(&metrics_buf_lock);

    if (metrics_buf_len == 0) {
        pthread_mutex_unlock(&metrics_buf_lock);
        return;
    }

    // Copie locale pour libérer le mutex rapidement
    int n = metrics_buf_len;
    MetricEntry local[METRICS_BUFFER_MAX];
    memcpy(local, metrics_buf, n * sizeof(MetricEntry));
    metrics_buf_len = 0;

    pthread_mutex_unlock(&metrics_buf_lock);

    // Écriture fichier hors mutex (I/O lente)
    for (int i = 0; i < n; i++) {
        char path[256];
        snprintf(path, sizeof(path),
                 PERSIST_DIR "/metrics/%s.csv", local[i].uuid);

        FILE* f = fopen(path, "a");
        if (!f) {
            mkdir(PERSIST_DIR "/metrics", 0755);
            f = fopen(path, "a");
            if (!f) { perror("[Persist] fopen metrics"); continue; }

            // En-tête CSV sur un nouveau fichier
            fprintf(f, "ts,cpu,ram,ram_mb,disk,disk_gb,queue,score,load\n");
        }

        const NodeMetrics* m = &local[i].metrics;
        fprintf(f, "%ld,%.4f,%.4f,%ld,%.4f,%ld,%d,%.4f,%.4f\n",
                (long)local[i].ts,
                m->cpu_usage, m->ram_usage, m->ram_used_mb,
                m->disk_usage, m->disk_used_gb,
                m->queue_len, m->score, m->load_avg);

        fclose(f);
    }

    printf("[Persist] %d entrées métriques écrites sur disque\n", n);
}

// ─── Thread flusher ───────────────────────────────────────────────────────────
static void* _flusher_loop(void* arg) {
    (void)arg;
    printf("[Persist] Thread flusher démarré (intervalle %ds)\n",
           METRICS_FLUSH_SEC);

    while (atomic_load(&flusher_running)) {
        sleep(METRICS_FLUSH_SEC);
        metrics_flush_now();
    }

    // Flush final avant sortie
    metrics_flush_now();
    printf("[Persist] Thread flusher arrêté\n");
    return NULL;
}

static void flusher_start(void) {
    // Crée les répertoires nécessaires au premier démarrage
    mkdir(PERSIST_DIR,            0755);
    mkdir(PERSIST_DIR "/nodes",   0755);
    mkdir(PERSIST_DIR "/metrics", 0755);

    atomic_store(&flusher_running, 1);
    if (pthread_create(&flusher_tid, NULL, _flusher_loop, NULL) != 0) {
        perror("[Persist] pthread_create flusher");
        atomic_store(&flusher_running, 0);
    }
}

static void flusher_stop(void) {
    atomic_store(&flusher_running, 0);
    pthread_join(flusher_tid, NULL);
}

// ════════════════════════════════════════════════════════════════════════════
//  IMPLÉMENTATION DE node.h — liste chaînée
// ════════════════════════════════════════════════════════════════════════════

void node_table_init(NodeTable* table) {
    table->head  = NULL;
    table->count = 0;
    pthread_mutex_init(&table->lock, NULL);
}

void node_table_destroy(NodeTable* table) {
    pthread_mutex_lock(&table->lock);
    NodeInfo* cur = table->head;
    while (cur) {
        NodeInfo* next = cur->next;
        free(cur);
        cur = next;
    }
    table->head  = NULL;
    table->count = 0;
    pthread_mutex_unlock(&table->lock);
    pthread_mutex_destroy(&table->lock);
}

// Appelé sous mutex par l'appelant
NodeInfo* node_table_find(NodeTable* table, const char* uuid) {
    for (NodeInfo* n = table->head; n; n = n->next)
        if (strcmp(n->uuid, uuid) == 0)
            return n;
    return NULL;
}

// Appelé sous mutex par l'appelant
NodeInfo* node_table_add(NodeTable* table, const char* uuid,
                          const char* ip, int port) {
    NodeInfo* node = calloc(1, sizeof(NodeInfo));
    if (!node) { perror("[NodeTable] calloc"); return NULL; }

    strncpy(node->uuid, uuid, sizeof(node->uuid) - 1);
    strncpy(node->ip,   ip,   sizeof(node->ip)   - 1);
    node->port           = port;
    node->status         = NODE_ACTIF;
    node->last_heartbeat = time(NULL);
    node->hardware.initialized = 0;
    memset(&node->metrics, 0, sizeof(NodeMetrics));

    // Insertion en tête (O(1))
    node->next   = table->head;
    table->head  = node;
    table->count++;
    return node;
}

// ════════════════════════════════════════════════════════════════════════════
//  HANDLERS DE MESSAGES
// ════════════════════════════════════════════════════════════════════════════

static void register_node(NodeTable* table, NetworkMessage* msg) {
    pthread_mutex_lock(&table->lock);

    if (node_table_find(table, msg->uuid)) {
        printf("[StateReceiver] Nœud %s déjà enregistré, HELLO ignoré\n",
               msg->uuid);
        pthread_mutex_unlock(&table->lock);
        return;
    }

    NodeInfo* node = node_table_add(table, msg->uuid, msg->ip, msg->port);
    if (node)
        printf("[StateReceiver] ✓ Nouveau nœud : uuid=%s ip=%s\n",
               node->uuid, node->ip);

    pthread_mutex_unlock(&table->lock);

    // Snapshot initial sur disque (hardware non encore rempli, mais IP/port connus)
    if (node)
        persist_node_snapshot(node);
}

static void update_heartbeat(NodeTable* table, NetworkMessage* msg) {
    pthread_mutex_lock(&table->lock);

    NodeInfo* node = node_table_find(table, msg->uuid);
    if (!node) {
        printf("[StateReceiver] ⚠ Heartbeat nœud inconnu %s\n", msg->uuid);
        pthread_mutex_unlock(&table->lock);
        return;
    }

    node->last_heartbeat       = time(NULL);
    node->metrics.cpu_usage    = msg->cpu_usage;
    node->metrics.ram_usage    = msg->ram_usage;
    node->metrics.ram_used_mb  = msg->ram_used_mb;
    node->metrics.disk_usage   = msg->disk_usage;
    node->metrics.disk_used_gb = msg->disk_used_gb;
    node->metrics.queue_len    = msg->queue_len;
    node->metrics.score        = msg->score;
    node->metrics.load_avg     = msg->load_avg;

    if (msg->cpu_usage > 0.85f || msg->ram_usage > 0.85f) {
        node->status = NODE_SURCHARGE;
        printf("[StateReceiver] ⚡ Nœud %s SURCHARGE\n", node->uuid);
    } else {
        if (node->status != NODE_ACTIF)
            printf("[StateReceiver] ↑ Nœud %s revient à ACTIF\n", node->uuid);
        node->status = NODE_ACTIF;
    }

    // Copie locale pour sortir de la section critique avant l'I/O
    char        uuid_copy[64];
    time_t      ts    = node->last_heartbeat;
    NodeMetrics mcopy = node->metrics;
    strncpy(uuid_copy, node->uuid, sizeof(uuid_copy) - 1);

    pthread_mutex_unlock(&table->lock);

    // Buffer RAM — flush asynchrone par le thread flusher
    metrics_buf_push(uuid_copy, ts, &mcopy);
}

static void init_heartbeat(NodeTable* table, NetworkMessage* msg) {
    pthread_mutex_lock(&table->lock);

    NodeInfo* node = node_table_find(table, msg->uuid);
    if (!node) {
        printf("[StateReceiver] ⚠ HEARTBEAT_INIT nœud inconnu %s\n", msg->uuid);
        pthread_mutex_unlock(&table->lock);
        return;
    }

    if (!node->hardware.initialized) {
        node->hardware.cpu_cores            = msg->cpu_cores;
        node->hardware.cpu_threads_per_core = msg->cpu_threads_per_core;
        node->hardware.cpu_freq_mhz         = msg->cpu_freq_mhz;
        node->hardware.ram_total_mb         = msg->ram_total_mb;
        node->hardware.disk_total_gb        = msg->disk_total_gb;
        strncpy(node->hardware.cpu_model,
                msg->cpu_model,     sizeof(node->hardware.cpu_model)     - 1);
        strncpy(node->hardware.disk_mount,
                msg->disk_mount,    sizeof(node->hardware.disk_mount)    - 1);
        strncpy(node->hardware.network_iface,
                msg->network_iface, sizeof(node->hardware.network_iface) - 1);
        node->hardware.initialized = 1;

        printf("[StateReceiver] ✓ Hardware nœud %s : %d cœurs %.0fMHz %ldMo RAM\n",
               node->uuid, node->hardware.cpu_cores,
               node->hardware.cpu_freq_mhz, node->hardware.ram_total_mb);
    }

    node->last_heartbeat    = time(NULL);
    node->metrics.cpu_usage = msg->cpu_usage;
    node->metrics.ram_usage = msg->ram_usage;
    node->metrics.queue_len = msg->queue_len;
    node->metrics.score     = msg->score;
    node->status            = NODE_ACTIF;

    // Copie locale avant de libérer le mutex
    NodeInfo snapshot = *node;
    char        uuid_copy[64];
    time_t      ts    = node->last_heartbeat;
    NodeMetrics mcopy = node->metrics;
    strncpy(uuid_copy, node->uuid, sizeof(uuid_copy) - 1);

    pthread_mutex_unlock(&table->lock);

    // Snapshot hardware sur disque (une seule fois)
    persist_node_snapshot(&snapshot);

    // Métriques initiales dans le buffer RAM
    metrics_buf_push(uuid_copy, ts, &mcopy);
}

// ════════════════════════════════════════════════════════════════════════════
//  BOUCLE DU THREAD RECEIVER
// ════════════════════════════════════════════════════════════════════════════

static atomic_int  receiver_running = 0;
static pthread_t   receiver_tid;

static void* _receiver_loop(void* arg) {
    NodeTable* table = (NodeTable*)arg;

    key_t key  = ftok("/tmp/parallax_queue", 42);
    int   msqid = msgget(key, 0666 | IPC_CREAT);
    if (msqid == -1) {
        perror("[StateReceiver] msgget()");
        return NULL;
    }

    printf("[StateReceiver] Démarré (msqid=%d)\n", msqid);

    NetworkMessage msg;

    while (atomic_load(&receiver_running)) {
        ssize_t ret = msgrcv(msqid, &msg, sizeof(msg) - sizeof(long),
                             0, IPC_NOWAIT);
        if (ret == -1) {
            usleep(100000);   // 100ms — pas de message, on repoll
            continue;
        }

        switch (msg.type) {
            case MSG_HELLO:          register_node(table, &msg);    break;
            case MSG_HEARTBEAT:      update_heartbeat(table, &msg); break;
            case MSG_HEARTBEAT_INIT: init_heartbeat(table, &msg);   break;
            default:
                printf("[StateReceiver] ⚠ Type inconnu : %d\n", msg.type);
        }
    }

    printf("[StateReceiver] Arrêté proprement\n");
    return NULL;
}

// ════════════════════════════════════════════════════════════════════════════
//  API PUBLIQUE
// ════════════════════════════════════════════════════════════════════════════

void state_receiver_thread_run(NodeTable* table) {
    flusher_start();   // démarre aussi le thread de persistence disque

    atomic_store(&receiver_running, 1);
    if (pthread_create(&receiver_tid, NULL, _receiver_loop, table) != 0) {
        perror("[StateReceiver] pthread_create()");
        atomic_store(&receiver_running, 0);
        flusher_stop();
    }
}

void state_receiver_stop(void) {
    atomic_store(&receiver_running, 0);
    pthread_join(receiver_tid, NULL);

    flusher_stop();    // flush final + arrêt du thread de persistence
}