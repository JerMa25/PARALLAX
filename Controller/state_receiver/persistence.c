#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdatomic.h>

#define PERSIST_DIR          "./Controller/parallax_data"
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

/**
 * Sauvegarde un snapshot du nœud (état + informations matérielles) au format JSON.
 * Écrase le fichier existant à chaque mise à jour.
 */
void persist_node_snapshot(const NodeInfo* node) {
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

/**
 * Ajoute une entrée de métriques dans le buffer mémoire partagé.
 * Si le buffer est plein, déclenche immédiatement un flush synchrone.
 */
void metrics_buf_push(const char* uuid, time_t ts, const NodeMetrics* m) {
    pthread_mutex_lock(&metrics_buf_lock);

    if (metrics_buf_len >= METRICS_BUFFER_MAX) {
        pthread_mutex_unlock(&metrics_buf_lock);
        metrics_flush_now();
        pthread_mutex_lock(&metrics_buf_lock);
        metrics_buf_len = 0;
    }

    MetricEntry* e = &metrics_buf[metrics_buf_len++];
    strncpy(e->uuid, uuid, sizeof(e->uuid) - 1);
    e->ts      = ts;
    e->metrics = *m;

    pthread_mutex_unlock(&metrics_buf_lock);
}

/**
 * Force l'écriture du contenu complet du buffer RAM de métriques
 * dans les fichiers CSV correspondants (un par nœud).
 */
void metrics_flush_now(void) {
    pthread_mutex_lock(&metrics_buf_lock);

    if (metrics_buf_len == 0) {
        pthread_mutex_unlock(&metrics_buf_lock);
        return;
    }

    int n = metrics_buf_len;
    MetricEntry local[METRICS_BUFFER_MAX];
    memcpy(local, metrics_buf, n * sizeof(MetricEntry));
    metrics_buf_len = 0;

    pthread_mutex_unlock(&metrics_buf_lock);

    for (int i = 0; i < n; i++) {
        char path[256];
        snprintf(path, sizeof(path),
                 PERSIST_DIR "/metrics/%s.csv", local[i].uuid);

        FILE* f = fopen(path, "a");
        if (!f) {
            mkdir(PERSIST_DIR "/metrics", 0755);
            f = fopen(path, "a");
            if (!f) { perror("[Persist] fopen metrics"); continue; }

            fprintf(f, "ts,cpu,ram,ram_mb,disk,disk_gb,queue,score,load\n");
        }

        const NodeMetrics* m = &local[i].metrics;
        fprintf(f, "%ld,%.4f,%.4f,%ld,%.4f,%ld,%d,%.4f,%.4f\n",
                (long)local[i].ts,
                m->cpu_usage, m->ram_usage, m->ram_used_mb,
                m->disk_usage, m->disk_used_mb,
                m->queue_len, m->score, m->load_avg[0]);

        fclose(f);
    }

    printf("[Persist] %d entrées métriques écrites sur disque\n", n);
}

/**
 * Boucle du thread flusher. Lance metrics_flush_now à intervalles réguliers.
 */
static void* _flusher_loop(void* arg) {
    (void)arg;
    printf("[Persist] Thread flusher démarré (intervalle %ds)\n", METRICS_FLUSH_SEC);

    while (atomic_load(&flusher_running)) {
        sleep(METRICS_FLUSH_SEC);
        metrics_flush_now();
    }

    metrics_flush_now();
    printf("[Persist] Thread flusher arrêté\n");
    return NULL;
}

/**
 * Démarre le sous-système de persistance et son thread flusher.
 * S'occupe de créer les répertoires nécessaires.
 */
void flusher_start(void) {
    mkdir(PERSIST_DIR,            0755);
    mkdir(PERSIST_DIR "/nodes",   0755);
    mkdir(PERSIST_DIR "/metrics", 0755);

    atomic_store(&flusher_running, 1);
    if (pthread_create(&flusher_tid, NULL, _flusher_loop, NULL) != 0) {
        perror("[Persist] pthread_create flusher");
        atomic_store(&flusher_running, 0);
    }
}

/**
 * Arrête proprement le thread flusher et attend sa fin.
 */
void flusher_stop(void) {
    if (atomic_load(&flusher_running)) {
        atomic_store(&flusher_running, 0);
        pthread_join(flusher_tid, NULL);
    }
}
