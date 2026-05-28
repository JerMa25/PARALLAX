#include "Monitoring.h"

#ifdef OS_LINUX
    #include "monitoring_linux.c"
#elif defined(OS_WINDOWS)
    #include "monitoring_windows.c"
#elif defined(OS_MACOS)
    #include "monitoring_macos.c"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include "../network/network_agent.h"
#include "../init.h"

// ══════════════════════════════════════════════════════════════════════════
//  FONCTION DEBUG - AFFICHAGE DES DONNÉES ENVOYÉES
// ════════════════════════════════════════════════════════════════════════════

/**
 * Affiche tous les détails des métriques avant envoi (UUID et données d'état)
 */
static void debug_print_sent_metrics(MachineMetrics* m, const char* msg_type) {
    if (!m) return;
    
    char timestamp_str[64];
    struct tm* timeinfo = localtime(&m->timestamp);
    strftime(timestamp_str, sizeof(timestamp_str), "%Y-%m-%d %H:%M:%S", timeinfo);
    
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════════════╗\n");
    printf("║ [SENDING] Message Type: %-53s ║\n", msg_type);
    printf("╚════════════════════════════════════════════════════════════════════╝\n");
    
    printf("  ▼ IDENTITÉ\n");
    printf("    • UUID:      %s\n", m->uuid);
    printf("    • IP:        %s:%d\n", m->ip, m->port);
    printf("    • Timestamp: %s (%ld)\n", timestamp_str, m->timestamp);
    printf("    • Type:      %d\n", m->type);
    
    printf("  ▼ CPU\n");
    printf("    • Usage:           %.2f%%\n", m->cpu_usage);
    printf("    • Load Average:    %.2f, %.2f, %.2f\n", m->load_avg[0], m->load_avg[1], m->load_avg[2]);
    printf("    • Cores:           %d\n", m->cpu_cores);
    printf("    • Threads/Core:    %d\n", m->cpu_threads_per_core);
    printf("    • Frequency:       %.2f MHz\n", m->cpu_freq_mhz);
    printf("    • Model:           %s\n", m->cpu_model);
    
    printf("  ▼ MÉMOIRE (RAM)\n");
    printf("    • Usage:           %.2f%%\n", m->mem_usage);
    printf("    • Available:       %.2f MB\n", m->mem_available_mb);
    printf("    • Used:            %ld MB\n", m->mem_used_mb);
    printf("    • Total:           %ld MB\n", m->mem_total_mb);
    
    printf("  ▼ DISQUE\n");
    printf("    • Usage:           %.2f%%\n", m->disk_usage);
    printf("    • Used:            %ld MB\n", m->disk_used_mb);
    printf("    • Total:           %ld MB\n", m->disk_total_mb);
    printf("    • Mount Point:     %s\n", m->disk_mount);
    
    printf("  ▼ RÉSEAU\n");
    printf("    • Bandwidth:       %.2f Mbps\n", m->network_bandwidth_mbps);
    printf("    • Connections:     %d\n", m->active_connections);
    printf("    • Interface:       %s\n", m->network_iface);
    
    printf("  ▼ SYSTÈME\n");
    printf("    • Active Processes:    %d\n", m->active_processes);
    printf("    • Context Switch Rate: %.2f/s\n", m->context_switch_rate);
    printf("    • Uptime:              %ld seconds\n", m->uptime_seconds);
    printf("    • Is Overloaded:       %s\n", m->is_overloaded ? "YES" : "NO");
    
    printf("  ▼ CALCULS\n");
    printf("    • Queue Length: %d\n", m->queue_len);
    printf("    • Score:        %.2f\n", m->score);
    
    printf("╔════════════════════════════════════════════════════════════════════╗\n\n");
}

// Global variables (Private State)
static MachineMetrics latest_metrics;
static pthread_mutex_t metrics_mutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int monitoring_running = 1;
static int first_run = 1;
static int heartbeat_sent = 0;    // Track if HEARTBEAT_INIT has been sent
static int msg_count = 0;         // Track number of messages sent

void *monitoring_thread_run(void *arg){
    (void)arg; // avoids warning "Unused parameter"

    while(monitoring_running){
        static MachineMetrics static_m;
        // Read static metrics only on first iteration
        if (first_run) {
            memset(&static_m, 0, sizeof(MachineMetrics));
            monitoring_read_static(&static_m);
            first_run = 0;
        }

        MachineMetrics m = static_m;

        // Read all metrics
        read_cpu_usage(&m);
        read_memory(&m);
        read_disk(&m);
        read_network(&m);
        read_system(&m);
        compute_flags(&m);

        // Update timestamp
        m.timestamp = time(NULL);
        
        // Fill UUID from agent
        strncpy(m.uuid, get_agent_uuid(), sizeof(m.uuid) - 1);
        
        // Fill IP and port (hardcoded for now, can be made dynamic later)
        get_local_ip(&m, "wlo1");
        m.port = 9000;  // Default worker listening port

        // Update global state
        pthread_mutex_lock(&metrics_mutex);
        latest_metrics = m;
        pthread_mutex_unlock(&metrics_mutex);
        
        // ═══ SEND MESSAGES ═══
        // Phase 1: Send HEARTBEAT_INIT (with static + dynamic metrics) once after 1st collection
        if (!heartbeat_sent) {
            m.type = MSG_HEARTBEAT_INIT;
            
            // DEBUG: Afficher les données avant envoi
            debug_print_sent_metrics(&m, "MSG_HEARTBEAT_INIT");
            
            message_t *pkt = (message_t *)malloc(sizeof(message_t) + sizeof(MachineMetrics));
            if (pkt) {
                pkt->mq_type=1;
               
                strcpy(pkt->type,HB_INIT_TYPE);
                pkt->size = sizeof(MachineMetrics);
                memcpy(pkt->data, &m, sizeof(MachineMetrics));

                send_msg("192.168.50.1", 9000, pkt);
                
                free(pkt);
                heartbeat_sent = 1;
                msg_count++;
                printf("[MONITORING] MSG_HEARTBEAT_INIT sent (msg #%d)\n", msg_count);
            }
        }
        // Phase 2+: Send HEARTBEAT (dynamic metrics only) regularly
        else {
            m.type = MSG_HEARTBEAT;
            
            // DEBUG: Afficher les données avant envoi
            debug_print_sent_metrics(&m, "MSG_HEARTBEAT");
            
            message_t *pkt = (message_t *)malloc(sizeof(message_t) + sizeof(MachineMetrics));
            if (pkt) {
                pkt->mq_type=1;
                strcpy(pkt->type,HB_TYPE);
                pkt->size = sizeof(MachineMetrics);
                memcpy(pkt->data, &m, sizeof(MachineMetrics));
                send_msg("192.168.50.1", 9000, pkt);
                free(pkt);
                msg_count++;
                printf("[MONITORING] MSG_HEARTBEAT sent (msg #%d)\n", msg_count);
            }
        }

        sleep(MONITORING_INTERVAL);
    }

    return NULL;
}

MachineMetrics monitoring_get_latest(void){
    MachineMetrics m;
    pthread_mutex_lock(&metrics_mutex);
    m = latest_metrics; // Copy the latest metrics
    pthread_mutex_unlock(&metrics_mutex);
    return m;
}

void monitoring_stop(void){
    monitoring_running = 0;
}