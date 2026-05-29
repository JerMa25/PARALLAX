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

        // Update global state
        pthread_mutex_lock(&metrics_mutex);
        latest_metrics = m;
        pthread_mutex_unlock(&metrics_mutex);
        
        // ═══ SEND MESSAGES ═══
        // Phase 1: Send HEARTBEAT_INIT (with static + dynamic metrics) once after 1st collection
        if (!heartbeat_sent) {
            m.type = MSG_HEARTBEAT_INIT;
            
            message_t *pkt = (message_t *)malloc(sizeof(message_t) + sizeof(MachineMetrics));
            if (pkt) {
                pkt->mq_type=1;
               
                strcpy(pkt->type,HB_INIT_TYPE);
                pkt->size = sizeof(MachineMetrics);
                memcpy(pkt->data, &m, sizeof(MachineMetrics));

                send_msg("192.168.50.1", 9000, NULL, pkt);
                
                free(pkt);
                heartbeat_sent = 1;
                msg_count++;
                printf("[MONITORING] MSG_HEARTBEAT_INIT sent (msg #%d)\n", msg_count);
            }
        }
        // Phase 2+: Send HEARTBEAT (dynamic metrics only) regularly
        else {
            m.type = MSG_HEARTBEAT;
            
            message_t *pkt = (message_t *)malloc(sizeof(message_t) + sizeof(MachineMetrics));
            if (pkt) {
                pkt->mq_type=1;
                strcpy(pkt->type,HB_TYPE);
                pkt->size = sizeof(MachineMetrics);
                memcpy(pkt->data, &m, sizeof(MachineMetrics));
                send_msg("192.168.50.1", 9000, NULL, pkt);
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