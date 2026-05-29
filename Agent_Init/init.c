#include "init.h"

#include "monitoring/Monitoring.h"
#include "network_agent.h"
#include "state_receiver.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>

// ===== PRIVATE STATE =====
static AgentState agent;
static volatile int agent_running = 1;

// ===== PRIVATE FUNCTIONS =====
static void generate_uuid(char *uuid){
    unsigned char bytes[16];

    FILE *f = fopen("/dev/urandom", "r");
    if (f) {
        fread(bytes, 1, 16, f);
        fclose(f);
    } else {
        // Fallback to time-based UUID if /dev/urandom is not available
        srand((unsigned int)time(NULL));
        for (int i = 0; i < 16; i++)
            bytes[i] = rand() % 256;
    }

    // obligatory bits for UUID v4
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3F) | 0x80; // variant RFC 4122

    // String format: 8-4-4-4-12
    sprintf(
        uuid, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], 
        bytes[13], bytes[14], bytes[15]
    );

}

static void load_or_create_uuid(void) {
    FILE *f = fopen(UUID_FILE, "r");
    if (f) {
        fscanf(f, "%36s", agent.uuid);
        fclose(f);
        printf("[INIT] UUID loaded: %s\n", agent.uuid);
    } else {
        generate_uuid(agent.uuid);
        f = fopen(UUID_FILE, "w");
        if (f) {
            fprintf(f, "%s", agent.uuid);
            fclose(f);
        }
        printf("[INIT] UUID generated: %s\n", agent.uuid);
    }
}

// Send HELLO message to Controller to announce presence
static void send_hello(void) {
    MachineMetrics msg;
    memset(&msg, 0, sizeof(MachineMetrics));
    
    strncpy(msg.uuid, agent.uuid, sizeof(msg.uuid) - 1);
    msg.type = MSG_HELLO;
    
    message_t *pkt = (message_t *)malloc(sizeof(message_t) + sizeof(MachineMetrics));
    if (pkt) {
       
        strcpy(pkt->type,HELLO_TYPE);
        pkt->size = sizeof(MachineMetrics);
        memcpy(pkt->data, &msg, sizeof(MachineMetrics));
        send_msg("192.168.50.1", 9000, NULL, pkt);
        free(pkt);
        printf("[INIT] HELLO sent: uuid=%s\n", agent.uuid);
    }
}

static AgentRole read_role(void) {
    FILE *f = fopen(CONF_FILE, "r");
    if (!f) {
        printf("[INIT] Config file not found, defaulting to ROLE_UNKNOWN\n");
        return ROLE_UNKNOWN;
    }

    char role_str[32];
    fscanf(f, "role=%31s", role_str);
    fclose(f);

    if (strcmp(role_str, "Worker") == 0) return ROLE_WORKER;
    if (strcmp(role_str, "Controller") == 0) return ROLE_CONTROLLER;
    if (strcmp(role_str, "Master") == 0) return ROLE_MASTER;
    
    printf("[INIT] Unknown role '%s', defaulting to ROLE_UNKNOWN\n", role_str);
    return ROLE_UNKNOWN;
}

static void start_threads(void){
    // ===== COMMON THREADS =====
    if (!agent.threads.monitoring_active && agent.role!=ROLE_CONTROLLER) {
        pthread_create(&agent.threads.monitoring, NULL, monitoring_thread_run, NULL);
        agent.threads.monitoring_active = 1;
        printf("[THREAD] Monitoring thread started\n");
    }

    if (!agent.threads.network_active) {
        static network_agent_config agent_net_cfg = {9000, "outgoing"};
        pthread_create(&agent.threads.network, NULL, network_thread_run, &agent_net_cfg);
        agent.threads.network_active = 1;
        printf("[THREAD] Network thread started\n");
    }

    // ===== ROLE-SPECIFIC THREADS =====
    switch (agent.role) {
    case ROLE_CONTROLLER:
        if (!agent.threads.state_receiver_active) {
            pthread_create(&agent.threads.state_receiver, NULL, state_receiver_thread_run, NULL);
            agent.threads.state_receiver_active = 1;
            printf("[THREAD] State Receiver thread started\n");
        }
        break;
    
    default:
        printf("[THREAD] No role-specific threads to start for ROLE_UNKNOWN\n");
        break;
    }
}

static void stop_threads(void) {
    switch (agent.role) {  
        case ROLE_CONTROLLER:
            if (agent.threads.state_receiver_active) {
                state_receiver_stop();
                pthread_join(agent.threads.state_receiver, NULL);
                agent.threads.state_receiver_active = 0;
                printf("[THREAD] State Receiver thread stopped\n");
            }
            break;
        default:
            printf("[THREAD] No role-specific threads to stop for ROLE_UNKNOWN\n");
            break;           
    }

    // Stop common threads
    if (agent.threads.monitoring_active) {
        monitoring_stop();
        pthread_join(agent.threads.monitoring, NULL);
        agent.threads.monitoring_active = 0;
        printf("[THREAD] Monitoring thread stopped\n");
    }

    if (agent.threads.network_active) {
        network_stop();
        pthread_join(agent.threads.network, NULL);
        agent.threads.network_active = 0;
        printf("[THREAD] Network thread stopped\n");
    }
}

static void watch_config(void) {
    struct stat st;
    time_t last_mod_time = 0;

    while (agent_running) {
        if (stat(CONF_FILE, &st) == 0) {
            if (st.st_mtime != last_mod_time) {
                printf("[CONFIG] Detected config change, reloading...\n");
                last_mod_time = st.st_mtime;

                AgentRole new_role = read_role();
                if (new_role != agent.role) {
                    printf("[CONFIG] Role changed from %d to %d\n", agent.role, new_role);
                    stop_threads();
                    agent.role = new_role;
                    start_threads();
                }
            }
        } else {
            printf("[CONFIG] Config file not found during watch: %s\n", CONF_FILE);
        }
        sleep(CONFIG_CHECK_INTERVAL);
    }
}

// ===== PUBLIC FUNCTIONS =====
void initialize_agent(void){
    // Initialize agent state
    memset(&agent, 0, sizeof(AgentState));

    // 1. Load or create UUID
    load_or_create_uuid();

    // 2. Read initial role from config
    agent.role = read_role();
    printf("[INIT] Initial role: %d\n", agent.role);

    // 3. Start threads based on role
    start_threads();
    
    // 4. Send HELLO after network thread is started (queue exists)
    // Give network thread time tconnection o initialize
    sleep(1);
    if (agent.role != ROLE_CONTROLLER) {
        send_hello();
    }

    // 5. Start config watcher in main thread (blocking)
    watch_config();
}

char* get_agent_uuid(void) {
    return agent.uuid;
}

void stop_agent(void){
    agent_running = 0; // Signal threads to stop
    stop_threads(); // Stop all threads gracefully
}