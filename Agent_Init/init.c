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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/msg.h>
#include "ms_queue.h"

// Global controller IP exposed to other files (e.g. Monitoring.c)
char controller_ip[16] = "127.0.0.1";

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

void get_local_ip(MachineMetrics *m,char * iface_name) {
    char command[128];
    snprintf(command, sizeof(command), "ip addr show %s", iface_name);

    FILE *fp = popen(command, "r");
    if (!fp) {
        strncpy(m->ip, "0.0.0.0", sizeof(m->ip) - 1);
        return;
    }

    char line[512];

    while (fgets(line, sizeof(line), fp)) {
        // Cherche "inet "
        char *inet_pos = strstr(line, "inet ");
        if (inet_pos) {
            inet_pos += 5; // avancer après "inet "

            // Copier jusqu'au '/'
            char *slash = strchr(inet_pos, '/');
            if (slash) {
                size_t len = slash - inet_pos;
                if (len < sizeof(m->ip)) {
                    strncpy(m->ip, inet_pos, len);
                    m->ip[len] = '\0';
                }
                break;
            }
        }
    }

    pclose(fp);

    // fallback si rien trouvé
    if (strlen(m->ip) == 0) {
        strncpy(m->ip, "0.0.0.0", sizeof(m->ip) - 1);
    }
}

static void load_or_create_uuid(void) {
    FILE *f = fopen(UUID_FILE, "r");
    int uuid_loaded = 0;
    
    if (f) {
        char temp_uuid[UUID_LENGTH];
        int read = fscanf(f, "%36s", temp_uuid);
        fclose(f);
        
        // Check if UUID was actually read and not empty
        if (read == 1 && strlen(temp_uuid) > 0) {
            strncpy(agent.uuid, temp_uuid, UUID_LENGTH - 1);
            agent.uuid[UUID_LENGTH - 1] = '\0';
            printf("[INIT] UUID loaded from file: %s\n", agent.uuid);
            uuid_loaded = 1;
        }
    }
    
    // If not loaded, generate new UUID
    if (!uuid_loaded) {
        generate_uuid(agent.uuid);
        printf("[INIT] UUID generated (new): %s\n", agent.uuid);
        
        // Save to file
        f = fopen(UUID_FILE, "w");
        if (f) {
            fprintf(f, "%s", agent.uuid);
            fclose(f);
            printf("[INIT] UUID saved to file\n");
        }
    }
}

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

// Send HELLO message to Controller to announce presence
static void send_hello(void) {
    // Get latest metrics to include in HELLO message
    MachineMetrics msg = monitoring_get_latest();
    
    // Ensure UUID is set
    strncpy(msg.uuid, agent.uuid, sizeof(msg.uuid) - 1);
    msg.type = MSG_HELLO;
    msg.timestamp = time(NULL);
    
    // Set IP and port (hardcoded for now, can be made dynamic later)
    get_local_ip(&msg, "wlo1"); // Assuming eth0 is the primary interface
    msg.port = 9000;  // Default worker listening port
    
    // DEBUG: Afficher les données avant envoi
    debug_print_sent_metrics(&msg, "MSG_HELLO");
    
   

    printf("Waiting for controller's IP reply on port 9001 via message queue...\n");
    map_entry *mq = find_by_msg_type(HELLO_TYPE);
    if (!mq) {
        if (create_mq(HELLO_TYPE, NETWORK_AGENT_MAX_DATA) != NULL) {
            mq = find_by_msg_type(HELLO_TYPE);
        }
    }
    
    if (!mq) {
        printf("[INIT] Failed to find or create HELLO_TYPE queue!\n");
        return;
    }

    int response_received = 0;
    queued_message item;
    
    while (!response_received) {
        message_t *pkt = (message_t *)malloc(sizeof(message_t) + sizeof(MachineMetrics));
        if (pkt) {
            strcpy(pkt->type, HELLO_TYPE);
            pkt->size = sizeof(MachineMetrics);
            memcpy(pkt->data, &msg, sizeof(MachineMetrics));
            
            send_broadcast(9001, pkt);
            free(pkt);
            printf("[INIT] HELLO sent: uuid=%s\n", agent.uuid);
        }

        // Wait up to 5 seconds for a reply
        for (int i = 0; i < 50; i++) {
            ssize_t received = msgrcv(mq->queue_id, &item, sizeof(item) - sizeof(long), NETWORK_AGENT_MTYPE, IPC_NOWAIT);
            if (received > 0) {
                if (strncmp(item.data, "IP:", 3) == 0) {
                    printf("\n--- Controller IP Received ---\n");
                    printf("Message Type: %s\n", item.type);
                    printf("Controller IP: %s\n", item.data + 3);
                    strncpy(controller_ip, item.data + 3, 15);
                    controller_ip[15] = '\0';
                    response_received = 1;
                    break;
                }
            }
            usleep(100000); // 100ms
        }
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

    // 3. Start Network thread first
    if (!agent.threads.network_active) {
        static network_agent_config agent_net_cfg = {9000, "outgoing"};
        pthread_create(&agent.threads.network, NULL, network_thread_run, &agent_net_cfg);
        agent.threads.network_active = 1;
        printf("[THREAD] Network thread started\n");
    }
    
    // 4. Send HELLO after network thread is started (queue exists)
    // Give network thread time to initialize
    sleep(1);
    if (agent.role != ROLE_CONTROLLER) {
        send_hello();
    }

    // 3. Start threads based on role
    start_threads();

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