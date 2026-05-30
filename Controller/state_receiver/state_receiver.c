#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/msg.h>
#include <pthread.h>
#include <stdatomic.h>

#include "node.h"
#include "../../parallax/state_message.h"
#include "state_receiver.h"
#include "persistence.h"
#include "ms_queue.h"
#include "network_agent.h"
#include "../../Agent_Init/init.h"



#include <stdio.h>
#include <time.h>
#include "state_message.h"

// ══════════════════════════════════════════════════════════════════════════
//  HANDLERS DE MESSAGES
// ════════════════════════════════════════════════════════════════════════════

/**
 * Enregistre un nouveau nœud dans la table globale lors de la réception d'un MSG_HELLO.
 */
static void register_node(MachineMetrics* msg) {
    if (!msg || strlen(msg->uuid) == 0) return;
    pthread_mutex_lock(&g_node_table.lock);

    if (node_table_find(&g_node_table, msg->uuid)) {
        printf("[StateReceiver] Nœud %s déjà enregistré, HELLO ignoré\n",
               msg->uuid);
        pthread_mutex_unlock(&g_node_table.lock);
        return;
    }

    NodeInfo* node = node_table_add(&g_node_table, msg->uuid, msg->ip, msg->port);
    if (node)
        printf("[StateReceiver] ✓ Nouveau nœud : uuid=%s ip=%s\n",
               node->uuid, node->ip);

    // Snapshot initial sur disque (le pointeur de noeud est copié pour minimiser la zone critique)
    if (node) {
        NodeInfo snapshot = *node;
        pthread_mutex_unlock(&g_node_table.lock);
        persist_node_snapshot(&snapshot);
    } else {
        pthread_mutex_unlock(&g_node_table.lock);
    }
}



MachineMetrics * get_all_node_metrics(){
    pthread_mutex_lock(&g_node_table.lock);
    int count = g_node_table.count;
    if (count == 0) {
        pthread_mutex_unlock(&g_node_table.lock);
        return NULL;
    }
    
    // Allocate count + 1 for sentinel
    MachineMetrics *metrics = malloc((count + 1) * sizeof(MachineMetrics));
    if (!metrics) {
        pthread_mutex_unlock(&g_node_table.lock);
        return NULL;
    }
    memset(metrics, 0, (count + 1) * sizeof(MachineMetrics)); // ensures sentinel's uuid is empty

    NodeInfo *curr = g_node_table.head;
    int i = 0;
    while (curr != NULL && i < count) {
        strncpy(metrics[i].uuid, curr->uuid, sizeof(metrics[i].uuid) - 1);
        strncpy(metrics[i].ip, curr->ip, sizeof(metrics[i].ip) - 1);
        metrics[i].port = curr->port;
        
        metrics[i].cpu_usage = curr->metrics.cpu_usage;
        metrics[i].mem_usage = curr->metrics.ram_usage;
        metrics[i].mem_used_mb = curr->metrics.ram_used_mb;
        metrics[i].disk_usage = curr->metrics.disk_usage;
        metrics[i].disk_used_mb = curr->metrics.disk_used_mb;
        metrics[i].queue_len = curr->metrics.queue_len;
        metrics[i].score = curr->metrics.score;
        metrics[i].load_avg[0] = curr->metrics.load_avg[0];
        metrics[i].load_avg[1] = curr->metrics.load_avg[1];
        metrics[i].load_avg[2] = curr->metrics.load_avg[2];
        
        metrics[i].cpu_cores = curr->hardware.cpu_cores;
        metrics[i].cpu_threads_per_core = curr->hardware.cpu_threads_per_core;
        metrics[i].cpu_freq_mhz = curr->hardware.cpu_freq_mhz;
        strncpy(metrics[i].cpu_model, curr->hardware.cpu_model, sizeof(metrics[i].cpu_model) - 1);
        metrics[i].mem_total_mb = curr->hardware.ram_total_mb;
        metrics[i].disk_total_mb = curr->hardware.disk_total_gb;
        strncpy(metrics[i].disk_mount, curr->hardware.disk_mount, sizeof(metrics[i].disk_mount) - 1);
        strncpy(metrics[i].network_iface, curr->hardware.network_iface, sizeof(metrics[i].network_iface) - 1);
        
        metrics[i].timestamp = curr->last_heartbeat;
        
        curr = curr->next;
        i++;
    }
    
    pthread_mutex_unlock(&g_node_table.lock);
    return metrics;
}


void * get_machine_xtics(void * arg){
    
    (void)arg;
    char * get_nodes_type = create_mq("NODES", sizeof(queued_message));
    map_entry * node_query_mq = find_by_msg_type("NODES");
    if (!node_query_mq) return NULL;

    queued_message qmsg;
    printf("Alive and kickin\n");
    while(1){
        ssize_t ret = msgrcv(node_query_mq->queue_id, &qmsg, sizeof(qmsg) - sizeof(long),
                             1L, IPC_NOWAIT); // NETWORK_AGENT_MTYPE is 1L
        //printf("recieved nothing in %d \n",node_query_mq->queue_id);
       
        if (ret == -1) {
            usleep(100000);
            continue;
        }

        // Use sender_ip and sender_port from the message header
        // These are set automatically by the network agent on the sender's side
        char reply_ip[16];
        strncpy(reply_ip, qmsg.sender_ip, sizeof(reply_ip) - 1);
        reply_ip[sizeof(reply_ip) - 1] = '\0';
        int reply_port = qmsg.sender_port;
        
        printf("[Controller] NODES query received from %s:%d\n", reply_ip, reply_port);
       
        MachineMetrics * metrics = get_all_node_metrics();
        int count = 0;
        if (metrics) {
            while (strlen(metrics[count].uuid) > 0) {
                count++;
            }
        }
       
        size_t payload_size = count * sizeof(MachineMetrics);
        size_t total_size = sizeof(message_t) + payload_size;
       
        message_t * resp = malloc(total_size);
        if (!resp) {
            free(metrics);
            continue;
        }
        memset(resp, 0, total_size);
        strncpy(resp->type, qmsg.recv_type, sizeof(resp->type) - 1);
        strncpy(resp->recv_type, qmsg.recv_type, sizeof(resp->recv_type) - 1);
        resp->size = payload_size;
        if (payload_size > 0) {
            memcpy(resp->data, metrics, payload_size);
        }
        free(metrics);

        send_msg(reply_ip, reply_port, "outgoing", resp);
        free(resp);
    }
    return NULL;
}



void print_machine_metrics(const MachineMetrics *m)
{
    if (m == NULL) {
        printf("MachineMetrics is NULL\n");
        return;
    }

    printf("========================================\n");
    printf("         MACHINE METRICS REPORT         \n");
    printf("========================================\n");

    printf("UUID                : %s\n", m->uuid);
    printf("IP Address          : %s\n", m->ip);
    printf("Port                : %d\n", m->port);
    printf("Message Type        : %d\n", m->type);

    printf("\n========== CPU ==========\n");
    printf("CPU Usage           : %.2f %%\n", m->cpu_usage);
    printf("Load Average        : %.2f %.2f %.2f\n",
           m->load_avg[0],
           m->load_avg[1],
           m->load_avg[2]);

    printf("CPU Cores           : %d\n", m->cpu_cores);
    printf("Threads/Core        : %d\n", m->cpu_threads_per_core);
    printf("CPU Frequency       : %.2f MHz\n", m->cpu_freq_mhz);
    printf("CPU Model           : %s\n", m->cpu_model);

    printf("\n========== MEMORY ==========\n");
    printf("Memory Usage        : %.2f %%\n", m->mem_usage);
    printf("Memory Available    : %.2f MB\n", m->mem_available_mb);
    printf("Memory Used         : %ld MB\n", m->mem_used_mb);
    printf("Memory Total        : %ld MB\n", m->mem_total_mb);

    printf("\n========== DISK ==========\n");
    printf("Disk Usage          : %.2f %%\n", m->disk_usage);
    printf("Disk Used           : %ld MB\n", m->disk_used_mb);
    printf("Disk Total          : %ld MB\n", m->disk_total_mb);
    printf("Disk Mount          : %s\n", m->disk_mount);

    printf("\n========== NETWORK ==========\n");
    printf("Bandwidth           : %.2f Mbps\n",
           m->network_bandwidth_mbps);

    printf("Active Connections  : %d\n", m->active_connections);
    printf("Network Interface   : %s\n", m->network_iface);

    printf("\n========== SYSTEM ==========\n");
    printf("Active Processes    : %d\n", m->active_processes);
    printf("Context Switch Rate : %.2f\n", m->context_switch_rate);
    printf("Uptime              : %ld seconds\n", m->uptime_seconds);

    printf("\n========== COMPUTED ==========\n");
    printf("Queue Length        : %d\n", m->queue_len);
    printf("Score               : %.2f\n", m->score);
    printf("Overloaded          : %s\n",
           m->is_overloaded ? "YES" : "NO");

    printf("\n========== TIMESTAMP ==========\n");
    printf("Timestamp           : %s", ctime(&m->timestamp));

    printf("========================================\n");
}



/**
 * Met à jour les métriques d'un nœud lors de la réception d'un MSG_HEARTBEAT.
 */
static void update_heartbeat(MachineMetrics* msg) {
    if (!msg || strlen(msg->uuid) == 0) return;
    pthread_mutex_lock(&g_node_table.lock);

    printf("\n\n\nFrom update Heartbeat");
     print_machine_metrics(msg);
   //printf("[StateReceiver] Received a Heartbeat\n");
    NodeInfo* node = node_table_find(&g_node_table, msg->uuid);
    if (!node) {
        printf("[StateReceiver] ⚠ Nœud inconnu %s, auto-enregistrement...\n", msg->uuid);
        node = node_table_add(&g_node_table, msg->uuid, msg->ip, msg->port);
        if (!node) {
            pthread_mutex_unlock(&g_node_table.lock);
            return;
        }
    }
    
    // Update node identity (IP/Port might have changed or recovered from stale snapshot)
    strncpy(node->ip, msg->ip, sizeof(node->ip) - 1);
    node->ip[sizeof(node->ip) - 1] = '\0';
    node->port = msg->port;

    // Mise à jour de l'état du noeud
    node->last_heartbeat       = time(NULL);
    node->metrics.cpu_usage    = msg->cpu_usage;
    node->metrics.ram_usage    = msg->mem_usage;
    node->metrics.ram_used_mb  = msg->mem_used_mb;
    node->metrics.disk_usage   = msg->disk_usage;
    node->metrics.disk_used_mb = msg->disk_used_mb;
    node->metrics.queue_len    = msg->queue_len;
    node->metrics.score        = msg->score;
    node->metrics.load_avg[0]  = msg->load_avg[0];
    node->metrics.load_avg[1]  = msg->load_avg[1];
    node->metrics.load_avg[2]  = msg->load_avg[2];

    // On marque simplement le nœud comme actif puisqu'on a reçu un heartbeat
    // (La détection précise de surcharge/panne sera gérée par un autre module)
    node->status = NODE_ACTIF;

    // Copie locale pour sortir de la section critique avant l'I/O asynchrone
    char        uuid_copy[64];
    time_t      ts    = node->last_heartbeat;
    NodeMetrics mcopy = node->metrics;
    strncpy(uuid_copy, node->uuid, sizeof(uuid_copy) - 1);

    pthread_mutex_unlock(&g_node_table.lock);

    // Buffer RAM — flush asynchrone par le thread flusher
    metrics_buf_push(uuid_copy, ts, &mcopy);
}

/**
 * Initialise le hardware d'un nœud lors de la réception d'un MSG_HEARTBEAT_INIT.
 */
static void init_heartbeat(MachineMetrics* msg) {
    if (!msg || strlen(msg->uuid) == 0) return;
    pthread_mutex_lock(&g_node_table.lock);


     printf("\n\n\nFrom update Heartbeat");
    print_machine_metrics(msg);

    
    NodeInfo* node = node_table_find(&g_node_table, msg->uuid);
    if (!node) {
        printf("[StateReceiver] ⚠ Nœud inconnu %s, auto-enregistrement...\n", msg->uuid);
        node = node_table_add(&g_node_table, msg->uuid, msg->ip, msg->port);
        if (!node) {
            pthread_mutex_unlock(&g_node_table.lock);
            return;
        }
    }

    // Update node identity
    strncpy(node->ip, msg->ip, sizeof(node->ip) - 1);
    node->ip[sizeof(node->ip) - 1] = '\0';
    node->port = msg->port;

    // Mise à jour des informations matérielles si pas encore initialisées
    if (!node->hardware.initialized) {
        node->hardware.cpu_cores            = msg->cpu_cores;
        node->hardware.cpu_threads_per_core = msg->cpu_threads_per_core;
        node->hardware.cpu_freq_mhz         = msg->cpu_freq_mhz;
        node->hardware.ram_total_mb         = msg->mem_total_mb;
        node->hardware.disk_total_gb        = msg->disk_total_mb;
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

    // Mise à jour classique de type heartbeat
    node->last_heartbeat    = time(NULL);
    node->metrics.cpu_usage = msg->cpu_usage;
    node->metrics.ram_usage = msg->mem_usage;
    node->metrics.queue_len = msg->queue_len;
    node->metrics.score     = msg->score;
    node->status            = NODE_ACTIF;

    // Copie locale avant de libérer le mutex
    NodeInfo snapshot = *node;
    char        uuid_copy[64];
    time_t      ts    = node->last_heartbeat;
    NodeMetrics mcopy = node->metrics;
    strncpy(uuid_copy, node->uuid, sizeof(uuid_copy) - 1);

    pthread_mutex_unlock(&g_node_table.lock);

    // Snapshot hardware sur disque (une seule fois)
    persist_node_snapshot(&snapshot);

    // Métriques initiales dans le buffer RAM
    metrics_buf_push(uuid_copy, ts, &mcopy);
}




void * heartbeat_init_func(void * arg){
        //parse entry
          printf("Entered here 4\n");
        map_entry * heartbeat_entry=(map_entry * )arg;
        queued_message qmsg;
        while(1){
            ssize_t ret = msgrcv(heartbeat_entry->queue_id, &qmsg, sizeof(qmsg) - sizeof(long),
                             1L, IPC_NOWAIT); // NETWORK_AGENT_MTYPE is 1L
        if (ret == -1) {
            usleep(100000);   // 100ms — pas de message, on repoll
            continue;
        }

        // Le payload du network_agent est dans qmsg.data
        MachineMetrics *msg = (MachineMetrics *)qmsg.data;
        init_heartbeat(msg);
        printf("received a hearbeat\n");
        }


}

void * heartbeat_func(void * arg){

      printf("Entered here 4\n");
     map_entry * heartbeat_entry=(map_entry * )arg;
        queued_message qmsg;
        while(1){
            ssize_t ret = msgrcv(heartbeat_entry->queue_id, &qmsg, sizeof(qmsg) - sizeof(long),
                             1L, IPC_NOWAIT); // NETWORK_AGENT_MTYPE is 1L
        if (ret == -1) {
            usleep(100000);   // 100ms — pas de message, on repoll
            continue;
        }

        // Le payload du network_agent est dans qmsg.data
        MachineMetrics *msg = (MachineMetrics *)qmsg.data;
        update_heartbeat(msg);
        printf("received a hearbeat\n");
        }
}


void * hello_func(void * arg){

    printf("Entered here 3\n");
    map_entry * heartbeat_entry=(map_entry * )arg;
    queued_message qmsg;
    
    while(1){
        ssize_t ret = msgrcv(heartbeat_entry->queue_id, &qmsg, sizeof(qmsg) - sizeof(long),
                         1L, IPC_NOWAIT); // NETWORK_AGENT_MTYPE is 1L
        if (ret == -1) {
            usleep(100000);   // 100ms — pas de message, on repoll
            continue;
        }

        // Ignore if this is a reply (starts with IP:) so we don't infinitely reply to a reply
        if (strncmp(qmsg.data, "IP:", 3) == 0) continue;

        // Le payload du network_agent est dans qmsg.data
        MachineMetrics *msg = (MachineMetrics *)qmsg.data;
        register_node(msg);
        printf("received a HELLO message\n");
        
        // Reply with our IP dynamically
        char iface[64] = {0};
        char my_ip[16] = {0};
        
        load_network_interface(iface, sizeof(iface));
        get_local_ip(my_ip, sizeof(my_ip), iface);
        message_t *reply = malloc(sizeof(message_t) + 64);
        strcpy(reply->type, HELLO_TYPE);
        strcpy(reply->recv_type, "");
        sprintf(reply->data, "IP:%s", my_ip);
        reply->size = strlen(reply->data) + 1;
        
        printf("Replying to HELLO with our IP: %s directly to agent at %s:%d\n", my_ip, msg->ip, msg->port);
        // Reply directly via reliable TCP unicast to the agent instead of a broadcast
        send_broadcast_message(9001, reply);
        free(reply);
    }
    return NULL;
}

// ════════════════════════════════════════════════════════════════════════════
//  BOUCLE DU THREAD RECEIVER
// ════════════════════════════════════════════════════════════════════════════

static atomic_int  receiver_running = 0;

/**
 * Fonction principale du thread de réception d'état.
 */
void* state_receiver_thread_run(void* arg) {
    (void)arg;

    printf("Started here");
    
    // Initialisation de la table globale
    node_table_init(&g_node_table);
    
    // (Note: La lecture depuis le disque au démarrage n'est pas gérée pour le moment. 
    //  On suppose le contrôleur toujours actif, et on reconstruit la RAM en temps réel 
    //  à partir des messages reçus).

    // Démarre le thread de persistence disque
    flusher_start();

    // Ouverture de la file de messages IPC via le network_agent
 



    
    char * mq_heartbeat_init_str = create_mq(HB_INIT_TYPE, sizeof(queued_message));

    
    char * mq_heartbeat_str = create_mq(HB_TYPE, sizeof(queued_message));

    

    char * mq_hello_str = create_mq(HELLO_TYPE, sizeof(queued_message));
    



    if (mq_heartbeat_init_str == NULL) {
        perror("[StateReceiver] create_mq()");
        return NULL;
    }
    
    map_entry * hearbeat_init = find_by_msg_type(mq_heartbeat_init_str);
    map_entry * heartbeat=find_by_msg_type(mq_heartbeat_str);
    map_entry * hello=find_by_msg_type(mq_hello_str);
    if (!hearbeat_init) {
        fprintf(stderr, "[StateReceiver] heartbeat init queue not created \n");
        return NULL;
    }

     if (!heartbeat) {
        fprintf(stderr, "[StateReceiver] heartbeat  queue not created \n");
        return NULL;
    }

     if (!hello) {
        fprintf(stderr, "[StateReceiver] hello queue not created \n");
        return NULL;
    }
    pthread_t hearbeat_init_thread;
    pthread_t heartbeat_thread;
    pthread_t hello_thread;
    pthread_t get_xtics_thread;
    pthread_create(&hearbeat_init_thread,NULL,heartbeat_init_func,(void * )hearbeat_init);
    pthread_create(&heartbeat_thread,NULL,heartbeat_func,(void * )heartbeat);
    pthread_create(&hello_thread,NULL,hello_func,(void * )hello);
    pthread_create(&get_xtics_thread,NULL,get_machine_xtics,NULL);


    
    
    atomic_store(&receiver_running, 1);

    while (atomic_load(&receiver_running)) {
        usleep(100000);
        
    }
    //kill threads here


    printf("[StateReceiver] Arrêté proprement\n");
    return NULL;
}

// ════════════════════════════════════════════════════════════════════════════
//  API PUBLIQUE
// ════════════════════════════════════════════════════════════════════════════

void state_receiver_stop(void) {
    atomic_store(&receiver_running, 0);
    // pthread_join is done in init.c (the caller who created this thread)
    
    flusher_stop();    // flush final + arrêt du thread de persistence
    node_table_destroy(&g_node_table);
}