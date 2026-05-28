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
    NodeInfo* node = node_table_find(&g_node_table, msg->uuid);
    if (!node) {
        printf("[StateReceiver] ⚠ Nœud inconnu %s, auto-enregistrement...\n", msg->uuid);
        node = node_table_add(&g_node_table, msg->uuid, msg->ip, msg->port);
        if (!node) {
            pthread_mutex_unlock(&g_node_table.lock);
            return;
        }
    }
    
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

        // Le payload du network_agent est dans qmsg.data
        MachineMetrics *msg = (MachineMetrics *)qmsg.data;
        register_node(msg);
        printf("received a hearbeat\n");
        }
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
    pthread_create(&hearbeat_init_thread,NULL,heartbeat_init_func,(void * )hearbeat_init);
    pthread_create(&heartbeat_thread,NULL,heartbeat_func,(void * )heartbeat);
    pthread_create(&hello_thread,NULL,hello_func,(void * )hello);


    
    
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