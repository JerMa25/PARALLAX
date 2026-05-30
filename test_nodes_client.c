#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/msg.h>

#include "network_agent.h"
#include "ms_queue.h"
#include "state_message.h"

void init_test_agent() {
    static network_agent_config cfg = {9002, "client_out"};
    pthread_t net_thread;
    pthread_create(&net_thread, NULL, network_thread_run, &cfg);
    
    // Give thread a moment to start
    usleep(500000); 
    printf("[TestClient] Network agent started on port %d.\n", cfg.port);
}

int query_nodes(char *ip, int port) {
    char client_info[32];
    sprintf(client_info, "127.0.0.1:9002");
    size_t total_size = sizeof(message_t) + strlen(client_info) + 1;
    message_t *msg = malloc(total_size);
    memset(msg, 0, total_size);

    msg->mq_type = 1;
    strcpy(msg->type, "NODES");
    strcpy(msg->recv_type, "REPLY_NODES");
    msg->size = strlen(client_info) + 1;
    strcpy(msg->data, client_info);

    create_mq("REPLY_NODES", 0);
    map_entry *entry = find_by_msg_type("REPLY_NODES");
    if (!entry) {
        printf("[TestClient] Failed to create or find REPLY_NODES queue.\n");
        free(msg);
        return -1;
    }
    
    send_msg(ip, port, "client_out", msg);
    printf("[TestClient] Sent NODES request to %s:%d.\n", ip, port);
    free(msg);

    queued_message resp_msg;
    printf("[TestClient] Waiting for response on REPLY_NODES...\n");
    ssize_t received = msgrcv(entry->queue_id, &resp_msg, sizeof(resp_msg) - sizeof(long), 1L, 0);
    if (received < 0) {
        perror("msgrcv REPLY_NODES");
        return -1;
    }
    
    message_t *resp = (message_t *)&resp_msg;
    
    if (resp->size == 0) {
        printf("[TestClient] Received empty nodes list.\n");
        return 0;
    }

    size_t num_metrics = resp->size / sizeof(MachineMetrics);
    MachineMetrics *metrics = (MachineMetrics *)resp->data;

    printf("[TestClient] Received %zu node(s).\n", num_metrics);
    for (size_t i = 0; i < num_metrics; i++) {
        printf("----------------------------------------\n");
        printf("Node %zu:\n", i + 1);
        printf("  UUID: %s\n", metrics[i].uuid);
        printf("  IP: %s:%d\n", metrics[i].ip, metrics[i].port);
        printf("  CPU Usage: %.2f%%\n", metrics[i].cpu_usage);
        printf("  Mem Usage: %.2f%% (%ld MB / %ld MB)\n", 
            metrics[i].mem_usage, metrics[i].mem_used_mb, metrics[i].mem_total_mb);
        printf("  Disk Usage: %.2f%% (%ld MB / %ld MB)\n", 
            metrics[i].disk_usage, metrics[i].disk_used_mb, metrics[i].disk_total_mb);
    }
    printf("----------------------------------------\n");

    return 0;
}

int main() {
    init_test_agent();

    char *target_ip = "127.0.0.1";
    int target_port = 9000; // Target the network agent port

    if (query_nodes(target_ip, target_port) < 0) {
        return 1;
    }

    usleep(100000);
    return 0;
}
