#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <unistd.h>

#include "ms_queue.h"
#include "network_agent.h"

#include "../../parallax/state_message.h"

int main(int argc, char *argv[]) {
    int port = 9000;
    if (argc > 1) {
        port = atoi(argv[1]);
    }

    char q_name[64];
    snprintf(q_name, sizeof(q_name), "worker_out_%d", port);

    printf("[MockController %d] Starting network agent on port %d...\n", port, port);
    network_agent_config *cfg = malloc(sizeof(network_agent_config));
    cfg->port = port;
    strcpy(cfg->queue_name, q_name);

    pthread_t net_thread;
    pthread_create(&net_thread, NULL, network_thread_run, cfg);
    usleep(500000);

    /* Create queue for NODES requests */
    char *nodes_q = create_mq("NODES", 0);
    map_entry *nodes_entry = find_by_msg_type(nodes_q);

    printf("[MockController %d] Listening for NODES requests. "
           "Worker is at 127.0.0.1:9001...\n", port);

    while (1) {
        queued_message msg;
        ssize_t rec = msgrcv(nodes_entry->queue_id, &msg, sizeof(msg) - sizeof(long),
                             NETWORK_AGENT_MTYPE, IPC_NOWAIT);
        if (rec > 0) {
            message_t *message = (message_t *)&msg;
            printf("[MockController %d] Received NODES request. Sending node info...\n", port);

            /* Advertise one worker: real worker_exec listening on port 9001 */
            MachineMetrics mock_metrics[2];
            memset(&mock_metrics, 0, sizeof(mock_metrics));

            strcpy(mock_metrics[0].uuid, "real-worker-1");
            strcpy(mock_metrics[0].ip,   "127.0.0.1");
            mock_metrics[0].port      = 9001;   /* real worker_exec port */
            mock_metrics[0].cpu_usage = 5.0f;
            mock_metrics[0].mem_usage = 10.0f;
            /* mock_metrics[1] is zeroed → acts as sentinel terminator */

            message_t *resp = malloc(sizeof(message_t) + sizeof(mock_metrics));
            memset(resp, 0, sizeof(message_t) + sizeof(mock_metrics));
            resp->mq_type = 1;
            strcpy(resp->type, message->recv_type);
            resp->size = sizeof(mock_metrics);
            memcpy(resp->data, mock_metrics, sizeof(mock_metrics));

            send_msg("127.0.0.1", 9005, q_name, resp);
            free(resp);
        }
        usleep(100000);
    }
    return 0;
}
