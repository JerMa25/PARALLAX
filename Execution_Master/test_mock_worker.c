#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <unistd.h>

#include "ms_queue.h"
#include "network_agent.h"

// Define the structures expected
typedef struct {
  char function_name[64];
  uint64_t data_count;
  uint8_t data[400]; // Fixed size for test
} recv_task_t;

typedef struct {
  char prog_name[64];
  char prog_code[7500];
} prog_t;

int main(int argc, char *argv[]) {
  int port = 9000;
  if (argc > 1) {
    port = atoi(argv[1]);
  }

  char q_name[64];
  snprintf(q_name, sizeof(q_name), "worker_out_%d", port);

  printf("[MockWorker %d] Starting network agent on port %d...\n", port, port);
  network_agent_config *cfg = malloc(sizeof(network_agent_config));
  cfg->port = port;
  strcpy(cfg->queue_name, q_name);

  pthread_t net_thread;
  pthread_create(&net_thread, NULL, network_thread_run, cfg);
  usleep(500000);

  // Create queue for NODES requests (Simulating Controller)
  char *nodes_q = create_mq("NODES", 0);
  map_entry *nodes_entry = find_by_msg_type(nodes_q);

  // Create queue for PROG requests
  char *prog_q = create_mq("PROG", 0);
  map_entry *prog_entry = find_by_msg_type(prog_q);

  printf("[MockWorker %d] Listening on PROG and NODES queues...\n", port);

  while (1) {
    queued_message msg;
    // Check NODES (only if we are the controller on 9000)
    if (port == 9000) {
      ssize_t rec =
          msgrcv(nodes_entry->queue_id, &msg, sizeof(msg) - sizeof(long),
                 NETWORK_AGENT_MTYPE, IPC_NOWAIT);
      if (rec > 0) {
        message_t *message = (message_t *)&msg;
        printf(
            "[MockWorker %d] Received NODES request. Sending dummy nodes...\n",
            port);

#include "../../parallax/state_message.h"
        MachineMetrics mock_metrics[2]; // Send 1 node, 1 empty terminator
        memset(&mock_metrics, 0, sizeof(mock_metrics));

        strcpy(mock_metrics[0].uuid, "mock-worker-1");
        strcpy(mock_metrics[0].ip, "127.0.0.1");
        mock_metrics[0].port = 9000;
        mock_metrics[0].cpu_usage = 5.0;
        mock_metrics[0].mem_usage = 10.0;

        message_t *resp = malloc(sizeof(message_t) + sizeof(mock_metrics));
        memset(resp, 0, sizeof(message_t) + sizeof(mock_metrics));
        resp->mq_type = 1;
        strcpy(resp->type, message->recv_type);
        resp->size = sizeof(mock_metrics);
        memcpy(resp->data, mock_metrics, sizeof(mock_metrics));

        send_msg("127.0.0.1", 9005, q_name, resp);
        free(resp);
      }
    }

    // Check PROG
    ssize_t rec = msgrcv(prog_entry->queue_id, &msg, sizeof(msg) - sizeof(long),
                         NETWORK_AGENT_MTYPE, IPC_NOWAIT);
    if (rec > 0) {
      message_t *message = (message_t *)&msg;

      if (strncmp(message->recv_type, "CHCK:", 5) == 0) {
        printf("[MockWorker %d] Received CHCK request. Replying NONE.\n", port);
        message_t *resp = malloc(sizeof(message_t) + 5);
        memset(resp, 0, sizeof(message_t) + 5);
        resp->mq_type = 1;
        strcpy(resp->type, message->recv_type + 5);
        resp->size = 5;
        strcpy(resp->data, "NONE");
        send_msg("127.0.0.1", 9005, q_name, resp);
        free(resp);
      } else if (strncmp(message->recv_type, "PROG:", 5) == 0) {
        printf("[MockWorker %d] Received PROG upload! Compiling...\n", port);
        message_t *resp = malloc(sizeof(message_t) + 64);
        memset(resp, 0, sizeof(message_t) + 64);
        resp->mq_type = 1;
        strcpy(resp->type, message->recv_type + 5);

        char t_name[64];
        snprintf(t_name, sizeof(t_name), "TASK_Q_%d", port);
        char *task_q = create_mq(t_name, 0);

        resp->size = strlen(task_q) + 1;
        strcpy(resp->data, task_q);
        send_msg("127.0.0.1", 9005, q_name, resp);
        free(resp);

        printf("[MockWorker %d] Waiting for TASK on %s...\n", port, task_q);
        map_entry *task_entry = find_by_msg_type(task_q);
        queued_message task_msg;
        ssize_t t_rec =
            msgrcv(task_entry->queue_id, &task_msg,
                   sizeof(task_msg) - sizeof(long), NETWORK_AGENT_MTYPE, 0);
        if (t_rec > 0) {
          message_t *t_m = (message_t *)&task_msg;
          printf("[MockWorker %d] Received TASK! Executing...\n", port);

          char result_str[64];
          snprintf(result_str, sizeof(result_str),
                   "Execution Success on Port %d", port);

          message_t *result = malloc(sizeof(message_t) + 64);
          memset(result, 0, sizeof(message_t) + 64);
          result->mq_type = 1;
          strcpy(result->type, t_m->recv_type);
          result->size = 64;
          strcpy(result->data, result_str);
          send_msg("127.0.0.1", 9005, q_name, result);
          free(result);
          printf("[MockWorker %d] Task complete!\n", port);
        }
      }
    }
    usleep(100000);
  }
  return 0;
}
