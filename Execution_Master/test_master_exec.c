#include "ms_queue.h"
#include "network_agent.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Defined in master_exec.c
extern void execute_fxn(void *data, size_t total_size, char *fxn_name,
                        int node_count);

// Global for master_exec.c to connect to Controller
char controller_ip[16] = "127.0.0.1";

int main() {
  printf("[TestExec] Starting network agent on port 9005...\n");
  static network_agent_config cfg = {9005, "master_out"};
  pthread_t net_thread;
  pthread_create(&net_thread, NULL, network_thread_run, &cfg);
  usleep(500000); // give it time to start

  // Create the message queue for receiving NODES responses from controller
  create_mq("NODES", 0);

  // Create sample dataset (array of integers)
  int payload[100];
  for (int i = 0; i < 100; i++) {
      payload[i] = i + 1; // Sum should be 5050
  }

  int expected_node_count = 1; // Testing 1 node

  printf("\n[TestExec] Calling execute_fxn to request %d node(s) and "
         "distribute task...\n",
         expected_node_count);
  printf("[TestExec] ⏳ WAITING for Controller at %s:9000 to respond to NODES "
         "query...\n",
         controller_ip);

  // This will block waiting for a reply from the controller!
  // Make sure the controller is running and listening on 127.0.0.1:9000
  execute_fxn(payload, sizeof(payload), "sum_array", expected_node_count);

  printf("\n[TestExec] execute_fxn completed successfully!\n");

  return 0;
}
