/**
 * test_worker_node.c
 * Standalone harness: starts a network agent on port 9001, then
 * runs worker_exec_thread to handle PROG / TASK messages.
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "network_agent.h"
#include "worker_exec.h"

int main(void) {
    printf("[WorkerNode] Starting network agent on port 9001...\n");

    static network_agent_config cfg = {9001, "worker_out"};
    pthread_t net_thread;
    pthread_create(&net_thread, NULL, network_thread_run, &cfg);

    /* Give the network agent time to bind */
    usleep(500000);
    printf("[WorkerNode] Network agent ready. Starting worker_exec_thread...\n");

    pthread_t exec_thread;
    pthread_create(&exec_thread, NULL, worker_exec_thread, NULL);

    pthread_join(exec_thread, NULL);
    return 0;
}
