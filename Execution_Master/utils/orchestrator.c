#include "node_details.h"
#include <stdlib.h>
#include <string.h>
#include<stdio.h>

#include "orchestrator.h"
#include "node_details.h"
#define MOCK_NODE_COUNT 4

NodeInfo *get_node_details() {

    NodeInfo *nodes = malloc(sizeof(NodeInfo) * MOCK_NODE_COUNT);

    if (nodes == NULL) {
        return NULL;
    }

    for (int i = 0; i < MOCK_NODE_COUNT; i++) {

        memset(&nodes[i], 0, sizeof(NodeInfo));

        // basic identity
        snprintf(nodes[i].uuid, sizeof(nodes[i].uuid),
                 "mock-node-%d", i);

        strcpy(nodes[i].ip, "127.0.0.1");

        nodes[i].port = 8000 + i;

        nodes[i].status = NODE_ACTIF;

        // hardware
        nodes[i].hardware.cpu_cores = 8;
        nodes[i].hardware.cpu_threads_per_core = 2;
        nodes[i].hardware.cpu_freq_mhz = 3200.0;

        strcpy(nodes[i].hardware.cpu_model,
               "Mock CPU");

        nodes[i].hardware.ram_total_mb = 16000;

        nodes[i].hardware.disk_total_gb = 512;

        strcpy(nodes[i].hardware.disk_mount, "/");

        strcpy(nodes[i].hardware.network_iface, "lo");

        nodes[i].hardware.initialized = 1;

        // metrics
        nodes[i].metrics.cpu_usage = 0.10f * i;
        nodes[i].metrics.ram_usage = 0.20f;
        nodes[i].metrics.ram_used_mb = 3200;

        nodes[i].metrics.disk_usage = 0.30f;
        nodes[i].metrics.disk_used_gb = 120;

        nodes[i].metrics.queue_len = i;

        nodes[i].metrics.score = 1.0f - nodes[i].metrics.cpu_usage;

        nodes[i].metrics.load_avg = 0.5f;
    }

    return nodes;
}






/*
 * Creates one task_assignment per node.
 *
 * data         -> pointer to full dataset
 * total_size   -> total bytes in dataset
 * function     -> function name to execute
 * nodes        -> array of nodes
 * node_count   -> number of nodes
 *
 * returns dynamically allocated task_assignment array
 */
task_assignment *
create_assignments(
    void *data,
    size_t total_size,
    const char *function,
    NodeInfo *nodes,
    int node_count
) {

    if (!data || !nodes || node_count <= 0) {
        return NULL;
    }

    task_assignment *assignments =
        malloc(sizeof(task_assignment) * node_count);

    if (!assignments) {
        return NULL;
    }

    /*
     * compute node weights
     */
    float *weights =
        malloc(sizeof(float) * node_count);

    if (!weights) {
        free(assignments);
        return NULL;
    }

    float total_weight = 0.0f;

    for (int i = 0; i < node_count; i++) {

        float available_cpu =
            1.0f - nodes[i].metrics.cpu_usage;

        float available_ram =
            1.0f - nodes[i].metrics.ram_usage;

        /*
         * weighted average
         * 60% CPU
         * 40% RAM
         */
        weights[i] =
            (available_cpu * 0.6f) +
            (available_ram * 0.4f);

        /*
         * avoid zero-weight nodes
         */
        if (weights[i] < 0.01f) {
            weights[i] = 0.01f;
        }

        total_weight += weights[i];
    }

    /*
     * IMPORTANT:
     * split by ELEMENTS not bytes
     */
    int *base = (int *)data;

    size_t total_elements =
        total_size / sizeof(int);

    size_t offset = 0;

    for (int i = 0; i < node_count; i++) {

        float ratio =
            weights[i] / total_weight;

        /*
         * number of int elements
         */
        size_t portion =
            (size_t)(total_elements * ratio);

        /*
         * last node gets remaining elements
         */
        if (i == node_count - 1) {

            portion =
                total_elements - offset;
        }

        /*
         * create chunk descriptor
         */
        chunk_data *chunk =
            malloc(sizeof(chunk_data));

        if (!chunk) {
            continue;
        }

        chunk->chunk =
            &base[offset];

        /*
         * store chunk size in bytes
         */
        chunk->chunk_size =
            portion * sizeof(int);

        /*
         * create task descriptor
         */
        task_descriptor *task =
            malloc(sizeof(task_descriptor));

        if (!task) {
            free(chunk);
            continue;
        }

        

        strncpy(task->function_name,
                function,
                sizeof(task->function_name) - 1);

        task->function_name[
            sizeof(task->function_name) - 1
        ] = '\0';

        assignments[i].task = task;
        assignments[i].chunk=chunk;

        worker_node *wn = malloc(sizeof(worker_node));

        snprintf(wn->uuid, sizeof(wn->uuid),
                "%s", nodes[i].uuid);

        snprintf(wn->ip, sizeof(wn->ip),
                "%s", nodes[i].ip);

        wn->port = nodes[i].port;

        assignments[i].target_node = wn;


        offset += portion;
    }

    free(weights);

    return assignments;
}
