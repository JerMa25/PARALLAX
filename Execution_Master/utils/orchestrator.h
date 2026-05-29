#ifndef ORCHESTRATOR_H
#define ORCHESTRATOR_H

#include"task.h"
#include "node_details.h"

/*
 * Describes the computation to execute.
 * Contains input data and the function identifier.
 */

typedef struct {

    char function_name[64];

} task_descriptor;


/*
 * Represents a scheduled execution unit.
 * Maps a task to the node selected to execute it.
 */
typedef struct {
    worker_node *target_node;
    task_descriptor *task;
    chunk_data * chunk;

} task_assignment;


task_assignment *
create_assignments(
    void *data,
    size_t total_size,
    const char *function,
    NodeInfo *nodes,
    int node_count
);


NodeInfo * get_node_details();


#endif