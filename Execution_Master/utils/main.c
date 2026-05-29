#include <stdio.h>
#include <stdlib.h>

#include "node_details.h"
#include "orchestrator.h"
#include"parallax_team.h"
int main() {

    int node_count = 4;

    /*
     * get mock nodes
     */
    NodeInfo *nodes = get_node_details(&node_count);

    if (!nodes) {
        printf("failed to get nodes\n");
        return 1;
    }

    /*
     * sample dataset
     */
    
    int values[100];

    for (int i = 0; i < 100; i++) {
        values[i] = i;
    }

    /*
     * create assignments
     */
    task_assignment *assignments =
        create_assignments(
            values,
            sizeof(values),
            "process_array",
            nodes,
            node_count
        );

    if (!assignments) {
        printf("failed to create assignments\n");

        free(nodes);

        return 1;
    }

    /*
     * display assignments
     */
    for (int i = 0; i < node_count; i++) {

        chunk_data *chunk =
            (chunk_data *)assignments[i].chunk;

        int *chunk_values = (int *)chunk->chunk;

        size_t int_count =
            chunk->chunk_size / sizeof(int);

        printf("\n");
        printf("=================================\n");
        printf("NODE %d\n", i);
        printf("=================================\n");

        printf("uuid          : %s\n",
               nodes[i].uuid);

        printf("ip            : %s\n",
               nodes[i].ip);

        printf("cpu usage     : %.2f\n",
               nodes[i].metrics.cpu_usage);

        printf("ram usage     : %.2f\n",
               nodes[i].metrics.ram_usage);

        printf("function      : %s\n",
               assignments[i].task->function_name);

        printf(" nb of chunk bytes   : %d\n",
               chunk->chunk_size);

        printf("chunk ints    : %zu\n",
               int_count);

        printf("data preview  : ");

        for (size_t j = 0;
             j < int_count && j < 5;
             j++) {

            printf("%d ",
                   chunk_values[j]);
        }

        printf("\n");
    }




    team *t = create_and_assign_task(assignments, node_count);

    team_start(t);
    team_wait(t);
    team_destroy(t);


    //create team from 
//werwe
    /*
     * cleanup
     */


    for (int i = 0; i < node_count; i++) {

        free(assignments[i].chunk);

        free(assignments[i].task);
    }

    free(assignments);

    free(nodes);

    return 0;
}