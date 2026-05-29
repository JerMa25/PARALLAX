
#include"network_agent.h"
#include<string.h>
#include<stdio.h>
#include"ms_queue.h"
#include<sys/msg.h>
#include"state_message.h"
#include"orchestrator.h"
#include"parallax_team.h"
extern char controller_ip[16];

void execute_fxn(void * data ,size_t total_size , char * fxn_name,int node_count){
    //first get worker xtics from controller
    // Allocate message with room for data payload
    message_t *message = malloc(sizeof(message_t) + 64);
    memset(message, 0, sizeof(message_t) + 64);
    message->mq_type = 1;
    strcpy(message->type, "NODES");
    strcpy(message->recv_type, "NODES"); // Tell the controller what type to label the response
    // Tell controller to reply to our network agent on port 9005
    snprintf(message->data, 64, "127.0.0.1:9005");
    message->size = strlen(message->data) + 1;
    
    send_msg(controller_ip, 9000, "master_out", message);
    free(message);

    //read nodes data that was sent to the NODES mq

    map_entry * node_mq=find_by_msg_type("NODES");
    queued_message received_msg;

    // Clear stale messages in the queue from previous runs
    while (msgrcv(node_mq->queue_id, &received_msg, sizeof(queued_message) - sizeof(long), 1L, IPC_NOWAIT) >= 0);

    while(1){

        ssize_t size = msgrcv(node_mq->queue_id, &received_msg, sizeof(queued_message) - sizeof(long), 1L, 0);
        if(size < 0){
            continue;
        }
        break;




        
    }

    MachineMetrics * metrics = (MachineMetrics *)received_msg.data;

    // Count how many valid nodes were actually returned
    int actual_node_count = 0;
    while (strlen(metrics[actual_node_count].uuid) > 0) {
        actual_node_count++;
    }

    if (actual_node_count == 0) {
        printf("[MasterExec] Error: 0 nodes connected to Controller! Aborting task.\n");
        return;
    }

    // Use the actual number of nodes instead of the hardcoded request
    node_count = (actual_node_count < node_count) ? actual_node_count : node_count;

    //create task assignments
    task_assignment * assignments = create_assignments(
                                        data,
                                        total_size,
                                        fxn_name,
                                        metrics,
                                        node_count
                                    );

    // display assignments
    for (int i = 0; i < node_count; i++) {
        chunk_data *chunk = (chunk_data *)assignments[i].chunk;
        int *chunk_values = (int *)chunk->chunk;
        size_t int_count = chunk->chunk_size / sizeof(int);

        printf("\n=================================\n");
        printf("NODE %d\n", i);
        printf("=================================\n");
        printf("uuid          : %s\n", metrics[i].uuid);
        printf("ip            : %s\n", metrics[i].ip);
        printf("cpu usage     : %.2f\n", metrics[i].cpu_usage);
        printf("ram usage     : %.2f\n", metrics[i].mem_usage);
        printf("function      : %s\n", assignments[i].task->function_name);
        printf("nb chunk bytes: %d\n", chunk->chunk_size);
        printf("chunk ints    : %zu\n", int_count);
        printf("data preview  : ");
        for (size_t j = 0; j < int_count && j < 5; j++) {
            printf("%d ", chunk_values[j]);
        }
        printf("\n");
    }

    team *t = create_and_assign_task(assignments, node_count);

    team_start(t);

    team_wait(t);
    //first join results

    //logic to aggregate results

    
    

    team_destroy(t);


    


    

    

}

