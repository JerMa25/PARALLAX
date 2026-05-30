#include<stdio.h>
#include<stdlib.h>
#include <sys/msg.h>
#include"pthread.h"
#include"node_details.h"
#include"orchestrator.h"
#include"network_agent.h"
#include"ms_queue.h"
#include"parallax_team.h"
#include"master_thread.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include<unistd.h>
#define PROG_DIR "./progs"

void * prog_listener_func(void * args){
    program_message_t *prog = (program_message_t *)args;
    if (!prog) return NULL;
    
    // Ensure PROG_DIR exists
    mkdir(PROG_DIR, 0777);
    
    char filepath[256];
    snprintf(filepath, sizeof(filepath), "%s/%s", PROG_DIR, prog->program_name);
    
    FILE *f = fopen(filepath, "wb");
    if (f) {
        fwrite(prog->code, 1, prog->code_size, f);
        fclose(f);
        printf("[Master] Saved program to %s\n", filepath);
    } else {
        perror("fopen prog file");
    }
    
    char compile_cmd[512];
    char run_cmd[512];

    //parse code
    



    //run code

    snprintf(compile_cmd, sizeof(compile_cmd), "gcc %s -o %s/bin_%s", filepath, PROG_DIR, prog->program_name);
    printf("[Master] Compiling program: %s\n", compile_cmd);
    if (system(compile_cmd) == 0) {
        printf("[Master] Program compiled successfully.\n");
        snprintf(run_cmd, sizeof(run_cmd), "%s/bin_%s", PROG_DIR, prog->program_name);
        printf("[Master] Executing program: %s\n", run_cmd);
        system(run_cmd);
    } else {
        printf("[Master] Program compilation failed.\n");
    }
    free(prog);
    return NULL;

} 


void * master_thread_start(void *args){
    //first create a mq for recieving programs
    

    char * progs=create_mq("PROG", 0);

    //create thread to listen on the mq
    printf("Waiting form\n");
    map_entry * prog_mq=find_by_msg_type(progs);
    queued_message msgp;

    while(1){
        //read something from mq
        ssize_t size=msgrcv( prog_mq->queue_id ,&msgp, sizeof(msgp), 1L, 0);
        if(size<0){
            continue;
        }

        printf("Received Program \n");
        
        program_message_t * program = (program_message_t *)malloc(sizeof(program_message_t));
        if (program) {
            memcpy(program, msgp.data, sizeof(program_message_t));
            pthread_t handler_thread;
            pthread_create(&handler_thread,NULL,prog_listener_func,(void *)program);
            pthread_detach(handler_thread);
        }
    }
    return NULL;
}

int main(){
    printf("[Master] Starting network agent on port 9000...\n");
    static network_agent_config cfg = {9000, "master_out"};
    pthread_t net_thread;
    pthread_create(&net_thread, NULL, network_thread_run, &cfg);
    usleep(500000);
    
    printf("[Master] Starting prog queue listener...\n");
    master_thread_start(NULL);
    
    return 0;
}