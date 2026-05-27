#include"worker_exec.h"
#include<sys/msg.h>
#include"ms_queue.h"
#include<stdio.h>
#include<pthread.h>
#include<string.h>
#include<stdlib.h>
#include<unistd.h>
#include<fcntl.h>
#include<sys/wait.h>
#define PROG_TYPE "PROG"
#define TASK_TYPE "TASK"
#define MASTER_IP "127.0.0.1"
#define MASTER_PORT 9001
void * execution_thread_func(void * arg){

    //parse execution context
    execution_context *  context=(execution_context *)arg;
    message_t * message=(message_t * )context->message;
    prog_t *prog = (prog_t *)message->data;

    char prog_name[64];
    snprintf(prog_name, sizeof(prog_name), "%.60s.c", prog->prog_name);

    //save code into file

    FILE *fp = fopen(prog_name, "w");
    if (fp == NULL) {
        perror("Error opening file");
        exit(1);
    }

    fwrite(prog->prog_code, 1, strlen(prog->prog_code), fp);
    fclose(fp);

    //create queue to get responses
    char *task_mq = create_mq(NULL, 0);

    message_t *resp = malloc(sizeof(message_t) + strlen(task_mq) + 1);
    if (!resp) {
        perror("malloc failed");
        exit(1);
    }
    memset(resp, 0, sizeof(message_t) + strlen(task_mq) + 1);

    resp->type = message->recv_type;
    resp->size = strlen(task_mq) + 1;

    strcpy(resp->data, task_mq);
    //reply the message wiht the queue message type
    send_msg(context->master_ip,context->port,resp);

    free(resp);



    
    //get the m_queue from the taskname
    map_entry *entry = find_by_msg_type(task_mq);

    printf("Generated this mq_id %s\n",task_mq);

    if (!entry) return NULL;
    int mq_id = entry->queue_id;

    //compile the program and generate binary
    int pid=fork();
     if (pid == 0) {

        

       char *args[] = {
            "gcc",
            prog_name,
            "logic.c",
            "-Wl,-e,worker_entry",
            "-o",
            prog->prog_name,
            NULL
        };

        execvp("gcc", args);
        perror("execvp failed");
    } else {
        wait(NULL);
    }





    while(1){

        queued_message msg;
        ssize_t received = msgrcv(
            mq_id,
            &msg,
            sizeof(msg) - sizeof(long),
            NETWORK_AGENT_MTYPE,
            0
        );

         message_t *message = (message_t *)&msg;
        
        recv_task_t * task=(recv_task_t *)message->data;
        printf("received function %s\n",task->function_name);
        printf("received data %p\n",task->data);
        

    }


}
int main() {

    char *code_mq = create_mq("PROG", 0);

    if (code_mq == NULL) {
        perror("Errror creating message queue for programs");
        return 1;
    }
    printf("Created PROG mq\n");
    queued_message msg;
    map_entry *entry = find_by_msg_type("PROG");
    if (!entry) {
        fprintf(stderr, "Could not find PROG queue\n");
        return 1;
    }
    int mq_id = entry->queue_id;

    //start network thread
     // Start network agent
   pthread_t net_thread;
   pthread_create(&net_thread,NULL,network_start,NULL);
    printf("Network agent started. Waiting for messages on both queues...\n");

    // Wait a brief moment to ensure the agent's background sender thread is fully up
    usleep(500000); // 0.5s

    while (1) {

        ssize_t received = msgrcv(
            mq_id,
            &msg,
            sizeof(msg) - sizeof(long),
            NETWORK_AGENT_MTYPE,
            0
        );

        if (received <= 0) {
            perror("msgrcv failed");
            continue;
        }

        message_t *message = (message_t *)&msg;
        printf("Received message of type %s\n", (char *)&message->type);

        if (memcmp(PROG_TYPE, &message->type, 4) == 0) {

                printf("Entered here \n");

           

            //start worker thread to listen to the master

            execution_context * context=(execution_context * )malloc(sizeof(execution_context));
            context->master_ip=MASTER_IP;
            context->port=MASTER_PORT;
            context->message=message;
            pthread_t worker_thread;
            pthread_create(&worker_thread,NULL,execution_thread_func,(void * )context);
        }

       
    }
}