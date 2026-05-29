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
#include<stddef.h>
#include<sys/stat.h>
#define PROG_TYPE "PROG"
#define TASK_TYPE "TASK"
#define MASTER_IP "127.0.0.1"
#define MASTER_PORT 9001

typedef struct {
    char prog_name[64];
    char task_mq[64];
} program_mapping;

#define MAX_PROGRAMS 100
program_mapping loaded_programs[MAX_PROGRAMS];
int loaded_programs_count = 0;
pthread_mutex_t mapping_mutex = PTHREAD_MUTEX_INITIALIZER;

void * execution_thread_func(void * arg){

    //parse execution context
    execution_context *  context=(execution_context *)arg;
    message_t * message=(message_t * )context->message;
    prog_t *prog = (prog_t *)message->data;

    mkdir("scratch", 0777);
    mkdir("bin", 0777);

    char prog_name[128];
    snprintf(prog_name, sizeof(prog_name), "scratch/%.60s.c", prog->prog_name);
    
    char bin_name[128];
    snprintf(bin_name, sizeof(bin_name), "bin/%.60s", prog->prog_name);

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

    pthread_mutex_lock(&mapping_mutex);
    if (loaded_programs_count < MAX_PROGRAMS) {
        strcpy(loaded_programs[loaded_programs_count].prog_name, prog->prog_name);
        strcpy(loaded_programs[loaded_programs_count].task_mq, task_mq);
        loaded_programs_count++;
    }
    pthread_mutex_unlock(&mapping_mutex);

    message_t *resp = malloc(sizeof(message_t) + strlen(task_mq) + 1);
    if (!resp) {
        perror("malloc failed");
        exit(1);
    }
    memset(resp, 0, sizeof(message_t) + strlen(task_mq) + 1);

    strcpy(resp->type, message->recv_type);
    resp->size = strlen(task_mq) + 1;

    strcpy(resp->data, task_mq);
    //reply the message wiht the queue message type
    send_msg(context->master_ip,context->port, NULL, resp);

    free(resp);



    
    //get the m_queue from the taskname
    map_entry *entry = find_by_msg_type(task_mq);

    printf("Generated this mq_id %s\n",task_mq);

    if (!entry) return NULL;
    int mq_id = entry->queue_id;

    //compile the program and generate binary
   int pid = fork();

    if (pid == 0) {

        char *args[] = {
            "gcc",
            prog_name,
            "logic.c",
            "-Wl,-e,worker_entry",
            "-o",
            bin_name,
            NULL
        };

        execvp("gcc", args);

        perror("gcc exec failed");
        exit(1);
    }

    wait(NULL);

    




    while(1){

        queued_message msg;
        ssize_t received = msgrcv(
            mq_id,
            &msg,
            sizeof(msg) - sizeof(long),
            1,
            0
        );

         message_t *message = (message_t *)&msg;
        
        recv_task_t * task=(recv_task_t *)message->data;
        printf("received function %s\n",task->function_name);
        printf("received data %p\n",task->data);
        

        /* execution phase */

    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe failed");
        continue;
    }

    int run_pid = fork();

    if (run_pid == 0) {
        close(pipefd[0]);
        char binary_path[128];

        snprintf(binary_path,
                sizeof(binary_path),
                "./bin/%s",
                prog->prog_name);

        char fd_str[16];
        snprintf(fd_str, sizeof(fd_str), "%d", pipefd[1]);

        char *arg[] = {
            binary_path,
            task->function_name,
            (char *)task->data,
            fd_str,
            NULL
        };

        execvp(binary_path, arg);

        perror("binary exec failed");
        exit(1);
    }

    close(pipefd[1]);
    char ret_buf[256];
    memset(ret_buf, 0, sizeof(ret_buf));
    read(pipefd[0], ret_buf, sizeof(ret_buf) - 1);
    close(pipefd[0]);

    int status;
    waitpid(run_pid, &status, 0);

    printf("Task executed. Result: %s\n", ret_buf);

    message_t *result_msg = malloc(sizeof(message_t) + strlen(ret_buf) + 1);
    memset(result_msg, 0, sizeof(message_t) + strlen(ret_buf) + 1);
    result_msg->mq_type = 1;
    strcpy(result_msg->type, "RESP");
    result_msg->size = strlen(ret_buf) + 1;
    strcpy(result_msg->data, ret_buf);
    
    send_msg(context->master_ip, context->port, NULL, result_msg);
    free(result_msg);

    }


}
int main() {

    printf("offsetof(message_t, data) = %zu\n", offsetof(message_t, data));
    printf("offsetof(queued_message, data) = %zu\n", offsetof(queued_message, data));
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
    static network_agent_config worker_net_cfg = {9000, "outgoing"};
    pthread_t net_thread;
    pthread_create(&net_thread, NULL, network_thread_run, &worker_net_cfg);
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

        if (memcmp("CHCK", &message->recv_type, 4) == 0) {
            char *requested_prog = (char*)message->data;
            printf("Received CHCK request for prog_name: %s\n", requested_prog);
            char found_mq[64] = "NONE";
            
            pthread_mutex_lock(&mapping_mutex);
            for (int i = 0; i < loaded_programs_count; i++) {
                if (strcmp(loaded_programs[i].prog_name, requested_prog) == 0) {
                    strcpy(found_mq, loaded_programs[i].task_mq);
                    break;
                }
            }
            pthread_mutex_unlock(&mapping_mutex);
            
            message_t *resp = malloc(sizeof(message_t) + strlen(found_mq) + 1);
            memset(resp, 0, sizeof(message_t) + strlen(found_mq) + 1);
            resp->mq_type = 1;
            strcpy(resp->type, message->recv_type);
            resp->size = strlen(found_mq) + 1;
            strcpy(resp->data, found_mq);
            
            send_msg(MASTER_IP, MASTER_PORT, NULL, resp);
            free(resp);
            continue;
        }

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