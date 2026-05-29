#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/msg.h>
#include "network_agent.h"
#include "worker_exec.h"
#include "ms_queue.h"

void init_master_agent() {
    static network_agent_config cfg = {9001, "client_out"};
    pthread_t net_thread;
    pthread_create(&net_thread, NULL, network_thread_run, &cfg);
    
    // Give threads a moment to start
    usleep(500000); 
    printf("[Master] Network agent started on port %d.\n", cfg.port);
}

int check_program_exists(char *ip, int port, const char *prog_name, char *task_mq_name) {
    size_t total_size = sizeof(message_t) + strlen(prog_name) + 1;
    message_t *msg = malloc(total_size);
    memset(msg, 0, total_size);

    msg->mq_type = 1;
    memcpy(msg->type, "PROG", 4);
    memcpy(msg->recv_type, "CHCK", 4);
    msg->size = strlen(prog_name) + 1;
    strcpy(msg->data, prog_name);

    create_mq("CHCK", 0);
    map_entry *entry = find_by_msg_type("CHCK");
    
    send_msg(ip, port, "client_out", msg);
    printf("[Master] Sent CHCK message for %s to worker_exec.\n", prog_name);
    free(msg);

    queued_message resp_msg;
    ssize_t received = msgrcv(entry->queue_id, &resp_msg, sizeof(resp_msg) - sizeof(long), 1L, 0);
    if (received < 0) {
        perror("msgrcv CHCK");
        return -1;
    }
    
    message_t *resp = (message_t *)&resp_msg;
    strcpy(task_mq_name, resp->data);
    printf("[Master] Received CHCK response: %s\n", task_mq_name);

    if (strcmp(task_mq_name, "NONE") == 0) return 0;
    return 1;
}

int send_prog_message(char *ip, int port) {
    prog_t prog;
    memset(&prog, 0, sizeof(prog));
    strcpy(prog.prog_name, "test_output");
    strcpy(
        prog.prog_code,
        "#include <stdio.h>\n"
        "#include <stdlib.h>\n"
        "#include <string.h>\n"
        "\n"
        "typedef void *(*fn)(void *);\n"
        "\n"
        "void *my_test_function(void *arg) {\n"
        "    char *input = (char*)arg;\n"
        "    printf(\"Executing my_test_function with data: %s\\n\", input);\n"
        "    char *result = malloc(strlen(input) + 64);\n"
        "    sprintf(result, \"Processed data: [%s]\", input);\n"
        "    return result;\n"
        "}\n"
        "\n"
        "fn matcher(char *name) {\n"
        "    if (strcmp(name, \"my_test_function\") == 0) {\n"
        "        return my_test_function;\n"
        "    }\n"
        "    return NULL;\n"
        "}\n"
        "\n"
        "int main() { return 0; }\n"
    );

    size_t total_size = sizeof(message_t) + sizeof(prog.prog_name) + strlen(prog.prog_code) + 1;
    message_t *msg = malloc(total_size);
    memset(msg, 0, total_size);

    msg->mq_type = 1;
    memcpy(msg->type, "PROG", 4);
    memcpy(msg->recv_type, "RESP", 4);
    msg->size = sizeof(prog.prog_name) + strlen(prog.prog_code) + 1;
    memcpy(msg->data, &prog, msg->size);

    send_msg(ip, port, "client_out", msg);
    printf("[Master] Sent PROG message to worker_exec.\n");
    free(msg);
    return 0;
}

int receive_task_mq(char *task_mq_name) {
    create_mq("RESP", 0);
    map_entry *entry = find_by_msg_type("RESP");

    queued_message resp_msg;
    ssize_t received = msgrcv(entry->queue_id, &resp_msg, sizeof(resp_msg) - sizeof(long), 1L, 0);
    if (received < 0) {
        perror("msgrcv RESP");
        return -1;
    }
    
    message_t *resp = (message_t *)&resp_msg;
    strcpy(task_mq_name, resp->data);
    printf("[Master] Received response! Extracted task_mq msg_type: %s\n", task_mq_name);
    return 0;
}

int send_task_message(char *ip, int port, const char *task_mq_name, const char *payload) {
    size_t data_len = strlen(payload) + 1;
    size_t task_size = sizeof(recv_task_t) + data_len;
    
    recv_task_t *task = malloc(task_size);
    memset(task, 0, task_size);
    strcpy(task->function_name, "my_test_function");
    task->data_count = 1;
    strcpy((char *)task->data, payload);

    size_t total_size = sizeof(message_t) + task_size;
    message_t *msg = malloc(total_size);
    memset(msg, 0, total_size);

    msg->mq_type = 1;
    memcpy(msg->type, task_mq_name, strlen(task_mq_name) + 1);
    memcpy(msg->recv_type, "RESP", 4); // Ask worker to reply to "RESP" queue
    msg->size = task_size;
    memcpy(msg->data, task, task_size);

    send_msg(ip, port, "client_out", msg);
    printf("[Master] Sent recv_task_t using target msg_type: %s\n", task_mq_name);
    
    free(task);
    free(msg);
    return 0;
}

int receive_task_result() {
    printf("[Master] Waiting for task execution result on queue RESP...\n");
    create_mq("RESP", 0);
    map_entry *entry = find_by_msg_type("RESP");

    queued_message resp_msg;
    ssize_t received = msgrcv(entry->queue_id, &resp_msg, sizeof(resp_msg) - sizeof(long), 1L, 0);
    if (received < 0) {
        perror("msgrcv RESP");
        return -1;
    }

    message_t *resp = (message_t *)&resp_msg;
    printf("[Master] Task result: %s\n", resp->data);
    return 0;
}

int main() {
    init_master_agent();

    char *worker_ip = "127.0.0.1";
    int worker_port = 9000;

    char task_mq_name[64];
    memset(task_mq_name, 0, sizeof(task_mq_name));
    
    int exists = check_program_exists(worker_ip, worker_port, "test_output", task_mq_name);
    if (exists < 0) return 1;

    if (!exists) {
        if (send_prog_message(worker_ip, worker_port) < 0) return 1;
        memset(task_mq_name, 0, sizeof(task_mq_name));
        if (receive_task_mq(task_mq_name) < 0) return 1;
    } else {
        printf("[Master] Program already exists on worker, skipping PROG send.\n");
    }

    if (send_task_message(worker_ip, worker_port, task_mq_name, "Task1_Payload") < 0) return 1;
    if (receive_task_result() < 0) return 1;

    if (send_task_message(worker_ip, worker_port, task_mq_name, "Task2_Payload") < 0) return 1;
    if (receive_task_result() < 0) return 1;

    // Optional: wait before exiting to let sockets flush
    usleep(100000);
    return 0;
}
