#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "network_agent.h"
#include "worker_exec.h"

#define MASTER_PORT 9001
#define WORKER_PORT 9000
#define WORKER_IP "127.0.0.1"

int setup_master_server() {
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(MASTER_PORT);
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(server_sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind on master port");
        return -1;
    }
    listen(server_sock, 1);
    printf("[Master] Listening on port %d for responses...\n", MASTER_PORT);
    return server_sock;
}

int connect_to_worker() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in worker_addr;
    memset(&worker_addr, 0, sizeof(worker_addr));
    worker_addr.sin_family = AF_INET;
    worker_addr.sin_port = htons(WORKER_PORT);
    inet_pton(AF_INET, WORKER_IP, &worker_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&worker_addr, sizeof(worker_addr)) < 0) {
        perror("connect to worker");
        return -1;
    }
    return sock;
}

int check_program_exists(int server_sock, const char *prog_name, char *task_mq_name) {
    int sock = connect_to_worker();
    if (sock < 0) return -1;

    message_t msg_header;
    memset(&msg_header, 0, sizeof(msg_header));
    msg_header.mq_type = 1;
    memcpy(&msg_header.type, "PROG", 4);
    memcpy(&msg_header.recv_type, "CHCK", 4);
    msg_header.size = strlen(prog_name) + 1;

    if (write(sock, &msg_header, sizeof(message_t)) != sizeof(message_t)) {
        perror("write CHCK header");
    }
    if (write(sock, prog_name, msg_header.size) != msg_header.size) {
        perror("write CHCK data");
    }
    printf("[Master] Sent CHCK message for %s to worker_exec.\n", prog_name);
    close(sock);

    int client_sock = accept(server_sock, NULL, NULL);
    if (client_sock < 0) {
        perror("accept CHCK resp");
        return -1;
    }

    message_t resp_header;
    if (read(client_sock, &resp_header, sizeof(message_t)) != sizeof(message_t)) {
        perror("read CHCK resp header");
        close(client_sock);
        return -1;
    }

    if (read(client_sock, task_mq_name, resp_header.size) != resp_header.size) {
        perror("read CHCK resp data");
        close(client_sock);
        return -1;
    }
    close(client_sock);
    
    printf("[Master] Received CHCK response: %s\n", task_mq_name);
    if (strcmp(task_mq_name, "NONE") == 0) {
        return 0; // Not found
    }
    return 1; // Found
}

int send_prog_message() {
    int sock = connect_to_worker();
    if (sock < 0) return -1;

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

    message_t msg_header;
    memset(&msg_header, 0, sizeof(msg_header));
    msg_header.mq_type = 1;
    memcpy(&msg_header.type, "PROG", 4);
    memcpy(&msg_header.recv_type, "RESP", 4);
    msg_header.size = sizeof(prog.prog_name) + strlen(prog.prog_code) + 1;

    if (write(sock, &msg_header, sizeof(message_t)) != sizeof(message_t)) {
        perror("write header");
    }
    if (write(sock, &prog, msg_header.size) != msg_header.size) {
        perror("write data");
    }
    printf("[Master] Sent PROG message to worker_exec.\n");
    close(sock);
    return 0;
}

int receive_task_mq(int server_sock, char *task_mq_name) {
    int client_sock = accept(server_sock, NULL, NULL);
    if (client_sock < 0) {
        perror("accept");
        return -1;
    }

    message_t resp_header;
    if (read(client_sock, &resp_header, sizeof(message_t)) != sizeof(message_t)) {
        perror("read resp header");
        close(client_sock);
        return -1;
    }

    if (read(client_sock, task_mq_name, resp_header.size) != resp_header.size) {
        perror("read resp data");
        close(client_sock);
        return -1;
    }
    close(client_sock);
    
    printf("[Master] Received response! Extracted task_mq msg_type: %s\n", task_mq_name);
    return 0;
}

int send_task_message(const char *task_mq_name) {
    int sock = connect_to_worker();
    if (sock < 0) return -1;

    size_t data_len = strlen("HelloFromTestClient") + 1;
    size_t task_size = sizeof(recv_task_t) + data_len;
    
    recv_task_t *task = malloc(task_size);
    memset(task, 0, task_size);
    strcpy(task->function_name, "my_test_function");
    task->data_count = 1;
    strcpy((char *)task->data, "HelloFromTestClient");

    message_t msg_header;
    memset(&msg_header, 0, sizeof(msg_header));
    msg_header.mq_type = 1;
    
    memcpy(&msg_header.type, task_mq_name, strlen(task_mq_name) + 1);
    msg_header.size = task_size;

    write(sock, &msg_header, sizeof(message_t));
    write(sock, task, msg_header.size);
    
    free(task);
    
    printf("[Master] Sent recv_task_t using target msg_type: %s\n", task_mq_name);
    close(sock);
    return 0;
}

int receive_task_result(int server_sock) {
    printf("[Master] Waiting for task execution result on port %d...\n", MASTER_PORT);
    int res_sock = accept(server_sock, NULL, NULL);
    if (res_sock >= 0) {
        message_t res_header;
        if (read(res_sock, &res_header, sizeof(message_t)) == sizeof(message_t)) {
            char result[256];
            memset(result, 0, sizeof(result));
            read(res_sock, result, res_header.size);
            printf("[Master] Task result: %s\n", result);
        }
        close(res_sock);
        return 0;
    }
    return -1;
}

int main() {
    int server_sock = setup_master_server();
    if (server_sock < 0) return 1;

    char task_mq_name[64];
    memset(task_mq_name, 0, sizeof(task_mq_name));
    
    int exists = check_program_exists(server_sock, "test_output", task_mq_name);
    if (exists < 0) return 1;

    if (!exists) {
        if (send_prog_message() < 0) return 1;
        memset(task_mq_name, 0, sizeof(task_mq_name));
        if (receive_task_mq(server_sock, task_mq_name) < 0) return 1;
    } else {
        printf("[Master] Program already exists on worker, skipping PROG send.\n");
    }

    if (send_task_message(task_mq_name) < 0) return 1;

    if (receive_task_result(server_sock) < 0) return 1;

    close(server_sock);
    return 0;
}
