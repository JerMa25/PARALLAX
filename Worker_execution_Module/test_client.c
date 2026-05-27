#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "network_agent.h"
#include "worker_exec.h"

int main() {
    // 1. Setup server socket on port 9001 to act as master and receive response
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(9001);
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(server_sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind on 9001");
        return 1;
    }
    listen(server_sock, 1);
    printf("[Master] Listening on port 9001 for responses...\n");

    // 2. Connect to worker_exec on port 9000
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in worker_addr;
    memset(&worker_addr, 0, sizeof(worker_addr));
    worker_addr.sin_family = AF_INET;
    worker_addr.sin_port = htons(9000);
    inet_pton(AF_INET, "127.0.0.1", &worker_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&worker_addr, sizeof(worker_addr)) < 0) {
        perror("connect to worker");
        return 1;
    }

    // 3. Construct and send the PROG message
    prog_t prog;
    memset(&prog, 0, sizeof(prog));
    strcpy(prog.prog_name, "test_output");
    strcpy(
        prog.prog_code,

        "#include <stdio.h>\n"
        "\n"
        "int main() {\n"
        "    printf(\"Hello from generated program!\\n\");\n"
        "    return 0;\n"
        "}\n"

        
    );

    message_t msg_header;
    memset(&msg_header, 0, sizeof(msg_header));
    msg_header.mq_type = 1;
    memcpy(&msg_header.type, "PROG", 4);
    memcpy(&msg_header.recv_type, "RESP", 4); // Response type will be RESP
    msg_header.size = sizeof(prog.prog_name) + strlen(prog.prog_code) + 1;

    if (write(sock, &msg_header, sizeof(message_t)) != sizeof(message_t)) {
        perror("write header");
    }
    if (write(sock, &prog, msg_header.size) != msg_header.size) {
        perror("write data");
    }
    printf("[Master] Sent PROG message to worker_exec.\n");
    close(sock);

    // 4. Accept response from worker on port 9001
    int client_sock = accept(server_sock, NULL, NULL);
    if (client_sock < 0) {
        perror("accept");
        return 1;
    }

    message_t resp_header;
    if (read(client_sock, &resp_header, sizeof(message_t)) != sizeof(message_t)) {
        perror("read resp header");
        return 1;
    }

    char task_mq_name[64];
    memset(task_mq_name, 0, sizeof(task_mq_name));
    if (read(client_sock, task_mq_name, resp_header.size) != resp_header.size) {
        perror("read resp data");
        return 1;
    }
    close(client_sock);

    printf("[Master] Received response! Extracted task_mq msg_type: %s\n", task_mq_name);

    // 5. Send recv_task_t using the newly extracted task_mq_name
    int sock2 = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(sock2, (struct sockaddr*)&worker_addr, sizeof(worker_addr)) < 0) {
        perror("connect 2 to worker");
        return 1;
    }

    recv_task_t task;
    memset(&task, 0, sizeof(task));
    strcpy(task.function_name, "my_test_function");
    task.data_count = 0;

    memset(&msg_header, 0, sizeof(msg_header));
    msg_header.mq_type = 1;
    
    // Set the target type to the task_mq_name we just received!
    // It fits perfectly because it was randomly generated to be 7 chars + \0
    memcpy(&msg_header.type, task_mq_name, strlen(task_mq_name) + 1);
    
    msg_header.size = sizeof(recv_task_t);

    write(sock2, &msg_header, sizeof(message_t));
    write(sock2, &task, msg_header.size);
    
    printf("[Master] Sent recv_task_t using target msg_type: %s\n", task_mq_name);

    close(sock2);
    close(server_sock);

    return 0;
}
