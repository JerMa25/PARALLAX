#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/msg.h>

// Include network agent and queue headers
#include "../Agent_Init/network/network_agent.h"
#include "../Agent_Init/network/ms_queue.h"
#include "utils/master_thread.h"

void init_test_client() {
    static network_agent_config cfg = {9002, "client_out"};
    pthread_t net_thread;
    pthread_create(&net_thread, NULL, network_thread_run, &cfg);
    
    // Give thread a moment to start
    usleep(500000); 
    printf("[TestClient] Network agent started on port %d.\n", cfg.port);
}

int send_prog_message(char *ip, int port) {
    program_message_t prog;
    memset(&prog, 0, sizeof(prog));
    strcpy(prog.program_name, "test_program.c");
    strcpy(
        prog.code,
        "#include <stdio.h>\n"
        "int main() {\n"
        "    printf(\"Hello from uploaded program!\\n\");\n"
        "    return 0;\n"
        "}\n"
    );
    prog.code_size = strlen(prog.code);

    size_t total_size = sizeof(message_t) + sizeof(program_message_t);
    message_t *msg = malloc(total_size);
    memset(msg, 0, total_size);

    msg->mq_type = 1;
    memcpy(msg->type, "PROG", 4);
    memcpy(msg->recv_type, "RESP", 4);
    msg->size = sizeof(program_message_t);
    memcpy(msg->data, &prog, msg->size);

    send_msg(ip, port, "client_out", msg);
    printf("[TestClient] Sent PROG message for '%s' to %s:%d.\n", prog.program_name, ip, port);
    free(msg);
    return 0;
}

int main() {
    init_test_client();

    // Default network agent port where the Execution_Master might be listening
    char *master_ip = "127.0.0.1";
    int master_port = 9000;

    if (send_prog_message(master_ip, master_port) < 0) return 1;

    // Optional: wait before exiting to let sockets flush
    usleep(100000);
    return 0;
}
