#include "network_agent.h"
#include "ms_queue.h"
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/msg.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

void get_local_ip(char *ip_buffer) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in serv;
    memset(&serv, 0, sizeof(serv));
    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = inet_addr("8.8.8.8");
    serv.sin_port = htons(53);
    connect(sock, (const struct sockaddr*) &serv, sizeof(serv));
    struct sockaddr_in name;
    socklen_t namelen = sizeof(name);
    getsockname(sock, (struct sockaddr*) &name, &namelen);
    inet_ntop(AF_INET, &name.sin_addr, ip_buffer, 16);
    close(sock);

}

int main() {
    // 1. Start the network agent as a controller (so it runs the UDP listener)
    network_agent_config cfg = {9000, "outgoing"};
    network_thread_run(&cfg);
    
    // 2. Create or get the message queue for "TEST_BCAST"
    map_entry *mq = find_by_msg_type("TEST_BCAST");
    if (!mq) {
        if (create_mq("TEST_BCAST", NETWORK_AGENT_MAX_DATA) != NULL) {
            mq = find_by_msg_type("TEST_BCAST");
        }
    }
    
    if (!mq) {
        printf("Failed to get message queue\n");
        network_stop();
        return 1;
    }
    
    printf("Network agent started. UDP listener is active.\n");
    printf("Waiting for broadcast in TEST_BCAST message queue...\n");
    
    queued_message item;
    while(1) {
        // 3. Receive the message from the queue
        ssize_t received = msgrcv(mq->queue_id, &item, sizeof(item) - sizeof(long), NETWORK_AGENT_MTYPE, 0);
        if (received > 0) {
            // Ignore our own broadcast if we happen to receive it
            if (strncmp(item.data, "IP:", 3) == 0) continue;
            
            printf("\n--- Broadcast Received via System Queue ---\n");
            printf("Message Type: %s\n", item.type);
            printf("Message Size: %lu\n", item.size);
            if (item.size > 0) {
                printf("Message Data: %s\n", item.data);
            }
            
            // 4. Reply with our IP on the same type
            char my_ip[16] = "127.0.0.1";
            
            
            message_t *reply = malloc(sizeof(message_t) + 64);
            strcpy(reply->type, "TEST_BCAST");
            strcpy(reply->recv_type, "");
            sprintf(reply->data, "IP:%s", my_ip);
            reply->size = strlen(reply->data) + 1;
            
            printf("Replying to sender with our IP: %s\n", my_ip);
            send_broadcast(9001, reply);
            free(reply);
            break;
        }
    }
    
    sleep(1);
    network_stop();
    return 0;
}
