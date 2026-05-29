#include "socket.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

int main() {
    // 1. Setup UDP socket to listen for the reply on 9001
    int recv_sock = socket(AF_INET, SOCK_DGRAM, 0);
    int opt = 1;
    setsockopt(recv_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in recv_addr;
    memset(&recv_addr, 0, sizeof(recv_addr));
    recv_addr.sin_family = AF_INET;
    recv_addr.sin_port = htons(9001);
    recv_addr.sin_addr.s_addr = INADDR_ANY;
    bind(recv_sock, (struct sockaddr *)&recv_addr, sizeof(recv_addr));

    // 2. Prepare the broadcast message
    message_t *msg = malloc(sizeof(message_t) + 32);
    strcpy(msg->type, "TEST_BCAST");
    strcpy(msg->recv_type, "");
    msg->size = 32;
    strcpy(msg->data, "Hello Broadcast!");
    
    printf("Sending broadcast message to port 9001...\n");
    send_broadcast_message(9001, msg);
    printf("Broadcast sent successfully.\n");
    free(msg);
    
    // 3. Listen for the reply
    printf("Waiting for receiver's IP reply on port 9001...\n");
    char buffer[2048];
    while(1) {
        ssize_t n = recvfrom(recv_sock, buffer, sizeof(buffer), 0, NULL, NULL);
        if (n > 0) {
            message_t *reply = (message_t *)buffer;
            
            // Ignore our own broadcast
            if (strcmp(reply->data, "Hello Broadcast!") == 0) continue;
            
            if (strncmp(reply->data, "IP:", 3) == 0) {
                printf("\n--- Reply Received ---\n");
                printf("Message Type: %s\n", reply->type);
                printf("Receiver IP: %s\n", reply->data + 3);
                break;
            }
        }
    }
    
    close(recv_sock);
    return 0;
}
