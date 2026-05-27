#ifndef NETWORK_AGENT_H
#define NETWORK_AGENT_H

#include "socket.h"

#define NETWORK_AGENT_MTYPE 1L
#define NETWORK_AGENT_MAX_DATA 65536
typedef struct {
    long mtype;
    uint64_t type;
    uint64_t size;
    char data[NETWORK_AGENT_MAX_DATA];
} queued_message;

typedef struct {
    long mtype;
    char ip[16];
    int port;
    uint64_t type;
    uint64_t size;
    char data[NETWORK_AGENT_MAX_DATA];
} outgoing_message;

void start();
void stop();
void send_msg(char *Ip, int port, message_t *message);

#endif
