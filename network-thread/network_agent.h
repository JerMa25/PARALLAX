#ifndef NETWORK_AGENT_H
#define NETWORK_AGENT_H

#include "socket.h"

void start();
void stop();
void send_msg(char *Ip, int port, message_t *message);

#endif
