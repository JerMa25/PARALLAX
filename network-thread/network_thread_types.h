#ifndef NETWORK_THREAD_TYPES_H
#define NETWORK_THREAD_TYPES_H
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/select.h>
#include <sys/socket.h>

#define UUID_LEN 16
#define UUID_HEX 33
#define MAX_PEERS 32
#define MAX_MSG 512
#define Q_IN  "/parallax_in"
#define Q_OUT "/parallax_out"

typedef struct { unsigned char uuid[UUID_LEN]; char ip[16]; int port; time_t seen; } peer_t;
typedef struct { unsigned char uuid[UUID_LEN]; char role[16]; int udp_port; } net_config_t;
typedef struct {
    net_config_t cfg;
    int sockfd, running;
    mqd_t mq_in, mq_out;
    peer_t peers[MAX_PEERS];
    int peer_count;
} net_state_t;
#endif
