#ifndef SOCKET_H
#define SOCKET_H
#include<stdint.h>

typedef struct {
    long mq_type;
    uint64_t type;
    uint64_t recv_type;
    uint64_t size;
    char data[];
} message_t;


typedef enum {
    CONN_ACTIVE,
    CONN_CLOSED
} conn_state;


typedef struct {
    int sockfd;
    char ip[16];
    int port;
    conn_state state;
} connection;

connection *create_listener(char *Ip, int port, int backlog);
connection *create_connection(char *Ip, int port);
int send_message(connection *connection, message_t *message);


#endif