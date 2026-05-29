#ifndef WORKER_EXEC_H
#define WORKER_EXEC_H
#include "network_agent.h"

typedef struct {
  char function_name[64];
  uint64_t data_count;
  uint64_t data[];

} recv_task_t;

typedef struct {
  char prog_name[64];

  char prog_code[NETWORK_AGENT_MAX_DATA];
} prog_t;

typedef struct {
    message_t * message;
    char * master_ip;
    int port;

} execution_context;

#endif