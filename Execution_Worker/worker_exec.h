#ifndef WORKER_EXEC_H
#define WORKER_EXEC_H
#include "network_agent.h"

typedef struct {
  char function_name[64];
  uint64_t data_count;
  uint8_t data[];
} recv_task_t;

typedef struct {
  char prog_name[64];

  char prog_code[7500];
} prog_t;

typedef struct {
    message_t * message;
    char * master_ip;
    int port;

} execution_context;

void *worker_exec_thread(void *arg);

#endif