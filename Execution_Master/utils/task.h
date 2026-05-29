#ifndef TASK_H
#define TASK_H

typedef  struct {
    char ip[16];
    int port;
    char uuid[64];
} worker_node;



typedef struct {
    void *chunk;
    int  chunk_size;
} chunk_data;




#endif