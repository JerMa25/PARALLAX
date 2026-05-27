#ifndef MS_QUEUE_H
#define MS_QUEUE_H


#include<stdint.h>
#include"linked_list.h"


typedef struct {
    char mq_path[256];
    char msg_type[64];
    int queue_id;
    int msg_len;
} map_entry;


typedef struct {
    int counter;
    char * base_path;
    node * head;
}map_registry;

extern map_registry * registry;

char * create_mq(char *msg_type, int msg_len);
map_entry * find_by_msg_type(char *msg_type);
void destroy_queues();
#endif