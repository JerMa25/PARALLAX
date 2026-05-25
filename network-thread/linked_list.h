#ifndef LINKED_LIST_H
#define LINKED_LIST_H

typedef struct node{
    void * data;
    struct node* next;
} node;


node * create_node(void *data);
void push_back(node *head, node *new_node);
void delete_node(node **head, node *target);
void destroy_list(node **head, void (*destroy_data)(void *));
#endif
