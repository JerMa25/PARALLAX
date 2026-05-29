#include "linked_list.h"
#include <stdlib.h>

node *create_node(void *data) {
    node *new_node = malloc(sizeof(node));

    if (new_node == NULL)
        return NULL;

    new_node->data = data;
    new_node->next = NULL;

    return new_node;
}

void push_back(node *head, node *new_node) {

    if (head == NULL || new_node == NULL)
        return;

    node *temp = head;

    while (temp->next != NULL) {
        temp = temp->next;
    }

    temp->next = new_node;
}

void delete_node(node **head, node *target) {

    if (head == NULL || *head == NULL || target == NULL)
        return;

    node *temp = *head;

    /* deleting head */
    if (temp == target) {
        *head = temp->next;
        free(temp);
        return;
    }

    while (temp->next != NULL && temp->next != target) {
        temp = temp->next;
    }

    if (temp->next == NULL)
        return;

    temp->next = target->next;

    free(target);
}