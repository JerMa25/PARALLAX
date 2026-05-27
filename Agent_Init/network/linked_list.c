#include "linked_list.h"
#include <stdlib.h>

/*
 * Cree un nouveau noeud de liste chainee contenant le pointeur data.
 * La fonction retourne NULL si l'allocation memoire echoue.
 */
node *create_node(void *data) {
    node *new_node = malloc(sizeof(node));

    if (new_node == NULL)
        return NULL;

    new_node->data = data;
    new_node->next = NULL;

    return new_node;
}

/*
 * Ajoute new_node a la fin de la liste dont head est le premier noeud.
 * La fonction ne fait rien si la liste ou le nouveau noeud est NULL.
 */
void push_back(node *head, node *new_node) {

    if (head == NULL || new_node == NULL)
        return;

    node *temp = head;

    while (temp->next != NULL) {
        temp = temp->next;
    }

    temp->next = new_node;
}

/*
 * Supprime un noeud precis de la liste.
 * Attention : cette fonction libere uniquement le noeud, pas la donnee pointee.
 */
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

/*
 * Detruit toute une liste chainee.
 * Si destroy_data est fourni, il est appele pour liberer la donnee de chaque noeud.
 */
void destroy_list(node **head, void (*destroy_data)(void *)) {

    if (head == NULL)
        return;

    node *temp = *head;

    while (temp != NULL) {
        node *next = temp->next;

        if (destroy_data != NULL)
            destroy_data(temp->data);

        free(temp);
        temp = next;
    }

    *head = NULL;
}
