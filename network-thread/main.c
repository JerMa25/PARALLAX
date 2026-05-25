#include "ms_queue.h"
#include <stdio.h>
#include <stdlib.h>
#include"linked_list.h"

/*
 * Programme de test minimal pour verifier la creation, la recherche
 * et la destruction des queues enregistrees dans la registry.
 */
int main()
{
    printf("=== Testing message queue registry ===\n");

    // 1. Create queues
    int q1 = create_mq("TASK", 128);
    int q2 = create_mq("CONTROL", 256);
    int q3 = create_mq("DATA", 512);

    printf("Created queues:\n");
    printf("TASK    -> msgid: %d\n", q1);
    printf("CONTROL -> msgid: %d\n", q2);
    printf("DATA    -> msgid: %d\n", q3);

    // 2. Test lookup success
    map_entry *e1 = find_by_msg_type("CONTROL");
    if (e1) {
        printf("\nFOUND CONTROL:\n");
        printf("type: %s | queue_id: %d | msg_len: %d\n",
               e1->msg_type, e1->queue_id, e1->msg_len);
    } else {
        printf("CONTROL not found\n");
    }

    // 3. Test lookup failure
    map_entry *e2 = find_by_msg_type("UNKNOWN");
    if (!e2) {
        printf("\nCorrectly did NOT find UNKNOWN type\n");
    }

    // 4. Dump registry (important debug step)
    printf("\n=== FULL REGISTRY DUMP ===\n");
    node *temp = registry->head;
    while (temp) {
        map_entry *e = (map_entry *)temp->data;
        printf("msg_type=%s | queue_id=%d | msg_len=%d\n",
               e->msg_type, e->queue_id, e->msg_len);
        temp = temp->next;
    }
    destroy_queues();
    
    return 0;
}
