#include"network_agent.h"
#include"socket.h"
#include"ms_queue.h"
#include<pthread.h>
#include<stdatomic.h>
#include<errno.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/msg.h>
#include<sys/socket.h>
#include<unistd.h>
#include<stddef.h>

#define NETWORK_AGENT_MTYPE 1L
#define NETWORK_AGENT_MAX_DATA 65536



static pthread_t listener_thread;
static pthread_t sender_thread;
static connection *local_connection = NULL;
static atomic_int agent_running = 0;
static atomic_int agent_started = 0;
static pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Lit exactement size octets depuis un descripteur.
 * Cette fonction evite les lectures partielles classiques avec les sockets TCP.
 */
static ssize_t read_exact(int fd, void *buffer, size_t size)
{
    size_t total = 0;
    char *cursor = (char *)buffer;

    while (total < size) {
        ssize_t n = read(fd, cursor + total, size - total);

        if (n == 0)
            return 0;

        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        total += (size_t)n;
    }

    return (ssize_t)total;
}

/*
 * Recupere une queue par type de message ou la cree si elle n'existe pas.
 * L'acces a la registry est protege pour eviter les conflits entre threads.
 */
static map_entry *get_or_create_mq(char *msg_type)
{
    pthread_mutex_lock(&registry_lock);

    map_entry *entry = find_by_msg_type(msg_type);
    if (entry == NULL) {
        if (create_mq(msg_type, NETWORK_AGENT_MAX_DATA) != NULL)
            entry = find_by_msg_type(msg_type);
    }

    pthread_mutex_unlock(&registry_lock);
    return entry;
}

/*
 * Nettoie les ressources globales de l'agent apres son arret.
 * Ferme le listener si necessaire, detruit les queues et marque l'agent comme arrete.
 */
static void cleanup_agent()
{
    if (local_connection != NULL) {
        if (local_connection->sockfd >= 0)
            close(local_connection->sockfd);
        free(local_connection);
        local_connection = NULL;
    }

    destroy_queues();
    atomic_store(&agent_started, 0);
}

/*
 * Thread d'ecoute reseau.
 * Il accepte les connexions entrantes, reconstruit un message_t, puis place
 * le paquet dans la queue locale correspondant au champ type du message.
 */
void * socket_listener(void * args){
    connection * local_conn=(connection *)args;
    /*
        1. listen for new message on the socket
        2. when it receives a new message , it deserialize it into message_t
        3. from the type of the message it get the message queue it it to write the data into using hte find_by_msg_type function
        4. it then write the data into that message queue
    */
    while (atomic_load(&agent_running)) {
        int client_fd = accept(local_conn->sockfd, NULL, NULL);

        if (client_fd < 0) {
            if (!atomic_load(&agent_running))
                break;
            if (errno == EINTR)
                continue;
            perror("accept");
            continue;
        }

        message_t header;
        ssize_t header_read = read_exact(client_fd, &header, sizeof(header));
        if (header_read != (ssize_t)sizeof(header)) {
            close(client_fd);
            continue;
        }

        if (header.size > NETWORK_AGENT_MAX_DATA) {
            fprintf(stderr, "network_agent: message too large (%lu bytes)\n",
                    (unsigned long)header.size);
            close(client_fd);
            continue;
        }

        queued_message item;
        memset(&item, 0, sizeof(item));
        item.mtype = NETWORK_AGENT_MTYPE;
        item.type = header.type;
        item.size = header.size;

        if (item.size > 0) {
            ssize_t data_read = read_exact(client_fd, item.data, item.size);
            if (data_read != (ssize_t)item.size) {
                close(client_fd);
                continue;
            }
        }

        close(client_fd);

        char msg_type[64];
        snprintf(msg_type, sizeof(msg_type), "%lu", (unsigned long)item.type);

        map_entry *entry = get_or_create_mq(msg_type);
        if (entry == NULL) {
            fprintf(stderr, "network_agent: unable to create mq for type %s\n",
                    msg_type);
            continue;
        }

        size_t payload_size = offsetof(queued_message, data) - sizeof(long) + item.size;
        if (msgsnd(entry->queue_id, &item, payload_size, 0) < 0)
            perror("msgsnd incoming");
    }

    return NULL;
}

/*
 * Thread d'envoi reseau.
 * Il surveille la queue outgoing, extrait les messages a envoyer,
 * ouvre une connexion vers IP + port, puis transmet le paquet.
 */
void * socket_sender(void * args){
    map_entry * outgoing_mq=(map_entry *)args;
    /*
    this is continously checking for message in the message queue,
    once it gets a new message
    it creates a socket connection to the target and sends the message
    
    */
    if (outgoing_mq == NULL)
        return NULL;

    while (atomic_load(&agent_running)) {
        outgoing_message item;
        ssize_t received = msgrcv(outgoing_mq->queue_id, &item,
                                  sizeof(item) - sizeof(long),
                                  NETWORK_AGENT_MTYPE, IPC_NOWAIT);

        if (received < 0) {
            if (errno == ENOMSG) {
                usleep(100000);
                continue;
            }
            if (!atomic_load(&agent_running))
                break;
            perror("msgrcv outgoing");
            usleep(100000);
            continue;
        }

        connection *remote = create_connection(item.ip, item.port);
        if (remote == NULL)
            continue;

        message_t *message = (message_t *)malloc(sizeof(message_t) + item.size);
        if (message == NULL) {
            close(remote->sockfd);
            free(remote);
            continue;
        }

        message->type = item.type;
        message->size = item.size;
        if (item.size > 0)
            memcpy(message->data, item.data, item.size);

        send_message(remote, message);

        free(message);
        close(remote->sockfd);
        free(remote);
    }

    return NULL;
}

/*
 * Demarre l'agent reseau local.
 * La fonction cree le listener, initialise la queue outgoing, puis lance
 * les threads d'ecoute et d'envoi sans bloquer l'appelant.
 */
void network_start(){
    if (atomic_exchange(&agent_started, 1))
        return;

    atomic_store(&agent_running, 1);

    //create listening socket for the local machine
    local_connection=create_listener("127.0.0.1",9000,1);
    if (local_connection == NULL) {
        atomic_store(&agent_running, 0);
        atomic_store(&agent_started, 0);
        return;
    }

    //create recieving message queue to store outgoing messages
    if (create_mq("outgoing", NETWORK_AGENT_MAX_DATA) == NULL) {
        atomic_store(&agent_running, 0);
        cleanup_agent();
        return;
    }
    
    map_entry * outgoing_mq=find_by_msg_type("outgoing");




    //start thread to listen for incoming messages
    if (pthread_create(&listener_thread,NULL,socket_listener,(void *)local_connection) != 0) {
        atomic_store(&agent_running, 0);
        cleanup_agent();
        return;
    }

    //start thread to listen for messages in outgoing mq and send them
    if (pthread_create(&sender_thread,NULL,socket_sender,(void * )outgoing_mq) != 0) {
        atomic_store(&agent_running, 0);
        shutdown(local_connection->sockfd, SHUT_RDWR);
        close(local_connection->sockfd);
        local_connection->sockfd = -1;
        pthread_join(listener_thread,NULL);
        cleanup_agent();
        return;
    }
}


/*
 * Arrete l'agent reseau.
 * La fonction signale l'arret aux threads, debloque le listener, attend les
 * threads avec pthread_join, puis libere les ressources.
 */
void network_stop(){
    if (!atomic_load(&agent_started))
        return;

    atomic_store(&agent_running, 0);

    if (local_connection != NULL) {
        connection *wake_conn = create_connection(local_connection->ip,
                                                 local_connection->port);
        if (wake_conn != NULL) {
            close(wake_conn->sockfd);
            free(wake_conn);
        }

        shutdown(local_connection->sockfd, SHUT_RDWR);
        close(local_connection->sockfd);
        local_connection->sockfd = -1;
        local_connection->state = CONN_CLOSED;
    }

    pthread_join(listener_thread,NULL);
    pthread_join(sender_thread,NULL);
    cleanup_agent();

}


/*
 * Ajoute un message dans la queue outgoing.
 * Le thread socket_sender se chargera ensuite de l'envoyer a Ip:port.
 */
void send_msg(char * Ip, int port, message_t*  message ){
    /*
        this function is to send the message to the outgoing message queue
        //it should get the map_entry of the outgoing message using the find_by_msg_type
        then write the message_t into the message queue
    */
    if (Ip == NULL || message == NULL)
        return;

    if (message->size > NETWORK_AGENT_MAX_DATA) {
        fprintf(stderr, "network_agent: outgoing message too large (%lu bytes)\n",
                (unsigned long)message->size);
        return;
    }

    map_entry *outgoing_mq = get_or_create_mq("outgoing");
    if (outgoing_mq == NULL) {
        fprintf(stderr, "network_agent: outgoing mq is unavailable\n");
        return;
    }

    outgoing_message item;
    memset(&item, 0, sizeof(item));
    item.mtype = NETWORK_AGENT_MTYPE;
    strncpy(item.ip, Ip, sizeof(item.ip) - 1);
    item.port = port;
    item.type = message->type;
    item.size = message->size;

    if (message->size > 0)
        memcpy(item.data, message->data, message->size);

    size_t payload_size = offsetof(outgoing_message, data) - sizeof(long) + item.size;

    if (msgsnd(outgoing_mq->queue_id, &item, payload_size, 0) < 0)
        perror("msgsnd outgoing");
}
