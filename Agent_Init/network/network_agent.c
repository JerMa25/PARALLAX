#include "network_agent.h"
#include "ms_queue.h"
#include "socket.h"
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/socket.h>
#include <netinet/in.h>     
#include <unistd.h>
#include <arpa/inet.h>
#define NETWORK_AGENT_MTYPE 1L

static pthread_t listener_thread;
static pthread_t sender_thread;
static pthread_t udp_listener_thread;
static connection *local_connection = NULL;
static atomic_int agent_running = 0;
static atomic_int agent_started = 0;
static int agent_port = 9000;
static pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Lit exactement size octets depuis un descripteur.
 * Cette fonction evite les lectures partielles classiques avec les sockets TCP.
 */
static ssize_t read_exact(int fd, void *buffer, size_t size) {
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
static map_entry *get_or_create_mq(char *msg_type) {
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
 * Ferme le listener si necessaire, detruit les queues et marque l'agent comme
 * arrete.
 */
static void cleanup_agent() {
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
 * Thread d'ecoute UDP (pour le controller).
 * Il ecoute sur le port donne et place les paquets dans la queue correspondante.
 */
void *udp_socket_listener(void *args) {
    int port = (int)(intptr_t)args;
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("udp socket");
        return NULL;
    }

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("udp bind");
        close(sockfd);
        return NULL;
    }

    printf("UDP listening on port %d\n", port);

    char *buffer = malloc(sizeof(message_t) + NETWORK_AGENT_MAX_DATA);
    if (!buffer) {
        close(sockfd);
        return NULL;
    }

    while (atomic_load(&agent_running)) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        ssize_t received = recvfrom(sockfd, buffer, sizeof(message_t) + NETWORK_AGENT_MAX_DATA, 0, (struct sockaddr *)&client_addr, &client_len);
        if (received < 0) {
            if (!atomic_load(&agent_running)) break;
            perror("recvfrom udp");
            continue;
        }

        if (received < (ssize_t)sizeof(message_t)) continue;

        message_t *header = (message_t *)buffer;

        if (header->size > NETWORK_AGENT_MAX_DATA) continue;

        queued_message item;
        memset(&item, 0, sizeof(item));
        item.mtype = NETWORK_AGENT_MTYPE;
        strcpy(item.type, header->type);
        strcpy(item.recv_type, header->recv_type);
        item.size = header->size;

        if (item.size > 0 && received >= (ssize_t)(sizeof(message_t) + item.size)) {
            memcpy(item.data, header->data, item.size);
        }

        map_entry *entry = get_or_create_mq(item.type);
        if (entry == NULL) continue;

        size_t payload_size = offsetof(queued_message, data) - sizeof(long) + item.size;
        if (msgsnd(entry->queue_id, &item, payload_size, 0) < 0) {
            perror("msgsnd udp incoming");
        }
    }
    
    free(buffer);
    close(sockfd);
    return NULL;
}

/*
 * Thread d'ecoute reseau.
 * Il accepte les connexions entrantes, reconstruit un message_t, puis place
 * le paquet dans la queue locale correspondant au champ type du message.
 */
void *socket_listener(void *args) {
  connection *local_conn = (connection *)args;


   printf("\n\n\n starting reciever thread \n\n");
  /*
      1. listen for new message on the socket
      2. when it receives a new message , it deserialize it into message_t
      3. from the type of the message it get the message queue it it to write
     the data into using hte find_by_msg_type function
      4. it then write the data into that message queue
  */
  printf("received something 1\n");
  while (atomic_load(&agent_running)) {

     printf("received something 2\n");
     printf("waiting for connection...\n");

    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(
        local_conn->sockfd,
        (struct sockaddr *)&client_addr,
        &client_len
    );

    if (client_fd < 0) {
        if (!atomic_load(&agent_running))
            break;

        perror("accept");
        continue;
    }

    char client_ip[INET_ADDRSTRLEN];

    inet_ntop(
        AF_INET,
        &client_addr.sin_addr,
        client_ip,
        sizeof(client_ip)
    );

    printf(
        "Client connected from %s:%d (fd=%d)\n",
        client_ip,
        ntohs(client_addr.sin_port),
        client_fd
    );

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
  
    strcpy(item.type,header.type);
    strcpy(item.recv_type,header.recv_type);
    strcpy(item.sender_ip, header.sender_ip);
    item.sender_port = header.sender_port;
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

    printf("received message with type %s\n", item.type);
   

    map_entry *entry = get_or_create_mq(item.type);
    if (entry == NULL) {
      fprintf(stderr, "network_agent: unable to create mq for type %s\n",
              msg_type);
      continue;
    }

    size_t payload_size =
        offsetof(queued_message, data) - sizeof(long) + item.size;
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
void *socket_sender(void *args) {

 
  map_entry *outgoing_mq = (map_entry *)args;
  /*
  this is continously checking for message in the message queue,
  once it gets a new message
  it creates a socket connection to the target and sends the message

  */
  if (outgoing_mq == NULL)
    return NULL;

  while (atomic_load(&agent_running)) {
    outgoing_message item;
    ssize_t received =
        msgrcv(outgoing_mq->queue_id, &item, sizeof(item) - sizeof(long),
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

    message_t *message = (message_t *)calloc(1, sizeof(message_t) + item.size);
    if (message == NULL) {
      close(remote->sockfd);
      free(remote);
      continue;
    }

   
    strcpy(message->type,item.type);
    strcpy(message->recv_type, item.recv_type);
    strcpy(message->sender_ip, item.sender_ip);
    message->sender_port = item.sender_port;
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
void *network_thread_run(void *args) {
  int port = 9000;
  char outgoing_q[64] = "outgoing";
  if (args != NULL) {
      network_agent_config *config = (network_agent_config *)args;
      port = config->port;
      if (config->queue_name[0] != '\0') {
          strcpy(outgoing_q, config->queue_name);
      }
  }

  agent_port = port;

  if (atomic_exchange(&agent_started, 1))
    return NULL;

  atomic_store(&agent_running, 1);

  // create listening socket for the local machine
  local_connection = create_listener("0.0.0.0", port, 1);

  
  if (local_connection == NULL) {
    atomic_store(&agent_running, 0);
    atomic_store(&agent_started, 0);
    return NULL;
  }

  // create recieving message queue to store outgoing messages
  if (create_mq(outgoing_q, NETWORK_AGENT_MAX_DATA) == NULL) {
    atomic_store(&agent_running, 0);
    cleanup_agent();
    return NULL;
  }

  map_entry *outgoing_mq = find_by_msg_type(outgoing_q);

  // start thread to listen for incoming messages
  if (pthread_create(&listener_thread, NULL, socket_listener,
                     (void *)local_connection) != 0) {
    atomic_store(&agent_running, 0);
    cleanup_agent();
    return NULL;
  }

  // start thread to listen for messages in outgoing mq and send them
  if (pthread_create(&sender_thread, NULL, socket_sender,
                     (void *)outgoing_mq) != 0) {

    atomic_store(&agent_running, 0);
    shutdown(local_connection->sockfd, SHUT_RDWR);
    close(local_connection->sockfd);
    local_connection->sockfd = -1;
    pthread_join(listener_thread, NULL);
    cleanup_agent();
    return NULL;
  }

  if (pthread_create(&udp_listener_thread, NULL, udp_socket_listener, (void *)(intptr_t)(port+1)) != 0) {
      perror("Failed to create UDP listener thread");
  }

  return NULL;
}

/*
 * Arrete l'agent reseau.
 * La fonction signale l'arret aux threads, debloque le listener, attend les
 * threads avec pthread_join, puis libere les ressources.
 */
void network_stop() {
  if (!atomic_load(&agent_started))
    return;

  atomic_store(&agent_running, 0);

  if (local_connection != NULL) {
    connection *wake_conn =
        create_connection(local_connection->ip, local_connection->port);
    if (wake_conn != NULL) {
        close(wake_conn->sockfd);
        free(wake_conn);
    }

    shutdown(local_connection->sockfd, SHUT_RDWR);
    close(local_connection->sockfd);
    local_connection->sockfd = -1;
    local_connection->state = CONN_CLOSED;
  }

  // Send dummy packet to UDP to wake up recvfrom
  int dummy_sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (dummy_sock >= 0) {
      struct sockaddr_in dummy_addr;
      memset(&dummy_addr, 0, sizeof(dummy_addr));
      dummy_addr.sin_family = AF_INET;
      dummy_addr.sin_port = htons(agent_port + 1);
      dummy_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
      sendto(dummy_sock, "stop", 4, 0, (struct sockaddr *)&dummy_addr, sizeof(dummy_addr));
      close(dummy_sock);
  }

  pthread_join(listener_thread, NULL);
  pthread_join(sender_thread, NULL);
  pthread_join(udp_listener_thread, NULL);
  cleanup_agent();
}

/*
 * Ajoute un message dans la queue outgoing.
 * Le thread socket_sender se chargera ensuite de l'envoyer a Ip:port.
 */
void send_msg(char *Ip, int port, char *queue_name, message_t *message) {
  /*
      this function is to send the message to the outgoing message queue
      //it should get the map_entry of the outgoing message using the
     find_by_msg_type then write the message_t into the message queue
  */
  if (Ip == NULL || message == NULL)
    return;

  if (message->size > NETWORK_AGENT_MAX_DATA) {
    fprintf(stderr, "network_agent: outgoing message too large (%lu bytes)\n",
            (unsigned long)message->size);
    return;
  }

  const char *q_name = (queue_name != NULL && queue_name[0] != '\0') ? queue_name : "outgoing";
  map_entry *outgoing_mq = get_or_create_mq((char*)q_name);
  if (outgoing_mq == NULL) {
    fprintf(stderr, "network_agent: outgoing mq is unavailable\n");
    return;
  }

  outgoing_message out;
  memset(&out, 0, sizeof(out));
  out.mtype = NETWORK_AGENT_MTYPE;
  strncpy(out.ip, Ip, sizeof(out.ip) - 1);
  out.ip[sizeof(out.ip) - 1] = '\0';
  out.port = port;
  
  strcpy(out.type, message->type);
  strncpy(out.recv_type, message->recv_type, sizeof(out.recv_type) - 1);
  out.recv_type[sizeof(out.recv_type) - 1] = '\0';
  
  if (strlen(message->sender_ip) == 0) {
      strncpy(out.sender_ip, "127.0.0.1", sizeof(out.sender_ip) - 1);
  } else {
      strncpy(out.sender_ip, message->sender_ip, sizeof(out.sender_ip) - 1);
  }
  out.sender_ip[sizeof(out.sender_ip) - 1] = '\0';
  
  out.sender_port = (message->sender_port == 0) ? agent_port : message->sender_port;
  out.size = message->size;

  if (message->size > 0)
    memcpy(out.data, message->data, message->size);

  size_t payload_size = offsetof(outgoing_message, data) - sizeof(long) + out.size;

  if (msgsnd(outgoing_mq->queue_id, &out, payload_size, 0) < 0)
    perror("msgsnd outgoing");
}

/*
 * Wrapper to send a broadcast message
 */
void send_broadcast(int port, message_t *message) {
    send_broadcast_message(port, message);
}