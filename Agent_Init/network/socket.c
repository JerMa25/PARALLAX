#include "socket.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>

/*
 * Cree une connexion TCP sortante vers l'adresse IP et le port donnes.
 * Retourne une structure connection active, ou NULL en cas d'erreur.
 */
connection *create_connection(char *Ip, int port)
{
    connection *conn = malloc(sizeof(connection));
    if (!conn) return NULL;

    conn->sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (conn->sockfd < 0) {
        perror("socket");
        free(conn);
        return NULL;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, Ip, &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(conn->sockfd);
        free(conn);
        return NULL;
    }

    if (connect(conn->sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(conn->sockfd);
        free(conn);
        return NULL;
    }

    strncpy(conn->ip, Ip, sizeof(conn->ip));
    conn->ip[sizeof(conn->ip) - 1] = '\0';

    conn->port = port;
    conn->state = CONN_ACTIVE;

    return conn;
}



/*
 * Cree un socket serveur TCP, l'attache a Ip:port, puis le place en ecoute.
 * Si Ip vaut NULL, le listener est attache par defaut a 127.0.0.1.
 */
connection *create_listener(char *Ip, int port, int backlog)
{
    connection *conn = malloc(sizeof(connection));
    if (!conn) return NULL;

    conn->sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (conn->sockfd < 0) {
        perror("socket");
        free(conn);
        return NULL;
    }

    int opt = 1;
    setsockopt(conn->sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    // 👉 LOCAL DEFAULT
   const char *bind_ip = (Ip == NULL) ? "0.0.0.0" : Ip;

    if (inet_pton(AF_INET, bind_ip, &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(conn->sockfd);
        free(conn);
        return NULL;
    }

    if (bind(conn->sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(conn->sockfd);
        free(conn);
        return NULL;
    }

    if (listen(conn->sockfd, backlog) < 0) {
        perror("listen");
        close(conn->sockfd);
        free(conn);
        return NULL;
    }

    strncpy(conn->ip, bind_ip, sizeof(conn->ip));
    conn->ip[sizeof(conn->ip) - 1] = '\0';

    conn->port = port;
    conn->state = CONN_ACTIVE;
    printf("listening for connections on port %d",port);

    return conn;
}



/*
 * Envoie un message_t complet sur une connexion TCP deja etablie.
 * Le nombre d'octets transmis correspond a l'en-tete message_t plus data[].
 */
int send_message(connection *connection, message_t *message)
{
    if (!connection || connection->sockfd < 0 || !message)
        return -1;

    size_t total_size = sizeof(message_t) + message->size;

    ssize_t sent = write(connection->sockfd, message, total_size);

    if (sent != (ssize_t)total_size) {
        perror("write");
        return -1;
    }

    return 0;
}

/*
 * Envoie un message de broadcast UDP sur le port donne.
 */
int send_broadcast_message(int port, message_t *message)
{
    if (!message) return -1;

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket broadcast");
        return -1;
    }

    int broadcastEnable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_BROADCAST, &broadcastEnable, sizeof(broadcastEnable)) < 0) {
        perror("setsockopt SO_BROADCAST");
        close(sockfd);
        return -1;
    }

    struct sockaddr_in broadcastAddr;
    memset(&broadcastAddr, 0, sizeof(broadcastAddr));
    broadcastAddr.sin_family = AF_INET;
    broadcastAddr.sin_port = htons(port);
    broadcastAddr.sin_addr.s_addr = inet_addr("255.255.255.255");

    size_t total_size = sizeof(message_t) + message->size;
    ssize_t sent = sendto(sockfd, message, total_size, 0, (struct sockaddr *)&broadcastAddr, sizeof(broadcastAddr));

    if (sent != (ssize_t)total_size) {
        perror("sendto broadcast");
        close(sockfd);
        return -1;
    }

    close(sockfd);
    return 0;
}
