#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>  //  Nécessaire pour les threads
#include "fonctions.h"

#define PORT 8888
#define MASTER_ADDRESS "127.0.0.1"

// Variable globale pour contrôler l'état du Worker
volatile int running = 1; 

// 1. La fonction exécutée par le thread
void *execution_thread_run(void *arg) {
    int worker_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in master_addr;
    
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, MASTER_ADDRESS, &master_addr.sin_addr);
    
    printf("Worker Thread : Connexion au Master... \n");
    if (connect(worker_fd, (struct sockaddr *)&master_addr, sizeof(master_addr)) < 0) {
        perror("Échec de la connexion ");
        close(worker_fd);
        return NULL;
    }
    printf("Worker Thread : Connecté ! \n");

    // Une boucle qui tourne tant que worker_stop() n'est pas appelé
    while (running) {
        struct Task task;
        
        // On reçoit l'en-tête
        int bytes_received = recv(worker_fd, &task, sizeof(task), 0);
        if (bytes_received <= 0) {
            printf("Master déconnecté ou erreur \n");
            break; 
        }

        // Allocation et réception des données
        double *data = malloc(task.data_count * sizeof(double));
        recv(worker_fd, data, task.data_count * sizeof(double), 0);

        // Exécution
        fn fonction_calcul = matcher(task.name_function);
        if (fonction_calcul != NULL) {
            void *resultat = fonction_calcul(data);
            send(worker_fd, resultat, task.data_count * sizeof(double), 0);
        } else {
            printf("Erreur : Fonction '%s' inconnue \n", task.name_function);
        }

        free(data);
    }

    close(worker_fd);
    printf("Worker Thread : Arrêté proprement. \n");
    return NULL;
}

// 2. La fonction pour arrêter le Worker
void execution_stop() {
    printf("Signal d'arrêt reçu... \n");
    running = 0; // Fait sortir le thread de sa boucle while
}

// 3. Le main sert maintenant juste à lancer et tester le thread
int main() {
    pthread_t thread_id;

    // Lancement du thread du Worker
    if (pthread_create(&thread_id, NULL, execution_thread_run, NULL) != 0) {
        perror("Erreur de création du thread ");
        return 1;
    }

    // Le programme principal attend 10 secondes puis arrête le worker pour le test
    sleep(10);
    execution_stop();

    // On attend que le thread se termine proprement
    pthread_join(thread_id, NULL);

    return 0;
}