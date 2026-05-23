#ifndef FONCTIONS_H
#define FONCTIONS_H

// Définition du type de pointeur de fonction
typedef void *(*fn)(void *);

// Énumération pour le type de message
typedef enum {
    MSG_FILE,
    MSG_TASK
} MessageType;

// Structure de la tâche envoyée par le Master
struct Task {
    MessageType type;     
    char name_function[32]; 
    int data_count;       
};

// Prototypes des fonctions pour que le Worker puisse les connaître
void *add(void *arg);
void *square(void *arg);
fn matcher(char *nom_fonction);

void *execution_thread_run(void *arg);
void execution_stop();

#endif