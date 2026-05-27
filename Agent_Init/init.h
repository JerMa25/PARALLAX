#ifndef INIT_H
#define INIT_H

#include <pthread.h>
#include <sys/stat.h>

// ===== CONSTANTS =====
#define UUID_FILE "../parallax/parallax_uuid.dat"
#define CONF_FILE "../parallax/parallax.conf"
#define CONFIG_CHECK_INTERVAL 2
#define UUID_LENGTH 37 // 36 chars + '\0'

// ===== ROLE =====
typedef enum {
    ROLE_UNKNOWN = 0,
    ROLE_WORKER = 1,
    ROLE_CONTROLLER = 2,
    ROLE_MASTER = 3
} AgentRole;

// ===== THREADS =====
typedef struct {
    // For Worker only

    // For Controller only
    pthread_t state_receiver;

    // For Master only

    // For Controller + Master

    // In common
    pthread_t monitoring;
    pthread_t network;

    // Activity flags
    int state_receiver_active;
    int monitoring_active;
    int network_active;
} AgentThreads;

// ===== AGENT STATE =====
typedef struct {
    char uuid[UUID_LENGTH];
    AgentRole role;
    AgentThreads threads;
} AgentState;

// ===== PUBLIC FUNCTIONS =====
void initialize_agent(void);
void stop_agent(void);

#endif // INIT_H