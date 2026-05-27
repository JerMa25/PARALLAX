#include "Monitoring.h"

#ifdef OS_LINUX
    #include "monitoring_linux.c"
#elif defined(OS_WINDOWS)
    #include "monitoring_windows.c"
#elif defined(OS_MACOS)
    #include "monitoring_macos.c"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>

// Global variables (Private State)
static MachineMetrics latest_metrics;
static pthread_mutex_t metrics_mutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int monitoring_running = 1;
static int first_run = 1;

void *monitoring_thread_run(void *arg){
    (void)arg; // avoids warning "Unused parameter"

    while(monitoring_running){
        MachineMetrics m;
        memset(&m, 0, sizeof(MachineMetrics)); // Initialize all fields to zero

        // Read static metrics only on first iteration
        if (first_run) {
            monitoring_read_static(&m);
            first_run = 0;
        }

        // Read all metrics
        read_cpu_usage(&m);
        read_memory(&m);
        read_disk(&m);
        read_network(&m);
        read_system(&m);
        compute_flags(&m);

        // Update timestamp
        m.timestamp = time(NULL);

        // Update global state
        pthread_mutex_lock(&metrics_mutex);
        latest_metrics = m;
        pthread_mutex_unlock(&metrics_mutex);

        sleep(MONITORING_INTERVAL);
    }

    return NULL;
}

MachineMetrics monitoring_get_latest(void){
    MachineMetrics m;
    pthread_mutex_lock(&metrics_mutex);
    m = latest_metrics; // Copy the latest metrics
    pthread_mutex_unlock(&metrics_mutex);
    return m;
}

void monitoring_stop(void){
    monitoring_running = 0;
}