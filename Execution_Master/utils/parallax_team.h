#ifndef PARALLAX_TEAM_H
#define PARALLAX_TEAM_H

#include"linked_list.h"
#include"pthread.h"
#include"barrier.h"
#include<stdbool.h>
#include"orchestrator.h"
#include"task.h"

typedef void * (* thread_func) (void *);

typedef struct {
    bool useGPU;
    int nbCPUS;


} compute_spec;






typedef struct {
    int tid;
    barrier_t * barrier;
    char function_name[64];
    chunk_data * chunk;
    worker_node * exec_node;
    void ** result_ptr;
} worker_context;


typedef struct {
    pthread_t tid;
    int id;
    thread_func func;
    worker_context * context;
    
    

} worker_t;


typedef struct {
    
    char * name;
    void * data;
    int thread_id;

} task_t;   

typedef struct {
    worker_t  * workers;
    task_t * tasks;

    barrier_t * barrier;
    void * (* reduce_fxn)(void * ,void *);
    int num_workers;
    void * * results;//will have an array with size =num_threads


} team;


team * team_init(int num_threads);
int team_start(team * team);
int team_wait(team * team);
void team_destroy(team * team);
void *team_reduce(team *t);

team *create_and_assign_task(task_assignment *assignments, int nb_assignments);





#endif