#ifndef BARRIER_H
#define BARRIER_H
#include<sys/syscall.h>
#include<unistd.h>
#include<linux/futex.h>
#include<stdatomic.h>
#include<stdlib.h>
typedef struct {
    atomic_int count;
    atomic_int generation;
    int total;
} barrier_t;


static void futex_wait(atomic_int * counter, int expected_value){
    syscall(
        SYS_futex,
        counter,
        FUTEX_WAIT,
        expected_value,
        NULL,
        NULL,
        0
    );
}


static void futex_wake(atomic_int * counter, int nb_to_wake){
    syscall(
        SYS_futex,
        counter,
        FUTEX_WAKE,
        nb_to_wake,
        NULL,
        NULL,
        0
    );
}

void barrier_wait(barrier_t *   barrier);
barrier_t * barrier_init(int count);

#endif