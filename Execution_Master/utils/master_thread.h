#ifndef MASTER_THREAD_H
#define MASTER_THREAD_H

#include <stdint.h>

#define MAX_PROGRAM_NAME 64
#define MAX_CODE_SIZE    7500

typedef struct {
    
    char program_name[MAX_PROGRAM_NAME];

    uint32_t code_size;

    char code[MAX_CODE_SIZE];

} program_message_t;


#endif