#include"logic.h"
#include<stdio.h>
#include<unistd.h>
#include<string.h>

__asm__(
    ".global worker_entry\n"
    "worker_entry:\n"
    "mov (%rsp), %edi\n"      // argc -> rdi (1st arg)
    "lea 8(%rsp), %rsi\n"     // argv -> rsi (2nd arg)
    "call worker_main\n"
);

void worker_main(int argc, char **argv) {
    printf("Coming from logic\n");

    if (argc < 2) {
        printf("No function name provided\n");
        _exit(1);
    }

    char *fxn_name = argv[1];
    printf("Function to execute: %s\n", fxn_name);

    if (argc > 2) {
        char *data_val = argv[2];
        printf("Data to process: %s\n", data_val);
    }

    void *ret = NULL;
    fn fxn = matcher(fxn_name);
    if (fxn) {
        ret = fxn(argc > 2 ? argv[2] : NULL);
    } else {
        printf("Function %s not found\n", fxn_name);
    }

    if (argc > 3) {
        int fd = 0;
        sscanf(argv[3], "%d", &fd);
        if (fd > 0) {
            if (ret) {
                write(fd, ret, strlen((char*)ret) + 1);
            } else {
                write(fd, "NULL", 5);
            }
            close(fd);
        }
    }

    _exit(0);
}