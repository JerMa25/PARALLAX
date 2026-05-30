#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef void *(*fn)(void *);

void *my_test_function(void *arg) {
    char *input = (char*)arg;
    printf("Executing my_test_function with data: %s\n", input);
    char *result = malloc(strlen(input) + 64);
    sprintf(result, "Processed data: [%s]", input);
    return result;
}

fn matcher(char *name) {
    if (strcmp(name, "my_test_function") == 0) {
        return my_test_function;
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 4) return 1;
    char *func_name = argv[1];
    int fd = atoi(argv[3]);
    fn func = matcher(func_name);
    if (func) {
        char *res = (char *)func(argv[2]);
        write(fd, res, strlen(res) + 1);
        free(res);
    }
    return 0;
}
