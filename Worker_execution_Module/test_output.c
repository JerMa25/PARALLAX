#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

int main() { return 0; }
