#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef void *(*fn)(void *);

void *sum_array(void *arg) {
    char *filename = (char*)arg;
    FILE *f = fopen(filename, "rb");
    if (!f) return strdup("0");
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    int count = size / sizeof(int);
    int *arr = malloc(size);
    fread(arr, sizeof(int), count, f);
    fclose(f);
    long long sum = 0;
    for(int i = 0; i < count; i++) {
        sum += arr[i];
    }
    free(arr);
    char *result = malloc(64);
    sprintf(result, "%lld", sum);
    return result;
}

fn matcher(char *name) {
    if (strcmp(name, "sum_array") == 0) {
        return sum_array;
    }
    return NULL;
}

int main() { return 0; }
