CC=gcc
CFLAGS=-std=gnu11 -Wall -Wextra -O2 -g
LDLIBS=-lrt -lpthread
SRCS=network_thread_common.c network_thread_start.c network_thread_shutdown.c
OBJS=$(SRCS:.c=.o)
TARGET=parallax_netthread
.PHONY: all lib clean
all: $(TARGET)
lib: libnetwork_thread.a
libnetwork_thread.a: $(OBJS)
	ar rcs $@ $^
$(TARGET): $(OBJS) main.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDLIBS)
%.o: %.c network_thread_types.h
	$(CC) $(CFLAGS) -c $< -o $@
clean:
	rm -f *.o *.a $(TARGET)
