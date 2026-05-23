# ─── Makefile — fault_manager PARALLAX ───────────────────────────────────────
CC      = gcc
CFLAGS  = -Wall -Wextra -std=c11 -g -O2 \
          -D_POSIX_C_SOURCE=200809L \
          -I../../Controller/state_receiver
LDFLAGS = -lpthread

# Sources du module
SRCS = fault_tolerance.c \
       fault_master.c \
       fault_worker_secondary.c \
       fault_watchdog.c

# Stubs des dépendances (state_receiver) pour compilation autonome des tests
STUB_SRCS = stubs/node_table_stub.c

TEST_SRC = test_fault_tolerance.c
BIN      = ft_test

all: $(BIN)

$(BIN): $(SRCS) $(STUB_SRCS) $(TEST_SRC) fault_tolerance.h
	$(CC) $(CFLAGS) $(SRCS) $(STUB_SRCS) $(TEST_SRC) -o $(BIN) $(LDFLAGS)

clean:
	rm -f $(BIN) *.o stubs/*.o

.PHONY: all clean
