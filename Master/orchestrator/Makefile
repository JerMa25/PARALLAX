# Makefile pour parallax_orchestrator

CC      := gcc
CFLAGS  := -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion \
           -std=c11 -D_GNU_SOURCE -g -O0
INCLUDE := -I . -I ../execution_master

# Sources de production
SRCS := worker_table.c task_pool.c job_table.c scheduler.c orchestrator.c

# Source du pont thread (intégration avec init.c de Tchami)
THREAD_SRCS := orchestrator_thread.c

# Tests
TEST_BINS := test_worker_table test_task_pool test_job_table \
             test_scheduler test_orchestrator

.PHONY: all test memcheck clean demo smoke

all: $(TEST_BINS)

test_worker_table: worker_table.c test_worker_table.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

test_task_pool: task_pool.c test_task_pool.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

test_job_table: task_pool.c job_table.c test_job_table.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

test_scheduler: worker_table.c task_pool.c scheduler.c test_scheduler.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

test_orchestrator: $(SRCS) test_orchestrator.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

demo: demo_main.c $(SRCS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o demo

# Test d'intégration thread + IPC EM↔Orchestrator
smoke_test_em_orch: $(SRCS) $(THREAD_SRCS) smoke_test_em_orch.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -lpthread -o $@

smoke: smoke_test_em_orch
	./smoke_test_em_orch

test: $(TEST_BINS)
	@echo "=== worker_table ==="
	@./test_worker_table
	@echo "=== task_pool ==="
	@./test_task_pool
	@echo "=== job_table ==="
	@./test_job_table
	@echo "=== scheduler ==="
	@./test_scheduler
	@echo "=== orchestrator ==="
	@./test_orchestrator

memcheck: $(TEST_BINS)
	@for t in $(TEST_BINS); do \
		echo "=== memcheck: $$t ==="; \
		valgrind --leak-check=full --error-exitcode=1 --quiet ./$$t; \
	done

clean:
	rm -f $(TEST_BINS) demo smoke_test_em_orch *.o
