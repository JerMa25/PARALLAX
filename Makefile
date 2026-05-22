# Makefile pour parallax_orchestrator
#
# Cibles principales :
#   make           - compile tous les tests
#   make test      - compile et lance tous les tests
#   make memcheck  - lance les tests sous valgrind
#   make clean     - supprime les binaires
#
# On compile en mode strict pour attraper le maximum de bugs
# au plus tôt.

CC      := gcc
CFLAGS  := -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion \
           -std=c11 -g -O0
INCLUDE := -I include -I tests

# Sources de production
SRCS := src/worker_table.c src/task_pool.c src/job_table.c src/scheduler.c src/orchestrator.c

# Tests
TEST_BINS := tests/test_worker_table \
             tests/test_task_pool \
             tests/test_job_table \
             tests/test_scheduler \
             tests/test_orchestrator

all: $(TEST_BINS)

tests/test_worker_table: src/worker_table.c tests/test_worker_table.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

tests/test_task_pool: src/task_pool.c tests/test_task_pool.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

tests/test_job_table: src/task_pool.c src/job_table.c tests/test_job_table.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

tests/test_scheduler: src/worker_table.c src/task_pool.c src/scheduler.c tests/test_scheduler.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

tests/test_orchestrator: src/worker_table.c src/task_pool.c src/job_table.c src/scheduler.c src/orchestrator.c tests/test_orchestrator.c
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@

demo: demo_main.c $(SRCS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o demo

.PHONY: all test memcheck clean demo

test: $(TEST_BINS)
	@echo "=== worker_table ==="
	@./tests/test_worker_table
	@echo
	@echo "=== task_pool ==="
	@./tests/test_task_pool
	@echo
	@echo "=== job_table ==="
	@./tests/test_job_table
	@echo
	@echo "=== scheduler ==="
	@./tests/test_scheduler
	@echo
	@echo "=== orchestrator ==="
	@./tests/test_orchestrator

memcheck: $(TEST_BINS)
	@echo "=== memcheck: worker_table ==="
	@valgrind --leak-check=full --error-exitcode=1 --quiet ./tests/test_worker_table
	@echo
	@echo "=== memcheck: task_pool ==="
	@valgrind --leak-check=full --error-exitcode=1 --quiet ./tests/test_task_pool
	@echo
	@echo "=== memcheck: job_table ==="
	@valgrind --leak-check=full --error-exitcode=1 --quiet ./tests/test_job_table
	@echo
	@echo "=== memcheck: scheduler ==="
	@valgrind --leak-check=full --error-exitcode=1 --quiet ./tests/test_scheduler
	@echo
	@echo "=== memcheck: orchestrator ==="
	@valgrind --leak-check=full --error-exitcode=1 --quiet ./tests/test_orchestrator

clean:
	rm -f $(TEST_BINS) tests/*.o src/*.o
