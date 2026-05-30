all: parallax master test test_exec

parallax: bin/parallax
master: bin/master_thread
test: bin/test_prog_client
test_exec: bin/test_master_exec
test_worker: bin/test_mock_worker

SRCS = utils/main.c utils/orchestrator.c utils/parallax_team.c utils/barrier.c ../Agent_Init/network/linked_list.c ../Agent_Init/network/network_agent.c ../Agent_Init/network/ms_queue.c ../Agent_Init/network/socket.c
MASTER_THREAD_SRCS = utils/master_thread.c ../Agent_Init/network/network_agent.c ../Agent_Init/network/ms_queue.c ../Agent_Init/network/socket.c ../Agent_Init/network/linked_list.c
TEST_SRCS = test_prog_client.c ../Agent_Init/network/network_agent.c ../Agent_Init/network/ms_queue.c ../Agent_Init/network/socket.c ../Agent_Init/network/linked_list.c
TEST_EXEC_SRCS = test_master_exec.c utils/master_exec.c utils/orchestrator.c utils/parallax_team.c utils/barrier.c ../Agent_Init/network/network_agent.c ../Agent_Init/network/ms_queue.c ../Agent_Init/network/socket.c ../Agent_Init/network/linked_list.c
TEST_WORKER_SRCS = test_mock_worker.c ../Agent_Init/network/network_agent.c ../Agent_Init/network/ms_queue.c ../Agent_Init/network/socket.c ../Agent_Init/network/linked_list.c

bin/parallax: $(SRCS) | bin
	gcc $(SRCS) -o bin/parallax -Iutils -I../Agent_Init/network -pthread

bin/master_thread: $(MASTER_THREAD_SRCS) | bin
	gcc $(MASTER_THREAD_SRCS) -o bin/master_thread -Iutils -I../Agent_Init/network -pthread

bin/test_prog_client: $(TEST_SRCS) | bin
	gcc $(TEST_SRCS) -o bin/test_prog_client -Iutils -I../Agent_Init/network -pthread

bin/test_master_exec: $(TEST_EXEC_SRCS) | bin
	gcc $(TEST_EXEC_SRCS) -o bin/test_master_exec -Iutils -I../Agent_Init/network -I../parallax -pthread

bin/test_mock_worker: $(TEST_WORKER_SRCS) | bin
	gcc $(TEST_WORKER_SRCS) -o bin/test_mock_worker -Iutils -I../Agent_Init/network -I../parallax -pthread

bin:
	mkdir -p bin

run: all
	./bin/parallax

clean:
	rm -rf bin