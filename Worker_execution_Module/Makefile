all:
	gcc worker_exec.c ../network-thread/*.c  -I ../network-thread/ -o worker

clean:
	rm -f worker

test:
	gcc test_client.c ../network-thread/*.c  -I ../network-thread/ -o test