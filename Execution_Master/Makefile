all: bin/parallax

SRCS = utils/main.c utils/orchestrator.c utils/parallax_team.c utils/barrier.c utils/linked_list.c

bin/parallax: $(SRCS) | bin
	gcc $(SRCS) -o bin/parallax -Iutils -pthread

bin:
	mkdir -p bin

run: all
	./bin/parallax

clean:
	rm -rf bin