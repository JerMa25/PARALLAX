CC      = gcc
CFLAGS  = -Wall -Wextra -g -pthread
TARGET  = parallax_agent

# Sources communes
SRCS = main.c \
       Agent_Init/init.c \
       Agent_Init/monitoring/Monitoring.c \
	   Agent_Init/network/*.c \
	   Controller/state_receiver/*.c \
	   Execution_Worker/worker_exec.c \
	   $(filter-out Execution_Master/utils/linked_list.c,\
             $(wildcard Execution_Master/utils/*.c))

# Détection OS
UNAME := $(shell uname)

ifeq ($(UNAME), Linux)
    CFLAGS += -DOS_LINUX
endif

ifeq ($(UNAME), Darwin)
    CFLAGS += -DOS_MACOS
endif

# Windows (MinGW)
ifeq ($(OS), Windows_NT)
    CFLAGS  += -DOS_WINDOWS
    LDFLAGS += -lpsapi -liphlpapi -lpdh
endif

# Includes
INCLUDES = -I./Agent_Init \
			-I./Agent_Init/network \
			-I./Agent_Init/monitoring \
		   	-I./Controller/state_receiver \
			-I./Execution_Worker \
			-I./parallax \
			-I./Execution_Master/utils


all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS)  $(SRCS) $(INCLUDES) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean