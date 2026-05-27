# Network Agent Module

This module provides an asynchronous, multi-threaded networking layer that bridges **System V Inter-Process Communication (IPC) Message Queues** and **TCP Sockets**. It is designed to effortlessly receive binary data over a network socket and route it directly to the appropriate local IPC queue, as well as pick up messages from an outgoing IPC queue and send them over a socket.

## Core Concepts

* **`message_t`**: The raw binary structure transmitted over TCP.
* **IPC Queues**: The system maps integer message types (e.g., `type = 1`) to string-named local IPC queues (e.g., queue `"1"`). 
* **Listener Thread**: Listens on port `9000`. When it receives a `message_t`, it reads the `type` field, converts it to a string, and places the payload into the local IPC queue with that matching string name.
* **Sender Thread**: Constantly monitors the local IPC queue named `"outgoing"`. When it finds a message there, it dynamically creates a TCP connection to the specified target and sends the data.

## 1. Initializing the Agent

Before sending or receiving over the network, you must start the agent. This spawns the listener and sender threads in the background.

```c
#include "network_agent.h"

int main() {
    // Starts the listener and sender threads in the background
    start();

    // ... your application logic ...

    // Cleanly shuts down the network agent and frees resources
    stop();
    return 0;
}
```

## 2. Receiving Messages

To receive a message of a specific `type` (e.g., `type = 42`) sent from another machine over the network:

1. **Create the target IPC queue.** The name of the queue *must* be the string equivalent of the integer type you expect.
2. **Read from the queue.** Use `msgrcv` with the expected `queued_message` struct.

```c
#include "ms_queue.h"
#include <sys/msg.h>
#include <stddef.h>

#define NETWORK_AGENT_MTYPE 1L

// Structure expected by the IPC queue
typedef struct {
    long mtype;
    uint64_t type;
    uint64_t size;
    char data[65536];
} queued_message;

void receive_example() {
    // 1. Create a queue named "42" to catch network messages where msg->type == 42
    char *mq_id = create_mq("42", 65536);
    int qid = find_by_msg_type(mq_id)->queue_id;
    
    queued_message msg;
    
    // 2. Wait for a message
    // Note: We use NETWORK_AGENT_MTYPE (1) to fetch the message
    ssize_t received = msgrcv(qid, &msg, sizeof(msg) - sizeof(long), NETWORK_AGENT_MTYPE, 0);
    
    if (received >= 0) {
        printf("Received %llu bytes!\n", (unsigned long long)msg.size);
        // Process msg.data here...
    }
}
```

## 3. Sending Messages

To send a message over the network to another node, you use the `send_msg` helper function provided by the network agent.

```c
#include "network_agent.h"
#include "socket.h"
#include <stdlib.h>
#include <string.h>

void send_example() {
    size_t payload_size = 14;
    
    // Allocate memory for the message structure + the flexible array data
    message_t *msg = (message_t *)malloc(sizeof(message_t) + payload_size);
    
    msg->type = 42; // The queue type the receiving machine is listening on
    msg->size = payload_size;
    memcpy(msg->data, "HELLO WORLD!!!", payload_size);

    // This pushes the message onto the "outgoing" IPC queue locally.
    // The background sender thread will immediately pick it up and transmit it 
    // over TCP to 127.0.0.1 on port 9001.
    send_msg("127.0.0.1", 9001, msg);

    free(msg);
}
```

### Note on C Struct Padding
Due to memory alignment padding inserted by the C compiler in structs like `outgoing_message` and `queued_message`, `network_agent.c` uses `offsetof(..., data)` to calculate exactly where the payload begins when interacting with `msgsnd` and `msgrcv`. This guarantees exact byte transmission without truncating trailing payloads.
