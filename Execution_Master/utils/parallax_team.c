#include "parallax_team.h"
#include "barrier.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "ms_queue.h"
#include "network_agent.h"
#include <sys/msg.h>

typedef struct {
  char function_name[64];
  uint64_t data_count;
  uint8_t data[];
} recv_task_t;

typedef struct {
  char prog_name[64];
  char prog_code[7500];
} prog_t;

int check_program_exists(char *ip, int port, const char *prog_name,
                         char *task_mq_name) {
  size_t total_size = sizeof(message_t) + strlen(prog_name) + 1;
  message_t *msg = malloc(total_size);
  memset(msg, 0, total_size);

  msg->mq_type = 1;
  
  strcpy(msg->sender_ip, "127.0.0.1");
  msg->sender_port = 9005;
  strcpy(msg->type, "PROG");
  char *recv_q = create_mq(NULL, 0); // dynamically generate queue name
  snprintf(msg->recv_type, sizeof(msg->recv_type), "CHCK:%s", recv_q);
  msg->size = strlen(prog_name) + 1;
  strcpy(msg->data, prog_name);

  map_entry *entry = find_by_msg_type(recv_q);

  send_msg(ip, port, "master_out", msg);
  printf("[Master] Sent CHCK message for %s to worker_exec.\n", prog_name);
  free(msg);

  queued_message resp_msg;
  ssize_t received = msgrcv(entry->queue_id, &resp_msg,
                            sizeof(resp_msg) - sizeof(long), 1L, 0);
  if (received < 0) {
    perror("msgrcv CHCK");
    return -1;
  }

  message_t *resp = (message_t *)&resp_msg;
  strcpy(task_mq_name, resp->data);
  printf("[Master] Received CHCK response: %s\n", task_mq_name);

  if (strcmp(task_mq_name, "NONE") == 0)
    return 0;
  return 1;
}

int send_prog_message_and_wait(char *ip, int port, char *task_mq_name) {
  prog_t prog;
  memset(&prog, 0, sizeof(prog));
  strcpy(prog.prog_name, "test_output");
  strcpy(
      prog.prog_code,
      "#include <stdio.h>\n"
      "#include <stdlib.h>\n"
      "#include <string.h>\n"
      "\n"
      "typedef void *(*fn)(void *);\n"
      "\n"
      "void *my_test_function(void *arg) {\n"
      "    char *input = (char*)arg;\n"
      "    printf(\"Executing my_test_function with data: %s\\n\", input);\n"
      "    char *result = malloc(strlen(input) + 64);\n"
      "    sprintf(result, \"Processed data: [%s]\", input);\n"
      "    return result;\n"
      "}\n"
      "\n"
      "fn matcher(char *name) {\n"
      "    if (strcmp(name, \"my_test_function\") == 0) {\n"
      "        return my_test_function;\n"
      "    }\n"
      "    return NULL;\n"
      "}\n"
      "\n"
      "int main() { return 0; }\n"
  );

  size_t total_size =
      sizeof(message_t) + sizeof(prog.prog_name) + strlen(prog.prog_code) + 1;
  message_t *msg = malloc(total_size);
  memset(msg, 0, total_size);

  msg->mq_type = 1;
  strcpy(msg->sender_ip, "127.0.0.1");
  msg->sender_port = 9005;
  strcpy(msg->type, "PROG");
  char *recv_q = create_mq(NULL, 0);
  snprintf(msg->recv_type, sizeof(msg->recv_type), "PROG:%s", recv_q);
  msg->size = sizeof(prog.prog_name) + strlen(prog.prog_code) + 1;
  memcpy(msg->data, &prog, msg->size);

  send_msg(ip, port, "master_out", msg);
  printf("[Master] Sent PROG message to worker_exec.\n");
  free(msg);

  map_entry *entry = find_by_msg_type(recv_q);
  queued_message resp_msg;
  ssize_t received = msgrcv(entry->queue_id, &resp_msg,
                            sizeof(resp_msg) - sizeof(long), 1L, 0);
  if (received < 0) {
    perror("msgrcv RESP");
    return -1;
  }

  message_t *resp = (message_t *)&resp_msg;
  strcpy(task_mq_name, resp->data);
  printf("[Master] Received PROG compilation response: %s\n", task_mq_name);
  return 0;
}

int send_task_message_and_wait(char *ip, int port, const char *task_mq_name,
                               const char *function_name,
                               chunk_data *chunk, void **result_ptr) {
  size_t task_size = sizeof(recv_task_t) + chunk->chunk_size;

  recv_task_t *task = malloc(task_size);
  memset(task, 0, task_size);
  strcpy(task->function_name, function_name);
  task->data_count = 1;
  memcpy(task->data, chunk->chunk, chunk->chunk_size);

  size_t total_size = sizeof(message_t) + task_size;
  message_t *msg = malloc(total_size);
  memset(msg, 0, total_size);

  msg->mq_type = 1;
  strcpy(msg->sender_ip, "127.0.0.1");
  msg->sender_port = 9005;
  strcpy(msg->type, task_mq_name);
  char *recv_q = create_mq(NULL, 0);
  strcpy(msg->recv_type, recv_q);
  msg->size = task_size;
  memcpy(msg->data, task, task_size);

  send_msg(ip, port, "master_out", msg);
  printf("[Master] Sent recv_task_t using target msg_type: %s\n", task_mq_name);

  free(task);
  free(msg);

  printf("[Master] Waiting for task execution result on queue %s...\n", recv_q);
  map_entry *entry = find_by_msg_type(recv_q);
  queued_message resp_msg;
  ssize_t received = msgrcv(entry->queue_id, &resp_msg,
                            sizeof(resp_msg) - sizeof(long), 1L, 0);
  if (received < 0) {
    perror("msgrcv RESP");
    return -1;
  }

  message_t *resp = (message_t *)&resp_msg;
  printf("[Master] Task result: %s\n", resp->data);

  // Save partial result to the array via the provided pointer
  if (result_ptr) {
    *result_ptr = strdup((char *)resp->data);
  }
  return 0;
}

void *thread_func_test(void *arg) {

  worker_context *param = (worker_context *)arg;

  printf("Thread with tid %d started\n", param->tid);

  printf("to execute function %s\n", param->function_name);

  printf("with ip %s\n", param->exec_node->ip);

  printf("sending to node with uuid %s\n", param->exec_node->uuid);

  chunk_data *chunk = param->chunk;

  int *data = (int *)chunk->chunk;

  int count = chunk->chunk_size / sizeof(int);

  printf("Chunk data: ");

  for (int i = 0; i < count; i++) {
    printf("%d ", data[i]);
  }

  printf("\n");


  sleep(param->tid * 3);

  printf("Thread with tid %d resumed and died\n", param->tid);

  // Handshake and Execute
  char task_mq_name[64];
  memset(task_mq_name, 0, sizeof(task_mq_name));

  printf("Checking if program exists on worker %s:%d\n", param->exec_node->ip,
         param->exec_node->port);
  int exists =
      check_program_exists(param->exec_node->ip, param->exec_node->port,
                           "test_output", task_mq_name);

  if (exists == 0) {
    printf("Program missing. Sending source code to worker...\n");
    if (send_prog_message_and_wait(param->exec_node->ip, param->exec_node->port,
                                   task_mq_name) < 0) {
      printf("Failed to send program.\n");
    }
  } else if (exists > 0) {
    printf("[Master] Program already exists on worker, skipping PROG send.\n");
  } else {
    printf("Failed to check program existence.\n");
  }

  printf("Sending task payload to worker task queue: %s\n", task_mq_name);
  if (send_task_message_and_wait(param->exec_node->ip, param->exec_node->port,
                                 task_mq_name, param->function_name, param->chunk,
                                 param->result_ptr) < 0) {
    printf("Failed to send task.\n");
  }
  barrier_wait(param->barrier);

  return NULL;
}

/*
int main(){



    pthread_t thread1;#include "parallax_team.h"
    pthread_t thread2;
    pthread_t thread3;
    barrier_t * barrier=barrier_init(3);

    thread_param param1,param2,param3;
    param1.tid=1;
    param1.barrier=barrier;
    param2.tid=2;
    param2.barrier=barrier;
    param3.tid=3;
    param3.barrier=barrier;

    printf("Main thread started\n");
    pthread_create(&thread1,NULL,thread_func_test, &param1);
    pthread_create(&thread2,NULL,thread_func_test, &param2);
    pthread_create(&thread3,NULL,thread_func_test, &param3);


    pthread_join(thread1,NULL);
    pthread_join(thread2,NULL);
    pthread_join(thread3,NULL);
    printf("Main thread Continued\n");



   team * t=team_init(3);
   team_start(t);
   team_wait(t);
   team_destroy(t);

}
*/

team *team_init(int nb_threads) {

  team *new_team = (team *)malloc(sizeof(team));

  new_team->num_workers = nb_threads;

  new_team->workers = malloc(sizeof(worker_t) * nb_threads);
  new_team->barrier = barrier_init(nb_threads);
  new_team->results = calloc(nb_threads, sizeof(void *));

  for (int i = 0; i < nb_threads; i++) {

    new_team->workers[i].id = i;

    new_team->workers[i].func = thread_func_test;

    worker_context *context = (worker_context *)malloc(sizeof(worker_context));

    context->tid = i;
    context->barrier = new_team->barrier;
    context->chunk = NULL;
    context->result_ptr = &new_team->results[i]; // Bind array slot

    new_team->workers[i].context = context;
  }

  new_team->reduce_fxn = NULL;
  new_team->tasks = NULL;

  return new_team;
}

int team_start(team *team) {

  for (int i = 0; i < team->num_workers; i++) {

    pthread_create(&team->workers[i].tid, NULL, team->workers[i].func,
                   (void *)team->workers[i].context);
  }

  return 0;
}

int team_wait(team *team) {
  for (int i = 0; i < team->num_workers; i++) {
    pthread_join(team->workers[i].tid, NULL);
  }
  return 0;
}

void team_destroy(team *t) {
  if (!t)
    return;

  for (int i = 0; i < t->num_workers; i++) {
    free(t->workers[i].context);
  }

  free(t->workers);
  free(t->results);
  free(t->barrier);

  free(t);
}

team *create_and_assign_task(task_assignment *assignments, int nb_assignments) {
  team *t = team_init(nb_assignments);

  for (int i = 0; i < nb_assignments; i++) {

    t->workers[i].context->exec_node = assignments[i].target_node;

    t->workers[i].context->chunk = assignments[i].chunk;

    strncpy(t->workers[i].context->function_name,
            assignments[i].task->function_name,
            sizeof(t->workers[i].context->function_name) - 1);

    t->workers[i]
        .context
        ->function_name[sizeof(t->workers[i].context->function_name) - 1] =
        '\0';
  }

  return t;
}