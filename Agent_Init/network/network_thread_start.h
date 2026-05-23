#include "network_thread_types.h"
int network_thread_run(net_state_t *st, const char *config_path);
void network_thread_recv_udp(net_state_t *st);
void network_thread_poll_queue(net_state_t *st);
