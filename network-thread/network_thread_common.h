#include "network_thread_types.h"
void log_info(const char *fmt, ...);
int config_load(const char *path, net_config_t *cfg);
int uuid_from_hex(const char *hex, unsigned char *out);
void uuid_to_hex(const unsigned char *uuid, char *out);
void peer_upsert(net_state_t *st, const unsigned char *uuid, const char *ip, int port);
peer_t *peer_find(net_state_t *st, const unsigned char *uuid);
