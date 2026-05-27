#include "network_thread_common.h"
#include <stdarg.h>

void log_info(const char *fmt, ...)
{
    va_list ap; va_start(ap, fmt);
    fprintf(stderr, "[net] "); vfprintf(stderr, fmt, ap); fprintf(stderr, "\n");
    va_end(ap);
}

static char *trim(char *s)
{
    while (isspace((unsigned char)*s)) s++;
    for (char *e = s + strlen(s); e > s && isspace((unsigned char)e[-1]); *--e = 0);
    return s;
}

static int hexval(char c)
{
    if ('0' <= c && c <= '9') return c - '0';
    if ('a' <= c && c <= 'f') return c - 'a' + 10;
    if ('A' <= c && c <= 'F') return c - 'A' + 10;
    return -1;
}

int uuid_from_hex(const char *hex, unsigned char *out)
{
    if (!hex || strlen(hex) < 32) return -1;
    for (int i = 0; i < UUID_LEN; i++) {
        int a = hexval(hex[2*i]), b = hexval(hex[2*i+1]);
        if (a < 0 || b < 0) return -1;
        out[i] = (unsigned char)((a << 4) | b);
    }
    return 0;
}

void uuid_to_hex(const unsigned char *uuid, char *out)
{
    static const char h[] = "0123456789abcdef";
    for (int i = 0; i < UUID_LEN; i++) out[2*i] = h[uuid[i] >> 4], out[2*i+1] = h[uuid[i] & 15];
    out[32] = 0;
}

int config_load(const char *path, net_config_t *cfg)
{
    FILE *f = fopen(path, "r"); char line[256], section[32] = ""; int ok = 0;
    memset(cfg, 0, sizeof(*cfg)); strcpy(cfg->role, "node"); cfg->udp_port = 7778;
    if (!f) { log_info("cannot open %s: %s", path, strerror(errno)); return -1; }
    while (fgets(line, sizeof line, f)) {
        char *p = trim(line); if (!*p || *p == '#' || *p == ';') continue;
        if (*p == '[') { sscanf(p, "[%31[^]]", section); continue; }
        char *eq = strchr(p, '='); if (!eq) continue; *eq = 0;
        char *k = trim(p), *v = trim(eq + 1);
        if (!strcmp(section, "node") && !strcmp(k, "uuid")) ok = !uuid_from_hex(v, cfg->uuid);
        else if (!strcmp(section, "node") && !strcmp(k, "role")) strncpy(cfg->role, v, sizeof cfg->role - 1);
        else if (!strcmp(section, "network") && !strcmp(k, "udp_port")) cfg->udp_port = atoi(v);
    }
    fclose(f); return ok ? 0 : -1;
}

void peer_upsert(net_state_t *st, const unsigned char *uuid, const char *ip, int port)
{
    peer_t *p = peer_find(st, uuid);
    if (!p && st->peer_count < MAX_PEERS) p = &st->peers[st->peer_count++];
    if (!p) return;
    memcpy(p->uuid, uuid, UUID_LEN); strncpy(p->ip, ip, sizeof p->ip - 1); p->port = port; p->seen = time(NULL);
}

peer_t *peer_find(net_state_t *st, const unsigned char *uuid)
{
    for (int i = 0; i < st->peer_count; i++)
        if (!memcmp(st->peers[i].uuid, uuid, UUID_LEN)) return &st->peers[i];
    return NULL;
}
