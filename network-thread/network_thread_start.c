#include "network_thread_start.h"
#include "network_thread_common.h"

static void udp_send(net_state_t *st, struct sockaddr_in *to, const char *s)
{ sendto(st->sockfd, s, strlen(s), 0, (struct sockaddr *)to, sizeof(*to)); }

static void handle_msg(net_state_t *st, char *buf)
{
    char *dst = buf + 4, *src, *payload;
    unsigned char duuid[UUID_LEN];
    src = strchr(dst, ':'); if (!src) return; *src++ = 0;
    payload = strchr(src, ':'); if (!payload) return; *payload++ = 0;
    if (strlen(dst) != 32 || strlen(src) != 32 || uuid_from_hex(dst, duuid)) return;
    if (!memcmp(duuid, st->cfg.uuid, UUID_LEN)) {
        mq_send(st->mq_in, payload, strlen(payload) + 1, 0);
        log_info("received from %s -> %s", src, payload);
    } else {
        peer_t *p = peer_find(st, duuid);
        if (!p) { log_info("unknown dst %s", dst); return; }
        struct sockaddr_in to = { .sin_family = AF_INET, .sin_port = htons(p->port) };
        inet_pton(AF_INET, p->ip, &to.sin_addr);
        udp_send(st, &to, buf);
        log_info("forwarded to %s", dst);
    }
}

int network_thread_start(net_state_t *st, const char *config_path)
{
    struct mq_attr a = { .mq_maxmsg = 10, .mq_msgsize = MAX_MSG };
    memset(st, 0, sizeof(*st));
    if (config_load(config_path, &st->cfg)) return -1;
    st->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (st->sockfd < 0) return -1;
    int yes = 1;
    setsockopt(st->sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    setsockopt(st->sockfd, SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes);
    struct sockaddr_in self = { .sin_family = AF_INET, .sin_port = htons(st->cfg.udp_port), .sin_addr.s_addr = htonl(INADDR_ANY) };
    if (bind(st->sockfd, (struct sockaddr *)&self, sizeof self) < 0) return -1;
    mq_unlink(Q_IN); mq_unlink(Q_OUT);
    st->mq_in = mq_open(Q_IN, O_CREAT | O_WRONLY | O_NONBLOCK, 0666, &a);
    st->mq_out = mq_open(Q_OUT, O_CREAT | O_RDONLY | O_NONBLOCK, 0666, &a);
    if (st->mq_in == (mqd_t)-1 || st->mq_out == (mqd_t)-1) return -1;
    st->running = 1;
    log_info("started role=%s udp=%d queues=%s,%s", st->cfg.role, st->cfg.udp_port, Q_IN, Q_OUT);
    return 0;
}

void network_thread_recv_udp(net_state_t *st)
{
    char buf[MAX_MSG];
    ssize_t n = recvfrom(st->sockfd, buf, sizeof(buf) - 1, MSG_DONTWAIT, NULL, NULL);
    if (n <= 0) return;
    buf[n] = 0;
    if (!strncmp(buf, "MSG:", 4)) handle_msg(st, buf);
}

void network_thread_poll_queue(net_state_t *st)
{
    char buf[MAX_MSG], dst[UUID_HEX], *payload; unsigned char duuid[UUID_LEN];
    while (mq_receive(st->mq_out, buf, sizeof buf, NULL) > 0) {
        payload = strchr(buf, ' ');
        if (!payload) { log_info("queue format: <dst_uuid> <message>"); continue; }
        *payload++ = 0; strncpy(dst, buf, sizeof dst - 1); dst[32] = 0;
        if (uuid_from_hex(dst, duuid)) continue;
        peer_t *p = peer_find(st, duuid);
        if (!p) { log_info("unknown peer %s", dst); continue; }
        char src[UUID_HEX], out[MAX_MSG];
        uuid_to_hex(st->cfg.uuid, src);
        snprintf(out, sizeof out, "MSG:%s:%s:%s", dst, src, payload);
        struct sockaddr_in to = { .sin_family = AF_INET, .sin_port = htons(p->port) };
        inet_pton(AF_INET, p->ip, &to.sin_addr);
        udp_send(st, &to, out);
        log_info("sent to %s @ %s:%d", dst, p->ip, p->port);
    }
}
