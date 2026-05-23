/*
 * fault_tolerance.c
 * Utilitaires communs du fault_manager PARALLAX
 *
 * Contient :
 *   - fm_init / fm_destroy
 *   - fm_elapsed_sec
 *   - fm_default_best_worker   (politique meilleur nœud)
 *   - fm_on_heartbeat_received (mise à jour NodeTable)
 *   - Helpers internes de log
 */

#define _POSIX_C_SOURCE 200809L

#include "fault_tolerance.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>

/* ─────────────────────────────────────────────────────────────────────
 * SECTION 1 — UTILITAIRES INTERNES
 * ───────────────────────────────────────────────────────────────────── */

/*
 * fm_elapsed_sec
 * Secondes écoulées depuis un timestamp time_t (last_heartbeat de NodeInfo).
 * On utilise time() et non CLOCK_MONOTONIC car NodeTable stocke time_t.
 */
double fm_elapsed_sec(time_t since)
{
    return difftime(time(NULL), since);
}

/* Chaîne lisible pour un type de panne */
static const char *fm_fault_str(fm_fault_type_t t)
{
    switch (t) {
        case FM_FAULT_HEARTBEAT_LOSS: return "HEARTBEAT_LOSS";
        case FM_FAULT_CPU_OVERLOAD:   return "CPU_OVERLOAD";
        case FM_FAULT_RAM_OVERLOAD:   return "RAM_OVERLOAD";
        case FM_FAULT_MASTER_DOWN:    return "MASTER_DOWN";
        case FM_FAULT_WORKER_CRASH:   return "WORKER_CRASH";
        default:                      return "UNKNOWN";
    }
}

/* Chaîne lisible pour un rôle */
static const char *fm_role_str(fm_role_t r)
{
    switch (r) {
        case FM_ROLE_MASTER:    return "MASTER";
        case FM_ROLE_SECONDARY: return "SECONDARY";
        case FM_ROLE_WORKER:    return "WORKER";
        default:                return "UNKNOWN";
    }
}

/*
 * fm_log_fault
 * Journalise un événement de panne sur stderr (horodatage ISO 8601),
 * puis appelle le callback on_fault si enregistré.
 */
static void fm_log_fault(fm_context_t *ctx, const fm_fault_event_t *evt)
{
    char timebuf[32];
    struct tm *tm_info = localtime(&evt->timestamp);
    strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%S", tm_info);

    fprintf(stderr,
            "[%s][FAULT_MANAGER] %-20s | role=%-9s | node=%.8s... | %s\n",
            timebuf,
            fm_fault_str(evt->type),
            fm_role_str(ctx->self_role),
            evt->node_uuid,
            evt->detail);

    if (ctx->on_fault)
        ctx->on_fault(evt, ctx->user_data);
}

/* ─────────────────────────────────────────────────────────────────────
 * SECTION 2 — INITIALISATION / DESTRUCTION
 * ───────────────────────────────────────────────────────────────────── */

int fm_init(fm_context_t  *ctx,
            fm_role_t      role,
            const char    *self_uuid,
            const char    *self_ip,
            NodeTable     *node_table)
{
    if (!ctx || !self_uuid || !self_ip || !node_table)
        return -1;

    memset(ctx, 0, sizeof(fm_context_t));

    strncpy(ctx->self_uuid, self_uuid, FM_UUID_LEN - 1);
    strncpy(ctx->self_ip,   self_ip,   sizeof(ctx->self_ip) - 1);
    ctx->self_role  = role;
    ctx->node_table = node_table;

    atomic_store(&ctx->running, 0);

    if (pthread_mutex_init(&ctx->lock, NULL) != 0) {
        perror("[FaultManager] pthread_mutex_init");
        return -1;
    }

    fprintf(stderr,
            "[FaultManager] Initialisé — uuid=%.8s... role=%s\n",
            ctx->self_uuid, fm_role_str(role));
    return 0;
}

void fm_destroy(fm_context_t *ctx)
{
    if (!ctx) return;

    /* Arrêt propre si encore en cours */
    if (atomic_load(&ctx->running))
        fault_manager_stop(ctx);

    pthread_mutex_destroy(&ctx->lock);
    fprintf(stderr, "[FaultManager] Détruit.\n");
}

/* ─────────────────────────────────────────────────────────────────────
 * SECTION 3 — POLITIQUE DU MEILLEUR NŒUD
 *
 * Score = (1 - cpu_usage) * (1 - ram_usage)
 *
 * Aligné sur :
 *   - master_score_worker()     dans l'ancien fault_master.c
 *   - scheduler_compute_score() dans orchestrator/scheduler.h
 *   - update_heartbeat()        dans state_receiver.c (seuil 0.85)
 *
 * Un nœud avec cpu_usage=0.20 et ram_usage=0.30
 * obtient  score = 0.80 * 0.70 = 0.56
 * Un nœud avec cpu_usage=0.90 obtient ≈ 0.07 → écarté.
 * ───────────────────────────────────────────────────────────────────── */

void fm_default_best_worker(const fm_worker_snapshot_t *candidates,
                             int n_candidates,
                             char *out_uuid,
                             void *user_data)
{
    (void)user_data;

    out_uuid[0] = '\0';

    if (!candidates || n_candidates <= 0)
        return;

    float best_score = -1.0f;
    int   best_idx   = -1;

    for (int i = 0; i < n_candidates; i++) {
        const fm_worker_snapshot_t *w = &candidates[i];

        /* Ignorer les nœuds non actifs */
        if (w->status != NODE_ACTIF)
            continue;

        /* Ignorer les nœuds déjà surchargés */
        if (w->cpu_usage > FM_CPU_OVERLOAD || w->ram_usage > FM_RAM_OVERLOAD)
            continue;

        float score = (1.0f - w->cpu_usage) * (1.0f - w->ram_usage);

        if (score > best_score) {
            best_score = score;
            best_idx   = i;
        }
    }

    if (best_idx >= 0) {
        strncpy(out_uuid, candidates[best_idx].uuid, FM_UUID_LEN - 1);
        out_uuid[FM_UUID_LEN - 1] = '\0';

        fprintf(stderr,
                "[FaultManager] Meilleur nœud : uuid=%.8s... score=%.3f "
                "(cpu=%.1f%% ram=%.1f%%)\n",
                out_uuid,
                best_score,
                candidates[best_idx].cpu_usage * 100.0f,
                candidates[best_idx].ram_usage * 100.0f);
    } else {
        fprintf(stderr,
                "[FaultManager] Aucun nœud disponible pour migration.\n");
    }
}

/* ─────────────────────────────────────────────────────────────────────
 * SECTION 4 — RÉCEPTION D'UN HEARTBEAT
 *
 * Appelé par state_receiver ou la couche réseau à chaque NetworkMessage
 * de type MSG_HEARTBEAT ou MSG_HEARTBEAT_INIT.
 *
 * Met à jour last_heartbeat et les métriques du nœud dans NodeTable,
 * puis recalcule le NodeStatus (NODE_ACTIF / NODE_SURCHARGE).
 *
 * Si le nœud est le maître primaire (contexte secondaire) :
 *   met à jour master_last_seen pour réinitialiser le watchdog.
 * ───────────────────────────────────────────────────────────────────── */

void fm_on_heartbeat_received(fm_context_t        *ctx,
                               const NetworkMessage *msg)
{
    if (!ctx || !msg) return;

    /* ── Mise à jour dans NodeTable ─────────────────────────────────── */
    pthread_mutex_lock(&ctx->node_table->lock);

    NodeInfo *node = node_table_find(ctx->node_table, msg->uuid);
    if (!node) {
        /* Nœud inconnu : on l'enregistre à la volée.
         * En production, ce cas est géré par MSG_HELLO. On l'insère
         * ici pour la robustesse (heartbeat avant HELLO). */
        node = node_table_add(ctx->node_table, msg->uuid,
                              msg->ip, msg->port);
        if (!node) {
            pthread_mutex_unlock(&ctx->node_table->lock);
            fprintf(stderr,
                    "[FaultManager] WARN : impossible d'ajouter nœud %s\n",
                    msg->uuid);
            return;
        }
        fprintf(stderr,
                "[FaultManager] Nouveau nœud enregistré via heartbeat : "
                "uuid=%.8s... ip=%s\n", msg->uuid, msg->ip);
    }

    /* Mise à jour des métriques dynamiques */
    node->last_heartbeat       = time(NULL);
    node->metrics.cpu_usage    = msg->cpu_usage;
    node->metrics.ram_usage    = msg->ram_usage;
    node->metrics.ram_used_mb  = msg->ram_used_mb;
    node->metrics.disk_usage   = msg->disk_usage;
    node->metrics.disk_used_gb = msg->disk_used_gb;
    node->metrics.queue_len    = msg->queue_len;
    node->metrics.score        = msg->score;
    node->metrics.load_avg     = msg->load_avg;

    /* Recalcul du statut */
    if (msg->cpu_usage > FM_CPU_OVERLOAD || msg->ram_usage > FM_RAM_OVERLOAD) {
        node->status = NODE_SURCHARGE;
    } else {
        /* Remettre actif même s'il était SUSPECT (heartbeat reçu) */
        if (node->status == NODE_SUSPECT || node->status == NODE_EN_PANNE)
            fprintf(stderr,
                    "[FaultManager] Nœud %.8s... récupéré → NODE_ACTIF\n",
                    msg->uuid);
        node->status = NODE_ACTIF;
    }

    /* Hardware (MSG_HEARTBEAT_INIT uniquement) */
    if (msg->type == MSG_HEARTBEAT_INIT && !node->hardware.initialized) {
        node->hardware.cpu_cores            = msg->cpu_cores;
        node->hardware.cpu_threads_per_core = msg->cpu_threads_per_core;
        node->hardware.cpu_freq_mhz         = msg->cpu_freq_mhz;
        node->hardware.ram_total_mb         = msg->ram_total_mb;
        node->hardware.disk_total_gb        = msg->disk_total_gb;
        strncpy(node->hardware.cpu_model,
                msg->cpu_model,     sizeof(node->hardware.cpu_model)     - 1);
        strncpy(node->hardware.disk_mount,
                msg->disk_mount,    sizeof(node->hardware.disk_mount)    - 1);
        strncpy(node->hardware.network_iface,
                msg->network_iface, sizeof(node->hardware.network_iface) - 1);
        node->hardware.initialized = 1;
    }

    pthread_mutex_unlock(&ctx->node_table->lock);

    /* ── Si c'est le maître : réinitialiser master_last_seen ────────── */
    pthread_mutex_lock(&ctx->lock);
    if (strncmp(ctx->master_uuid, msg->uuid, FM_UUID_LEN) == 0)
        ctx->master_last_seen = time(NULL);
    pthread_mutex_unlock(&ctx->lock);
}
