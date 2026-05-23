/*
 * fault_worker_secondary.c
 * Gestion des pannes côté Nœud SECONDAIRE — PARALLAX
 *
 * Responsabilités :
 *   1. Surveiller le maître primaire via NodeTable
 *        - silence > FM_SUSPECT_SEC → signalement
 *        - silence > FM_FAILED_SEC  → déclenchement promotion
 *   2. Attendre FM_ELECTION_TIMEOUT_MS puis confirmer la panne
 *   3. Se promouvoir maître (changer de rôle, notifier les workers,
 *      déclencher l'élection d'un nouveau secondaire)
 *
 * Intégration :
 *   - Lit NodeTable (state_receiver/node.h)
 *   - master_last_seen mis à jour par fm_on_heartbeat_received()
 *   - Notifie via ctx->on_master_promoted()
 *   - Appelle fm_master_trigger_election() après promotion
 *
 * Thread lancé par : fault_manager_thread_run()
 */

#define _POSIX_C_SOURCE 200809L

#include "fault_tolerance.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

/* Déclaré dans fault_master.c, utilisé après promotion */
extern void fm_master_watchdog_loop(fm_context_t *ctx);
extern void fm_master_trigger_election(fm_context_t *ctx);

/* ─────────────────────────────────────────────────────────────────────
 * SECTION A — DÉTECTION DE LA PANNE DU MAÎTRE
 * ───────────────────────────────────────────────────────────────────── */

/*
 * fm_secondary_is_master_alive
 * Retourne true si le maître primaire a envoyé un heartbeat
 * dans les FM_SUSPECT_SEC dernières secondes.
 *
 * Consulte NodeTable ET master_last_seen (mis à jour par
 * fm_on_heartbeat_received) pour deux niveaux de vérification.
 */
static bool fm_secondary_is_master_alive(fm_context_t *ctx)
{
    /* Vérification via ctx->master_last_seen (rapide, sans lock table) */
    pthread_mutex_lock(&ctx->lock);
    time_t last_seen = ctx->master_last_seen;
    char   master_uuid[FM_UUID_LEN];
    strncpy(master_uuid, ctx->master_uuid, FM_UUID_LEN - 1);
    master_uuid[FM_UUID_LEN - 1] = '\0';
    pthread_mutex_unlock(&ctx->lock);

    /* Si master_last_seen n'a jamais été initialisé : chercher dans table */
    if (last_seen == 0) {
        pthread_mutex_lock(&ctx->node_table->lock);
        NodeInfo *m = node_table_find(ctx->node_table, master_uuid);
        if (m) last_seen = m->last_heartbeat;
        pthread_mutex_unlock(&ctx->node_table->lock);
    }

    if (last_seen == 0)
        return false; /* jamais vu */

    return fm_elapsed_sec(last_seen) < (double)FM_SUSPECT_SEC;
}

/*
 * fm_secondary_confirm_master_down
 * Double-vérification après FM_ELECTION_TIMEOUT_MS.
 * Retourne true si le maître est toujours absent.
 */
static bool fm_secondary_confirm_master_down(fm_context_t *ctx)
{
    fprintf(stderr,
            "[FaultManager/Secondary] Attente confirmation panne maître "
            "(%d ms)...\n", FM_ELECTION_TIMEOUT_MS);

    /* Attente fractionnée : vérification toutes les secondes */
    int waited_ms = 0;
    while (waited_ms < FM_ELECTION_TIMEOUT_MS
           && atomic_load(&ctx->running))
    {
        sleep(1);
        waited_ms += 1000;

        /* Si le maître répond pendant l'attente → annuler */
        if (fm_secondary_is_master_alive(ctx)) {
            fprintf(stderr,
                    "[FaultManager/Secondary] Maître répond à nouveau "
                    "(après %d ms). Promotion annulée.\n", waited_ms);
            return false;
        }
    }

    /* Confirmation finale dans NodeTable */
    pthread_mutex_lock(&ctx->lock);
    char master_uuid[FM_UUID_LEN];
    strncpy(master_uuid, ctx->master_uuid, FM_UUID_LEN - 1);
    master_uuid[FM_UUID_LEN - 1] = '\0';
    pthread_mutex_unlock(&ctx->lock);

    pthread_mutex_lock(&ctx->node_table->lock);
    NodeInfo *m = node_table_find(ctx->node_table, master_uuid);
    bool still_dead = (!m || fm_elapsed_sec(m->last_heartbeat)
                            > (double)FM_FAILED_SEC);
    if (m && still_dead)
        m->status = NODE_EN_PANNE;
    pthread_mutex_unlock(&ctx->node_table->lock);

    return still_dead;
}

/* ─────────────────────────────────────────────────────────────────────
 * SECTION B — PROMOTION EN MAÎTRE
 * ───────────────────────────────────────────────────────────────────── */

/*
 * fm_secondary_promote_to_master
 * Promeut ce nœud secondaire en maître primaire.
 *
 * Étapes :
 *   1. Changer le rôle interne → FM_ROLE_MASTER
 *   2. Notifier tous les workers actifs (via callback on_master_promoted)
 *      → la couche réseau broadcastera MASTER_CHANGED avec new_ip/port
 *   3. Déclencher l'élection d'un nouveau secondaire
 *      (fm_master_trigger_election utilise la politique best_worker)
 *   4. Basculer la boucle watchdog → mode maître
 */
void fm_secondary_promote_to_master(fm_context_t *ctx)
{
    fprintf(stderr,
            "[FaultManager/Secondary] *** PROMOTION EN MAÎTRE *** "
            "(uuid=%.8s... ip=%s)\n",
            ctx->self_uuid, ctx->self_ip);

    /* ── 1. Changer le rôle ────────────────────────────────────────── */
    pthread_mutex_lock(&ctx->lock);
    ctx->self_role = FM_ROLE_MASTER;

    /* Mettre à jour le champ "master connu" avec nous-mêmes */
    strncpy(ctx->master_uuid, ctx->self_uuid, FM_UUID_LEN - 1);
    strncpy(ctx->master_ip,   ctx->self_ip,   sizeof(ctx->master_ip) - 1);
    ctx->master_last_seen = time(NULL);
    pthread_mutex_unlock(&ctx->lock);

    /* ── 2. Émettre l'événement de panne maître vers l'orchestrateur ─ */
    fm_fault_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.type      = FM_FAULT_MASTER_DOWN;
    evt.timestamp = time(NULL);

    pthread_mutex_lock(&ctx->lock);
    strncpy(evt.node_uuid, ctx->master_uuid, FM_UUID_LEN - 1);
    pthread_mutex_unlock(&ctx->lock);

    snprintf(evt.detail, FM_LOG_SIZE,
             "Maître primaire EN_PANNE. Secondaire %.8s... promu maître.",
             ctx->self_uuid);

    if (ctx->on_fault)
        ctx->on_fault(&evt, ctx->user_data);

    /* ── 3. Notifier la couche réseau (broadcast MASTER_CHANGED) ───── */
    if (ctx->on_master_promoted) {
        ctx->on_master_promoted(ctx->self_uuid,
                                ctx->self_ip,
                                ctx->master_port,
                                ctx->user_data);
    } else {
        fprintf(stderr,
                "[FaultManager/Secondary] WARN : on_master_promoted "
                "non enregistré — broadcast MASTER_CHANGED manquant.\n");
    }

    /* ── 4. Notifier les workers actifs dans NodeTable ──────────────── */
    int notified = 0;
    pthread_mutex_lock(&ctx->node_table->lock);
    for (NodeInfo *node = ctx->node_table->head; node; node = node->next) {
        if (strncmp(node->uuid, ctx->self_uuid, FM_UUID_LEN) == 0)
            continue;
        if (node->status == NODE_ACTIF || node->status == NODE_SURCHARGE) {
            fprintf(stderr,
                    "[FaultManager/Secondary→Master] "
                    "Notification worker=%.8s... : nouveau maître=%.8s...\n",
                    node->uuid, ctx->self_uuid);
            notified++;
            /*
             * En production : envoyer un message MASTER_CHANGED via
             * la socket ouverte vers ce worker (couche réseau de Ngonga).
             * Ici la notification passe par le callback on_master_promoted
             * qui reçoit la liste de workers depuis NodeTable.
             */
        }
    }
    pthread_mutex_unlock(&ctx->node_table->lock);

    fprintf(stderr,
            "[FaultManager/Secondary→Master] %d workers notifiés.\n",
            notified);

    /* ── 5. Élire un nouveau secondaire ─────────────────────────────── */
    fm_master_trigger_election(ctx);
}

/* ─────────────────────────────────────────────────────────────────────
 * SECTION C — WATCHDOG SECONDAIRE
 *
 * Surveille le maître primaire à la cadence FM_HEARTBEAT_SEC.
 * Machine à états :
 *
 *   maître ACTIF
 *     └─ silence > FM_SUSPECT_SEC → log SUSPECT
 *
 *   maître SUSPECT
 *     ├─ heartbeat reçu (fm_on_heartbeat_received) → ACTIF
 *     └─ silence > FM_FAILED_SEC
 *           └─ attente FM_ELECTION_TIMEOUT_MS
 *                ├─ maître répond → ACTIF (annulation)
 *                └─ toujours mort → fm_secondary_promote_to_master()
 *                                      └─ boucle bascule vers mode MAÎTRE
 * ───────────────────────────────────────────────────────────────────── */

/*
 * fm_secondary_watchdog_loop
 * Corps du thread watchdog côté secondaire.
 * Bascule automatiquement en mode maître après promotion.
 */
void fm_secondary_watchdog_loop(fm_context_t *ctx)
{
    fprintf(stderr,
            "[FaultManager/Secondary] Watchdog démarré — surveillance maître "
            "(intervalle=%ds, suspect=%ds, mort=%ds)\n",
            FM_HEARTBEAT_SEC, FM_SUSPECT_SEC, FM_FAILED_SEC);

    bool master_suspected = false;

    while (atomic_load(&ctx->running)) {
        sleep(FM_HEARTBEAT_SEC);

        if (!atomic_load(&ctx->running))
            break;

        /* ── Vérification du maître ────────────────────────────────── */
        pthread_mutex_lock(&ctx->lock);
        time_t last_seen = ctx->master_last_seen;
        pthread_mutex_unlock(&ctx->lock);

        /* Fallback : chercher dans NodeTable si master_last_seen non initialisé */
        if (last_seen == 0) {
            pthread_mutex_lock(&ctx->lock);
            char muuid[FM_UUID_LEN];
            strncpy(muuid, ctx->master_uuid, FM_UUID_LEN - 1);
            muuid[FM_UUID_LEN - 1] = '\0';
            pthread_mutex_unlock(&ctx->lock);

            pthread_mutex_lock(&ctx->node_table->lock);
            NodeInfo *m = node_table_find(ctx->node_table, muuid);
            if (m) last_seen = m->last_heartbeat;
            pthread_mutex_unlock(&ctx->node_table->lock);
        }

        double elapsed = (last_seen > 0) ? fm_elapsed_sec(last_seen) : 999.0;

        if (elapsed > (double)FM_FAILED_SEC) {
            /* ── Panne confirmée : démarrer séquence de promotion ───── */
            if (!master_suspected) {
                fprintf(stderr,
                        "[FaultManager/Secondary] Maître injoignable depuis "
                        "%.0fs (> %ds) → démarrage séquence de promotion.\n",
                        elapsed, FM_FAILED_SEC);
            }

            /* Double-confirmation avec délai d'élection */
            if (fm_secondary_confirm_master_down(ctx)) {
                fm_secondary_promote_to_master(ctx);

                /*
                 * Après promotion, ce thread devient le watchdog maître.
                 * fm_secondary_promote_to_master a déjà changé ctx->self_role.
                 */
                fprintf(stderr,
                        "[FaultManager] Rôle basculé → MASTER, "
                        "reprise de la boucle watchdog maître.\n");
                fm_master_watchdog_loop(ctx);

                /* fm_master_watchdog_loop retourne quand running=0 */
                return;
            }

            master_suspected = false; /* annulé */

        } else if (elapsed > (double)FM_SUSPECT_SEC && !master_suspected) {
            /* ── Passage en état SUSPECT ────────────────────────────── */
            master_suspected = true;
            fprintf(stderr,
                    "[FaultManager/Secondary] Maître SUSPECT "
                    "(silence=%.0fs > %ds).\n",
                    elapsed, FM_SUSPECT_SEC);

            /* Mettre à jour le statut dans NodeTable */
            pthread_mutex_lock(&ctx->lock);
            char muuid[FM_UUID_LEN];
            strncpy(muuid, ctx->master_uuid, FM_UUID_LEN - 1);
            pthread_mutex_unlock(&ctx->lock);

            pthread_mutex_lock(&ctx->node_table->lock);
            NodeInfo *m = node_table_find(ctx->node_table, muuid);
            if (m && m->status == NODE_ACTIF)
                m->status = NODE_SUSPECT;
            pthread_mutex_unlock(&ctx->node_table->lock);

        } else if (elapsed <= (double)FM_SUSPECT_SEC && master_suspected) {
            /* Maître revenu */
            master_suspected = false;
            fprintf(stderr,
                    "[FaultManager/Secondary] Maître récupéré "
                    "(silence=%.0fs < %ds).\n",
                    elapsed, FM_SUSPECT_SEC);
        }
    }

    fprintf(stderr, "[FaultManager/Secondary] Watchdog arrêté.\n");
}
