/*
 * fault_master.c
 * Gestion des pannes côté Nœud MAÎTRE — PARALLAX
 *
 * Responsabilités :
 *   1. Watchdog : surveille périodiquement la NodeTable
 *        - worker silence > FM_SUSPECT_SEC → NODE_SUSPECT
 *        - worker silence > FM_FAILED_SEC  → NODE_EN_PANNE  → migration
 *        - secondaire tombé                 → déclenche élection
 *   2. Migration des tâches d'un worker mort vers le meilleur nœud
 *      disponible (politique : score = (1-cpu)*(1-ram))
 *   3. Élection d'un nouveau secondaire via Bully simplifié
 *      (meilleur score parmi les workers ACTIFS)
 *
 * Intégration :
 *   - Lit NodeTable (state_receiver/node.h)
 *   - Notifie l'orchestrateur via ctx->on_fault  (EVT_WORKER_FAILED)
 *   - Le callback best_worker_for_migration permet à l'orchestrateur
 *     d'injecter sa propre politique (défaut : fm_default_best_worker)
 *
 * Thread lancé par : fault_manager_thread_run()
 *                    (appelé depuis fault_watchdog.c)
 */

#define _POSIX_C_SOURCE 200809L

#include "fault_tolerance.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

/* ─────────────────────────────────────────────────────────────────────
 * HELPERS INTERNES
 * ───────────────────────────────────────────────────────────────────── */

/*
 * collect_worker_snapshots
 * Prend un snapshot atomique des workers de NodeTable.
 * Exclut le nœud dont l'UUID est failed_uuid (nœud venant de tomber).
 * Remplit out[0..n-1] et retourne le nombre de snapshots copiés.
 *
 * Appelé sous ctx->node_table->lock.
 */
static int collect_worker_snapshots(NodeTable            *table,
                                    const char           *failed_uuid,
                                    fm_worker_snapshot_t *out,
                                    int                   max_out)
{
    int n = 0;
    for (NodeInfo *node = table->head; node && n < max_out; node = node->next) {

        /* Exclure le nœud défaillant */
        if (failed_uuid && strncmp(node->uuid, failed_uuid, FM_UUID_LEN) == 0)
            continue;

        /* N'inclure que les nœuds actifs ou suspects (pas encore confirmé mort) */
        if (node->status != NODE_ACTIF && node->status != NODE_SURCHARGE)
            continue;

        fm_worker_snapshot_t *s = &out[n++];
        strncpy(s->uuid, node->uuid, FM_UUID_LEN - 1);
        s->uuid[FM_UUID_LEN - 1] = '\0';
        strncpy(s->ip, node->ip, sizeof(s->ip) - 1);
        s->port            = node->port;
        s->status          = node->status;
        s->last_heartbeat  = node->last_heartbeat;
        s->cpu_usage       = node->metrics.cpu_usage;
        s->ram_usage       = node->metrics.ram_usage;
        s->score           = node->metrics.score;
    }
    return n;
}

/*
 * fm_master_select_target
 * Choisit le meilleur nœud cible pour une migration.
 * Utilise le callback injecté ou fm_default_best_worker par défaut.
 * Retourne 0 si un nœud est trouvé (out_uuid rempli), -1 sinon.
 */
static int fm_master_select_target(fm_context_t *ctx,
                                   const char   *failed_uuid,
                                   char         *out_uuid)
{
    /* Snapshot des workers disponibles */
    fm_worker_snapshot_t candidates[FM_MAX_WORKERS];
    int n = 0;

    pthread_mutex_lock(&ctx->node_table->lock);
    n = collect_worker_snapshots(ctx->node_table, failed_uuid,
                                 candidates, FM_MAX_WORKERS);
    pthread_mutex_unlock(&ctx->node_table->lock);

    if (n == 0) {
        out_uuid[0] = '\0';
        return -1;
    }

    /* Appel à la politique de sélection */
    if (ctx->best_worker_for_migration) {
        ctx->best_worker_for_migration(candidates, n, out_uuid, ctx->user_data);
    } else {
        fm_default_best_worker(candidates, n, out_uuid, NULL);
    }

    return (out_uuid[0] != '\0') ? 0 : -1;
}

/* ─────────────────────────────────────────────────────────────────────
 * SECTION A — GESTION D'UNE PANNE WORKER
 * ───────────────────────────────────────────────────────────────────── */

/*
 * fm_master_handle_worker_failure
 * Point d'entrée principal quand un worker est confirmé EN_PANNE.
 *
 * Algorithme :
 *   1. Marquer le nœud NODE_EN_PANNE dans NodeTable
 *   2. Notifier l'orchestrateur (EVT_WORKER_FAILED via on_fault)
 *   3. Sélectionner le meilleur nœud cible (politique score)
 *   4. Si cible trouvée → demander la migration (EVT_WORKER_FAILED
 *      porte le node_uuid de la cible dans le champ detail)
 *   5. Sinon → journaliser l'impossibilité (tâches resteront PENDING
 *      dans task_pool côté orchestrateur)
 */
void fm_master_handle_worker_failure(fm_context_t *ctx,
                                     const char   *worker_uuid)
{
    if (!ctx || !worker_uuid) return;

    fprintf(stderr,
            "[FaultManager/Master] Prise en charge panne worker=%.8s...\n",
            worker_uuid);

    /* ── 1. Marquer EN_PANNE dans NodeTable ──────────────────────── */
    pthread_mutex_lock(&ctx->node_table->lock);
    NodeInfo *node = node_table_find(ctx->node_table, worker_uuid);
    if (node)
        node->status = NODE_EN_PANNE;
    pthread_mutex_unlock(&ctx->node_table->lock);

    /* ── 2. Notifier l'orchestrateur ─────────────────────────────── */
    fm_fault_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.type      = FM_FAULT_HEARTBEAT_LOSS;
    evt.timestamp = time(NULL);
    strncpy(evt.node_uuid, worker_uuid, FM_UUID_LEN - 1);
    snprintf(evt.detail, FM_LOG_SIZE,
             "Worker %.8s... EN_PANNE (silence > %ds). "
             "Redistribution des tâches requise.",
             worker_uuid, FM_FAILED_SEC);

    if (ctx->on_fault)
        ctx->on_fault(&evt, ctx->user_data);

    fprintf(stderr,
            "[FaultManager/Master] %s\n", evt.detail);

    /* ── 3. Trouver le meilleur nœud cible ───────────────────────── */
    char target_uuid[FM_UUID_LEN] = {0};
    int  rc = fm_master_select_target(ctx, worker_uuid, target_uuid);

    if (rc < 0) {
        fprintf(stderr,
                "[FaultManager/Master] WARN : aucun worker disponible "
                "pour migration des tâches de %.8s... "
                "→ tâches resteront PENDING.\n",
                worker_uuid);

        /* On renvoie un second événement pour informer l'orchestrateur
         * qu'aucune migration n'est possible immédiatement. */
        fm_fault_event_t no_target_evt;
        memset(&no_target_evt, 0, sizeof(no_target_evt));
        no_target_evt.type      = FM_FAULT_WORKER_CRASH;
        no_target_evt.timestamp = time(NULL);
        strncpy(no_target_evt.node_uuid, worker_uuid, FM_UUID_LEN - 1);
        snprintf(no_target_evt.detail, FM_LOG_SIZE,
                 "Cluster saturé : aucune cible de migration disponible "
                 "pour les tâches de %.8s...", worker_uuid);

        if (ctx->on_fault)
            ctx->on_fault(&no_target_evt, ctx->user_data);
        return;
    }

    /* ── 4. Signaler la cible de migration ───────────────────────── */
    fprintf(stderr,
            "[FaultManager/Master] Migration → nœud cible=%.8s...\n",
            target_uuid);

    /*
     * L'orchestrateur reçoit un deuxième événement FM_FAULT_NONE
     * dont le detail encode "MIGRATE:<failed_uuid>→<target_uuid>".
     * L'orchestrateur (RFC-001 : scheduler + task_pool) effectue
     * la reassignation réelle des tâches PENDING/ASSIGNED.
     */
    fm_fault_event_t migrate_evt;
    memset(&migrate_evt, 0, sizeof(migrate_evt));
    migrate_evt.type      = FM_FAULT_NONE;
    migrate_evt.timestamp = time(NULL);
    strncpy(migrate_evt.node_uuid, target_uuid, FM_UUID_LEN - 1);
    snprintf(migrate_evt.detail, FM_LOG_SIZE,
             "MIGRATE:%.36s>%.36s",
             worker_uuid, target_uuid);

    if (ctx->on_fault)
        ctx->on_fault(&migrate_evt, ctx->user_data);
}

/* ─────────────────────────────────────────────────────────────────────
 * SECTION B — ÉLECTION D'UN NOUVEAU SECONDAIRE
 *
 * Appelée quand :
 *   - Le secondaire actuel est tombé (détecté par le watchdog)
 *   - Le secondaire vient de se promouvoir maître (il n'y a plus
 *     de secondaire : l'ancien maître devenu worker doit en élire un)
 *
 * Algorithme Bully simplifié :
 *   Parmi les workers NODE_ACTIF, choisir celui avec le meilleur score.
 *   L'informer de son nouveau rôle via le callback on_master_promoted
 *   (qui déclenche le broadcast ELECT_SECONDARY côté réseau).
 * ───────────────────────────────────────────────────────────────────── */

void fm_master_trigger_election(fm_context_t *ctx)
{
    if (!ctx) return;

    fprintf(stderr,
            "[FaultManager/Master] Déclenchement élection nouveau secondaire...\n");

    /* Snapshot de tous les workers actifs, sans exclusion */
    fm_worker_snapshot_t candidates[FM_MAX_WORKERS];
    int n = 0;

    pthread_mutex_lock(&ctx->node_table->lock);
    n = collect_worker_snapshots(ctx->node_table, NULL,
                                 candidates, FM_MAX_WORKERS);
    pthread_mutex_unlock(&ctx->node_table->lock);

    if (n == 0) {
        fprintf(stderr,
                "[FaultManager/Master] WARN : Élection impossible, "
                "aucun worker actif.\n");
        return;
    }

    /* Sélection du meilleur candidat */
    char elected_uuid[FM_UUID_LEN] = {0};
    if (ctx->best_worker_for_migration) {
        ctx->best_worker_for_migration(candidates, n,
                                       elected_uuid, ctx->user_data);
    } else {
        fm_default_best_worker(candidates, n, elected_uuid, NULL);
    }

    if (elected_uuid[0] == '\0') {
        fprintf(stderr,
                "[FaultManager/Master] WARN : Élection — aucun candidat valide.\n");
        return;
    }

    /* Retrouver l'IP et le port du nœud élu */
    char elected_ip[16]  = {0};
    int  elected_port    = 0;

    pthread_mutex_lock(&ctx->node_table->lock);
    NodeInfo *elected_node = node_table_find(ctx->node_table, elected_uuid);
    if (elected_node) {
        strncpy(elected_ip, elected_node->ip, sizeof(elected_ip) - 1);
        elected_port = elected_node->port;
    }
    pthread_mutex_unlock(&ctx->node_table->lock);

    fprintf(stderr,
            "[FaultManager/Master] Nouveau secondaire élu : "
            "uuid=%.8s... ip=%s port=%d\n",
            elected_uuid, elected_ip, elected_port);

    /*
     * Mémoriser le nouveau secondaire dans le contexte.
     * La couche réseau (callback on_master_promoted ou message réseau
     * ELECT_SECONDARY) notifie le nœud élu qui devra :
     *   - changer son rôle → SECONDARY
     *   - démarrer fm_init() avec FM_ROLE_SECONDARY
     *   - synchroniser l'état depuis le maître
     */
    pthread_mutex_lock(&ctx->lock);
    strncpy(ctx->master_uuid, elected_uuid, FM_UUID_LEN - 1);
    strncpy(ctx->master_ip,   elected_ip,   sizeof(ctx->master_ip)   - 1);
    ctx->master_port = elected_port;
    pthread_mutex_unlock(&ctx->lock);

    /* Notifier via le callback de promotion
     * (utilisé ici pour ELECT_SECONDARY, pas seulement pour self-promotion) */
    if (ctx->on_master_promoted)
        ctx->on_master_promoted(elected_uuid, elected_ip, elected_port,
                                ctx->user_data);
}

/* ─────────────────────────────────────────────────────────────────────
 * SECTION C — WATCHDOG MAÎTRE
 *
 * Tourne en boucle à la fréquence FM_HEARTBEAT_SEC.
 * Parcourt la NodeTable et applique la machine à états :
 *
 *   NODE_ACTIF
 *     └─ silence > FM_SUSPECT_SEC → NODE_SUSPECT
 *
 *   NODE_SUSPECT
 *     ├─ heartbeat reçu         → NODE_ACTIF (via fm_on_heartbeat_received)
 *     └─ silence > FM_FAILED_SEC→ NODE_EN_PANNE
 *                                   └─ fm_master_handle_worker_failure()
 *
 *   NODE_SURCHARGE
 *     └─ géré par fm_on_heartbeat_received (retour NODE_ACTIF si charge OK)
 *
 * Le secondaire est également surveillé :
 *   silence > FM_FAILED_SEC → fm_master_trigger_election()
 * ───────────────────────────────────────────────────────────────────── */

/*
 * fm_master_watchdog_tick
 * Une itération du watchdog maître.
 * Appelé toutes les FM_HEARTBEAT_SEC secondes depuis la boucle principale.
 */
static void fm_master_watchdog_tick(fm_context_t *ctx)
{
    /* Snapshot de la table pour itérer sans tenir le lock trop longtemps */
    /* On itère directement sous lock car les opérations sont rapides     */

    pthread_mutex_lock(&ctx->node_table->lock);

    char   failed_uuids[FM_MAX_WORKERS][FM_UUID_LEN];
    int    n_failed = 0;
    bool   secondary_dead = false;
    char   secondary_uuid[FM_UUID_LEN] = {0};

    for (NodeInfo *node = ctx->node_table->head; node; node = node->next) {

        /* Ne pas se surveiller soi-même */
        if (strncmp(node->uuid, ctx->self_uuid, FM_UUID_LEN) == 0)
            continue;

        double elapsed = fm_elapsed_sec(node->last_heartbeat);

        if (node->status == NODE_EN_PANNE || node->status == NODE_EN_MAINTENANCE)
            continue;  /* déjà traité */

        if (elapsed > (double)FM_FAILED_SEC) {
            /* Panne confirmée */
            NodeStatus prev = node->status;
            node->status = NODE_EN_PANNE;

            fprintf(stderr,
                    "[FaultManager/Master] Nœud %.8s... : "
                    "silence=%.0fs > %ds → EN_PANNE (était %s)\n",
                    node->uuid, elapsed, FM_FAILED_SEC,
                    (prev == NODE_SUSPECT) ? "SUSPECT" : "ACTIF/SURCHARGE");

            /* Mémoriser pour traitement hors lock */
            if (n_failed < FM_MAX_WORKERS) {
                strncpy(failed_uuids[n_failed], node->uuid, FM_UUID_LEN - 1);
                failed_uuids[n_failed][FM_UUID_LEN - 1] = '\0';
                n_failed++;
            }

            /* Détecter si c'est le secondaire connu */
            pthread_mutex_lock(&ctx->lock);
            if (strncmp(ctx->master_uuid, node->uuid, FM_UUID_LEN) == 0) {
                secondary_dead = true;
                strncpy(secondary_uuid, node->uuid, FM_UUID_LEN - 1);
            }
            pthread_mutex_unlock(&ctx->lock);

        } else if (elapsed > (double)FM_SUSPECT_SEC
                   && node->status == NODE_ACTIF) {
            /* Passage en SUSPECT */
            node->status = NODE_SUSPECT;
            fprintf(stderr,
                    "[FaultManager/Master] Nœud %.8s... → SUSPECT "
                    "(silence=%.0fs)\n",
                    node->uuid, elapsed);
        }
    }

    pthread_mutex_unlock(&ctx->node_table->lock);

    /* ── Traitement des pannes hors lock (peut appeler on_fault) ──── */
    for (int i = 0; i < n_failed; i++) {
        fm_master_handle_worker_failure(ctx, failed_uuids[i]);
    }

    /* ── Élection si le secondaire est tombé ──────────────────────── */
    if (secondary_dead) {
        fprintf(stderr,
                "[FaultManager/Master] Secondaire %.8s... mort "
                "→ élection.\n", secondary_uuid);
        fm_master_trigger_election(ctx);
    }
}

/*
 * fm_master_watchdog_loop
 * Corps du thread watchdog côté maître.
 * Tourne à la cadence FM_HEARTBEAT_SEC secondes.
 */
void fm_master_watchdog_loop(fm_context_t *ctx)
{
    fprintf(stderr,
            "[FaultManager/Master] Watchdog démarré "
            "(intervalle=%ds, suspect=%ds, mort=%ds)\n",
            FM_HEARTBEAT_SEC, FM_SUSPECT_SEC, FM_FAILED_SEC);

    while (atomic_load(&ctx->running)) {
        sleep(FM_HEARTBEAT_SEC);

        if (!atomic_load(&ctx->running))
            break;

        fm_master_watchdog_tick(ctx);
    }

    fprintf(stderr, "[FaultManager/Master] Watchdog arrêté.\n");
}
