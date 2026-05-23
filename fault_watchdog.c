/*
 * fault_watchdog.c
 * Point d'entrée des threads du fault_manager — PARALLAX
 *
 * Implémente les fonctions imposées par la convention de nommage du projet :
 *
 *   fault_manager_thread_run(void *ctx_arg)  ← lance le watchdog
 *   fault_manager_stop(fm_context_t *ctx)    ← arrêt propre
 *
 * Le thread interne est aiguillé selon le rôle du nœud :
 *   FM_ROLE_MASTER    → fm_master_watchdog_loop()
 *   FM_ROLE_SECONDARY → fm_secondary_watchdog_loop()
 *                         (bascule automatiquement en master après promotion)
 */

#define _POSIX_C_SOURCE 200809L

#include "fault_tolerance.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

/* Déclarées dans fault_master.c et fault_worker_secondary.c */
extern void fm_master_watchdog_loop(fm_context_t *ctx);
extern void fm_secondary_watchdog_loop(fm_context_t *ctx);

/* ─────────────────────────────────────────────────────────────────────
 * fault_manager_thread_run
 *
 * Signature imposée par le projet : <nom>_thread_run(void *arg)
 *
 * arg doit pointer vers un fm_context_t initialisé par fm_init().
 * Le thread est détaché ou joint selon la politique de l'appelant.
 *
 * Utilisation typique :
 *
 *   fm_context_t ctx;
 *   fm_init(&ctx, FM_ROLE_MASTER, uuid, ip, &node_table);
 *   ctx.on_fault = my_fault_callback;
 *   ctx.on_master_promoted = my_promoted_callback;
 *
 *   pthread_t tid;
 *   pthread_create(&tid, NULL, fault_manager_thread_run, &ctx);
 *   pthread_join(tid, NULL);
 * ───────────────────────────────────────────────────────────────────── */

void *fault_manager_thread_run(void *ctx_arg)
{
    fm_context_t *ctx = (fm_context_t *)ctx_arg;

    if (!ctx) {
        fprintf(stderr,
                "[FaultManager] ERREUR : fault_manager_thread_run appelé "
                "avec ctx=NULL\n");
        return NULL;
    }

    atomic_store(&ctx->running, 1);

    fprintf(stderr,
            "[FaultManager] Thread démarré — rôle=%s uuid=%.8s...\n",
            (ctx->self_role == FM_ROLE_MASTER)    ? "MASTER"    :
            (ctx->self_role == FM_ROLE_SECONDARY) ? "SECONDARY" : "WORKER",
            ctx->self_uuid);

    switch (ctx->self_role) {

        case FM_ROLE_MASTER:
            fm_master_watchdog_loop(ctx);
            break;

        case FM_ROLE_SECONDARY:
            /*
             * Le watchdog secondaire peut basculer en mode maître
             * après une promotion. Dans ce cas il appelle lui-même
             * fm_master_watchdog_loop() et retourne quand running=0.
             */
            fm_secondary_watchdog_loop(ctx);
            break;

        case FM_ROLE_WORKER:
        default:
            fprintf(stderr,
                    "[FaultManager] WARN : rôle WORKER non géré par "
                    "fault_manager_thread_run. Thread inactif.\n");
            while (atomic_load(&ctx->running))
                sleep(1);
            break;
    }

    fprintf(stderr, "[FaultManager] Thread terminé.\n");
    return NULL;
}

/* ─────────────────────────────────────────────────────────────────────
 * fault_manager_stop
 *
 * Signature imposée par le projet : <nom>_stop(fm_context_t *ctx)
 *
 * Positionne running=0, puis join le thread watchdog.
 * Bloquant : retourne seulement quand le thread est terminé.
 * ───────────────────────────────────────────────────────────────────── */

void fault_manager_stop(fm_context_t *ctx)
{
    if (!ctx) return;

    if (!atomic_load(&ctx->running)) {
        fprintf(stderr, "[FaultManager] Déjà arrêté.\n");
        return;
    }

    fprintf(stderr, "[FaultManager] Arrêt demandé...\n");
    atomic_store(&ctx->running, 0);

    /* Join du thread watchdog principal */
    if (ctx->watchdog_tid) {
        pthread_join(ctx->watchdog_tid, NULL);
        ctx->watchdog_tid = 0;
    }

    /* Join du thread de promotion si actif (secondaire → maître) */
    if (ctx->promotion_tid) {
        pthread_join(ctx->promotion_tid, NULL);
        ctx->promotion_tid = 0;
    }

    fprintf(stderr, "[FaultManager] Arrêt complet.\n");
}
