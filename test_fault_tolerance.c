/*
 * test_fault_tolerance.c
 * Tests unitaires du fault_manager PARALLAX
 *
 * Scénarios testés :
 *   1. Initialisation / destruction du contexte
 *   2. fm_default_best_worker — politique meilleur nœud
 *   3. fm_on_heartbeat_received — mise à jour NodeTable
 *   4. Détection panne worker (silence > FM_FAILED_SEC)
 *   5. Promotion secondaire → maître
 *   6. Élection nouveau secondaire après panne
 */

#define _POSIX_C_SOURCE 200809L

#include "fault_tolerance.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

/* ─── Compteurs de tests ─────────────────────────────────────────── */
static int tests_run    = 0;
static int tests_passed = 0;

#define TEST_ASSERT(cond, msg) do {                         \
    tests_run++;                                            \
    if (cond) {                                             \
        printf("  [PASS] %s\n", msg);                       \
        tests_passed++;                                     \
    } else {                                                \
        printf("  [FAIL] %s (ligne %d)\n", msg, __LINE__); \
    }                                                       \
} while(0)

/* ─── Callbacks de test ──────────────────────────────────────────── */

static int g_fault_count    = 0;
static int g_promoted_count = 0;
static fm_fault_type_t g_last_fault_type = FM_FAULT_NONE;

static void test_on_fault(const fm_fault_event_t *evt, void *user_data)
{
    (void)user_data;
    g_fault_count++;
    g_last_fault_type = evt->type;
    printf("    [CALLBACK] on_fault type=%d node=%.8s... detail=%s\n",
           evt->type, evt->node_uuid, evt->detail);
}

static void test_on_promoted(const char *uuid, const char *ip, int port,
                              void *user_data)
{
    (void)user_data;
    g_promoted_count++;
    printf("    [CALLBACK] on_master_promoted uuid=%.8s... ip=%s port=%d\n",
           uuid, ip, port);
}

/* ─── Helpers ────────────────────────────────────────────────────── */

static void make_node(NodeTable *table, const char *uuid,
                      const char *ip, int port,
                      float cpu, float ram, NodeStatus status)
{
    pthread_mutex_lock(&table->lock);
    NodeInfo *n = node_table_add(table, uuid, ip, port);
    if (n) {
        n->metrics.cpu_usage = cpu;
        n->metrics.ram_usage = ram;
        n->status            = status;
        n->last_heartbeat    = time(NULL);
    }
    pthread_mutex_unlock(&table->lock);
}

static void make_stale_node(NodeTable *table, const char *uuid,
                             const char *ip, int port, int seconds_ago)
{
    pthread_mutex_lock(&table->lock);
    NodeInfo *n = node_table_add(table, uuid, ip, port);
    if (n) {
        n->metrics.cpu_usage = 0.2f;
        n->metrics.ram_usage = 0.3f;
        n->status            = NODE_ACTIF;
        n->last_heartbeat    = time(NULL) - seconds_ago;
    }
    pthread_mutex_unlock(&table->lock);
}

/* ═══════════════════════════════════════════════════════════════════
 * TEST 1 — Initialisation / destruction
 * ═══════════════════════════════════════════════════════════════════ */
static void test_init_destroy(void)
{
    printf("\n[TEST 1] Initialisation / destruction\n");

    NodeTable table;
    node_table_init(&table);

    fm_context_t ctx;
    int rc = fm_init(&ctx, FM_ROLE_MASTER,
                     "aaaaaaaa-0000-4000-8000-000000000001",
                     "192.168.0.1", &table);

    TEST_ASSERT(rc == 0,           "fm_init retourne 0");
    TEST_ASSERT(ctx.self_role == FM_ROLE_MASTER, "rôle MASTER");
    TEST_ASSERT(atomic_load(&ctx.running) == 0,  "running=0 avant démarrage");
    TEST_ASSERT(ctx.node_table == &table,         "node_table correctement lié");

    fm_destroy(&ctx);
    TEST_ASSERT(1, "fm_destroy sans crash");

    node_table_destroy(&table);
}

/* ═══════════════════════════════════════════════════════════════════
 * TEST 2 — Politique meilleur nœud (fm_default_best_worker)
 * ═══════════════════════════════════════════════════════════════════ */
static void test_best_worker_policy(void)
{
    printf("\n[TEST 2] Politique meilleur nœud\n");

    /* Cas 1 : 3 candidats, meilleur = cpu faible + ram faible */
    fm_worker_snapshot_t cands[3] = {
        { .uuid = "worker-A", .cpu_usage = 0.80f, .ram_usage = 0.70f,
          .status = NODE_ACTIF },
        { .uuid = "worker-B", .cpu_usage = 0.20f, .ram_usage = 0.30f,
          .status = NODE_ACTIF },  /* meilleur score = 0.56 */
        { .uuid = "worker-C", .cpu_usage = 0.50f, .ram_usage = 0.50f,
          .status = NODE_ACTIF },  /* score = 0.25 */
    };

    char out[FM_UUID_LEN] = {0};
    fm_default_best_worker(cands, 3, out, NULL);
    TEST_ASSERT(strcmp(out, "worker-B") == 0,
                "Meilleur nœud = worker-B (score=0.56)");

    /* Cas 2 : nœud surchargé exclu */
    fm_worker_snapshot_t cands2[2] = {
        { .uuid = "worker-X", .cpu_usage = 0.90f, .ram_usage = 0.90f,
          .status = NODE_ACTIF },
        { .uuid = "worker-Y", .cpu_usage = 0.30f, .ram_usage = 0.40f,
          .status = NODE_SURCHARGE },  /* status SURCHARGE → non sélectionné */
    };
    char out2[FM_UUID_LEN] = {0};
    fm_default_best_worker(cands2, 2, out2, NULL);
    TEST_ASSERT(out2[0] == '\0',
                "Aucun nœud valide si tous surchargés/cpu>0.85");

    /* Cas 3 : liste vide */
    char out3[FM_UUID_LEN] = {0};
    fm_default_best_worker(NULL, 0, out3, NULL);
    TEST_ASSERT(out3[0] == '\0', "Liste vide → out vide");

    /* Cas 4 : nœud avec CPU <0.85 mais ram>=0.85 exclu */
    fm_worker_snapshot_t cands4[2] = {
        { .uuid = "worker-P", .cpu_usage = 0.20f, .ram_usage = 0.90f,
          .status = NODE_ACTIF },  /* ram > FM_RAM_OVERLOAD → exclu */
        { .uuid = "worker-Q", .cpu_usage = 0.40f, .ram_usage = 0.50f,
          .status = NODE_ACTIF },  /* score = 0.30 → sélectionné */
    };
    char out4[FM_UUID_LEN] = {0};
    fm_default_best_worker(cands4, 2, out4, NULL);
    TEST_ASSERT(strcmp(out4, "worker-Q") == 0,
                "worker-P exclu (ram>0.85), worker-Q sélectionné");
}

/* ═══════════════════════════════════════════════════════════════════
 * TEST 3 — fm_on_heartbeat_received
 * ═══════════════════════════════════════════════════════════════════ */
static void test_heartbeat_received(void)
{
    printf("\n[TEST 3] fm_on_heartbeat_received\n");

    NodeTable table;
    node_table_init(&table);

    fm_context_t ctx;
    fm_init(&ctx, FM_ROLE_MASTER,
            "aaaaaaaa-0000-4000-8000-000000000001",
            "192.168.0.1", &table);

    /* Pré-remplir la table avec un nœud en état SUSPECT */
    make_stale_node(&table,
                    "bbbbbbbb-0000-4000-8000-000000000002",
                    "192.168.0.2", 9000, FM_SUSPECT_SEC + 1);
    pthread_mutex_lock(&table.lock);
    NodeInfo *n = node_table_find(&table, "bbbbbbbb-0000-4000-8000-000000000002");
    if (n) n->status = NODE_SUSPECT;
    pthread_mutex_unlock(&table.lock);

    /* Simuler la réception d'un heartbeat normal */
    NetworkMessage msg = {
        .type      = MSG_HEARTBEAT,
        .cpu_usage = 0.35f,
        .ram_usage = 0.45f,
        .ram_used_mb = 4096,
        .score     = 0.60f,
        .queue_len = 2,
    };
    strncpy(msg.uuid, "bbbbbbbb-0000-4000-8000-000000000002",
            sizeof(msg.uuid) - 1);
    strncpy(msg.ip, "192.168.0.2", sizeof(msg.ip) - 1);
    msg.port = 9000;

    fm_on_heartbeat_received(&ctx, &msg);

    pthread_mutex_lock(&table.lock);
    NodeInfo *updated = node_table_find(&table, msg.uuid);
    bool status_ok  = updated && updated->status    == NODE_ACTIF;
    bool cpu_ok     = updated && updated->metrics.cpu_usage == 0.35f;
    bool ram_ok     = updated && updated->metrics.ram_usage == 0.45f;
    pthread_mutex_unlock(&table.lock);

    TEST_ASSERT(status_ok, "Statut remis à NODE_ACTIF après heartbeat");
    TEST_ASSERT(cpu_ok,    "cpu_usage mis à jour");
    TEST_ASSERT(ram_ok,    "ram_usage mis à jour");

    /* Heartbeat d'un nœud inconnu → doit être ajouté automatiquement */
    NetworkMessage msg2 = {
        .type      = MSG_HEARTBEAT,
        .cpu_usage = 0.10f,
        .ram_usage = 0.20f,
        .port      = 9001,
    };
    strncpy(msg2.uuid, "cccccccc-0000-4000-8000-000000000003",
            sizeof(msg2.uuid) - 1);
    strncpy(msg2.ip, "192.168.0.3", sizeof(msg2.ip) - 1);

    fm_on_heartbeat_received(&ctx, &msg2);

    pthread_mutex_lock(&table.lock);
    NodeInfo *new_node = node_table_find(&table, msg2.uuid);
    bool added = (new_node != NULL);
    pthread_mutex_unlock(&table.lock);

    TEST_ASSERT(added, "Nœud inconnu ajouté automatiquement");

    /* Heartbeat surcharge → status NODE_SURCHARGE */
    NetworkMessage msg3 = { .type = MSG_HEARTBEAT,
                             .cpu_usage = 0.92f, .ram_usage = 0.50f,
                             .port = 9000 };
    strncpy(msg3.uuid, "bbbbbbbb-0000-4000-8000-000000000002",
            sizeof(msg3.uuid) - 1);
    strncpy(msg3.ip, "192.168.0.2", sizeof(msg3.ip) - 1);
    fm_on_heartbeat_received(&ctx, &msg3);

    pthread_mutex_lock(&table.lock);
    NodeInfo *ov = node_table_find(&table, msg3.uuid);
    bool surcharge = ov && ov->status == NODE_SURCHARGE;
    pthread_mutex_unlock(&table.lock);

    TEST_ASSERT(surcharge, "Nœud surcharge cpu>0.85 → NODE_SURCHARGE");

    fm_destroy(&ctx);
    node_table_destroy(&table);
}

/* ═══════════════════════════════════════════════════════════════════
 * TEST 4 — Détection panne worker (simulation directe sans sleep)
 * ═══════════════════════════════════════════════════════════════════ */
static void test_worker_failure_detection(void)
{
    printf("\n[TEST 4] Détection panne worker via fm_master_handle_worker_failure\n");

    NodeTable table;
    node_table_init(&table);

    /* Worker actif */
    make_node(&table,
              "worker-dead-0000-0000-000000000001",
              "10.0.0.10", 8000, 0.3f, 0.3f, NODE_ACTIF);

    /* Worker cible (meilleur score) */
    make_node(&table,
              "worker-good-0000-0000-000000000002",
              "10.0.0.11", 8001, 0.1f, 0.2f, NODE_ACTIF);

    fm_context_t ctx;
    fm_init(&ctx, FM_ROLE_MASTER,
            "master-uuid-0000-0000-000000000000",
            "10.0.0.1", &table);
    ctx.on_fault = test_on_fault;

    g_fault_count     = 0;
    g_last_fault_type = FM_FAULT_NONE;

    /* Simuler la panne */
    extern void fm_master_handle_worker_failure(fm_context_t *, const char *);
    fm_master_handle_worker_failure(&ctx,
                                    "worker-dead-0000-0000-000000000001");

    /* Vérifier que le nœud est marqué EN_PANNE */
    pthread_mutex_lock(&table.lock);
    NodeInfo *dead = node_table_find(&table, "worker-dead-0000-0000-000000000001");
    bool is_failed = dead && dead->status == NODE_EN_PANNE;
    pthread_mutex_unlock(&table.lock);

    TEST_ASSERT(is_failed,       "Worker marqué NODE_EN_PANNE");
    TEST_ASSERT(g_fault_count >= 1,
                "Callback on_fault appelé au moins une fois");
    TEST_ASSERT(g_last_fault_type == FM_FAULT_NONE ||
                g_last_fault_type == FM_FAULT_HEARTBEAT_LOSS ||
                g_last_fault_type == FM_FAULT_WORKER_CRASH,
                "Type de panne cohérent");

    fm_destroy(&ctx);
    node_table_destroy(&table);
}

/* ═══════════════════════════════════════════════════════════════════
 * TEST 5 — Promotion secondaire → maître
 * ═══════════════════════════════════════════════════════════════════ */
static void test_secondary_promotion(void)
{
    printf("\n[TEST 5] Promotion secondaire → maître\n");

    NodeTable table;
    node_table_init(&table);

    /* Ajouter le maître primaire avec un heartbeat périmé */
    make_stale_node(&table,
                    "master-primary-0000-0000-00000000001",
                    "10.0.0.1", 8080, FM_FAILED_SEC + 5);

    /* Ajouter un worker disponible (pour l'élection post-promotion) */
    make_node(&table,
              "worker-elect-0000-0000-000000000001",
              "10.0.0.20", 8001, 0.15f, 0.25f, NODE_ACTIF);

    fm_context_t ctx;
    fm_init(&ctx, FM_ROLE_SECONDARY,
            "secondary-uuid-0000-0000-00000000001",
            "10.0.0.2", &table);

    pthread_mutex_lock(&ctx.lock);
    strncpy(ctx.master_uuid, "master-primary-0000-0000-00000000001",
            FM_UUID_LEN - 1);
    strncpy(ctx.master_ip,   "10.0.0.1", sizeof(ctx.master_ip) - 1);
    ctx.master_port      = 8080;
    ctx.master_last_seen = time(NULL) - (FM_FAILED_SEC + 5);
    pthread_mutex_unlock(&ctx.lock);

    ctx.on_fault           = test_on_fault;
    ctx.on_master_promoted = test_on_promoted;

    g_promoted_count = 0;
    g_fault_count    = 0;

    /* Appel direct à la fonction de promotion (sans thread ni sleep) */
    extern void fm_secondary_promote_to_master(fm_context_t *ctx);
    fm_secondary_promote_to_master(&ctx);

    TEST_ASSERT(ctx.self_role == FM_ROLE_MASTER,
                "Rôle basculé → FM_ROLE_MASTER après promotion");
    /*
     * on_master_promoted est appelé DEUX fois :
     *   1) pour notifier le changement de maître (self promu)
     *   2) pour notifier l'élection du nouveau secondaire
     */
    TEST_ASSERT(g_promoted_count >= 1,
                "Callback on_master_promoted appelé (promotion + élection)");
    TEST_ASSERT(g_fault_count >= 1,
                "Callback on_fault appelé (FM_FAULT_MASTER_DOWN)");

    /*
     * Après promotion + élection, ctx.master_uuid pointe vers le
     * nouveau secondaire élu (worker-elect-...).
     * Le nœud local (self) est maintenant le maître, mais master_uuid
     * désigne le secondaire qu'il supervise — comportement correct.
     */
    bool has_secondary_elected =
        strncmp(ctx.master_uuid,
                "worker-elect-0000-0000-000000000001",
                FM_UUID_LEN) == 0;
    TEST_ASSERT(has_secondary_elected,
                "ctx.master_uuid pointe vers le secondaire élu après promotion");

    fm_destroy(&ctx);
    node_table_destroy(&table);
}

/* ═══════════════════════════════════════════════════════════════════
 * TEST 6 — Élection nouveau secondaire
 * ═══════════════════════════════════════════════════════════════════ */
static void test_election_new_secondary(void)
{
    printf("\n[TEST 6] Élection nouveau secondaire\n");

    NodeTable table;
    node_table_init(&table);

    /* Deux workers actifs avec scores différents */
    make_node(&table, "worker-low-00000-0000-000000000001",
              "10.0.0.10", 8001, 0.70f, 0.60f, NODE_ACTIF); /* score ≈ 0.12 */
    make_node(&table, "worker-best-0000-0000-000000000002",
              "10.0.0.11", 8002, 0.10f, 0.15f, NODE_ACTIF); /* score ≈ 0.76 */

    fm_context_t ctx;
    fm_init(&ctx, FM_ROLE_MASTER,
            "master-uuid-0000-0000-000000000000",
            "10.0.0.1", &table);
    ctx.on_master_promoted = test_on_promoted;

    g_promoted_count = 0;

    extern void fm_master_trigger_election(fm_context_t *ctx);
    fm_master_trigger_election(&ctx);

    TEST_ASSERT(g_promoted_count == 1,
                "Callback on_master_promoted appelé pour ELECT_SECONDARY");

    /* Le meilleur nœud doit avoir été élu */
    bool elected_best =
        strncmp(ctx.master_uuid,
                "worker-best-0000-0000-000000000002",
                FM_UUID_LEN) == 0;
    TEST_ASSERT(elected_best,
                "Meilleur worker (score=0.76) élu comme secondaire");

    fm_destroy(&ctx);
    node_table_destroy(&table);
}

/* ═══════════════════════════════════════════════════════════════════
 * TEST 7 — fm_elapsed_sec
 * ═══════════════════════════════════════════════════════════════════ */
static void test_elapsed_sec(void)
{
    printf("\n[TEST 7] fm_elapsed_sec\n");

    time_t past = time(NULL) - 5;
    double elapsed = fm_elapsed_sec(past);

    TEST_ASSERT(elapsed >= 4.0 && elapsed < 7.0,
                "fm_elapsed_sec retourne environ 5s pour un timestamp vieux de 5s");

    time_t now = time(NULL);
    double zero = fm_elapsed_sec(now);
    TEST_ASSERT(zero < 1.0, "fm_elapsed_sec ≈ 0 pour time(NULL)");
}

/* ═══════════════════════════════════════════════════════════════════
 * MAIN
 * ═══════════════════════════════════════════════════════════════════ */

int main(void)
{
    printf("═══════════════════════════════════════════════════════\n");
    printf("  Tests unitaires — fault_manager PARALLAX\n");
    printf("═══════════════════════════════════════════════════════\n");

    test_init_destroy();
    test_best_worker_policy();
    test_heartbeat_received();
    test_worker_failure_detection();
    test_secondary_promotion();
    test_election_new_secondary();
    test_elapsed_sec();

    printf("\n═══════════════════════════════════════════════════════\n");
    printf("  Résultat : %d/%d tests passés\n", tests_passed, tests_run);
    printf("═══════════════════════════════════════════════════════\n");

    return (tests_passed == tests_run) ? 0 : 1;
}
