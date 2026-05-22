/*
 * test_worker_table.c
 *
 * Tests unitaires de la table des workers.
 * Couvre : création, ajout, mise à jour, recherche, suppression,
 *          comptage par état, snapshot, détection de timeouts.
 */

#include "test_framework.h"
#include "worker_table.h"

#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Helpers de test                                                     */
/* ------------------------------------------------------------------ */

static worker_capabilities_t make_caps(uint32_t ram, uint16_t cpu) {
    worker_capabilities_t c = { .ram_mb = ram, .cpu_count = cpu, .cpu_mhz = 2400 };
    return c;
}

/* ------------------------------------------------------------------ */
/* Tests                                                               */
/* ------------------------------------------------------------------ */

static void test_create_and_destroy(void) {
    TEST_BEGIN("create_and_destroy");
    worker_table_t *t = worker_table_create(4);
    ASSERT_TRUE(t != NULL);
    ASSERT_EQ_INT(worker_table_count(t), 0);
    worker_table_destroy(t);
    /* destroy(NULL) doit être safe */
    worker_table_destroy(NULL);
    TEST_END_PASS();
}

static void test_add_single_worker(void) {
    TEST_BEGIN("add_single_worker");
    worker_table_t *t = worker_table_create(4);
    worker_capabilities_t caps = make_caps(4096, 4);

    int rc = worker_table_add_or_update(t, "uuid-aaa", "node-a", &caps);
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(worker_table_count(t), 1);

    worker_t *w = worker_table_find(t, "uuid-aaa");
    ASSERT_TRUE(w != NULL);
    ASSERT_EQ_STR(w->node_id, "uuid-aaa");
    ASSERT_EQ_STR(w->node_name, "node-a");
    ASSERT_EQ_INT(w->caps.ram_mb, 4096);
    ASSERT_EQ_INT(w->caps.cpu_count, 4);
    ASSERT_EQ_INT(w->state, WORKER_AVAILABLE);

    worker_table_destroy(t);
    TEST_END_PASS();
}

static void test_update_preserves_state_and_counters(void) {
    TEST_BEGIN("update_preserves_state_and_counters");
    worker_table_t *t = worker_table_create(4);
    worker_capabilities_t c1 = make_caps(2048, 2);
    worker_capabilities_t c2 = make_caps(8192, 8);

    worker_table_add_or_update(t, "uuid-bbb", "old-name", &c1);
    /* On simule du vécu sur ce worker */
    worker_t *w = worker_table_find(t, "uuid-bbb");
    w->state = WORKER_BUSY;
    w->tasks_assigned = 42;
    w->tasks_completed = 40;

    /* Mise à jour : nouvelles caps, nouveau nom, mais state/compteurs
     * doivent rester intacts */
    int rc = worker_table_add_or_update(t, "uuid-bbb", "new-name", &c2);
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(worker_table_count(t), 1);

    w = worker_table_find(t, "uuid-bbb");
    ASSERT_EQ_STR(w->node_name, "new-name");
    ASSERT_EQ_INT(w->caps.ram_mb, 8192);
    ASSERT_EQ_INT(w->caps.cpu_count, 8);
    ASSERT_EQ_INT(w->state, WORKER_BUSY);
    ASSERT_EQ_INT(w->tasks_assigned, 42);
    ASSERT_EQ_INT(w->tasks_completed, 40);

    worker_table_destroy(t);
    TEST_END_PASS();
}

static void test_grow_beyond_initial_capacity(void) {
    TEST_BEGIN("grow_beyond_initial_capacity");
    worker_table_t *t = worker_table_create(2);  /* petite capacité */
    worker_capabilities_t caps = make_caps(1024, 1);

    char id[PARALLAX_UUID_LEN];
    for (int i = 0; i < 20; i++) {
        snprintf(id, sizeof(id), "uuid-%03d", i);
        int rc = worker_table_add_or_update(t, id, "n", &caps);
        ASSERT_EQ_INT(rc, 0);
    }
    ASSERT_EQ_INT(worker_table_count(t), 20);

    /* Vérifie qu'on retrouve tous les workers */
    for (int i = 0; i < 20; i++) {
        snprintf(id, sizeof(id), "uuid-%03d", i);
        worker_t *w = worker_table_find(t, id);
        ASSERT_TRUE(w != NULL);
    }

    worker_table_destroy(t);
    TEST_END_PASS();
}

static void test_remove(void) {
    TEST_BEGIN("remove");
    worker_table_t *t = worker_table_create(4);
    worker_capabilities_t caps = make_caps(1024, 1);

    worker_table_add_or_update(t, "uuid-1", "n1", &caps);
    worker_table_add_or_update(t, "uuid-2", "n2", &caps);
    worker_table_add_or_update(t, "uuid-3", "n3", &caps);

    ASSERT_EQ_INT(worker_table_count(t), 3);
    int rc = worker_table_remove(t, "uuid-2");
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(worker_table_count(t), 2);
    ASSERT_TRUE(worker_table_find(t, "uuid-2") == NULL);
    ASSERT_TRUE(worker_table_find(t, "uuid-1") != NULL);
    ASSERT_TRUE(worker_table_find(t, "uuid-3") != NULL);

    /* Remove inexistant */
    rc = worker_table_remove(t, "uuid-nope");
    ASSERT_EQ_INT(rc, -1);

    worker_table_destroy(t);
    TEST_END_PASS();
}

static void test_state_transitions(void) {
    TEST_BEGIN("state_transitions");
    worker_table_t *t = worker_table_create(4);
    worker_capabilities_t caps = make_caps(1024, 1);
    worker_table_add_or_update(t, "uuid-x", "nx", &caps);

    int rc = worker_table_set_state(t, "uuid-x", WORKER_BUSY);
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(worker_table_find(t, "uuid-x")->state, WORKER_BUSY);

    rc = worker_table_set_state(t, "uuid-x", WORKER_AVAILABLE);
    ASSERT_EQ_INT(rc, 0);

    rc = worker_table_set_state(t, "uuid-nope", WORKER_BUSY);
    ASSERT_EQ_INT(rc, -1);

    worker_table_destroy(t);
    TEST_END_PASS();
}

static void test_count_in_state(void) {
    TEST_BEGIN("count_in_state");
    worker_table_t *t = worker_table_create(4);
    worker_capabilities_t caps = make_caps(1024, 1);

    worker_table_add_or_update(t, "u1", "n1", &caps);
    worker_table_add_or_update(t, "u2", "n2", &caps);
    worker_table_add_or_update(t, "u3", "n3", &caps);

    /* Tous AVAILABLE par défaut */
    ASSERT_EQ_INT(worker_table_count_in_state(t, WORKER_AVAILABLE), 3);
    ASSERT_EQ_INT(worker_table_count_in_state(t, WORKER_BUSY), 0);

    worker_table_set_state(t, "u2", WORKER_BUSY);
    worker_table_set_state(t, "u3", WORKER_FAILED);

    ASSERT_EQ_INT(worker_table_count_in_state(t, WORKER_AVAILABLE), 1);
    ASSERT_EQ_INT(worker_table_count_in_state(t, WORKER_BUSY), 1);
    ASSERT_EQ_INT(worker_table_count_in_state(t, WORKER_FAILED), 1);

    worker_table_destroy(t);
    TEST_END_PASS();
}

static void test_snapshot_by_state(void) {
    TEST_BEGIN("snapshot_by_state");
    worker_table_t *t = worker_table_create(4);
    worker_capabilities_t caps = make_caps(2048, 2);

    worker_table_add_or_update(t, "u1", "n1", &caps);
    worker_table_add_or_update(t, "u2", "n2", &caps);
    worker_table_add_or_update(t, "u3", "n3", &caps);
    worker_table_set_state(t, "u2", WORKER_BUSY);

    worker_t snap[10];
    size_t n = worker_table_snapshot_by_state(t, WORKER_AVAILABLE, snap, 10);
    ASSERT_EQ_INT(n, 2);
    /* Les deux entrées sont u1 et u3, ordre non garanti */
    int found_u1 = 0, found_u3 = 0;
    for (size_t i = 0; i < n; i++) {
        if (strcmp(snap[i].node_id, "u1") == 0) found_u1 = 1;
        if (strcmp(snap[i].node_id, "u3") == 0) found_u3 = 1;
    }
    ASSERT_TRUE(found_u1 && found_u3);

    worker_table_destroy(t);
    TEST_END_PASS();
}

static void test_detect_timeouts(void) {
    TEST_BEGIN("detect_timeouts");
    worker_table_t *t = worker_table_create(4);
    worker_capabilities_t caps = make_caps(1024, 1);

    worker_table_add_or_update(t, "u-fresh", "f", &caps);
    worker_table_add_or_update(t, "u-stale", "s", &caps);
    worker_table_add_or_update(t, "u-never", "n", &caps);  /* jamais de hb */

    /* Simule des timestamps :
     *   maintenant = 100000 ms
     *   timeout = 5000 ms
     *   u-fresh : hb à 99000  (1s ago)  → OK
     *   u-stale : hb à 80000  (20s ago) → TIMEOUT
     *   u-never : hb à 0                 → ne doit PAS timeout */
    worker_table_touch_heartbeat(t, "u-fresh", 99000);
    worker_table_touch_heartbeat(t, "u-stale", 80000);

    char suspects[10][PARALLAX_UUID_LEN];
    size_t n = worker_table_detect_timeouts(t, 100000, 5000, suspects, 10);
    ASSERT_EQ_INT(n, 1);
    ASSERT_EQ_STR(suspects[0], "u-stale");

    ASSERT_EQ_INT(worker_table_find(t, "u-fresh")->state, WORKER_AVAILABLE);
    ASSERT_EQ_INT(worker_table_find(t, "u-stale")->state, WORKER_SUSPECT);
    ASSERT_EQ_INT(worker_table_find(t, "u-never")->state, WORKER_AVAILABLE);

    /* Re-appel : pas de double comptage (déjà SUSPECT) */
    n = worker_table_detect_timeouts(t, 100000, 5000, suspects, 10);
    ASSERT_EQ_INT(n, 0);

    worker_table_destroy(t);
    TEST_END_PASS();
}

static void test_invalid_inputs(void) {
    TEST_BEGIN("invalid_inputs");
    worker_table_t *t = worker_table_create(4);

    /* node_id vide */
    int rc = worker_table_add_or_update(t, "", "n", NULL);
    ASSERT_EQ_INT(rc, -2);
    /* node_id NULL */
    rc = worker_table_add_or_update(t, NULL, "n", NULL);
    ASSERT_EQ_INT(rc, -2);
    /* table NULL */
    rc = worker_table_add_or_update(NULL, "u1", "n", NULL);
    ASSERT_EQ_INT(rc, -2);

    ASSERT_EQ_INT(worker_table_count(t), 0);
    worker_table_destroy(t);
    TEST_END_PASS();
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(void) {
    printf("Running worker_table tests...\n\n");
    test_create_and_destroy();
    test_add_single_worker();
    test_update_preserves_state_and_counters();
    test_grow_beyond_initial_capacity();
    test_remove();
    test_state_transitions();
    test_count_in_state();
    test_snapshot_by_state();
    test_detect_timeouts();
    test_invalid_inputs();
    TEST_REPORT();
    return TEST_EXIT_CODE();
}
