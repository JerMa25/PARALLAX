/*
 * test_task_pool.c
 *
 * Tests unitaires de task_pool.
 */

#include "test_framework.h"
#include "task_pool.h"

#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

/* Fabrique une tâche minimale avec un petit payload contenant le task_id. */
static task_t make_task(uint32_t id, uint8_t max_retries) {
    task_t t;
    memset(&t, 0, sizeof(t));
    t.task_id = id;
    t.state = TASK_PENDING;
    t.created_ms = 1000;
    t.max_retries = max_retries;
    /* payload : 4 octets contenant l'id en little-endian */
    t.payload = malloc(4);
    t.payload[0] = (uint8_t)(id & 0xff);
    t.payload[1] = (uint8_t)((id >> 8) & 0xff);
    t.payload[2] = (uint8_t)((id >> 16) & 0xff);
    t.payload[3] = (uint8_t)((id >> 24) & 0xff);
    t.payload_size = 4;
    return t;
}

static void free_helper_task(task_t *t) {
    free(t->payload);
    t->payload = NULL;
}

static task_result_t make_result(uint32_t task_id, const char *worker_id,
                                  bool success, const char *output) {
    task_result_t r;
    memset(&r, 0, sizeof(r));
    r.task_id = task_id;
    strncpy(r.worker_id, worker_id, PARALLAX_UUID_LEN - 1);
    r.success = success;
    r.exit_code = success ? 0 : 1;
    r.execution_ms = 50;
    if (output) {
        size_t n = strlen(output);
        r.output = malloc(n);
        memcpy(r.output, output, n);
        r.output_size = n;
    }
    return r;
}

static void free_helper_result(task_result_t *r) {
    free(r->output);
    r->output = NULL;
}

/* ------------------------------------------------------------------ */
/* Tests                                                               */
/* ------------------------------------------------------------------ */

static void test_create_and_destroy(void) {
    TEST_BEGIN("create_and_destroy");
    task_pool_t *p = task_pool_create(0);
    ASSERT_TRUE(p != NULL);
    ASSERT_EQ_INT(task_pool_count(p), 0);
    task_pool_destroy(p);
    task_pool_destroy(NULL);
    TEST_END_PASS();
}

static void test_add_single(void) {
    TEST_BEGIN("add_single");
    task_pool_t *p = task_pool_create(4);
    task_t t = make_task(42, 3);
    int rc = task_pool_add(p, &t);
    free_helper_task(&t);
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(task_pool_count(p), 1);

    task_t *found = task_pool_find(p, 42);
    ASSERT_TRUE(found != NULL);
    ASSERT_EQ_INT(found->task_id, 42);
    ASSERT_EQ_INT(found->state, TASK_PENDING);
    ASSERT_EQ_INT(found->payload_size, 4);
    /* Vérifie que le payload a bien été dupliqué */
    ASSERT_EQ_INT(found->payload[0], 42);

    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_add_duplicate_id(void) {
    TEST_BEGIN("add_duplicate_id");
    task_pool_t *p = task_pool_create(4);
    task_t t1 = make_task(7, 3);
    task_t t2 = make_task(7, 3);
    ASSERT_EQ_INT(task_pool_add(p, &t1), 0);
    int rc = task_pool_add(p, &t2);
    ASSERT_EQ_INT(rc, -2);  /* duplicate */
    free_helper_task(&t1);
    free_helper_task(&t2);
    ASSERT_EQ_INT(task_pool_count(p), 1);
    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_fifo_pop(void) {
    TEST_BEGIN("fifo_pop");
    task_pool_t *p = task_pool_create(4);
    for (uint32_t i = 1; i <= 5; i++) {
        task_t t = make_task(i, 3);
        task_pool_add(p, &t);
        free_helper_task(&t);
    }

    task_t *first = task_pool_peek_pending(p);
    ASSERT_TRUE(first != NULL);
    ASSERT_EQ_INT(first->task_id, 1);

    /* peek ne change pas l'état */
    ASSERT_EQ_INT(task_pool_count_in_state(p, TASK_PENDING), 5);

    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_assign_and_complete(void) {
    TEST_BEGIN("assign_and_complete");
    task_pool_t *p = task_pool_create(4);
    task_t t = make_task(100, 3);
    task_pool_add(p, &t);
    free_helper_task(&t);

    int rc = task_pool_mark_assigned(p, 100, "worker-A", 1500);
    ASSERT_EQ_INT(rc, 0);
    task_t *found = task_pool_find(p, 100);
    ASSERT_EQ_INT(found->state, TASK_ASSIGNED);
    ASSERT_EQ_STR(found->assigned_worker, "worker-A");
    ASSERT_EQ_INT(found->assigned_ms, 1500);

    task_result_t r = make_result(100, "worker-A", true, "OK_OUTPUT");
    rc = task_pool_mark_completed(p, 100, &r, 2000);
    free_helper_result(&r);
    ASSERT_EQ_INT(rc, 0);

    found = task_pool_find(p, 100);
    ASSERT_EQ_INT(found->state, TASK_COMPLETED);
    ASSERT_EQ_INT(found->completed_ms, 2000);
    /* Le payload doit avoir été libéré */
    ASSERT_TRUE(found->payload == NULL);

    const task_result_t *got = task_pool_get_result(p, 100);
    ASSERT_TRUE(got != NULL);
    ASSERT_EQ_INT(got->success, 1);
    ASSERT_EQ_INT(got->output_size, 9);
    ASSERT_TRUE(memcmp(got->output, "OK_OUTPUT", 9) == 0);

    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_late_result_from_zombie_worker(void) {
    /* Scénario critique de la doc d'archi :
     * - T100 assignée à A
     * - A timeout, T100 requeue
     * - T100 réassignée à B
     * - Résultat tardif de A arrive : DOIT être rejeté
     */
    TEST_BEGIN("late_result_from_zombie_worker");
    task_pool_t *p = task_pool_create(4);
    task_t t = make_task(100, 3);
    task_pool_add(p, &t);
    free_helper_task(&t);

    task_pool_mark_assigned(p, 100, "worker-A", 1000);
    task_pool_requeue(p, 100, 6000);  /* A a timeout */
    task_t *tt = task_pool_find(p, 100);
    ASSERT_EQ_INT(tt->state, TASK_REQUEUED);
    ASSERT_EQ_INT(tt->retry_count, 1);
    ASSERT_EQ_STR(tt->assigned_worker, "");  /* effacé */

    task_pool_mark_assigned(p, 100, "worker-B", 7000);
    tt = task_pool_find(p, 100);
    ASSERT_EQ_STR(tt->assigned_worker, "worker-B");

    /* Résultat tardif de A : doit être REJETÉ */
    task_result_t late = make_result(100, "worker-A", true, "LATE_FROM_A");
    int rc = task_pool_mark_completed(p, 100, &late, 8000);
    free_helper_result(&late);
    ASSERT_EQ_INT(rc, -3);  /* worker mismatch */

    /* La tâche reste ASSIGNED à B */
    tt = task_pool_find(p, 100);
    ASSERT_EQ_INT(tt->state, TASK_ASSIGNED);
    ASSERT_EQ_STR(tt->assigned_worker, "worker-B");

    /* Résultat correct de B : OK */
    task_result_t legit = make_result(100, "worker-B", true, "OK_FROM_B");
    rc = task_pool_mark_completed(p, 100, &legit, 9000);
    free_helper_result(&legit);
    ASSERT_EQ_INT(rc, 0);

    const task_result_t *got = task_pool_get_result(p, 100);
    ASSERT_TRUE(got != NULL);
    ASSERT_TRUE(memcmp(got->output, "OK_FROM_B", 9) == 0);

    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_late_result_after_completion(void) {
    /* Variante : T100 est déjà COMPLETED, un autre résultat arrive.
     * Doit être ignoré (état incompatible). */
    TEST_BEGIN("late_result_after_completion");
    task_pool_t *p = task_pool_create(4);
    task_t t = make_task(100, 3);
    task_pool_add(p, &t);
    free_helper_task(&t);

    task_pool_mark_assigned(p, 100, "worker-A", 1000);
    task_result_t r1 = make_result(100, "worker-A", true, "FIRST");
    task_pool_mark_completed(p, 100, &r1, 2000);
    free_helper_result(&r1);

    /* Un autre résultat arrive (peut-être un retry doublon) */
    task_result_t r2 = make_result(100, "worker-A", true, "SECOND");
    int rc = task_pool_mark_completed(p, 100, &r2, 3000);
    free_helper_result(&r2);
    ASSERT_EQ_INT(rc, -2);  /* état incompatible : pas ASSIGNED */

    /* Le premier résultat reste */
    const task_result_t *got = task_pool_get_result(p, 100);
    ASSERT_TRUE(memcmp(got->output, "FIRST", 5) == 0);

    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_requeue_until_max_retries(void) {
    TEST_BEGIN("requeue_until_max_retries");
    task_pool_t *p = task_pool_create(4);
    task_t t = make_task(50, 2);  /* max_retries = 2 */
    task_pool_add(p, &t);
    free_helper_task(&t);

    /* Assignation 1 */
    task_pool_mark_assigned(p, 50, "w1", 1000);
    int rc = task_pool_requeue(p, 50, 2000);
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(task_pool_find(p, 50)->retry_count, 1);

    /* Assignation 2 */
    task_pool_mark_assigned(p, 50, "w2", 3000);
    rc = task_pool_requeue(p, 50, 4000);
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(task_pool_find(p, 50)->retry_count, 2);

    /* Assignation 3 : 3e retry > max_retries=2 → FAILED */
    task_pool_mark_assigned(p, 50, "w3", 5000);
    rc = task_pool_requeue(p, 50, 6000);
    ASSERT_EQ_INT(rc, 1);  /* 1 = passé en FAILED */
    ASSERT_EQ_INT(task_pool_find(p, 50)->state, TASK_FAILED);

    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_requeue_worker_tasks(void) {
    TEST_BEGIN("requeue_worker_tasks");
    task_pool_t *p = task_pool_create(8);
    for (uint32_t i = 1; i <= 5; i++) {
        task_t t = make_task(i, 3);
        task_pool_add(p, &t);
        free_helper_task(&t);
    }

    /* Assigner 3 tâches au worker mort, 2 au survivant */
    task_pool_mark_assigned(p, 1, "DEAD", 100);
    task_pool_mark_assigned(p, 2, "ALIVE", 100);
    task_pool_mark_assigned(p, 3, "DEAD", 100);
    task_pool_mark_assigned(p, 4, "ALIVE", 100);
    task_pool_mark_assigned(p, 5, "DEAD", 100);

    uint32_t requeued[10] = {0}, failed[10] = {0};
    size_t n = task_pool_requeue_worker_tasks(p, "DEAD", 5000,
                                               requeued, 10, failed, 10);
    ASSERT_EQ_INT(n, 3);

    /* Les 3 tâches du DEAD sont REQUEUED */
    ASSERT_EQ_INT(task_pool_find(p, 1)->state, TASK_REQUEUED);
    ASSERT_EQ_INT(task_pool_find(p, 3)->state, TASK_REQUEUED);
    ASSERT_EQ_INT(task_pool_find(p, 5)->state, TASK_REQUEUED);
    /* Les 2 tâches du survivant restent ASSIGNED */
    ASSERT_EQ_INT(task_pool_find(p, 2)->state, TASK_ASSIGNED);
    ASSERT_EQ_INT(task_pool_find(p, 4)->state, TASK_ASSIGNED);

    /* peek_pending doit maintenant renvoyer 1, 3, 5 ou un de ceux-là */
    task_t *next = task_pool_peek_pending(p);
    ASSERT_TRUE(next != NULL);
    ASSERT_TRUE(next->task_id == 1 || next->task_id == 3 ||
                next->task_id == 5);

    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_stats(void) {
    TEST_BEGIN("stats");
    task_pool_t *p = task_pool_create(8);
    for (uint32_t i = 1; i <= 6; i++) {
        task_t t = make_task(i, 3);
        task_pool_add(p, &t);
        free_helper_task(&t);
    }
    task_pool_mark_assigned(p, 1, "A", 100);
    task_pool_mark_assigned(p, 2, "B", 100);
    task_result_t r = make_result(1, "A", true, "OK");
    task_pool_mark_completed(p, 1, &r, 200);
    free_helper_result(&r);
    task_pool_mark_failed(p, 3, "bug", 200);

    task_pool_stats_t s;
    task_pool_get_stats(p, &s);
    ASSERT_EQ_INT(s.total, 6);
    ASSERT_EQ_INT(s.completed, 1);
    ASSERT_EQ_INT(s.assigned, 1);
    ASSERT_EQ_INT(s.failed, 1);
    ASSERT_EQ_INT(s.pending, 3);
    ASSERT_EQ_INT(s.requeued, 0);

    task_pool_destroy(p);
    TEST_END_PASS();
}

static void test_capacity_growth(void) {
    TEST_BEGIN("capacity_growth");
    task_pool_t *p = task_pool_create(2);  /* petite capacité */
    for (uint32_t i = 1; i <= 50; i++) {
        task_t t = make_task(i, 3);
        int rc = task_pool_add(p, &t);
        free_helper_task(&t);
        ASSERT_EQ_INT(rc, 0);
    }
    ASSERT_EQ_INT(task_pool_count(p), 50);
    /* Toutes retrouvables ? */
    for (uint32_t i = 1; i <= 50; i++) {
        ASSERT_TRUE(task_pool_find(p, i) != NULL);
    }
    task_pool_destroy(p);
    TEST_END_PASS();
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(void) {
    printf("Running task_pool tests...\n\n");
    test_create_and_destroy();
    test_add_single();
    test_add_duplicate_id();
    test_fifo_pop();
    test_assign_and_complete();
    test_late_result_from_zombie_worker();
    test_late_result_after_completion();
    test_requeue_until_max_retries();
    test_requeue_worker_tasks();
    test_stats();
    test_capacity_growth();
    TEST_REPORT();
    return TEST_EXIT_CODE();
}