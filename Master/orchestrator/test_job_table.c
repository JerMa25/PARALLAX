/*
 * test_job_table.c
 *
 * Tests unitaires de job_table, en particulier de la logique
 * d'agrégation des états de tâches en état de job.
 */

#include "test_framework.h"
#include "job_table.h"

#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

/* Ajoute n tâches PENDING à la pool d'un job, ids 1..n, max_retries=2. */
static void seed_pool(job_table_t *jt, uint64_t job_id, uint32_t n) {
    task_pool_t *pool = job_table_get_pool(jt, job_id);
    for (uint32_t i = 1; i <= n; i++) {
        task_t t;
        memset(&t, 0, sizeof(t));
        t.task_id = i;
        t.state = TASK_PENDING;
        t.max_retries = 2;
        t.payload = malloc(1);
        t.payload[0] = (uint8_t)i;
        t.payload_size = 1;
        task_pool_add(pool, &t);
        free(t.payload);
    }
}

/* Marque toutes les tâches d'un pool comme COMPLETED (utilitaire test). */
static void complete_all(task_pool_t *pool, const char *worker, size_t n) {
    for (uint32_t i = 1; i <= (uint32_t)n; i++) {
        task_pool_mark_assigned(pool, i, worker, 1000);
        task_result_t r;
        memset(&r, 0, sizeof(r));
        r.task_id = i;
        strncpy(r.worker_id, worker, PARALLAX_UUID_LEN - 1);
        r.success = true;
        task_pool_mark_completed(pool, i, &r, 2000);
    }
}

/* ------------------------------------------------------------------ */
/* Tests                                                               */
/* ------------------------------------------------------------------ */

static void test_create_and_destroy(void) {
    TEST_BEGIN("create_and_destroy");
    job_table_t *t = job_table_create(0);
    ASSERT_TRUE(t != NULL);
    ASSERT_EQ_INT(job_table_count(t), 0);
    job_table_destroy(t);
    job_table_destroy(NULL);
    TEST_END_PASS();
}

static void test_add_job_basic(void) {
    TEST_BEGIN("add_job_basic");
    job_table_t *t = job_table_create(4);
    uint64_t id1 = job_table_add_job(t, "alice", 1000);
    uint64_t id2 = job_table_add_job(t, "bob", 1100);
    ASSERT_TRUE(id1 == 1);
    ASSERT_TRUE(id2 == 2);
    ASSERT_EQ_INT(job_table_count(t), 2);

    job_info_t info;
    ASSERT_EQ_INT(job_table_get_info(t, id1, &info), 0);
    ASSERT_EQ_STR(info.client_id, "alice");
    ASSERT_EQ_INT(info.state, JOB_SUBMITTED);
    ASSERT_EQ_INT(info.submitted_ms, 1000);
    ASSERT_EQ_INT(info.stats.total, 0);

    /* Pool existe et est vide */
    task_pool_t *pool = job_table_get_pool(t, id1);
    ASSERT_TRUE(pool != NULL);
    ASSERT_EQ_INT(task_pool_count(pool), 0);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_mark_ready(void) {
    TEST_BEGIN("mark_ready");
    job_table_t *t = job_table_create(4);
    uint64_t jid = job_table_add_job(t, "alice", 1000);
    seed_pool(t, jid, 3);

    int rc = job_table_mark_ready(t, jid);
    ASSERT_EQ_INT(rc, 0);

    job_info_t info;
    job_table_get_info(t, jid, &info);
    ASSERT_EQ_INT(info.state, JOB_READY);

    /* Double mark_ready : -2 (mauvaise transition) */
    rc = job_table_mark_ready(t, jid);
    ASSERT_EQ_INT(rc, -2);

    /* Job inexistant */
    rc = job_table_mark_ready(t, 99999);
    ASSERT_EQ_INT(rc, -1);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_reevaluate_running(void) {
    /* Job READY → assigne 1 tâche → reevaluate → JOB_RUNNING */
    TEST_BEGIN("reevaluate_running");
    job_table_t *t = job_table_create(4);
    uint64_t jid = job_table_add_job(t, "alice", 1000);
    seed_pool(t, jid, 5);
    job_table_mark_ready(t, jid);

    task_pool_t *pool = job_table_get_pool(t, jid);
    task_pool_mark_assigned(pool, 1, "worker-A", 1500);

    job_state_t s = job_table_reevaluate(t, jid, 1500);
    ASSERT_EQ_INT(s, JOB_RUNNING);

    job_info_t info;
    job_table_get_info(t, jid, &info);
    ASSERT_EQ_INT(info.started_ms, 1500);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_reevaluate_all_completed(void) {
    /* Toutes tâches COMPLETED → JOB_COMPLETED */
    TEST_BEGIN("reevaluate_all_completed");
    job_table_t *t = job_table_create(4);
    uint64_t jid = job_table_add_job(t, "alice", 1000);
    seed_pool(t, jid, 4);
    job_table_mark_ready(t, jid);

    task_pool_t *pool = job_table_get_pool(t, jid);
    complete_all(pool, "worker-A", 4);

    job_state_t s = job_table_reevaluate(t, jid, 5000);
    ASSERT_EQ_INT(s, JOB_COMPLETED);

    job_info_t info;
    job_table_get_info(t, jid, &info);
    ASSERT_EQ_INT(info.completed_ms, 5000);
    ASSERT_EQ_INT(info.stats.completed, 4);

    /* Reevaluate à nouveau ne change pas completed_ms */
    s = job_table_reevaluate(t, jid, 9999);
    job_table_get_info(t, jid, &info);
    ASSERT_EQ_INT(info.completed_ms, 5000);  /* inchangé */

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_reevaluate_partial_success(void) {
    /* 2 tâches OK, 2 FAILED → JOB_COMPLETED_PARTIAL */
    TEST_BEGIN("reevaluate_partial_success");
    job_table_t *t = job_table_create(4);
    uint64_t jid = job_table_add_job(t, "alice", 1000);
    seed_pool(t, jid, 4);
    job_table_mark_ready(t, jid);

    task_pool_t *pool = job_table_get_pool(t, jid);
    /* Tâches 1, 2 : succès */
    for (uint32_t i = 1; i <= 2; i++) {
        task_pool_mark_assigned(pool, i, "w1", 1000);
        task_result_t r = {0};
        r.task_id = i;
        strncpy(r.worker_id, "w1", PARALLAX_UUID_LEN - 1);
        r.success = true;
        task_pool_mark_completed(pool, i, &r, 2000);
    }
    /* Tâches 3, 4 : FAILED définitif */
    task_pool_mark_failed(pool, 3, "bug", 2000);
    task_pool_mark_failed(pool, 4, "bug", 2000);

    job_state_t s = job_table_reevaluate(t, jid, 3000);
    ASSERT_EQ_INT(s, JOB_COMPLETED_PARTIAL);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_reevaluate_all_failed(void) {
    /* Toutes FAILED → JOB_FAILED */
    TEST_BEGIN("reevaluate_all_failed");
    job_table_t *t = job_table_create(4);
    uint64_t jid = job_table_add_job(t, "alice", 1000);
    seed_pool(t, jid, 3);
    job_table_mark_ready(t, jid);

    task_pool_t *pool = job_table_get_pool(t, jid);
    for (uint32_t i = 1; i <= 3; i++) {
        task_pool_mark_failed(pool, i, "broken", 2000);
    }
    job_state_t s = job_table_reevaluate(t, jid, 3000);
    ASSERT_EQ_INT(s, JOB_FAILED);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_reevaluate_in_progress(void) {
    /* Mix de COMPLETED, ASSIGNED, PENDING → reste RUNNING */
    TEST_BEGIN("reevaluate_in_progress");
    job_table_t *t = job_table_create(4);
    uint64_t jid = job_table_add_job(t, "alice", 1000);
    seed_pool(t, jid, 5);
    job_table_mark_ready(t, jid);

    task_pool_t *pool = job_table_get_pool(t, jid);
    /* Tâche 1 : COMPLETED */
    task_pool_mark_assigned(pool, 1, "w1", 1000);
    task_result_t r = {0};
    r.task_id = 1;
    strncpy(r.worker_id, "w1", PARALLAX_UUID_LEN - 1);
    r.success = true;
    task_pool_mark_completed(pool, 1, &r, 2000);
    /* Tâche 2 : ASSIGNED */
    task_pool_mark_assigned(pool, 2, "w2", 2000);
    /* Tâches 3-5 : PENDING */

    job_state_t s = job_table_reevaluate(t, jid, 2500);
    ASSERT_EQ_INT(s, JOB_RUNNING);

    job_info_t info;
    job_table_get_info(t, jid, &info);
    ASSERT_EQ_INT(info.completed_ms, 0);  /* pas encore terminé */

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_cancel(void) {
    TEST_BEGIN("cancel");
    job_table_t *t = job_table_create(4);
    uint64_t jid = job_table_add_job(t, "alice", 1000);
    seed_pool(t, jid, 5);
    job_table_mark_ready(t, jid);

    int rc = job_table_cancel(t, jid, 3000);
    ASSERT_EQ_INT(rc, 0);

    job_info_t info;
    job_table_get_info(t, jid, &info);
    ASSERT_EQ_INT(info.state, JOB_CANCELLED);
    ASSERT_EQ_INT(info.completed_ms, 3000);

    /* Reevaluate après CANCELLED ne doit PAS changer l'état */
    task_pool_t *pool = job_table_get_pool(t, jid);
    complete_all(pool, "w1", 5);
    job_state_t s = job_table_reevaluate(t, jid, 4000);
    ASSERT_EQ_INT(s, JOB_CANCELLED);  /* reste CANCELLED malgré tout OK */

    /* Double cancel : -2 */
    rc = job_table_cancel(t, jid, 5000);
    ASSERT_EQ_INT(rc, -2);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_remove_job(void) {
    TEST_BEGIN("remove_job");
    job_table_t *t = job_table_create(4);
    uint64_t j1 = job_table_add_job(t, "a", 1000);
    uint64_t j2 = job_table_add_job(t, "b", 1000);
    uint64_t j3 = job_table_add_job(t, "c", 1000);
    seed_pool(t, j1, 3);
    seed_pool(t, j2, 3);
    seed_pool(t, j3, 3);

    int rc = job_table_remove_job(t, j2);
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(job_table_count(t), 2);

    /* j2 introuvable, j1 et j3 toujours là */
    ASSERT_TRUE(job_table_get_pool(t, j2) == NULL);
    ASSERT_TRUE(job_table_get_pool(t, j1) != NULL);
    ASSERT_TRUE(job_table_get_pool(t, j3) != NULL);

    /* Remove inexistant */
    rc = job_table_remove_job(t, 99999);
    ASSERT_EQ_INT(rc, -1);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_count_in_state(void) {
    TEST_BEGIN("count_in_state");
    job_table_t *t = job_table_create(4);
    uint64_t j1 = job_table_add_job(t, "a", 1000);
    uint64_t j2 = job_table_add_job(t, "b", 1000);
    uint64_t j3 = job_table_add_job(t, "c", 1000);
    seed_pool(t, j1, 1);
    seed_pool(t, j2, 1);
    seed_pool(t, j3, 1);
    job_table_mark_ready(t, j1);
    job_table_mark_ready(t, j2);
    /* j3 reste SUBMITTED */

    ASSERT_EQ_INT(job_table_count_in_state(t, JOB_SUBMITTED), 1);
    ASSERT_EQ_INT(job_table_count_in_state(t, JOB_READY), 2);
    ASSERT_EQ_INT(job_table_count_in_state(t, JOB_RUNNING), 0);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_id_uniqueness_after_remove(void) {
    /* Vérifie qu'on ne réutilise PAS les ids après suppression
     * (sinon des messages tardifs pourraient écraser le mauvais job). */
    TEST_BEGIN("id_uniqueness_after_remove");
    job_table_t *t = job_table_create(4);
    uint64_t j1 = job_table_add_job(t, "a", 1000);
    job_table_remove_job(t, j1);
    uint64_t j2 = job_table_add_job(t, "b", 2000);

    ASSERT_TRUE(j2 != j1);  /* nouvel id, jamais le même */
    ASSERT_TRUE(j2 == 2);   /* compteur monotone */

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_snapshot_all(void) {
    TEST_BEGIN("snapshot_all");
    job_table_t *t = job_table_create(4);
    uint64_t j1 = job_table_add_job(t, "alice", 1000);
    uint64_t j2 = job_table_add_job(t, "bob", 1100);
    seed_pool(t, j1, 2);
    seed_pool(t, j2, 3);

    job_info_t infos[10];
    size_t n = job_table_snapshot_all(t, infos, 10);
    ASSERT_EQ_INT(n, 2);

    int found_alice = 0, found_bob = 0;
    for (size_t i = 0; i < n; i++) {
        if (strcmp(infos[i].client_id, "alice") == 0) {
            found_alice = 1;
            ASSERT_EQ_INT(infos[i].stats.total, 2);
        }
        if (strcmp(infos[i].client_id, "bob") == 0) {
            found_bob = 1;
            ASSERT_EQ_INT(infos[i].stats.total, 3);
        }
    }
    ASSERT_TRUE(found_alice && found_bob);

    job_table_destroy(t);
    TEST_END_PASS();
}

static void test_zero_tasks_job(void) {
    /* Edge case : un job sans tâches. Reevaluate ne doit pas crasher
     * et ne doit pas le faire passer en COMPLETED par accident. */
    TEST_BEGIN("zero_tasks_job");
    job_table_t *t = job_table_create(4);
    uint64_t jid = job_table_add_job(t, "alice", 1000);
    job_table_mark_ready(t, jid);

    job_state_t s = job_table_reevaluate(t, jid, 2000);
    ASSERT_EQ_INT(s, JOB_READY);  /* PAS COMPLETED par défaut */

    job_table_destroy(t);
    TEST_END_PASS();
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(void) {
    printf("Running job_table tests...\n\n");
    test_create_and_destroy();
    test_add_job_basic();
    test_mark_ready();
    test_reevaluate_running();
    test_reevaluate_all_completed();
    test_reevaluate_partial_success();
    test_reevaluate_all_failed();
    test_reevaluate_in_progress();
    test_cancel();
    test_remove_job();
    test_count_in_state();
    test_id_uniqueness_after_remove();
    test_snapshot_all();
    test_zero_tasks_job();
    TEST_REPORT();
    return TEST_EXIT_CODE();
}
