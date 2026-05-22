/*
 * test_scheduler.c
 *
 * Tests unitaires du scheduler Deficit Round Robin pondéré.
 *
 * Tests prioritaires :
 *   - Convergence exacte des ratios sur grand N
 *   - Déterminisme (même séquence à exécution répétée)
 *   - Équité à l'arrivée d'un worker en cours de route
 *   - Robustesse à la désynchronisation worker_table ↔ scheduler
 */

#include "test_framework.h"
#include "scheduler.h"

#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

/* Setup commun : crée un système avec workers et pool initialisés. */
typedef struct {
    scheduler_t    *sched;
    worker_table_t *wt;
    task_pool_t    *pool;
} test_ctx_t;

static test_ctx_t make_ctx(void) {
    test_ctx_t c;
    c.sched = scheduler_create(8);
    c.wt = worker_table_create(8);
    c.pool = task_pool_create(64);
    return c;
}

static void destroy_ctx(test_ctx_t *c) {
    scheduler_destroy(c->sched);
    worker_table_destroy(c->wt);
    task_pool_destroy(c->pool);
}

/* Ajoute un worker dans worker_table ET le scheduler. */
static void add_worker_full(test_ctx_t *c, const char *id, uint64_t score) {
    worker_capabilities_t caps = { .ram_mb = 1024, .cpu_count = 1, .cpu_mhz = 2000 };
    worker_table_add_or_update(c->wt, id, "node", &caps);
    /* Force AVAILABLE par défaut, c'est le cas après add_or_update */
    scheduler_register_worker(c->sched, id, score);
}

/* Ajoute n tâches pending, ids 1..n. */
static void seed_tasks(test_ctx_t *c, uint32_t n) {
    for (uint32_t i = 1; i <= n; i++) {
        task_t t;
        memset(&t, 0, sizeof(t));
        t.task_id = i;
        t.state = TASK_PENDING;
        t.max_retries = 2;
        t.payload = malloc(1);
        t.payload[0] = (uint8_t)i;
        t.payload_size = 1;
        task_pool_add(c->pool, &t);
        free(t.payload);
    }
}

/*
 * Simule une "complétion" d'une tâche : libère le worker.
 * Dans la vraie vie, ce serait fait par la réception d'un résultat
 * (qui appelle task_pool_mark_completed et set_state(AVAILABLE)).
 */
static void complete_task(test_ctx_t *c, const char *worker_id, uint32_t task_id) {
    task_result_t r;
    memset(&r, 0, sizeof(r));
    r.task_id = task_id;
    strncpy(r.worker_id, worker_id, PARALLAX_UUID_LEN - 1);
    r.success = true;
    task_pool_mark_completed(c->pool, task_id, &r, 5000);
    worker_table_set_state(c->wt, worker_id, WORKER_AVAILABLE);
}

/* Compte les attributions pour chaque worker dans actions[]. */
static size_t count_for(const schedule_action_t *actions, size_t n,
                         const char *worker_id) {
    size_t k = 0;
    for (size_t i = 0; i < n; i++) {
        if (strcmp(actions[i].worker_id, worker_id) == 0) k++;
    }
    return k;
}

/* ------------------------------------------------------------------ */
/* Tests                                                               */
/* ------------------------------------------------------------------ */

static void test_create_and_destroy(void) {
    TEST_BEGIN("create_and_destroy");
    scheduler_t *s = scheduler_create(0);
    ASSERT_TRUE(s != NULL);
    ASSERT_EQ_INT(scheduler_worker_count(s), 0);
    ASSERT_EQ_INT(scheduler_total_assignments(s), 0);
    scheduler_destroy(s);
    scheduler_destroy(NULL);
    TEST_END_PASS();
}

static void test_register_worker(void) {
    TEST_BEGIN("register_worker");
    scheduler_t *s = scheduler_create(4);
    ASSERT_EQ_INT(scheduler_register_worker(s, "w1", 100), 0);
    ASSERT_EQ_INT(scheduler_register_worker(s, "w2", 200), 0);
    ASSERT_EQ_INT(scheduler_worker_count(s), 2);

    /* Score = 0 invalide */
    ASSERT_EQ_INT(scheduler_register_worker(s, "w3", 0), -2);
    /* worker_id vide invalide */
    ASSERT_EQ_INT(scheduler_register_worker(s, "", 100), -2);

    /* Mise à jour de score */
    ASSERT_EQ_INT(scheduler_register_worker(s, "w1", 500), 0);
    ASSERT_EQ_INT(scheduler_worker_count(s), 2);  /* pas de nouveau */

    scheduler_destroy(s);
    TEST_END_PASS();
}

static void test_compute_score(void) {
    TEST_BEGIN("compute_score");
    worker_capabilities_t c1 = { .ram_mb = 1024, .cpu_count = 2, .cpu_mhz = 0 };
    /* score = 1024 + 2*1024 = 3072 */
    ASSERT_EQ_INT(scheduler_compute_score(&c1), 3072);

    worker_capabilities_t c2 = { .ram_mb = 8192, .cpu_count = 8, .cpu_mhz = 0 };
    ASSERT_EQ_INT(scheduler_compute_score(&c2), 16384);

    ASSERT_EQ_INT(scheduler_compute_score(NULL), 0);
    TEST_END_PASS();
}

/*
 * Test fondamental : avec scores égaux, on doit obtenir un round-robin
 * parfait. Sur 6 attributions avec 3 workers de score identique : 2/2/2.
 */
static void test_equal_scores_perfect_round_robin(void) {
    TEST_BEGIN("equal_scores_perfect_round_robin");
    test_ctx_t c = make_ctx();
    add_worker_full(&c, "wA", 100);
    add_worker_full(&c, "wB", 100);
    add_worker_full(&c, "wC", 100);
    seed_tasks(&c, 6);

    schedule_action_t actions[10];
    /*
     * Attention : avec k=1 par appel, chaque attribution rend le worker
     * BUSY. Pour distribuer plusieurs tâches, il faut soit des workers
     * différents soit faire complete_task() entre.
     * Ici on teste la première vague : 3 workers AVAILABLE, on devrait
     * en avoir 3 attribués en un appel.
     */
    size_t n = scheduler_schedule(c.sched, c.wt, c.pool, 1, 1000,
                                    actions, 10);
    ASSERT_EQ_INT(n, 3);
    /* Un par worker : counts = 1, 1, 1 */
    ASSERT_EQ_INT(count_for(actions, n, "wA"), 1);
    ASSERT_EQ_INT(count_for(actions, n, "wB"), 1);
    ASSERT_EQ_INT(count_for(actions, n, "wC"), 1);

    /* On simule la fin des 3 tâches */
    complete_task(&c, "wA", actions[0].task_id);
    complete_task(&c, "wB", actions[1].task_id);
    complete_task(&c, "wC", actions[2].task_id);

    /* Deuxième vague */
    schedule_action_t actions2[10];
    size_t n2 = scheduler_schedule(c.sched, c.wt, c.pool, 1, 2000,
                                     actions2, 10);
    ASSERT_EQ_INT(n2, 3);
    /* Total = 6, et chacun a maintenant 2 */
    scheduler_worker_stat_t stats[10];
    size_t ns = scheduler_snapshot_stats(c.sched, stats, 10);
    ASSERT_EQ_INT(ns, 3);
    for (size_t i = 0; i < ns; i++) {
        ASSERT_EQ_INT(stats[i].received_count, 2);
    }

    destroy_ctx(&c);
    TEST_END_PASS();
}

/*
 * Test fondamental : convergence des ratios.
 *
 * Le test précédent (libération synchrone de tous les workers) ne
 * stresse pas le scheduler : avec k=1 et des workers tous AVAILABLE,
 * le scheduler attribue trivialement une tâche à chacun par round.
 *
 * Le test correct doit modéliser une situation où les workers ne sont
 * PAS toujours tous disponibles. Concrètement, on simule des durées
 * d'exécution INVERSEMENT proportionnelles aux scores : un worker
 * puissant termine vite, un faible termine lentement. Cela revient à
 * dire que le worker rapide est plus souvent AVAILABLE.
 *
 * Mais avec k=1 et durées proportionnelles, le ratio s'établit
 * naturellement. Le scheduler pondéré est *utile* quand les durées
 * sont DÉCORRÉLÉES des scores. Pour stresser réellement la dette,
 * on simule l'inverse : tous les workers libérés en même temps,
 * mais on borne le nombre d'attributions par "quantum" de temps
 * pour forcer le scheduler à choisir.
 *
 * Astuce technique : en attribuant 1 seule tâche par appel à
 * scheduler_schedule (max_actions=1), on stresse réellement la
 * sélection par dette : à chaque appel, le scheduler doit choisir
 * UN worker parmi les disponibles. C'est ce cas qui doit converger
 * vers les ratios des scores.
 */
static void test_ratio_convergence_5_3_2(void) {
    TEST_BEGIN("ratio_convergence_5_3_2");
    test_ctx_t c = make_ctx();
    add_worker_full(&c, "wA", 5000);
    add_worker_full(&c, "wB", 3000);
    add_worker_full(&c, "wC", 2000);

    seed_tasks(&c, 1000);

    /*
     * On force max_actions=1 : chaque appel ne fait qu'une attribution.
     * Avant l'appel suivant, on libère le worker concerné.
     * Cela simule une exécution séquentielle sur k=1 workers réels :
     * une tâche est en cours, le scheduler attend la prochaine
     * libération, puis attribue.
     */
    size_t total = 0;
    schedule_action_t buf[1];
    while (total < 1000) {
        size_t n = scheduler_schedule(c.sched, c.wt, c.pool, 1,
                                        1000 + total, buf, 1);
        if (n == 0) break;
        complete_task(&c, buf[0].worker_id, buf[0].task_id);
        total++;
    }
    ASSERT_EQ_INT(total, 1000);

    scheduler_worker_stat_t stats[10];
    scheduler_snapshot_stats(c.sched, stats, 10);

    uint64_t a = 0, b = 0, cc = 0;
    for (size_t i = 0; i < 3; i++) {
        if (strcmp(stats[i].worker_id, "wA") == 0) a = stats[i].received_count;
        if (strcmp(stats[i].worker_id, "wB") == 0) b = stats[i].received_count;
        if (strcmp(stats[i].worker_id, "wC") == 0) cc = stats[i].received_count;
    }
    /*
     * Convergence : sur 1000 tâches avec scores (5000, 3000, 2000) :
     *   quota_A = 1000 * 5000 / 10000 = 500
     *   quota_B = 1000 * 3000 / 10000 = 300
     *   quota_C = 1000 * 2000 / 10000 = 200
     * On doit converger vers ces valeurs avec une erreur <= 1
     * (à cause des départages déterministes en cas d'égalité).
     */
    ASSERT_TRUE(a >= 499 && a <= 501);
    ASSERT_TRUE(b >= 299 && b <= 301);
    ASSERT_TRUE(cc >= 199 && cc <= 201);
    ASSERT_EQ_INT(a + b + cc, 1000);

    destroy_ctx(&c);
    TEST_END_PASS();
}

/*
 * Test du déterminisme : deux runs identiques produisent les mêmes
 * actions dans le même ordre.
 */
static void test_determinism(void) {
    TEST_BEGIN("determinism");

    /* Run 1 */
    test_ctx_t c1 = make_ctx();
    add_worker_full(&c1, "wA", 5000);
    add_worker_full(&c1, "wB", 3000);
    add_worker_full(&c1, "wC", 2000);
    seed_tasks(&c1, 30);
    char run1[30][PARALLAX_UUID_LEN] = {0};
    size_t cnt1 = 0;
    schedule_action_t buf[10];
    while (cnt1 < 30) {
        size_t n = scheduler_schedule(c1.sched, c1.wt, c1.pool, 1,
                                        1000 + cnt1, buf, 10);
        if (n == 0) break;
        for (size_t i = 0; i < n; i++) {
            strncpy(run1[cnt1], buf[i].worker_id, PARALLAX_UUID_LEN - 1);
            complete_task(&c1, buf[i].worker_id, buf[i].task_id);
            cnt1++;
        }
    }
    destroy_ctx(&c1);

    /* Run 2 strictement identique */
    test_ctx_t c2 = make_ctx();
    add_worker_full(&c2, "wA", 5000);
    add_worker_full(&c2, "wB", 3000);
    add_worker_full(&c2, "wC", 2000);
    seed_tasks(&c2, 30);
    char run2[30][PARALLAX_UUID_LEN] = {0};
    size_t cnt2 = 0;
    while (cnt2 < 30) {
        size_t n = scheduler_schedule(c2.sched, c2.wt, c2.pool, 1,
                                        1000 + cnt2, buf, 10);
        if (n == 0) break;
        for (size_t i = 0; i < n; i++) {
            strncpy(run2[cnt2], buf[i].worker_id, PARALLAX_UUID_LEN - 1);
            complete_task(&c2, buf[i].worker_id, buf[i].task_id);
            cnt2++;
        }
    }
    destroy_ctx(&c2);

    /* Les deux séquences doivent être identiques */
    ASSERT_EQ_INT(cnt1, 30);
    ASSERT_EQ_INT(cnt2, 30);
    for (size_t i = 0; i < 30; i++) {
        ASSERT_EQ_STR(run1[i], run2[i]);
    }
    TEST_END_PASS();
}

/*
 * Worker qui rejoint en cours de route : convergence vers l'équilibre
 * à long terme.
 *
 * Ce test vérifie un comportement subtil. À l'arrivée de C alors que
 * A et B ont déjà reçu 25 tâches chacun, le scheduler doit faire
 * converger les ratios vers (1/3, 1/3, 1/3) à long terme — pas à
 * chaque instant.
 *
 * Concrètement, au moment où C arrive :
 *   - total_assignments = 50
 *   - C est initialisé avec received = 50 * 100 / 300 = 16
 *   - dette(A) = 50*100 - 25*300 = -2500 (a beaucoup reçu)
 *   - dette(B) = idem, -2500
 *   - dette(C) = 50*100 - 16*300 = +200
 *
 * C va donc recevoir préférentiellement les prochaines tâches, jusqu'à
 * ce que les ratios convergent. Sur 30 nouvelles attributions, on doit
 * arriver vers 32/32/32 (équilibre parfait à 80 total / 3 ≈ 26.67).
 *
 * Le test vérifie cette convergence asymptotique.
 */
static void test_late_joining_worker_is_fair(void) {
    TEST_BEGIN("late_joining_worker_is_fair");
    test_ctx_t c = make_ctx();
    add_worker_full(&c, "wA", 100);
    add_worker_full(&c, "wB", 100);
    seed_tasks(&c, 100);

    schedule_action_t buf[1];
    size_t total = 0;
    while (total < 50) {
        size_t n = scheduler_schedule(c.sched, c.wt, c.pool, 1,
                                        1000 + total, buf, 1);
        if (n == 0) break;
        complete_task(&c, buf[0].worker_id, buf[0].task_id);
        total++;
    }
    ASSERT_EQ_INT(total, 50);

    /* Ajout du 3e worker à mi-parcours */
    add_worker_full(&c, "wC", 100);

    /* Init de received : doit valoir ~16 pour que C n'ait pas une
     * dette artificiellement élevée mais soit à son quota actuel. */
    scheduler_worker_stat_t stats[10];
    scheduler_snapshot_stats(c.sched, stats, 10);
    uint64_t recv_C_init = 0;
    for (size_t i = 0; i < 3; i++) {
        if (strcmp(stats[i].worker_id, "wC") == 0)
            recv_C_init = stats[i].received_count;
    }
    ASSERT_TRUE(recv_C_init >= 15 && recv_C_init <= 17);

    /* 30 rounds supplémentaires : convergence vers l'équilibre */
    while (total < 80) {
        size_t n = scheduler_schedule(c.sched, c.wt, c.pool, 1,
                                        1000 + total, buf, 1);
        if (n == 0) break;
        complete_task(&c, buf[0].worker_id, buf[0].task_id);
        total++;
    }
    ASSERT_EQ_INT(total, 80);

    scheduler_snapshot_stats(c.sched, stats, 10);
    uint64_t a = 0, b = 0, cc = 0;
    for (size_t i = 0; i < 3; i++) {
        if (strcmp(stats[i].worker_id, "wA") == 0) a = stats[i].received_count;
        if (strcmp(stats[i].worker_id, "wB") == 0) b = stats[i].received_count;
        if (strcmp(stats[i].worker_id, "wC") == 0) cc = stats[i].received_count;
    }
    /*
     * Convergence des received_count :
     * Note importante : received_count inclut l'ancrage virtuel de
     * 16 attribué à C lors de son arrivée (pour préserver l'équité).
     * Les attributions réelles totales (scheduler_total_assignments)
     * doivent être 80, mais a + b + cc inclut l'ancrage virtuel.
     *
     * À l'équilibre des dettes (total=80, 3 workers égaux) :
     *   received_w = total * S_w / sum_S = 80/3 ≈ 26.67
     * MAIS comme C est parti de 16 (init virtuel) et a un budget
     * réel à recevoir, les dettes s'égalisent à received = 32 chacun.
     * Mesure observée : 32/32/32.
     */
    ASSERT_TRUE(a >= 30 && a <= 34);
    ASSERT_TRUE(b >= 30 && b <= 34);
    ASSERT_TRUE(cc >= 30 && cc <= 34);
    /* Le compteur réel d'attributions du scheduler = 80 */
    ASSERT_EQ_INT(scheduler_total_assignments(c.sched), 80);

    /*
     * Pas de monopole de C : il a reçu plus que 0 mais pas tout.
     * diff_C = cc - recv_C_init représente les tâches RÉELLEMENT
     * attribuées à C par le scheduler après son arrivée. Sur 30
     * attributions totales avec 3 workers égaux, ça donne ~16 pour C
     * (rattrapage), ~7 pour A et ~7 pour B.
     */
    uint64_t diff_C = cc - recv_C_init;
    ASSERT_TRUE(diff_C < 30);  /* pas tout pris */
    ASSERT_TRUE(diff_C > 10);  /* mais beaucoup quand même : rattrapage */

    destroy_ctx(&c);
    TEST_END_PASS();
}

/*
 * Désynchronisation : un worker AVAILABLE dans worker_table mais pas
 * dans le scheduler doit être ignoré, sans crash.
 */
static void test_unregistered_worker_is_ignored(void) {
    TEST_BEGIN("unregistered_worker_is_ignored");
    test_ctx_t c = make_ctx();
    /* wA est dans les deux structures */
    add_worker_full(&c, "wA", 100);
    /* wGHOST n'est QUE dans worker_table */
    worker_capabilities_t caps = {.ram_mb = 1024, .cpu_count = 1};
    worker_table_add_or_update(c.wt, "wGHOST", "ghost", &caps);

    seed_tasks(&c, 5);
    schedule_action_t buf[10];
    size_t n = scheduler_schedule(c.sched, c.wt, c.pool, 1, 1000, buf, 10);
    /* Une seule attribution doit être faite (à wA), wGHOST ignoré */
    ASSERT_EQ_INT(n, 1);
    ASSERT_EQ_STR(buf[0].worker_id, "wA");

    destroy_ctx(&c);
    TEST_END_PASS();
}

/*
 * Aucun worker AVAILABLE : schedule() doit renvoyer 0 sans rien casser.
 */
static void test_no_available_workers(void) {
    TEST_BEGIN("no_available_workers");
    test_ctx_t c = make_ctx();
    add_worker_full(&c, "wA", 100);
    /* Marquer wA comme BUSY */
    worker_table_set_state(c.wt, "wA", WORKER_BUSY);
    seed_tasks(&c, 5);

    schedule_action_t buf[10];
    size_t n = scheduler_schedule(c.sched, c.wt, c.pool, 1, 1000, buf, 10);
    ASSERT_EQ_INT(n, 0);
    /* Le pool ne doit pas être affecté */
    ASSERT_EQ_INT(task_pool_count_in_state(c.pool, TASK_PENDING), 5);

    destroy_ctx(&c);
    TEST_END_PASS();
}

/*
 * Aucune tâche pending : pas d'attribution.
 */
static void test_no_pending_tasks(void) {
    TEST_BEGIN("no_pending_tasks");
    test_ctx_t c = make_ctx();
    add_worker_full(&c, "wA", 100);
    /* Pas de tâches dans le pool */

    schedule_action_t buf[10];
    size_t n = scheduler_schedule(c.sched, c.wt, c.pool, 1, 1000, buf, 10);
    ASSERT_EQ_INT(n, 0);

    destroy_ctx(&c);
    TEST_END_PASS();
}

/*
 * Test que mark_assigned a bien été appelé : la tâche passe en ASSIGNED
 * et le worker passe en BUSY.
 */
static void test_state_propagation(void) {
    TEST_BEGIN("state_propagation");
    test_ctx_t c = make_ctx();
    add_worker_full(&c, "wA", 100);
    seed_tasks(&c, 1);

    schedule_action_t buf[10];
    size_t n = scheduler_schedule(c.sched, c.wt, c.pool, 42, 1000, buf, 10);
    ASSERT_EQ_INT(n, 1);
    ASSERT_EQ_INT(buf[0].job_id, 42);

    /* La tâche est ASSIGNED à wA */
    task_t *t = task_pool_find(c.pool, 1);
    ASSERT_EQ_INT(t->state, TASK_ASSIGNED);
    ASSERT_EQ_STR(t->assigned_worker, "wA");
    /* wA est BUSY */
    worker_t *w = worker_table_find(c.wt, "wA");
    ASSERT_EQ_INT(w->state, WORKER_BUSY);

    destroy_ctx(&c);
    TEST_END_PASS();
}

/*
 * Unregister : le scheduler retire correctement le worker, sum_scores
 * est ajusté, et les workers restants ne sont pas perturbés.
 */
static void test_unregister(void) {
    TEST_BEGIN("unregister");
    scheduler_t *s = scheduler_create(4);
    scheduler_register_worker(s, "wA", 100);
    scheduler_register_worker(s, "wB", 200);
    scheduler_register_worker(s, "wC", 300);
    ASSERT_EQ_INT(scheduler_worker_count(s), 3);

    int rc = scheduler_unregister_worker(s, "wB");
    ASSERT_EQ_INT(rc, 0);
    ASSERT_EQ_INT(scheduler_worker_count(s), 2);

    rc = scheduler_unregister_worker(s, "wB");  /* déjà parti */
    ASSERT_EQ_INT(rc, -1);

    scheduler_destroy(s);
    TEST_END_PASS();
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(void) {
    printf("Running scheduler tests...\n\n");
    test_create_and_destroy();
    test_register_worker();
    test_compute_score();
    test_equal_scores_perfect_round_robin();
    test_ratio_convergence_5_3_2();
    test_determinism();
    test_late_joining_worker_is_fair();
    test_unregistered_worker_is_ignored();
    test_no_available_workers();
    test_no_pending_tasks();
    test_state_propagation();
    test_unregister();
    TEST_REPORT();
    return TEST_EXIT_CODE();
}
