/*
 * test_framework.h
 *
 * Mini-framework de tests, header-only, zéro dépendance.
 * Conçu pour rester lisible : pas de macro magique, juste l'essentiel.
 */

#ifndef PARALLAX_TEST_FRAMEWORK_H
#define PARALLAX_TEST_FRAMEWORK_H

#include <stdio.h>
#include <string.h>

static int  g_tests_run     = 0;
static int  g_tests_passed  = 0;
static int  g_tests_failed  = 0;
static const char *g_current_test = "";

#define TEST_BEGIN(name) do { \
    g_current_test = name; \
    g_tests_run++; \
    printf("  [ RUN  ] %s\n", name); \
} while (0)

#define TEST_END_PASS() do { \
    g_tests_passed++; \
    printf("  [  OK  ] %s\n", g_current_test); \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        g_tests_failed++; \
        printf("  [ FAIL ] %s\n           %s:%d: ASSERT_TRUE(%s)\n", \
               g_current_test, __FILE__, __LINE__, #cond); \
        return; \
    } \
} while (0)

#define ASSERT_EQ_INT(actual, expected) do { \
    long long _a = (long long)(actual); \
    long long _e = (long long)(expected); \
    if (_a != _e) { \
        g_tests_failed++; \
        printf("  [ FAIL ] %s\n           %s:%d: expected %lld, got %lld\n", \
               g_current_test, __FILE__, __LINE__, _e, _a); \
        return; \
    } \
} while (0)

#define ASSERT_EQ_STR(actual, expected) do { \
    const char *_a = (actual); \
    const char *_e = (expected); \
    if (!_a || !_e || strcmp(_a, _e) != 0) { \
        g_tests_failed++; \
        printf("  [ FAIL ] %s\n           %s:%d: expected \"%s\", got \"%s\"\n", \
               g_current_test, __FILE__, __LINE__, \
               _e ? _e : "(null)", _a ? _a : "(null)"); \
        return; \
    } \
} while (0)

#define TEST_RUN(fn) do { fn(); if (g_tests_failed == 0 || \
    g_tests_passed + g_tests_failed == g_tests_run) TEST_END_PASS(); } while(0)

/* Version simple : on appelle la fonction, on signale OK si pas de FAIL.
 * À usage interne d'un main de test. */
#define TEST_REPORT() do { \
    printf("\n=== %d run, %d passed, %d failed ===\n", \
           g_tests_run, g_tests_passed, g_tests_failed); \
} while (0)

#define TEST_EXIT_CODE() (g_tests_failed == 0 ? 0 : 1)

#endif /* PARALLAX_TEST_FRAMEWORK_H */
