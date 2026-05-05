#ifndef STATE_RECEIVER_H
#define STATE_RECEIVER_H

#include "node.h"

// ─── State Receiver Thread ────────────────────────────────────────────────────
void  state_receiver_thread_run(NodeTable* table);  // lance le thread
void  state_receiver_stop(void);                    // arrête proprement le thread

// ─── Watchdog Thread ──────────────────────────────────────────────────────────
void  watchdog_thread_run(NodeTable* table);        // lance le thread
void  watchdog_stop(void);                          // arrête proprement le thread

#endif