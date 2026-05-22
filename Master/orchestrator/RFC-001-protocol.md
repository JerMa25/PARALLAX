# PARALLAX RFC-001 : Protocole d'événements et d'actions de l'orchestrator

**Version** : 1.0
**Auteur** : NZIELEU Nathan (orchestrator)
**Audience** : Ngonga (réseau), Bala/Ulrich (Execution Master), Roy (Parser),
              Farelle (State Receiver), Kouam (Panne Detection), Tchapda (Worker)

---

## 1. Modèle d'interaction

L'orchestrator est un module **passif, event-driven, single-threaded**. Il :
- consomme des `orchestrator_event_t` via `orchestrator_handle_event()`
- produit des `orchestrator_action_t` via `orchestrator_drain_outgoing()`
- ne fait aucun I/O direct

La couche réseau (Ngonga) est responsable de :
- décoder les paquets entrants en `orchestrator_event_t` et les pousser
- consommer les `orchestrator_action_t` et les envoyer sur le wire

---

## 2. Événements entrants (vers l'orchestrator)

### 2.1 `EVT_JOB_SUBMITTED`
**Émetteur** : Execution Master (après que le Parser a décomposé le programme)

```c
struct evt_job_submitted_t {
    char     client_id[64];      // chercheur qui soumet
    const task_t *tasks;          // tableau des tâches
    size_t    n_tasks;
};
```

Chaque `task_t` contient un `task_id` unique dans le job, un `payload`
(opaque, typiquement du code C source) et un `payload_size`. L'orchestrator
copie tout en interne — l'émetteur peut libérer après l'appel.

### 2.2 `EVT_TASK_RESULT`
**Émetteur** : couche réseau (sur réception du résultat d'un worker)

```c
struct evt_task_result_t {
    uint64_t      job_id;
    task_result_t result;        // task_id, worker_id, success, output, ...
};
```

L'orchestrator gère les résultats tardifs et les rejette silencieusement
(at-least-once + idempotence).

### 2.3 `EVT_WORKER_JOINED`
**Émetteur** : Controller (State Receiver) sur réception d'un HELLO

```c
struct evt_worker_joined_t {
    char node_id[37];                    // UUID stable
    char node_name[64];                  // hostname
    worker_capabilities_t caps;          // ram_mb, cpu_count, cpu_mhz
};
```

### 2.4 `EVT_WORKER_LEFT`
**Émetteur** : Controller sur graceful shutdown

```c
struct evt_worker_left_t { char node_id[37]; };
```

### 2.5 `EVT_WORKER_HEARTBEAT`
**Émetteur** : Controller (State Receiver) à chaque heartbeat reçu

```c
struct evt_worker_heartbeat_t { char node_id[37]; };
```

### 2.6 `EVT_WORKER_FAILED`
**Émetteur** : Controller (Panne Detection) après confirmation de mort

```c
struct evt_worker_failed_t {
    char node_id[37];
    char reason[64];                     // "timeout", "crash", "overload", ...
};
```

### 2.7 `EVT_TICK`
**Émetteur** : main loop (à intervalle régulier, ex. toutes les 1s)

Pas de données. L'orchestrator s'en sert pour détecter les timeouts
proactivement.

---

## 3. Actions sortantes (de l'orchestrator)

### 3.1 `ACT_DISPATCH_TASK`
**Consommateur** : couche réseau (Ngonga) → envoyer la tâche au worker

```c
struct act_dispatch_task_t {
    char     worker_id[37];
    uint64_t job_id;
    uint32_t task_id;
    uint8_t *payload;                    // possédé par l'action
    size_t   payload_size;
};
```

Le consommateur DOIT appeler `orchestrator_action_free_payload()` après
usage (le payload est alloué par l'orchestrator).

### 3.2 `ACT_NOTIFY_JOB_DONE`
**Consommateur** : Execution Master → notifier le client

```c
struct act_notify_job_done_t {
    uint64_t job_id;
    char     client_id[64];
    job_state_t final_state;             // COMPLETED, COMPLETED_PARTIAL, FAILED
    task_pool_stats_t stats;             // completed, failed, ...
};
```

### 3.3 `ACT_LOG`
**Consommateur** : système de log (stdout, fichier, syslog, ...)

```c
struct act_log_t {
    char message[256];
    int  severity;                       // 0=info, 1=warn, 2=error
};
```

---

## 4. Garanties et propriétés

- **Idempotence des résultats** : un même TASK_RESULT reçu deux fois est
  rejeté la deuxième fois (log warn, pas d'erreur).
- **Résultats de zombies rejetés** : si une tâche a été réassignée (worker A
  timeout → worker B), le résultat tardif de A est ignoré.
- **At-least-once** : les workers DOIVENT supposer qu'une tâche peut être
  exécutée plusieurs fois. Les tâches doivent être idempotentes.
- **Borne de retries** : chaque tâche a `max_retries` (défaut 3). Au-delà,
  elle passe en `TASK_FAILED` définitif.
- **Job partiel** : un job avec certaines tâches en `FAILED` se termine en
  `JOB_COMPLETED_PARTIAL`. Le client est notifié avec les stats.

---

## 5. Format de payload de tâche (à figer avec Tchapda)

Décision actuelle : **C source à compiler à la volée côté worker**.

Convention proposée pour le payload :
```c
// Le payload est du code C source (string terminée par '\0' ou non,
// la taille est dans payload_size).
// Le worker compile en .so via gcc -shared, dlopen, et appelle
// la fonction unique :
//
//   void parallax_task(const char *input, size_t input_len,
//                       char **output, size_t *output_len);
//
// L'output est alloué par malloc côté worker, libéré par le runtime.
```

**À valider avec Tchapda et Roy.**

---

## 6. Numérotation des ports (à figer avec Ngonga)

Proposition (cf. discussion précédente) :
- 47000/UDP : découverte (HELLO/HELLO_ACK)
- 47001/UDP : heartbeat
- 47002/UDP : élection bully
- 47010/TCP : communications applicatives (dispatch tâches, résultats)

---

## 7. Versioning

Toute évolution du protocole doit incrémenter la version. Les structures
ajoutent uniquement à la fin pour préserver la compatibilité ascendante.

Champ `version` recommandé dans tous les messages réseau (cf. RFC réseau
de Ngonga, à venir).
