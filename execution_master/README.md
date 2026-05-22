# Execution Master — PARALLAX

**Auteur :** Bala Andegue François Lionnel  
**Branche :** `execution_master_BALA`  
**Rôle dans le nœud :** `ROLE_MASTER`

---

## Rôle et responsabilité

L'Execution Master est le **chef d'orchestre côté client** du système PARALLAX. Il reçoit un job de calcul (ex. : `A × B`), le découpe en tâches élémentaires et les confie à l'Orchestrateur pour distribution sur le cluster. Il attend ensuite les résultats et les recombine.

**Ce qu'il fait :**
- Décompose `A` en chunks positionnels (Parser)
- Soumet le bag-of-tasks à l'Orchestrateur via IPC
- Attend passivement la réponse de l'Orchestrateur
- Reconstruit le résultat final (Combiner)

**Ce qu'il ne fait PAS :**
- Exécuter des calculs → rôle des **Workers** (`feature-execution-worker`)
- Gérer les retries de tâches → rôle de l'**Orchestrateur** (`orchestrator`)
- Superviser les timeouts des workers → rôle du **Watchdog** (`feature/state-receiver`)
- Connaître l'état du cluster → rôle du **Controller** (`master_servant`)

---

## Architecture interne

```
em_run_job(A, B)
     │
     ├─[1]─ em_parser_decompose()    → em_task_t[]
     │          A = 987654321
     │          chunks : 321×10^0  654×10^3  987×10^6
     │          em_parser_verify()   → intégrité garantie
     │
     ├─[2]─ em_ipc_submit_job()      → IPC (System V msg queue)
     │          → EVT_JOB_SUBMITTED vers l'Orchestrateur
     │
     ├─[3]─ em_ipc_wait_result()     ← IPC (bloquant, avec timeout)
     │          ← ACT_NOTIFY_JOB_DONE depuis l'Orchestrateur
     │
     └─[4]─ em_combiner_reconstruct() → em_job_result_t
                Σ (partial_i × 10^position_i) = A × B
```

---

## Structure des fichiers

```
execution_master/
├── em_types.h           Types partagés (em_job_t, em_task_t, em_task_result_t…)
├── em_parser.h/c        Décomposition positionnelle du job en tâches
├── em_combiner.h/c      Recombinaison Σ(partial × 10^position)
├── em_ipc.h/c           Canal IPC System V vers l'Orchestrateur
├── execution_master.h   Interface publique du moteur
├── execution_master.c   Moteur principal (init, run, shutdown, logging)
├── main.c               Point d'entrée + simulateur d'orchestrateur
└── Makefile             Cibles : all, sim, clean, help
```

---

## Algorithme du Parser

La décomposition de `A` en chunks de `chunk_digits` chiffres décimaux est positionnelle, de droite à gauche :

```
A = 987654321,  chunk_digits = 3,  divisor = 1000

iter 0 : chunk = 987654321 % 1000 = 321,  remaining = 987654,  position = 0
iter 1 : chunk = 987654    % 1000 = 654,  remaining = 987,     position = 3
iter 2 : chunk = 987       % 1000 = 987,  remaining = 0,       position = 6

Tâches produites :
  task[0] : chunk=321, multiplier=B, position=0   →  321×B × 10^0
  task[1] : chunk=654, multiplier=B, position=3   →  654×B × 10^3
  task[2] : chunk=987, multiplier=B, position=6   →  987×B × 10^6

Vérification : 321 + 654×10³ + 987×10⁶ = 987 654 321 = A  ✓
```

`em_parser_verify()` contrôle cette propriété avant toute soumission.

---

## IPC avec l'Orchestrateur

| Direction               | Clé IPC      | Type de message     |
|-------------------------|--------------|---------------------|
| EM → Orchestrateur      | `0x504C5801` | `EM_MSG_JOB_SUBMIT` |
| Orchestrateur → EM      | `0x504C5802` | `EM_MSG_JOB_RESULT` |

Mécanisme : **System V Message Queues** (`msgget` / `msgsnd` / `msgrcv`), cohérent avec la branche `feature/state-receiver`.

**Point d'intégration :** quand le Network Thread (`network-thread`) sera intégré, seul `em_ipc.c` sera remplacé. L'interface `em_ipc_*` reste stable pour l'Execution Master.

---

## Compilation et test

```bash
# Binaire normal (nécessite un vrai Orchestrateur)
make

# Binaire standalone avec simulateur d'Orchestrateur intégré
make sim

# Nettoyage
make clean
```

### Exécution standalone

```bash
./execution_master_sim 987654321 56789
```

### Sortie attendue

```
╔══════════════════════════════════════════════╗
║           PARALLAX — Execution Master        ║
║          Calcul distribué de A × B           ║
╚══════════════════════════════════════════════╝

  A = 987654321
  B = 56789
  Attendu = 56088834169

[EXEC_MASTER] [INFO] Parser OK : 3 tâches créées.
[EXEC_MASTER] [INFO] Soumission du job à l'Orchestrateur...
[SIM_ORCH] tâche[0] : 321 × 56789 = 18229269  (position 10^0)
[SIM_ORCH] tâche[1] : 654 × 56789 = 37139706  (position 10^3)
[SIM_ORCH] tâche[2] : 987 × 56789 = 56051343  (position 10^6)

╔══════════════════════════════════════╗
║  Résultat calculé : 56088834169     ║
║  Résultat attendu : 56088834169     ║
║  Vérification     : OK ✓            ║
╚══════════════════════════════════════╝
```

---

## Intégration dans Agent_Init

Dans le nœud Master, l'Execution Master est démarré comme thread `parser` par `Agent_Init` :

```c
// Agent_Init/init.c — à compléter lors de l'intégration
case ROLE_MASTER:
    pthread_create(&agent.threads.parser, NULL,
                   em_thread_entry, &em_config);   // ← Execution Master
    pthread_create(&agent.threads.orchestrator, NULL,
                   orchestrator_thread_run, NULL);
    ...
```

---

## Interfaces avec les autres modules

| Module              | Branche                 | Interface                           |
|---------------------|-------------------------|-------------------------------------|
| Agent Init          | `Agent_Init`            | Thread lancé par `start_threads()`  |
| Orchestrateur       | `orchestrator`          | IPC System V (`em_ipc.h`)           |
| Network Thread      | `network-thread`        | Remplacement futur de `em_ipc.c`    |
| Fault Tolerance     | `gestion-des-pannes`    | Transparent (géré par l'Orchestrateur) |
| Monitoring          | `Monitoring`            | Aucune (passif)                     |
