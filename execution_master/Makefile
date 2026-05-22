# ==============================================================================
# Makefile — Execution Master (PARALLAX)
# ==============================================================================

CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic -O2 -g
LDFLAGS = -lpthread

TARGET     = execution_master
TARGET_SIM = execution_master_sim

SRCS = execution_master.c \
       em_parser.c         \
       em_combiner.c       \
       em_ipc.c            \
       main.c

OBJS     = $(SRCS:.c=.o)
OBJS_SIM = $(SRCS:.c=.sim.o)

.PHONY: all sim clean help

## Binaire normal (nécessite un vrai Orchestrateur en face)
all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "[BUILD] $(TARGET) prêt."

## Binaire standalone avec simulateur d'orchestrateur intégré
sim: $(OBJS_SIM)
	$(CC) $(CFLAGS) -DSTANDALONE -o $(TARGET_SIM) $^ $(LDFLAGS)
	@echo "[BUILD] $(TARGET_SIM) prêt (mode simulateur)."

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

%.sim.o: %.c
	$(CC) $(CFLAGS) -DSTANDALONE -c -o $@ $<

clean:
	rm -f $(OBJS) $(OBJS_SIM) $(TARGET) $(TARGET_SIM)
	@echo "[CLEAN] Répertoire nettoyé."

help:
	@echo "Cibles disponibles :"
	@echo "  all    : compile le binaire principal (nécessite Orchestrateur)"
	@echo "  sim    : compile avec simulateur d'Orchestrateur intégré"
	@echo "  clean  : supprime les fichiers compilés"
