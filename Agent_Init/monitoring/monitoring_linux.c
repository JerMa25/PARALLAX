#include "Monitoring.h"
#include <stdio.h>
#include <string.h>
#include <sys/statvfs.h>

// ============================================================
// STATIC METRICS — lues une seule fois
// ============================================================
void monitoring_read_static(MachineMetrics *m) {
    // ===== CPU static info =====
    FILE *f = fopen("/proc/cpuinfo", "r");
    if (f) {
        char line[256];
        int core_ids[256] = {0};
        int core_count = 0;
        m->cpu_threads_per_core = 0;

        while (fgets(line, sizeof(line), f)) {
            // Nombre de threads (processor count)
            int processor = 0;
            if (sscanf(line, "processor : %d", &processor) == 1)
                m->cpu_threads_per_core++;

            // Modèle CPU
            if (strncmp(line, "model name", 10) == 0 && m->cpu_model[0] == '\0') {
                char *colon = strchr(line, ':');
                if (colon) {
                    strncpy(m->cpu_model, colon + 2, sizeof(m->cpu_model) - 1);
                    // Retirer le \n
                    m->cpu_model[strcspn(m->cpu_model, "\n")] = '\0';
                }
            }

            // Fréquence CPU
            float freq = 0;
            if (sscanf(line, "cpu MHz : %f", &freq) == 1 && m->cpu_freq_mhz == 0)
                m->cpu_freq_mhz = freq;

            // Cores physiques
            int core_id = 0;
            if (sscanf(line, "core id : %d", &core_id) == 1) {
                int already = 0;
                for (int i = 0; i < core_count; i++)
                    if (core_ids[i] == core_id) { already = 1; break; }
                if (!already) core_ids[core_count++] = core_id;
            }
        }
        fclose(f);
        m->cpu_cores = core_count > 0 ? core_count : m->cpu_threads_per_core;
    }

    // ===== Disk mount =====
    strncpy(m->disk_mount, "/", sizeof(m->disk_mount) - 1);

    // ===== Disk total =====
    struct statvfs st;
    if (statvfs("/", &st) == 0)
        m->disk_total_mb = (st.f_blocks * st.f_frsize) / (1024 * 1024);

    // ===== Memory total =====
    FILE *memf = fopen("/proc/meminfo", "r");
    if (memf) {
        char line[256];
        while (fgets(line, sizeof(line), memf)) {
            long val = 0;
            if (sscanf(line, "MemTotal: %ld kB", &val) == 1) {
                m->mem_total_mb = val / 1024;
                break;
            }
        }
        fclose(memf);
    }

    // ===== Network interface =====
    FILE *netf = fopen("/proc/net/dev", "r");
    if (netf) {
        char line[256];
        fgets(line, sizeof(line), netf); // skip header 1
        fgets(line, sizeof(line), netf); // skip header 2
        while (fgets(line, sizeof(line), netf)) {
            char iface[32];
            sscanf(line, " %31s", iface);
            iface[strcspn(iface, ":")] = '\0'; // retirer le ':'
            if (strcmp(iface, "lo") == 0) continue;
            strncpy(m->network_iface, iface, sizeof(m->network_iface) - 1);
            break;
        }
        fclose(netf);
    }
}

// ============================================================
// DYNAMIC METRICS — lues à chaque cycle
// ============================================================

// ============================================================
// CPU USAGE
// ============================================================
static void read_cpu_usage(MachineMetrics *metrics){
    static long prev_idle = 0, prev_total = 0;
    
    FILE *f = fopen("/proc/stat", "r");
    if (!f) return;

    long user, nice, system, idle, iowait, irq, softirq;
    fscanf(f, "cpu %ld %ld %ld %ld %ld %ld %ld", 
        &user, &nice, &system, &idle, &iowait, &irq, &softirq);
    fclose(f);

    long total = user + nice + system + idle + iowait + irq + softirq;
    long idle_total = idle + iowait;

    long delta_total = total - prev_total;
    long delta_idle = idle_total - prev_idle;

    if (delta_total > 0)
        metrics->cpu_usage = (1.0f - (float)delta_idle / delta_total) * 100.0f;
    else
        metrics->cpu_usage = 0.0f;

    prev_total = total;
    prev_idle = idle_total;
}

// ============================================================
// MEMORY
// ============================================================
static void read_memory(MachineMetrics *metrics){
    FILE *f = fopen("/proc/meminfo", "r");
    if (!f) return;

    long mem_available_kb = 0;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        if (sscanf(line, "MemAvailable: %ld kB", &mem_available_kb) == 1) continue;
    }
    fclose(f);

    metrics->mem_available_mb = mem_available_kb / 1024.0f;
    metrics->mem_used_mb = metrics->mem_total_mb - metrics->mem_available_mb;
    metrics->mem_usage = (metrics->mem_total_mb > 0) ? 
        ((float)metrics->mem_used_mb / (float)metrics->mem_total_mb) * 100.0f : 0.0f;
}

// ============================================================
// DISK
// ============================================================
static void read_disk(MachineMetrics *metrics){
    struct statvfs st;
    if (statvfs("/", &st) == 0) {
        metrics->disk_used_mb = ((st.f_blocks - st.f_bfree) * st.f_frsize) / (1024.0f * 1024.0f);
    }

    static long prev_io_ms = 0;
    static long prev_time = 0;

    FILE *f = fopen("/proc/diskstats", "r");
    if (!f) return;

    char line[256];
    long io_ms = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strstr(line, "sda ") || strstr(line, "nvme0n1 ") || strstr(line, "vda ")) {
            sscanf(line, "%*d %*d %*s %*d %*d %*d %*d %*d %*d %*d %ld", 
                &io_ms);
            break;
        }
    }
    fclose(f);

    long now = time(NULL);
    long delta_time = (now - prev_time) * 1000;
    long delta_io = io_ms - prev_io_ms;

    if (delta_time > 0)
        metrics->disk_usage = (float)delta_io / delta_time * 100.0f;
    else
        metrics->disk_usage = 0.0f;

    prev_io_ms = io_ms;
    prev_time = now;
}

// ============================================================
// NETWORK
// ============================================================
static void read_network(MachineMetrics *metrics){
    static long prev_rx = 0, prev_tx = 0;
    static long prev_time = 0;

    FILE *f = fopen("/proc/net/dev", "r");
    if (!f) return;
 
    char line[256];
    long rx_bytes = 0, tx_bytes = 0;

    fgets(line, sizeof(line), f);
    fgets(line, sizeof(line), f);

    while (fgets(line, sizeof(line), f)) {
        char iface[32];
        sscanf(line, "%31s", iface);
        if (strcmp(iface, "lo:") == 0) continue; // Skip loopback
        
        sscanf(line, "%*s %ld %*d %*d %*d %*d %*d %*d %*d %ld", 
            &rx_bytes, &tx_bytes);
        break;
    }
    fclose(f);

    long now = time(NULL);
    long delta_time = now - prev_time;
    long delta_bytes = (rx_bytes - prev_rx) + (tx_bytes - prev_tx);

    if (delta_time > 0)
        metrics->network_bandwidth_mbps = (delta_bytes / (float)delta_time) / (1024.0f * 1024.0f);
    else
        metrics->network_bandwidth_mbps = 0.0f;

    prev_rx = rx_bytes;
    prev_tx = tx_bytes;
    prev_time = now;

    f = fopen("/proc/net/tcp", "r");
    if (!f) return;

    int count = 0;
    fgets(line, sizeof(line), f);
    while (fgets(line, sizeof(line), f)) count++;
    fclose(f);

    metrics->active_connections = count;
}

// ============================================================
// SYSTEM
// ============================================================
static void read_system(MachineMetrics *metrics){
    static long prev_context_switches = 0;
    static long prev_time = 0;

    FILE *f = fopen("/proc/loadavg", "r");
    if (f){
        int active = 0, total = 0;
        fscanf(f, "%f %f %f %d/%d", 
            &metrics->load_avg[0], 
            &metrics->load_avg[1], 
            &metrics->load_avg[2],
            &active,
            &total
        );
        metrics->active_processes = active;
        fclose(f);
    }

    f = fopen("/proc/uptime", "r");
    if (f) {
        float uptime_sec = 0;
        fscanf(f, "%f", &uptime_sec);
        metrics->uptime_seconds = (long)uptime_sec;
        fclose(f);
    }

    f = fopen("/proc/stat", "r");
    if (f) {
        char line[256];
        long context_switches = 0;
        while (fgets(line, sizeof(line), f)) {
            if (sscanf(line, "ctxt %ld", &context_switches) == 1) break;
        }
        fclose(f);

        long now = time(NULL);
        long delta_time = now - prev_time;
        long delta_context = context_switches - prev_context_switches;

        if (delta_time > 0)
            metrics->context_switch_rate = (float)delta_context / delta_time;
        else
            metrics->context_switch_rate = 0.0f;

        prev_context_switches = context_switches;
        prev_time = now;
    }           
}

// ============================================================
// FLAGS
// ============================================================
static void compute_flags(MachineMetrics *metrics){
    metrics->is_overloaded = 0;
    
    if(metrics->cpu_usage > OVERLOAD_CPU_THRESHOLD){
        metrics->is_overloaded = 1;
        return;
    } 
    
    if(metrics->mem_total_mb > 0) {
        float mem_usage_percent = (metrics->mem_used_mb / (float)metrics->mem_total_mb) * 100.0f;
        if(mem_usage_percent > OVERLOAD_MEM_THRESHOLD)
            metrics->is_overloaded = 1;
    }        
}