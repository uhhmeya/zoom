#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <iostream>

constexpr int MAX_THREADS = 250;
constexpr int MAX_KEYS = 100;
constexpr int RETIRED_THRESHOLD = 100;
constexpr int COOLDOWN_THRES = 10'000;

constexpr auto acq_rel = std::memory_order_acq_rel;
constexpr auto release = std::memory_order_release;
constexpr auto acquire = std::memory_order_acquire;
constexpr auto relaxed = std::memory_order_relaxed;

enum HP_Index {
    K = 0,
    V = 1
};

enum TransitionType {
    EIF_TRANS,
    DIF_TRANS,
    FUF_TRANS,
    FUF_ABORT_TRANS,
    FUF_ABORT_DELETE_TRANS,
    FXD_TRANS,
    FXD_ABORT_TRANS
};

struct alignas(64) SpinMetrics {
    std::vector<int> spins_per_req;
    std::vector<int> cooldowns_per_req;
    std::vector<double> spin_time_ms_per_req;
    uint64_t reqs_that_spun{0};
    uint64_t successful_spins{0};
    uint64_t aborted_spins{0};

    SpinMetrics() {
        spins_per_req.reserve(100000);
        cooldowns_per_req.reserve(100000);
        spin_time_ms_per_req.reserve(100000);
    }
};

struct alignas(64) HP_Slot {
    std::atomic<void*> slot[2]{ nullptr, nullptr };
    std::atomic<bool> in_use{ false };
};

struct alignas(64) TB_slot {
    std::atomic<std::string*> k{nullptr};
    std::atomic<std::string*> v{nullptr};
    std::atomic<char> s{'E'};
};

struct alignas(64) TransitionMetrics {
    std::vector<double> EIF_times;   // insert into empty
    std::vector<double> DIF_times;   // insert into deleted
    std::vector<double> FUF_times;   // update
    std::vector<double> FXD_times;   // delete
    std::vector<double> FUF_abort_times; // abort (key swapped)
    std::vector<double> FUF_abort_delete_times; // abort (key deleted)
    std::vector<double> FXD_abort_times; // abort (job already done)

    // Counts
    uint64_t EIF_count{0};
    uint64_t DIF_count{0};
    uint64_t FUF_count{0};
    uint64_t FUF_abort_count{0}; // key swapped during set
    uint64_t FUF_abort_delete_count{0}; // key deleted during set
    uint64_t FXD_count{0};
    uint64_t FXD_abort_count{0}; // key deleted during del

    TransitionMetrics() {
        EIF_times.reserve(10000);
        DIF_times.reserve(10000);
        FUF_times.reserve(100000);
        FXD_times.reserve(10000);
        FUF_abort_times.reserve(1000);
        FUF_abort_delete_times.reserve(1000);
        FXD_abort_times.reserve(1000);
    }
};

std::vector<TransitionMetrics> transition_metrics(MAX_THREADS);
std::vector<HP_Slot> hp(MAX_THREADS);
std::vector<TB_slot> tb(MAX_KEYS);
std::vector<SpinMetrics> spin_metrics(MAX_THREADS);

thread_local std::vector<std::string*> retired_list;
thread_local int my_hp_index = -1;

int get_my_hp_index() {
    if (my_hp_index == -1) {
        for (int i = 0; i < MAX_THREADS; i++) {
            bool expected = false;
            if (hp[i].in_use.compare_exchange_strong(expected, true, acq_rel, relaxed)) {
                my_hp_index = i;
                return i;
            }
        }
        throw std::runtime_error("No HP slots available");
    }
    return my_hp_index;
}

void log_transition(TransitionType type,
                   std::chrono::high_resolution_clock::time_point start,
                   std::chrono::high_resolution_clock::time_point end){

    double duration_ms = std::chrono::duration<double>(end - start).count() * 1000.0;
    auto& tm = transition_metrics[my_hp_index];

    switch(type) {

        case EIF_TRANS:
            tm.EIF_times.push_back(duration_ms);
            tm.EIF_count++;
            break;

        case DIF_TRANS:
            tm.DIF_times.push_back(duration_ms);
            tm.DIF_count++;
            break;

        case FUF_TRANS:
            tm.FUF_times.push_back(duration_ms);
            tm.FUF_count++;
            break;

        case FUF_ABORT_TRANS:
            tm.FUF_abort_times.push_back(duration_ms);
            tm.FUF_abort_count++;
            break;

        case FUF_ABORT_DELETE_TRANS:
            tm.FUF_abort_delete_times.push_back(duration_ms);
            tm.FUF_abort_delete_count++;
            break;

        case FXD_TRANS:
            tm.FXD_times.push_back(duration_ms);
            tm.FXD_count++;
            break;

        case FXD_ABORT_TRANS:
            tm.FXD_abort_times.push_back(duration_ms);
            tm.FXD_abort_count++;
            break;
    }
}

void log_spins(const int spins, const int cooldowns,
    const double spin_time_ms, const bool success) {
    spin_metrics[my_hp_index].spins_per_req.push_back(spins);
    spin_metrics[my_hp_index].cooldowns_per_req.push_back(cooldowns);
    spin_metrics[my_hp_index].spin_time_ms_per_req.push_back(spin_time_ms);
    spin_metrics[my_hp_index].reqs_that_spun++;
    if (success) {
        spin_metrics[my_hp_index].successful_spins++;
    } else {
        spin_metrics[my_hp_index].aborted_spins++;
    }
}

size_t hash(const std::string& key) {
    size_t h = 0;
    for (const char c : key) h = h * 31 + c;
    return h;
}

size_t hash2(const std::string& key) {
    size_t h = 5381;
    for (const char c : key) h = ((h << 5) + h) ^ c;
    return h | 1;
}

template<typename T>
T* protect(std::atomic<T*>& container, const int idx) {
    T* ptr;
    do {
        ptr = container.load(acquire);
        if (ptr == nullptr) return nullptr;
        hp[my_hp_index].slot[idx].store(ptr, release);
    } while (ptr != container.load(acquire));
    return ptr;
}

void clear_hp(const int idx) {
    hp[my_hp_index].slot[idx].store(nullptr, relaxed);
}

void clear_hp_both() {
    hp[my_hp_index].slot[K].store(nullptr, relaxed);
    hp[my_hp_index].slot[V].store(nullptr, relaxed);
}

bool can_delete(const void* ptr) {
    for (int i = 0; i < MAX_THREADS; i++) {
        if (!hp[i].in_use.load(acquire)) continue;
        if (hp[i].slot[K].load(acquire) == ptr) return false;
        if (hp[i].slot[V].load(acquire) == ptr) return false;
    }
    return true;
}

void freeScan() {
    auto ptr = retired_list.begin();
    while (ptr != retired_list.end()) {
        if (can_delete(*ptr)) {
            delete *ptr;
            ptr = retired_list.erase(ptr);
        } else {
            ++ptr;
        }
    }
}

void retire(std::string* ptr) {
    if (ptr == nullptr) return;
    retired_list.push_back(ptr);
    if (retired_list.size() >= RETIRED_THRESHOLD) {
        freeScan();
    }
}

std::string format_number(double num) {
    std::ostringstream oss;
    oss << std::fixed;
    if (num >= 1'000'000) {
        oss << std::setprecision(2) << (num / 1'000'000.0) << "M";
    } else if (num >= 1'000) {
        oss << std::setprecision(2) << (num / 1'000.0) << "K";
    } else if (num >= 100) {
        oss << std::setprecision(1) << num;
    } else if (num >= 10) {
        oss << std::setprecision(2) << num;
    } else {
        oss << std::setprecision(3) << num;
    }
    return oss.str();
}

std::string get_spin_metrics(int total_set_ops) {
    std::vector<int> all_spins;
    std::vector<int> all_cooldowns;
    std::vector<double> all_spin_times;
    uint64_t total_reqs_that_spun = 0;
    uint64_t total_successful = 0;
    uint64_t total_aborted = 0;

    std::vector<double> per_thread_avg_spins;
    std::vector<int> per_thread_max_cooldowns;

    for (const auto& metrics : spin_metrics) {
        all_spins.insert(all_spins.end(),
                        metrics.spins_per_req.begin(),
                        metrics.spins_per_req.end());
        all_cooldowns.insert(all_cooldowns.end(),
                            metrics.cooldowns_per_req.begin(),
                            metrics.cooldowns_per_req.end());
        all_spin_times.insert(all_spin_times.end(),
                             metrics.spin_time_ms_per_req.begin(),
                             metrics.spin_time_ms_per_req.end());
        total_reqs_that_spun += metrics.reqs_that_spun;
        total_successful += metrics.successful_spins;
        total_aborted += metrics.aborted_spins;

        if (!metrics.spins_per_req.empty()) {
            double thread_total = 0;
            for (int s : metrics.spins_per_req) thread_total += s;
            per_thread_avg_spins.push_back(thread_total / metrics.spins_per_req.size());
        }

        if (!metrics.cooldowns_per_req.empty()) {
            int thread_max_cooldown = *std::max_element(metrics.cooldowns_per_req.begin(),
                                                        metrics.cooldowns_per_req.end());
            per_thread_max_cooldowns.push_back(thread_max_cooldown);
        }
    }

    if (all_spins.empty()) {
        return "    Spinning:     No requests spun\n";
    }

    std::sort(all_spins.begin(), all_spins.end());
    std::sort(all_cooldowns.begin(), all_cooldowns.end());
    std::sort(all_spin_times.begin(), all_spin_times.end());

    uint64_t total_spins = 0;
    for (int s : all_spins) total_spins += s;

    uint64_t total_cooldowns = 0;
    for (int c : all_cooldowns) total_cooldowns += c;

    double total_spin_time_ms = 0;
    for (double t : all_spin_times) total_spin_time_ms += t;

    double avg_spins = static_cast<double>(total_spins) / all_spins.size();
    int max_spins = all_spins.back();
    int min_spins = all_spins[0];
    int p50_spins = all_spins[all_spins.size() * 50 / 100];
    int p95_spins = all_spins[all_spins.size() * 95 / 100];
    int p99_spins = all_spins[all_spins.size() * 99 / 100];
    int p999_spins = all_spins[all_spins.size() * 999 / 1000];

    double avg_spin_time = total_spin_time_ms / all_spin_times.size();
    double p50_spin_time = all_spin_times[all_spin_times.size() * 50 / 100];
    double p95_spin_time = all_spin_times[all_spin_times.size() * 95 / 100];
    double p99_spin_time = all_spin_times[all_spin_times.size() * 99 / 100];
    double p999_spin_time = all_spin_times[all_spin_times.size() * 999 / 1000];
    double max_spin_time = all_spin_times.back();

    double success_rate = (static_cast<double>(total_successful) / total_reqs_that_spun) * 100;
    double abort_rate = (static_cast<double>(total_aborted) / total_reqs_that_spun) * 100;
    double set_spin_rate = (static_cast<double>(total_reqs_that_spun) / total_set_ops) * 100;

    int reqs_with_cooldown = 0;
    for (int c : all_cooldowns) {
        if (c > 0) reqs_with_cooldown++;
    }

    double avg_spins_with_cooldown = 0;
    double avg_spins_without_cooldown = 0;
    int count_with = 0, count_without = 0;
    for (size_t i = 0; i < all_spins.size(); i++) {
        if (all_cooldowns[i] > 0) {
            avg_spins_with_cooldown += all_spins[i];
            count_with++;
        } else {
            avg_spins_without_cooldown += all_spins[i];
            count_without++;
        }
    }
    if (count_with > 0) avg_spins_with_cooldown /= count_with;
    if (count_without > 0) avg_spins_without_cooldown /= count_without;

    std::sort(per_thread_avg_spins.begin(), per_thread_avg_spins.end());
    double min_thread_avg = per_thread_avg_spins.empty() ? 0 : per_thread_avg_spins[0];
    double max_thread_avg = per_thread_avg_spins.empty() ? 0 : per_thread_avg_spins.back();

    std::sort(per_thread_max_cooldowns.begin(), per_thread_max_cooldowns.end());
    int min_thread_max_cooldown = per_thread_max_cooldowns.empty() ? 0 : per_thread_max_cooldowns[0];
    int max_thread_max_cooldown = per_thread_max_cooldowns.empty() ? 0 : per_thread_max_cooldowns.back();

    std::ostringstream oss;
    oss << std::fixed;

    oss << "    Spinning:     reqs=" << format_number(total_reqs_that_spun)
        << " (" << std::setprecision(1) << set_spin_rate << "% of SETs)"
        << " | success=" << std::setprecision(1) << success_rate
        << "% | abort=" << abort_rate << "%\n";

    oss << "                  spins: min=" << min_spins
        << " | avg=" << format_number(avg_spins)
        << " | p50=" << format_number(p50_spins)
        << " | p95=" << format_number(p95_spins)
        << " | p99=" << format_number(p99_spins)
        << " | p999=" << format_number(p999_spins)
        << " | max=" << format_number(max_spins) << "\n";

    oss << "                  time (ms): avg=" << std::setprecision(3) << avg_spin_time
        << " | p50=" << p50_spin_time
        << " | p95=" << p95_spin_time
        << " | p99=" << p99_spin_time
        << " | p999=" << p999_spin_time
        << " | max=" << max_spin_time
        << " | total=" << std::setprecision(1) << total_spin_time_ms << "\n";

    oss << "                  reqs with ≥1 cooldown=" << format_number(reqs_with_cooldown)
        << " (" << std::setprecision(1) << (static_cast<double>(reqs_with_cooldown) / all_cooldowns.size() * 100) << "%)"
        << " | total cooldowns=" << format_number(total_cooldowns)
        << " | max=" << all_cooldowns.back() << "\n";

    if (count_with > 0) {
        oss << "                  avg spins per req with ≥1 cooldown: "
            << format_number(avg_spins_with_cooldown) << "\n";
    }

    if (count_without > 0) {
        oss << "                  avg spins per req with 0 cooldowns: "
            << format_number(avg_spins_without_cooldown) << "\n";
    }

    if (!per_thread_avg_spins.empty()) {
        oss << "                  per-thread: min=" << format_number(min_thread_avg)
            << " | max=" << format_number(max_thread_avg)
            << " | Δ=" << format_number(max_thread_avg - min_thread_avg) << "\n";
    }

    if (!per_thread_max_cooldowns.empty()) {
        oss << "                  per-thread max cooldowns: min=" << min_thread_max_cooldown
            << " | max=" << max_thread_max_cooldown
            << " | Δ=" << (max_thread_max_cooldown - min_thread_max_cooldown) << "\n";
    }

    return oss.str();
}

std::string get_transition_metrics() {
    std::vector<double> all_EIF, all_DIF, all_FUF, all_FXD, all_FUF_abort, all_FUF_abort_delete, all_FXD_abort;
    uint64_t total_EIF = 0, total_DIF = 0, total_FUF = 0, total_FXD = 0, total_FUF_abort = 0, total_FUF_abort_delete = 0, total_FXD_abort = 0;

    for (const auto& tm : transition_metrics) {
        all_EIF.insert(all_EIF.end(), tm.EIF_times.begin(), tm.EIF_times.end());
        all_DIF.insert(all_DIF.end(), tm.DIF_times.begin(), tm.DIF_times.end());
        all_FUF.insert(all_FUF.end(), tm.FUF_times.begin(), tm.FUF_times.end());
        all_FXD.insert(all_FXD.end(), tm.FXD_times.begin(), tm.FXD_times.end());
        all_FUF_abort.insert(all_FUF_abort.end(), tm.FUF_abort_times.begin(), tm.FUF_abort_times.end());
        all_FUF_abort_delete.insert(all_FUF_abort_delete.end(), tm.FUF_abort_delete_times.begin(), tm.FUF_abort_delete_times.end());
        all_FXD_abort.insert(all_FXD_abort.end(), tm.FXD_abort_times.begin(), tm.FXD_abort_times.end());

        total_EIF += tm.EIF_count;
        total_DIF += tm.DIF_count;
        total_FUF += tm.FUF_count;
        total_FXD += tm.FXD_count;
        total_FUF_abort += tm.FUF_abort_count;
        total_FUF_abort_delete += tm.FUF_abort_delete_count;
        total_FXD_abort += tm.FXD_abort_count;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(4);

    auto format_transition = [&](const std::string& name,
                                 const std::vector<double>& times,
                                 uint64_t count) -> std::string {
        if (times.empty()) {
            return "    " + name + ": count=0\n";
        }

        std::vector<double> sorted = times;
        std::sort(sorted.begin(), sorted.end());

        double min_t = sorted[0];
        double max_t = sorted.back();
        double total = 0;
        for (double t : sorted) total += t;
        double mean_t = total / sorted.size();

        double p50 = sorted[sorted.size() * 50 / 100];
        double p95 = sorted[sorted.size() * 95 / 100];
        double p99 = sorted[sorted.size() * 99 / 100];
        double p999 = sorted[sorted.size() * 999 / 1000];

        std::ostringstream line;
        line << std::fixed << std::setprecision(4);
        line << "    " << name << ": "
             << "count=" << count << " | "
             << "min=" << min_t << "ms | "
             << "mean=" << mean_t << "ms | "
             << "p50=" << p50 << "ms | "
             << "p95=" << p95 << "ms | "
             << "p99=" << p99 << "ms | "
             << "p999=" << p999 << "ms | "
             << "max=" << max_t << "ms\n";
        return line.str();
    };

    oss << "    Transitions:\n";
    oss << format_transition("E→I→F (insert empty)    ", all_EIF, total_EIF);
    oss << format_transition("D→I→F (insert deleted)  ", all_DIF, total_DIF);
    oss << format_transition("F→U→F (update)          ", all_FUF, total_FUF);
    oss << format_transition("F→X→D (delete)          ", all_FXD, total_FXD);

    if (total_FUF_abort > 0) {
        oss << format_transition("F→U→F (abort swap)     ", all_FUF_abort, total_FUF_abort);
    }

    if (total_FUF_abort_delete > 0) {
        oss << format_transition("F→U→D (abort delete)   ", all_FUF_abort_delete, total_FUF_abort_delete);
    }

    if (total_FXD_abort > 0) {
        oss << format_transition("F→X→D (abort)          ", all_FXD_abort, total_FXD_abort);
    }

    uint64_t total_transitions = total_EIF + total_DIF + total_FUF + total_FXD + total_FUF_abort + total_FUF_abort_delete + total_FXD_abort;
    if (total_transitions > 0) {
        oss << std::setprecision(1);
        oss << "    Distribution: "
            << "EIF=" << (total_EIF * 100.0 / total_transitions) << "% | "
            << "DIF=" << (total_DIF * 100.0 / total_transitions) << "% | "
            << "FUF=" << (total_FUF * 100.0 / total_transitions) << "% | "
            << "FXD=" << (total_FXD * 100.0 / total_transitions) << "%";

        if (total_FUF_abort > 0) {
            oss << " | FUF_ABORT_SWAP=" << (total_FUF_abort * 100.0 / total_transitions) << "%";
        }

        if (total_FUF_abort_delete > 0) {
            oss << " | FUF_ABORT_DEL=" << (total_FUF_abort_delete * 100.0 / total_transitions) << "%";
        }

        if (total_FXD_abort > 0) {
            oss << " | FXD_ABORT=" << (total_FXD_abort * 100.0 / total_transitions) << "%";
        }

        oss << "\n";
    }

    return oss.str();
}

std::string* get(const std::string& kB) {
    const size_t y = hash(kB);
    const size_t step = hash2(kB);
    const size_t table_size = tb.size();

    for (size_t j = 0; j < table_size; j++) {
        const size_t i = (y + j * step) % table_size;
        auto& CPSi = tb[i].s;
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        const char Si = CPSi.load(acquire);

        if (Si == 'E') return nullptr;
        if (Si != 'F') continue;

        // protect
        std::string* ptr_ki = protect(CPKi, K);
        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        // wrong key
        if (*ptr_ki != kB) {
            clear_hp(K);
            continue;
        }

        // right key deleted
        std::string* ptr_vi = protect(CPVi, V);
        if (ptr_vi == nullptr) {
            clear_hp_both();
            continue;
        }

        // value not of key
        if (ptr_ki != CPKi.load(acquire)) {
            clear_hp_both();
            continue;
        }
        clear_hp_both();
        return ptr_vi;
    }
    return nullptr;
}

void key_deleted_during_spin(bool did_spin, int spin_count, int cooldowns_hit,
    std::chrono::high_resolution_clock::time_point spin_start) {
    // end spin
    if (did_spin) {
        auto spin_end = std::chrono::high_resolution_clock::now();
        double spin_time_ms = std::chrono::duration<double>(spin_end - spin_start).count() * 1000.0;
        log_spins(spin_count, cooldowns_hit, spin_time_ms, false);
    }
    clear_hp(K);
}

void set(const std::string& kA, const std::string& vA) {
    const size_t y = hash(kA);
    const size_t step = hash2(kA);
    const size_t table_size = tb.size();
    std::string* ptr_kA = nullptr;
    std::string* ptr_vA = nullptr;

    for (size_t j = 0; j < table_size; j++) {
        const size_t i = (y + j * step) % table_size;
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        auto& CPSi = tb[i].s;
        const char Si = CPSi.load(acquire);

        // EIF
        if (Si == 'E') {
            char expected = 'E';
            if (CPSi.compare_exchange_strong(expected, 'I', acq_rel, relaxed)) {
                auto trans_start = std::chrono::high_resolution_clock::now();

                ptr_kA = new std::string(kA);
                ptr_vA = new std::string(vA);
                CPKi.store(ptr_kA, relaxed);
                CPVi.store(ptr_vA, relaxed);
                CPSi.store('F', release);

                auto trans_end = std::chrono::high_resolution_clock::now();
                log_transition(EIF_TRANS, trans_start, trans_end);
                return;
            }
            continue;
        }

        // DIF
        if (Si == 'D') {
            char expected = 'D';
            if (CPSi.compare_exchange_strong(expected, 'I', acq_rel, relaxed)) {
                auto trans_start = std::chrono::high_resolution_clock::now();

                ptr_kA = new std::string(kA);
                ptr_vA = new std::string(vA);
                std::string* old_k = CPKi.exchange(ptr_kA, acq_rel);
                std::string* old_v = CPVi.exchange(ptr_vA, acq_rel);
                CPSi.store('F', release);
                retire(old_k);
                retire(old_v);

                auto trans_end = std::chrono::high_resolution_clock::now();
                log_transition(DIF_TRANS, trans_start, trans_end);
                return;
            }
            continue;
        }

        if (Si != 'F') continue;

        // protect
        std::string* ptr_ki = protect(CPKi, K);
        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        // bad key
        if (*ptr_ki != kA) {
            clear_hp(K);
            continue;
        }

        // right key deleted
        if (ptr_ki != CPKi.load(acquire)) {
            clear_hp(K);
            continue;
        }

        int spin_count = 0;
        int cooldowns_hit = 0;
        bool did_spin = false;
        std::chrono::high_resolution_clock::time_point spin_start;

        char updated_Si = CPSi.load(acquire);

        while (true) {

            // spin
            if (updated_Si != 'F') {
                spin_count++;

                // start timer
                if (!did_spin) {
                    spin_start = std::chrono::high_resolution_clock::now();
                    did_spin = true;
                }

                if (updated_Si == 'D' ) {
                    key_deleted_during_spin(did_spin, spin_count, cooldowns_hit, spin_start);
                    break;
                }

                // cooldown
                if (spin_count % COOLDOWN_THRES == 0) {
                    cooldowns_hit++;
                    int sleep_ms;
                    if             (cooldowns_hit <= 30)         sleep_ms = 10;
                    else if     (cooldowns_hit <= 50)         sleep_ms = 20;
                    else if     (cooldowns_hit <= 70)         sleep_ms = 30;
                    else if     (cooldowns_hit <= 90)         sleep_ms = 50;
                    else if     (cooldowns_hit <= 100)       sleep_ms = 60;
                    else                                                    sleep_ms = 60;
                    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                }

                updated_Si = CPSi.load(acquire);
                continue;
            }

            // key deleted/swapped ; probe
            if (ptr_ki != CPKi.load(acquire)) {
                key_deleted_during_spin(did_spin, spin_count, cooldowns_hit, spin_start);
                break;
            }

            // send cas - start of FUF
            if (CPSi.compare_exchange_strong(updated_Si, 'U', acq_rel, relaxed)) {
                auto trans_start = std::chrono::high_resolution_clock::now();

                // key deleted/swapped ; probe (ABORT CASE)
                if (ptr_ki != CPKi.load(acquire)) {
                    auto trans_end = std::chrono::high_resolution_clock::now();

                    // Check what happened to the key pointer
                    std::string* current_ki = CPKi.load(acquire);
                    if (current_ki == nullptr) {
                        // Key was deleted by another thread → U to D
                        CPSi.store('D', release);
                        log_transition(FUF_ABORT_DELETE_TRANS, trans_start, trans_end);
                    } else {
                        // Key was swapped by another thread → U to F
                        CPSi.store('F', release);
                        log_transition(FUF_ABORT_TRANS, trans_start, trans_end);
                    }

                    key_deleted_during_spin(did_spin, spin_count, cooldowns_hit, spin_start);
                    break;
                }

                // cas approved - end FUF
                ptr_vA = new std::string(vA);
                std::string *old_ptr_vi = CPVi.exchange(ptr_vA, acq_rel);
                CPSi.store('F', release);
                clear_hp(K);
                retire(old_ptr_vi);

                auto trans_end = std::chrono::high_resolution_clock::now();
                log_transition(FUF_TRANS, trans_start, trans_end);

                // end spin
                if (did_spin) {
                    auto spin_end = std::chrono::high_resolution_clock::now();
                    double spin_time_ms = std::chrono::duration<double>(spin_end - spin_start).count() * 1000.0;
                    log_spins(spin_count, cooldowns_hit, spin_time_ms, true);
                }
                return;
            }
            // cas failed : spin!
        }
        // key deleted : probe!
    }
}

void del(const std::string& kx) {
    const size_t y = hash(kx);
    const size_t step = hash2(kx);
    const size_t table_size = tb.size();

    for (size_t j = 0; j < table_size; j++) {
        const size_t i = (y + j * step) % table_size;
        auto& CPSi = tb[i].s;
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        char Si = CPSi.load(acquire);

        if (Si == 'E') return;
        if (Si != 'F') continue;

        std::string* ptr_ki = protect(CPKi, K);

        // key deleted/swap
        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        // wrong key
        if (*ptr_ki != kx) {
            clear_hp(K);
            continue;
        }

        // key deleted/swap
        if (ptr_ki != CPKi.load(acquire)) {
            clear_hp(K);
            continue;
        }

        char expected = 'F';
        if (CPSi.compare_exchange_strong(expected, 'X', acq_rel, relaxed)) {

            auto trans_start = std::chrono::high_resolution_clock::now();

            // key deleted / swapped
            if (ptr_ki != CPKi.load(acquire)) {
                CPSi.store('D', release); // FXD abort (job already done)
                clear_hp(K);
                auto trans_end = std::chrono::high_resolution_clock::now();
                log_transition(FXD_ABORT_TRANS, trans_start, trans_end);
                return;
            }

            std::string* ptr_k = CPKi.exchange(nullptr, acq_rel);
            std::string* ptr_v = CPVi.exchange(nullptr, acq_rel);
            CPSi.store('D', release);
            clear_hp_both();
            retire(ptr_k);
            retire(ptr_v);

            auto trans_end = std::chrono::high_resolution_clock::now();
            log_transition(FXD_TRANS, trans_start, trans_end);
            return;
        }
        clear_hp(K);
        continue;
    }
}