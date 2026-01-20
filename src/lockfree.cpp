#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <chrono>


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

inline void log_spins(int spins, int cooldowns, double spin_time_ms, bool success) {
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

inline void clear_hp(const int idx) {
    hp[my_hp_index].slot[idx].store(nullptr, relaxed);
}

inline void clear_hp_both() {
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

    double avg_cooldowns = static_cast<double>(total_cooldowns) / all_cooldowns.size();
    int max_cooldowns = all_cooldowns.back();

    double avg_spin_time = total_spin_time_ms / all_spin_times.size();
    double p50_spin_time = all_spin_times[all_spin_times.size() * 50 / 100];
    double p95_spin_time = all_spin_times[all_spin_times.size() * 95 / 100];
    double p99_spin_time = all_spin_times[all_spin_times.size() * 99 / 100];
    double max_spin_time = all_spin_times.back();

    double success_rate = (static_cast<double>(total_successful) / total_reqs_that_spun) * 100;
    double abort_rate = (static_cast<double>(total_aborted) / total_reqs_that_spun) * 100;
    double set_spin_rate = (static_cast<double>(total_reqs_that_spun) / total_set_ops) * 100;

    int reqs_with_cooldown = 0;
    for (int c : all_cooldowns) {
        if (c > 0) reqs_with_cooldown++;
    }
    double cooldown_rate = (static_cast<double>(reqs_with_cooldown) / all_cooldowns.size()) * 100;

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
        << " | max=" << format_number(max_spins) << "\n";

    oss << "                  time (ms): avg=" << std::setprecision(3) << avg_spin_time
        << " | p50=" << p50_spin_time
        << " | p95=" << p95_spin_time
        << " | p99=" << p99_spin_time
        << " | max=" << max_spin_time
        << " | total=" << std::setprecision(1) << total_spin_time_ms << "\n";

    oss << "                  cooldowns: " << format_number(reqs_with_cooldown) << " reqs"
        << " (" << std::setprecision(1) << cooldown_rate << "%)"
        << " | avg=" << std::setprecision(2) << avg_cooldowns
        << " | max=" << max_cooldowns << "\n";

    if (count_with > 0 && count_without > 0) {
        oss << "                  correlation: w/ cooldown=" << format_number(avg_spins_with_cooldown)
            << " | w/o cooldown=" << format_number(avg_spins_without_cooldown) << "\n";
    }

    if (!per_thread_avg_spins.empty()) {
        oss << "                  per-thread: min=" << format_number(min_thread_avg)
            << " | max=" << format_number(max_thread_avg)
            << " | Δ=" << format_number(max_thread_avg - min_thread_avg) << "\n";
    }

    return oss.str();
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

        // E I F
        if (Si == 'E') {
            char expected = 'E';
            if (CPSi.compare_exchange_strong(expected, 'I', acq_rel, relaxed)) {
                ptr_kA = new std::string(kA);
                ptr_vA = new std::string(vA);
                CPKi.store(ptr_kA, relaxed);
                CPVi.store(ptr_vA, relaxed);
                CPSi.store('F', release);
                return;
            }
            continue;
        }

        // D I F
        if (Si == 'D') {
            char expected = 'D';
            if (CPSi.compare_exchange_strong(expected, 'I', acq_rel, relaxed)) {
                ptr_kA = new std::string(kA);
                ptr_vA = new std::string(vA);
                std::string* old_k = CPKi.exchange(ptr_kA, acq_rel);
                std::string* old_v = CPVi.exchange(ptr_vA, acq_rel);
                CPSi.store('F', release);
                retire(old_k);
                retire(old_v);
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

        int spins_in_req = 0;
        int cooldowns_in_req = 0;
        bool did_spin = false;
        auto spin_start = std::chrono::high_resolution_clock::now();

        while (true) {
            char new_Si = CPSi.load(acquire);

            // spin
            if (new_Si != 'F') {
                spins_in_req++;

                if (spins_in_req % COOLDOWN_THRES == 0) {
                    cooldowns_in_req++;
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }

                did_spin = true;
                continue;
            }

            // key deleted ; probe
            if (ptr_ki != CPKi.load(acquire)) {
                if (did_spin) {
                    auto spin_end = std::chrono::high_resolution_clock::now();
                    double spin_time_ms = std::chrono::duration<double>(spin_end - spin_start).count() * 1000.0;
                    log_spins(spins_in_req, cooldowns_in_req, spin_time_ms, false);
                }
                clear_hp(K);
                break;
            }

            // send cas
            if (CPSi.compare_exchange_strong(new_Si, 'U', acq_rel, relaxed)) {

                // key deleted ; probe
                if (ptr_ki != CPKi.load(acquire)) {
                    CPSi.store('F', release);
                    if (did_spin) {
                        auto spin_end = std::chrono::high_resolution_clock::now();
                        double spin_time_ms = std::chrono::duration<double>(spin_end - spin_start).count() * 1000.0;
                        log_spins(spins_in_req, cooldowns_in_req, spin_time_ms, false);
                    }
                    clear_hp(K);
                    break;
                }

                // cas approved
                ptr_vA = new std::string(vA);
                std::string *old_ptr_vi = CPVi.exchange(ptr_vA, acq_rel);
                CPSi.store('F', release);
                clear_hp(K);
                retire(old_ptr_vi);
                if (did_spin) {
                    auto spin_end = std::chrono::high_resolution_clock::now();
                    double spin_time_ms = std::chrono::duration<double>(spin_end - spin_start).count() * 1000.0;
                    log_spins(spins_in_req, cooldowns_in_req, spin_time_ms, true);
                }
                return;
            }
            // cas failed : spin!
        }
        // key deleted : probe!
    }
    throw std::runtime_error("Full Table");
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

        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        if (*ptr_ki != kx) {
            clear_hp(K);
            continue;
        }

        // key deleted
        if (ptr_ki != CPKi.load(acquire)) {
            clear_hp(K);
            continue;
        }

        char expected = 'F';
        if (CPSi.compare_exchange_strong(expected, 'X', acq_rel, relaxed)) {

            // key deleted
            if (ptr_ki != CPKi.load(acquire)) {
                CPSi.store('F', release);
                clear_hp(K);
                continue;
            }

            std::string* ptr_k = CPKi.exchange(nullptr, acq_rel);
            std::string* ptr_v = CPVi.exchange(nullptr, acq_rel);
            CPSi.store('D', release);
            clear_hp_both();
            retire(ptr_k);
            retire(ptr_v);
            return;
        }
        clear_hp(K);
        continue;
    }
}