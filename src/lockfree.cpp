#include <atomic>
#include <sstream>
#include <string>
#include <vector>

int MAX_THREADS = 100;
int MAX_KEYS = 1000;
constexpr int RETIRED_THRESHOLD = 100;

enum class ProtectStatus {
    Success,
    Deleted,
    Changed
};

template<typename T>
struct Result_Of_Protect_Attempt {
    T* ptr;
    ProtectStatus status;
};

struct ProtectMetrics {
    std::atomic<uint64_t> success{0};
    std::atomic<uint64_t> deleted{0};
    std::atomic<uint64_t> changed{0};
} protection_attempt_metrics;

struct ContentionMetrics {
    std::atomic<uint64_t> key_retries_exhausted{0};
    std::atomic<uint64_t> value_retries_exhausted{0};
    std::atomic<uint64_t> key_found_val_deleted{0};
} contention_metrics;

enum HP_Index {
    K = 0,
    V = 1
};

struct HP_Slot {
    std::atomic<void*> slot[2]{ nullptr, nullptr };
    std::atomic<bool> in_use{ false };
};

struct TB_slot {
    std::atomic<std::string*> k{nullptr};
    std::atomic<std::string*> v{nullptr};
    std::atomic<char> s{'E'};
};

std::vector<HP_Slot> hp(MAX_THREADS);
std::vector<TB_slot> tb(MAX_KEYS);

thread_local std::vector<std::string*> retired_list;
thread_local int my_hp_index = -1;

int get_my_hp_index() {
    if (my_hp_index == -1) {
        for (int i = 0; i < MAX_THREADS; i++) {
            bool expected = false;
            if (hp[i].in_use.compare_exchange_strong(expected, true)) {
                my_hp_index = i;
                return i;
            }
        }
        throw std::runtime_error("No HP slots available");
    }
    return my_hp_index;
}

size_t hash(const std::string& key) {
    size_t h = 0;
    for (const char c : key) h = h * 31 + c;
    return h % tb.size();
}

template<typename T>
Result_Of_Protect_Attempt<T> protect(std::atomic<T*>& container_of_ptr_ki, const int idx) {
    T* ptr_ki = container_of_ptr_ki.load();

    if (ptr_ki == nullptr) {
        protection_attempt_metrics.deleted.fetch_add(1, std::memory_order_relaxed);
        return {nullptr, ProtectStatus::Deleted};
    }

    hp[my_hp_index].slot[idx].store(ptr_ki);

    if (ptr_ki == container_of_ptr_ki.load()) {
        protection_attempt_metrics.success.fetch_add(1, std::memory_order_relaxed);
        return {ptr_ki, ProtectStatus::Success};
    }

    hp[my_hp_index].slot[idx].store(nullptr);

    if (container_of_ptr_ki.load() == nullptr) {
        protection_attempt_metrics.deleted.fetch_add(1, std::memory_order_relaxed);
        return {nullptr, ProtectStatus::Deleted};
    }

    protection_attempt_metrics.changed.fetch_add(1, std::memory_order_relaxed);
    return {nullptr, ProtectStatus::Changed};
}

template<typename T>
Result_Of_Protect_Attempt<T> protect_with_retry(std::atomic<T*>& container,const int hp_idx,const int max_retries = 3){
    auto [ptr, status] = protect(container, hp_idx);

    for (int retry = 0; retry < max_retries && status == ProtectStatus::Changed; retry++) {
        auto [ptr_retry, status_retry] = protect(container, hp_idx);
        ptr = ptr_retry;
        status = status_retry;
    }

    return {ptr, status};
}

bool can_delete(const void* ptr) {
    for (int i = 0; i < MAX_THREADS; i++) {
        if (hp[i].slot[K].load() == ptr) return false;
        if (hp[i].slot[V].load() == ptr) return false;
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

void get(const std::string& kB) {
    const size_t y = hash(kB);
    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (y+j) % tb.size();
        auto& CPSi = tb[i].s;
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        const char Si = CPSi.load();

        if (Si == 'E') return;
        if (Si != 'F') continue;

        auto [mb_ptr_ki, k_status] = protect_with_retry(CPKi, K, 3);

        if (k_status == ProtectStatus::Changed) {
            contention_metrics.key_retries_exhausted.fetch_add(1, std::memory_order_relaxed);
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        if (k_status == ProtectStatus::Deleted) continue;

        auto ptr_ki = mb_ptr_ki;
        if (CPSi.load() != 'F') {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        if (*ptr_ki != kB) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        auto [mb_ptr_vi, v_status] = protect_with_retry(CPVi, V, 1);
        if (v_status == ProtectStatus::Changed) {
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            contention_metrics.value_retries_exhausted.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        if (v_status == ProtectStatus::Deleted) {
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            contention_metrics.key_found_val_deleted.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        if (ptr_ki != CPKi.load()) {
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            continue;
        }

        hp[my_hp_index].slot[K].store(nullptr);
        hp[my_hp_index].slot[V].store(nullptr);
        return;
    }
}

void set(const std::string& kA, const std::string& vA) {
    const size_t y = hash(kA);
    const auto ptr_kA = new std::string(kA);
    const auto ptr_vA = new std::string(vA);
    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (y + j) % tb.size();
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        auto& CPSi = tb[i].s;
        const char Si = CPSi.load();

        if (Si == 'E') {
            char expected = 'E';
            if (CPSi.compare_exchange_strong(expected, 'I')) {
                CPKi.store(ptr_kA);
                CPVi.store(ptr_vA);
                CPSi.store('F');
                return;
            }
            continue;
        }

        if (Si == 'D') {
            char expected = 'D';
            if (CPSi.compare_exchange_strong(expected, 'I')) {
                std::string* old_k = CPKi.exchange(ptr_kA);
                std::string* old_v = CPVi.exchange(ptr_vA);
                CPSi.store('F');
                retire(old_k);
                retire(old_v);
                return;
            }
            continue;
        }

        if (Si != 'F') continue;

        auto [mb_ptr_ki, k_status] = protect_with_retry(CPKi, K, 3);
        if (k_status == ProtectStatus::Changed) {
            contention_metrics.key_retries_exhausted.fetch_add(1, std::memory_order_relaxed);
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        if (k_status == ProtectStatus::Deleted) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        auto ptr_ki = mb_ptr_ki;
        if (*ptr_ki == kA) {
            char exp = 'F';
            if (CPSi.compare_exchange_strong(exp, 'U')) {
                std::string* old_ptr_vi = CPVi.exchange(ptr_vA);
                CPSi.store('F');
                hp[my_hp_index].slot[K].store(nullptr);
                retire(old_ptr_vi);
                delete ptr_kA;
                return;
            }
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }
        hp[my_hp_index].slot[K].store(nullptr);
    }
    delete ptr_kA;
    delete ptr_vA;
    throw std::runtime_error("Full Table");
}

void del(const std::string& kx) {
    const size_t y = hash(kx);
    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (y+j) % tb.size();
        auto& CPSi = tb[i].s;
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        char Si = CPSi.load();

        if (Si == 'E') return;
        if (Si != 'F') continue;

        auto [mb_ptr_ki, k_status] = protect_with_retry(CPKi, K, 3);

        if (k_status == ProtectStatus::Changed) {
            contention_metrics.key_retries_exhausted.fetch_add(1, std::memory_order_relaxed);
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        if (k_status == ProtectStatus::Deleted) continue;

        std::string* ptr_ki = mb_ptr_ki;
        if (*ptr_ki != kx) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        char expected = 'F';
        if (CPSi.compare_exchange_strong(expected, 'X')) {
            std::string* ptr_k = CPKi.exchange(nullptr);
            std::string* ptr_v = CPVi.exchange(nullptr);
            CPSi.store('D');
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            retire(ptr_k);
            retire(ptr_v);
            return;
        }
        hp[my_hp_index].slot[K].store(nullptr);
        continue;
    }
}