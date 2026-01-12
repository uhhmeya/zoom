#include <atomic>
#include <string>
#include <vector>
#include <thread>
int MAX_THREADS = 100;
int MAX_KEYS = 1000;

enum class State : uint8_t {
    EMPTY,
    FULL,
    DELETED
};

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

    // r : Si = Full
    // thr2 --> del ptr_ki
    // protect(CPKi)
    // ptr_ki == null
    std::atomic<uint64_t> deleted{0};

    // r : Si = Full
    // protect(container_ptr_vi)
    // ptr_vi == pointer ∈ container
    // hp.add(ptr_vi)
    // thr2 --> set(ki, v')
    // ptr_vi =/= pointer ∈ container
    std::atomic<uint64_t> changed{0};
} protection_attempt_metrics;

struct ContentionMetrics {
    std::atomic<uint64_t> key_retries_exhausted{0};
    std::atomic<uint64_t> value_retries_exhausted{0};
    std::atomic<uint64_t>  key_found_val_deleted{0};
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
    std::atomic<State> s{State::EMPTY};
};

std::vector<HP_Slot> hp(MAX_THREADS);
std::vector<TB_slot> tb(MAX_KEYS);

thread_local int my_hp_index = -1;

int get_my_hp_index() {
    // claim slot
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

    // access claimed slot
    return my_hp_index;
}

size_t hash(const std::string& key) {
    size_t h = 0;
    for (const char c : key) h = h * 31 + c;
    return h % tb.size();
}

void clear_hp() {
    for (int i = 0; i < MAX_THREADS; i++) {
        hp[i].slot[K].store(nullptr);
        hp[i].slot[V].store(nullptr);
        hp[i].in_use.store(false);
    }
}

template<typename T>
Result_Of_Protect_Attempt<T> protect(std::atomic<T*>& container_of_ptr_ki, const int idx) {
    T* ptr_ki = container_of_ptr_ki.load();

    // other thread deleted ptr
    if (ptr_ki == nullptr) {
        protection_attempt_metrics.deleted.fetch_add(1, std::memory_order_relaxed);
        return {nullptr, ProtectStatus::Deleted};
    }

    hp[my_hp_index].slot[idx].store(ptr_ki);

    // ptr is protected
    if (ptr_ki == container_of_ptr_ki.load()) {
        protection_attempt_metrics.success.fetch_add(1, std::memory_order_relaxed);
        return {ptr_ki, ProtectStatus::Success};
    }

    hp[my_hp_index].slot[idx].store(nullptr);

    // other thread deleted ptr
    if (container_of_ptr_ki.load() == nullptr) {
        protection_attempt_metrics.deleted.fetch_add(1, std::memory_order_relaxed);
        return {nullptr, ProtectStatus::Deleted};
    }

    // other thread replaced ptr
    protection_attempt_metrics.changed.fetch_add(1, std::memory_order_relaxed);
    return {nullptr, ProtectStatus::Changed};
}

bool can_delete(const void* ptr) {
    for (int i = 0; i < MAX_THREADS; i++) {
        if (hp[i].slot[K].load() == ptr) return false;
        if (hp[i].slot[V].load() == ptr) return false;
    }
    return true;
}

std::string get(const std::string& kB) {
    const size_t y = hash(kB);
    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (y+j) % tb.size();

        auto& container_of_Si = tb[i].s;
        const State Si = container_of_Si.load();
        if (Si == State::EMPTY) return "φ";
        if (Si == State::DELETED) continue;

        auto& container_of_ptr_ki = tb[i].k;
        auto [maybe_ptr_ki, k_status] = protect(container_of_ptr_ki, K);

        // 3 retry for ptr_ki
        for (int retry = 0; retry < 3 && k_status == ProtectStatus::Changed; retry++) {
            auto [ptr_ki_retry, k_status_retry] = protect(container_of_ptr_ki, K);
            maybe_ptr_ki = ptr_ki_retry;
            k_status = k_status_retry;
        }

        // all retries failed
        if (k_status == ProtectStatus::Changed) {
            contention_metrics.key_retries_exhausted.fetch_add(1, std::memory_order_relaxed);
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        // other thread deleted ptr_ki
        if (k_status == ProtectStatus::Deleted)
            continue;

        // probes if ki was deleted
        auto ptr_ki = maybe_ptr_ki;
        if (container_of_Si.load() != State::FULL) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        if (*ptr_ki != kB) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        // Key matches
        auto& container_of_ptr_vi = tb[i].v;
        auto [maybe_ptr_vi, v_status]= protect(container_of_ptr_vi, V);

        // one retry for ptr_vi
        if (v_status == ProtectStatus::Changed) {
            auto [ptr_vi_retry, v_status_retry] = protect(container_of_ptr_vi, V);
            maybe_ptr_vi = ptr_vi_retry;
            v_status = v_status_retry;
        }

        // Key was found but value was deleted
        if (v_status == ProtectStatus::Deleted) {
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            contention_metrics.key_found_val_deleted.fetch_add(1, std::memory_order_relaxed);
            return "φ";
        }

        // Key was found but value keeps changing
        if (v_status == ProtectStatus::Changed) {
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            contention_metrics.value_retries_exhausted.fetch_add(1, std::memory_order_relaxed);
            return "φ";
        }

        // now ptr_ki & ptr_vi are protected. check if ptr_ki was swapped
        if (ptr_ki != tb[i].k.load()) {
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            continue;
        }

        // now we know ki & vi are connected
        auto ptr_vx = maybe_ptr_vi;
        std::string vx = *ptr_vx;
        hp[my_hp_index].slot[K].store(nullptr);
        hp[my_hp_index].slot[V].store(nullptr);
        return vx;
    }
    return "φ"; // kB ∉ db
}

bool set(const std::string& kA, const std::string& vA) {
    const size_t y = hash(kA);
    const auto ptr_kA = new std::string(kA);
    const auto ptr_vA = new std::string(vA);
    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (y + j) % tb.size();
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        auto& CPSi = tb[i].s;
        const State Si = CPSi.load();

        if (Si == State::EMPTY) {
            std::string* prev_CPKi = nullptr;
            if (CPKi.compare_exchange_strong(prev_CPKi, ptr_kA)) {
                CPVi.store(ptr_vA);
                CPSi.store(State::FULL);
                return true;
            }
            continue;
        }

        if (Si == State::DELETED) {

            // deletes ghost values
            std::string* ghost_v = CPVi.load();
            if (ghost_v != nullptr) {
                std::string* expected = ghost_v;
                if (CPVi.compare_exchange_strong(expected, nullptr))
                    if (can_delete(ghost_v))
                        delete ghost_v;
            }

            auto expected = State::DELETED;
            if (CPSi.compare_exchange_strong(expected, State::EMPTY)) {
                std::string* prev_CPKi = nullptr;
                if (CPKi.compare_exchange_strong(prev_CPKi, ptr_kA)) {
                    CPVi.store(ptr_vA);
                    CPSi.store(State::FULL);
                    return true;
                }
            }
            continue;
        }

        auto [maybe_ptr_ki, k_status] = protect(CPKi, K);

        // 3 retries for ptr_ki
        for (int retry = 0; retry < 3 && k_status == ProtectStatus::Changed; retry++) {
            auto [ptr_ki_retry, k_status_retry] = protect(CPKi, K);
            maybe_ptr_ki = ptr_ki_retry;
            k_status = k_status_retry;
        }

        // All retries failed
        if (k_status == ProtectStatus::Changed) {
            contention_metrics.key_retries_exhausted.fetch_add(1, std::memory_order_relaxed);
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        // Other thread deleted ptr_ki
        if (k_status == ProtectStatus::Deleted) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        auto ptr_ki = maybe_ptr_ki;
        if (CPSi.load() != State::FULL) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        if (*ptr_ki == kA) {
            std::string* old_vi = CPVi.exchange(ptr_vA);
            hp[my_hp_index].slot[K].store(nullptr);
            // retire(old_vi);
            delete ptr_kA;
            return true;
        }
        hp[my_hp_index].slot[K].store(nullptr);
    }

    // Table full
    delete ptr_kA;
    delete ptr_vA;
    throw std::runtime_error("Full Table");
}

bool del(const std::string& kx) {
    const size_t y = hash(kx);
    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (y+j) % tb.size();
        State s_i = tb[i].s.load();
        if (s_i == State::EMPTY) return false;
        if (s_i == State::DELETED) continue;

        auto maybe_ptr_ki = protect(tb[i].k, K);

        // other thread deleted ki ; keep probing
        if (!maybe_ptr_ki.has_value())
            continue;

        std::string* ptr_ki = maybe_ptr_ki.value();

        // ki != kx ; keep probing
        if (*ptr_ki != kx) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        hp[my_hp_index].slot[K].store(nullptr);

        auto maybe_ptr_vx = protect(tb[i].v, V);

        // kx ^ vx were deleted by other thread
        if (!maybe_ptr_vx.has_value()) {
            return false;
        }

        std::string* ptr_vx = maybe_ptr_vx.value();

        tb[i].s.store(State::DELETED);
        tb[i].k.store(nullptr);
        tb[i].v.store(nullptr);
        hp[my_hp_index].slot[K].store(nullptr);
        hp[my_hp_index].slot[V].store(nullptr);

        if (can_delete(ptr_ki))
            delete ptr_ki;

        if (can_delete(ptr_vx))
            delete ptr_vx;

        return true;
    }

    // key ∉ DB
    return false;
}









