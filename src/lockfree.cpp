#include <atomic>
#include <sstream>
#include <string>
#include <vector>

constexpr int MAX_THREADS = 250;
constexpr int MAX_KEYS = 100;
constexpr int RETIRED_THRESHOLD = 100;
constexpr int MAX_SPINS = 1'000'000;

constexpr auto acq_rel = std::memory_order_acq_rel; // no jumps/ falls
constexpr auto release = std::memory_order_release; // above can't fall
constexpr auto acquire = std::memory_order_acquire; // below can't jump
constexpr auto relaxed = std::memory_order_relaxed; // jumps/falls = ok

enum HP_Index {
    K = 0,
    V = 1
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
    get_my_hp_index();
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
            continue; // forward consistency
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

void set(const std::string& kA, const std::string& vA) {
    get_my_hp_index();
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
                if (!ptr_kA) ptr_kA = new std::string(kA);
                if (!ptr_vA) ptr_vA = new std::string(vA);
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
                if (!ptr_kA) ptr_kA = new std::string(kA);
                if (!ptr_vA) ptr_vA = new std::string(vA);
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

        int spin_count = 0;
        while (spin_count < MAX_SPINS) {
            char exp = CPSi.load(acquire);

            // spin until Si == F
            if (exp != 'F') {
                spin_count++;
                continue;
            }

            // key deleted ; probe
            if (ptr_ki != CPKi.load(acquire)) {
                clear_hp(K);
                break;
            }

            // Claim slot
            if (CPSi.compare_exchange_strong(exp, 'U', acq_rel, relaxed)) {

                // key deleted ; keep probing
                if (ptr_ki != CPKi.load(acquire)) {
                    CPSi.store('F', release);
                    clear_hp(K);
                    break;
                }

                // cas approved
                if (!ptr_vA) ptr_vA = new std::string(vA);
                std::string* old_ptr_vi = CPVi.exchange(ptr_vA, acq_rel);
                CPSi.store('F', release);
                clear_hp(K);
                retire(old_ptr_vi);
                delete ptr_kA;
                return;
            }

            // cas failed
            spin_count++;
        }

        // key deleted during spin
        if (ptr_ki != CPKi.load(acquire)) continue;

        throw std::runtime_error("span for too long.. thread starvation :( ");
        clear_hp(K);
        continue;
    }
    delete ptr_kA;
    delete ptr_vA;
    throw std::runtime_error("Full Table");
}

void del(const std::string& kx) {
    get_my_hp_index();
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