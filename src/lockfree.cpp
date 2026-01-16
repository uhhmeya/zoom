#include <atomic>
#include <sstream>
#include <string>
#include <vector>

int MAX_THREADS = 250;
int MAX_KEYS = 100;
constexpr int RETIRED_THRESHOLD = 100;

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
std::atomic<int> active_thread_count{0};

thread_local std::vector<std::string*> retired_list;
thread_local int my_hp_index = -1;

int get_my_hp_index() {
    if (my_hp_index == -1) {
        for (int i = 0; i < MAX_THREADS; i++) {
            bool expected = false;
            if (hp[i].in_use.compare_exchange_strong(expected, true,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                my_hp_index = i;
                active_thread_count.fetch_add(1, std::memory_order_relaxed);
                return i;
            }
        }
        throw std::runtime_error("No HP slots available");
    }
    return my_hp_index;
}

void release_hp_index() {
    if (my_hp_index != -1) {
        hp[my_hp_index].slot[K].store(nullptr, std::memory_order_relaxed);
        hp[my_hp_index].slot[V].store(nullptr, std::memory_order_relaxed);
        hp[my_hp_index].in_use.store(false, std::memory_order_release);
        active_thread_count.fetch_sub(1, std::memory_order_relaxed);
        my_hp_index = -1;
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
        ptr = container.load(std::memory_order_acquire);
        if (ptr == nullptr) return nullptr;
        hp[my_hp_index].slot[idx].store(ptr, std::memory_order_release);
    } while (ptr != container.load(std::memory_order_acquire));
    return ptr;
}

inline void clear_hp(const int idx) {
    hp[my_hp_index].slot[idx].store(nullptr, std::memory_order_relaxed);
}

inline void clear_hp_both() {
    hp[my_hp_index].slot[K].store(nullptr, std::memory_order_relaxed);
    hp[my_hp_index].slot[V].store(nullptr, std::memory_order_relaxed);
}

bool can_delete(const void* ptr) {
    for (int i = 0; i < MAX_THREADS; i++) {
        if (!hp[i].in_use.load(std::memory_order_acquire)) continue;
        if (hp[i].slot[K].load(std::memory_order_acquire) == ptr) return false;
        if (hp[i].slot[V].load(std::memory_order_acquire) == ptr) return false;
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
        const char Si = CPSi.load(std::memory_order_acquire);

        if (Si == 'E') return nullptr;
        if (Si != 'F') continue;

        std::string* ptr_ki = protect(CPKi, K);
        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        if (*ptr_ki != kB) {
            clear_hp(K);
            continue;
        }

        std::string* ptr_vi = protect(CPVi, V);
        if (ptr_vi == nullptr) {
            clear_hp_both();
            return nullptr;
        }

        if (ptr_ki != CPKi.load(std::memory_order_acquire)) {
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
        const char Si = CPSi.load(std::memory_order_acquire);

        if (Si == 'E') {
            char expected = 'E';
            if (CPSi.compare_exchange_strong(expected, 'I',
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                if (!ptr_kA) ptr_kA = new std::string(kA);
                if (!ptr_vA) ptr_vA = new std::string(vA);
                CPKi.store(ptr_kA, std::memory_order_relaxed);
                CPVi.store(ptr_vA, std::memory_order_relaxed);
                CPSi.store('F', std::memory_order_release);
                return;
            }
            continue;
        }

        if (Si == 'D') {
            char expected = 'D';
            if (CPSi.compare_exchange_strong(expected, 'I',
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                if (!ptr_kA) ptr_kA = new std::string(kA);
                if (!ptr_vA) ptr_vA = new std::string(vA);
                std::string* old_k = CPKi.exchange(ptr_kA, std::memory_order_acq_rel);
                std::string* old_v = CPVi.exchange(ptr_vA, std::memory_order_acq_rel);
                CPSi.store('F', std::memory_order_release);
                retire(old_k);
                retire(old_v);
                return;
            }
            continue;
        }

        if (Si != 'F') continue;

        std::string* ptr_ki = protect(CPKi, K);
        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        if (*ptr_ki != kA) {
            clear_hp(K);
            continue;
        }

        // deleted key
        if (ptr_ki != CPKi.load(std::memory_order_acquire)) {
            clear_hp(K);
            continue;
        }

        char exp = 'F';
        if (CPSi.compare_exchange_strong(exp, 'U',
                std::memory_order_acq_rel, std::memory_order_relaxed)) {

            // deleted key
            if (ptr_ki != CPKi.load(std::memory_order_acquire)) {
                CPSi.store('F', std::memory_order_release);
                clear_hp(K);
                continue;
            }

            if (!ptr_vA) ptr_vA = new std::string(vA);
            std::string* old_ptr_vi = CPVi.exchange(ptr_vA, std::memory_order_acq_rel);
            CPSi.store('F', std::memory_order_release);
            clear_hp(K);
            retire(old_ptr_vi);
            delete ptr_kA;
            return;
        }
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
        char Si = CPSi.load(std::memory_order_acquire);

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
        if (ptr_ki != CPKi.load(std::memory_order_acquire)) {
            clear_hp(K);
            continue;
        }

        char expected = 'F';
        if (CPSi.compare_exchange_strong(expected, 'X',
                std::memory_order_acq_rel, std::memory_order_relaxed)) {

            // key deleted
            if (ptr_ki != CPKi.load(std::memory_order_acquire)) {
                CPSi.store('F', std::memory_order_release);
                clear_hp(K);
                continue;
            }

            std::string* ptr_k = CPKi.exchange(nullptr, std::memory_order_acq_rel);
            std::string* ptr_v = CPVi.exchange(nullptr, std::memory_order_acq_rel);
            CPSi.store('D', std::memory_order_release);
            clear_hp_both();
            retire(ptr_k);
            retire(ptr_v);
            return;
        }
        clear_hp(K);
        continue;
    }
}