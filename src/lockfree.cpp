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
T* protect(std::atomic<T*>& container, const int idx) {
    T* ptr_ki = container.load();
    if (ptr_ki == nullptr) return nullptr;

    // free ptr_ki
    // reclaim ptr_ki -> k' ABA!!!!

    hp[my_hp_index].slot[idx].store(ptr_ki);

    if (ptr_ki == container.load())
        return ptr_ki;

    hp[my_hp_index].slot[idx].store(nullptr);
    return nullptr;
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

        // protect key
        std::string* ptr_ki = protect(CPKi, K);
        if (ptr_ki == nullptr) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

       // wrong key
        if (*ptr_ki != kB) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        // protect value
        std::string* ptr_vi = protect(CPVi, V);
        if (ptr_vi == nullptr) {
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            return;
        }

        // key deleted
        if (ptr_ki != CPKi.load()) {
            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            continue;
        }

        hp[my_hp_index].slot[K].store(nullptr);
        hp[my_hp_index].slot[V].store(nullptr);
        return;  // *ptr_vi
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

        // protect key
        std::string* ptr_ki = protect(CPKi, K);
        if (ptr_ki == nullptr) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        // wrong key
        if (*ptr_ki != kA) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        // key deleted
        if (ptr_ki != CPKi.load()) {
            hp[my_hp_index].slot[K].store(nullptr);
            return;
        }

        char exp = 'F';
        if (CPSi.compare_exchange_strong(exp, 'U')) {

            // key deleted
            if (ptr_ki != CPKi.load()) {
                CPSi.store('F');
                hp[my_hp_index].slot[K].store(nullptr);
                return;
            }

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

        std::string* ptr_ki = protect(CPKi, K);

        if (ptr_ki == nullptr) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        // wrong key
        if (*ptr_ki != kx) {
            hp[my_hp_index].slot[K].store(nullptr);
            continue;
        }

        // key deleted
        if (ptr_ki != CPKi.load()) {
            hp[my_hp_index].slot[K].store(nullptr);
            return;
        }

        char expected = 'F';
        if (CPSi.compare_exchange_strong(expected, 'X')) {

            // key deleted
            if (ptr_ki != CPKi.load()) {
                CPSi.store('F');
                hp[my_hp_index].slot[K].store(nullptr);
                return;
            }

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