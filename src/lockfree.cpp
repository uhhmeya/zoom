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

std::string get(const std::string& kx) {
    const size_t y = hash(kx);
    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (y+j) % tb.size();
        const State s_i = tb[i].s.load();
        if (s_i == State::EMPTY) return "";
        if (s_i == State::DELETED) continue;

        const std::string* ptr_ki = tb[i].k.load(); //r

        // ki == kx
        if (ptr_ki && *ptr_ki == kx)
            return *tb[i].v.load(); //r

    }
    return nullptr;
}


bool set(const std::string& kx, const std::string& vx) {
    const size_t idx = hash(kx);

    const auto ptr_kx = new std::string(kx);
    const auto ptr_vx = new std::string(vx);

    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (idx + j) % tb.size();

        const std::string* ptr_ki = tb[i].k.load();
        const State s_i = tb[i].s.load();

        // 1 : φ φ --> kx vx
        if (s_i == State::EMPTY) {
            std::string* exp = nullptr;
            if (tb[i].k.compare_exchange_strong(exp, ptr_kx)) {
                //  exp == *ptr_ki
                //  swap ptr_ki w ptr_kx
                tb[i].v.store(ptr_vx); // swap ptr_vi w ptr_vx
                tb[i].s.store(State::FULL); // si = full
                return true;
            }
            continue; // exp =/= *ptr_k
        }

        // 2 : tombstone
        if (s_i == State::DELETED)
            continue;


        // 3 : kx vi ⟶ kx vx
        if (ptr_ki && *ptr_ki == kx) {
            std::string* vi = tb[i].v.exchange(ptr_vx);
            delete vi;
            delete ptr_kx;
            return true;
        }

        // 4 : kz... (probe)
    }

    // 5 : full table
    delete ptr_kx;
    delete ptr_vx;
    return false;
}

bool del(const std::string& kx) {
    const size_t idx = hash(kx);

    for (size_t j = 0; j < tb.size(); j++) {
        const size_t i = (idx+j) % tb.size();

        State s_i = tb[i].s.load();

        if (s_i == State::EMPTY) return false;

        if (s_i == State::DELETED) continue;

        const std::string* ptr_ki = tb[i].k.load();
        const std::string* ptr_vi = tb[i].v.load();

        if (ptr_ki && *ptr_ki == kx) { // found
            tb[i].s.store(State::DELETED);
            tb[i].k.store(nullptr);
            tb[i].v.store(nullptr);
            delete ptr_ki;
            delete ptr_vi;
            return true;
        }
    }
    return false;  // not found
}









