#include "include/hp.h"

using std::runtime_error;

int get_my_hp_index() {
    if (my_hp_index == -1) {
        for (int i = 0; i < MAX_THREADS; i++) {
            bool expected = false;
            if (hp[i].in_use.compare_exchange_strong(expected, true, acq_rel, relaxed)) {
                my_hp_index = i;
                return i;
            }
        }
        throw runtime_error("No HP slots available");
    }
    return my_hp_index;
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

void retire(string* ptr) {
    if (ptr == nullptr) return;
    retired_list.push_back(ptr);
    if (retired_list.size() >= RETIRED_THRESHOLD) {
        freeScan();
    }
}