#pragma once

#include "types.h"
#include <atomic>
#include <stdexcept>

int get_my_hp_index();

template<typename T>
T* protect(std::atomic<T*>& container, int idx);

void clear_hp(int idx);
void clear_hp_both();
bool can_delete(const void* ptr);
void freeScan();
void retire(std::string* ptr);

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