#pragma once

#include "types.h"
#include <string>
#include <cstddef>

size_t hash(const std::string& key);
size_t hash2(const std::string& key);

void key_deleted_during_spin(bool did_spin, int spin_count, int cooldowns_hit, TimePoint spin_start);
std::string* get(const std::string& kB);
void set(const std::string& kA, const std::string& vA);
void del(const std::string& kx);