#pragma once

#include "types.h"
#include <string>

void log_transition(TransitionType type, TimePoint start, TimePoint end);
void log_spins(int spins, int cooldowns, double spin_time_ms, bool success);
std::string format_number(double num);
std::string get_spin_metrics(int total_set_ops);
std::string get_transition_metrics();