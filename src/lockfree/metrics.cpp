#include "include/metrics.h"
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <iostream>

using std::ostringstream;
using std::string;

void log_transition(TransitionType type, TimePoint start, TimePoint end) {
    double duration_ms = chrono::duration<double>(end - start).count() * 1000.0;
    auto& tm = transition_metrics[my_hp_index];

    switch(type) {
        case EIF_TRANS:
            tm.EIF_times.push_back(duration_ms);
            tm.EIF_count++;
            break;

        case DIF_TRANS:
            tm.DIF_times.push_back(duration_ms);
            tm.DIF_count++;
            break;

        case FUF_TRANS:
            tm.FUF_times.push_back(duration_ms);
            tm.FUF_count++;
            break;

        case FUF_ABORT_TRANS:
            tm.FUF_abort_times.push_back(duration_ms);
            tm.FUF_abort_count++;
            break;

        case FUF_ABORT_DELETE_TRANS:
            tm.FUF_abort_delete_times.push_back(duration_ms);
            tm.FUF_abort_delete_count++;
            break;

        case FXD_TRANS:
            tm.FXD_times.push_back(duration_ms);
            tm.FXD_count++;
            break;

        case FXD_ABORT_TRANS:
            tm.FXD_abort_times.push_back(duration_ms);
            tm.FXD_abort_count++;
            break;
    }
}

void log_spins(int spins, int cooldowns, double spin_time_ms, bool success) {
    spin_metrics[my_hp_index].spins_per_req.push_back(spins);
    spin_metrics[my_hp_index].cooldowns_per_req.push_back(cooldowns);
    spin_metrics[my_hp_index].spin_time_ms_per_req.push_back(spin_time_ms);
    spin_metrics[my_hp_index].reqs_that_spun++;
    if (success) {
        spin_metrics[my_hp_index].successful_spins++;
    } else {
        spin_metrics[my_hp_index].aborted_spins++;
    }
}

string format_number(double num) {
    ostringstream oss;
    oss << std::fixed;
    if (num >= 1'000'000) {
        oss << std::setprecision(2) << (num / 1'000'000.0) << "M";
    } else if (num >= 1'000) {
        oss << std::setprecision(2) << (num / 1'000.0) << "K";
    } else if (num >= 100) {
        oss << std::setprecision(1) << num;
    } else if (num >= 10) {
        oss << std::setprecision(2) << num;
    } else {
        oss << std::setprecision(3) << num;
    }
    return oss.str();
}

string get_spin_metrics(int total_set_ops) {
    vector<int> all_spins;
    vector<int> all_cooldowns;
    vector<double> all_spin_times;
    uint64_t total_reqs_that_spun = 0;
    uint64_t total_successful = 0;
    uint64_t total_aborted = 0;

    vector<double> per_thread_avg_spins;
    vector<int> per_thread_max_cooldowns;

    for (const auto& metrics : spin_metrics) {
        all_spins.insert(all_spins.end(),
                        metrics.spins_per_req.begin(),
                        metrics.spins_per_req.end());
        all_cooldowns.insert(all_cooldowns.end(),
                            metrics.cooldowns_per_req.begin(),
                            metrics.cooldowns_per_req.end());
        all_spin_times.insert(all_spin_times.end(),
                             metrics.spin_time_ms_per_req.begin(),
                             metrics.spin_time_ms_per_req.end());
        total_reqs_that_spun += metrics.reqs_that_spun;
        total_successful += metrics.successful_spins;
        total_aborted += metrics.aborted_spins;

        if (!metrics.spins_per_req.empty()) {
            double thread_total = 0;
            for (int s : metrics.spins_per_req) thread_total += s;
            per_thread_avg_spins.push_back(thread_total / metrics.spins_per_req.size());
        }

        if (!metrics.cooldowns_per_req.empty()) {
            int thread_max_cooldown = *std::max_element(metrics.cooldowns_per_req.begin(),
                                                        metrics.cooldowns_per_req.end());
            per_thread_max_cooldowns.push_back(thread_max_cooldown);
        }
    }

    if (all_spins.empty()) {
        return "    Spinning:     No requests spun\n";
    }

    std::sort(all_spins.begin(), all_spins.end());
    std::sort(all_cooldowns.begin(), all_cooldowns.end());
    std::sort(all_spin_times.begin(), all_spin_times.end());

    uint64_t total_spins = 0;
    for (int s : all_spins) total_spins += s;

    uint64_t total_cooldowns = 0;
    for (int c : all_cooldowns) total_cooldowns += c;

    double total_spin_time_ms = 0;
    for (double t : all_spin_times) total_spin_time_ms += t;

    double avg_spins = static_cast<double>(total_spins) / all_spins.size();
    int max_spins = all_spins.back();
    int min_spins = all_spins[0];
    int p50_spins = all_spins[all_spins.size() * 50 / 100];
    int p95_spins = all_spins[all_spins.size() * 95 / 100];
    int p99_spins = all_spins[all_spins.size() * 99 / 100];
    int p999_spins = all_spins[all_spins.size() * 999 / 1000];

    double avg_spin_time = total_spin_time_ms / all_spin_times.size();
    double p50_spin_time = all_spin_times[all_spin_times.size() * 50 / 100];
    double p95_spin_time = all_spin_times[all_spin_times.size() * 95 / 100];
    double p99_spin_time = all_spin_times[all_spin_times.size() * 99 / 100];
    double p999_spin_time = all_spin_times[all_spin_times.size() * 999 / 1000];
    double max_spin_time = all_spin_times.back();

    double success_rate = (static_cast<double>(total_successful) / total_reqs_that_spun) * 100;
    double abort_rate = (static_cast<double>(total_aborted) / total_reqs_that_spun) * 100;
    double set_spin_rate = (static_cast<double>(total_reqs_that_spun) / total_set_ops) * 100;

    int reqs_with_cooldown = 0;
    for (int c : all_cooldowns) {
        if (c > 0) reqs_with_cooldown++;
    }

    double avg_spins_with_cooldown = 0;
    double avg_spins_without_cooldown = 0;
    int count_with = 0, count_without = 0;
    for (size_t i = 0; i < all_spins.size(); i++) {
        if (all_cooldowns[i] > 0) {
            avg_spins_with_cooldown += all_spins[i];
            count_with++;
        } else {
            avg_spins_without_cooldown += all_spins[i];
            count_without++;
        }
    }
    if (count_with > 0) avg_spins_with_cooldown /= count_with;
    if (count_without > 0) avg_spins_without_cooldown /= count_without;

    std::sort(per_thread_avg_spins.begin(), per_thread_avg_spins.end());
    double min_thread_avg = per_thread_avg_spins.empty() ? 0 : per_thread_avg_spins[0];
    double max_thread_avg = per_thread_avg_spins.empty() ? 0 : per_thread_avg_spins.back();

    std::sort(per_thread_max_cooldowns.begin(), per_thread_max_cooldowns.end());
    int min_thread_max_cooldown = per_thread_max_cooldowns.empty() ? 0 : per_thread_max_cooldowns[0];
    int max_thread_max_cooldown = per_thread_max_cooldowns.empty() ? 0 : per_thread_max_cooldowns.back();

    ostringstream oss;
    oss << std::fixed;

    oss << "\n    Spinning:\n";
    oss << "    Summary: reqs=" << format_number(total_reqs_that_spun)
        << " (" << std::setprecision(1) << set_spin_rate << "% of SETs)"
        << " | success=" << std::setprecision(1) << success_rate
        << "% | abort=" << abort_rate << "%\n";

    oss << "    Spins:   min=" << min_spins
        << " | avg=" << format_number(avg_spins)
        << " | p50=" << format_number(p50_spins)
        << " | p95=" << format_number(p95_spins)
        << " | p99=" << format_number(p99_spins)
        << " | p999=" << format_number(p999_spins)
        << " | max=" << format_number(max_spins) << "\n";

    oss << "    Time:    avg=" << std::setprecision(3) << avg_spin_time << "ms"
        << " | p50=" << p50_spin_time << "ms"
        << " | p95=" << p95_spin_time << "ms"
        << " | p99=" << p99_spin_time << "ms"
        << " | p999=" << p999_spin_time << "ms"
        << " | max=" << max_spin_time << "ms"
        << " | total=" << std::setprecision(1) << total_spin_time_ms << "ms\n";

    oss << "    Cooldown: reqs=" << format_number(reqs_with_cooldown)
        << " (" << std::setprecision(1) << (static_cast<double>(reqs_with_cooldown) / all_cooldowns.size() * 100) << "%)"
        << " | total=" << format_number(total_cooldowns)
        << " | max=" << all_cooldowns.back() << "\n";

    if (count_with > 0) {
        oss << "    Avg spins (with cooldown):    "
            << format_number(avg_spins_with_cooldown) << "\n";
    }

    if (count_without > 0) {
        oss << "    Avg spins (without cooldown): "
            << format_number(avg_spins_without_cooldown) << "\n";
    }

    if (!per_thread_avg_spins.empty()) {
        oss << "    Per-thread avg: min=" << format_number(min_thread_avg)
            << " | max=" << format_number(max_thread_avg)
            << " | Δ=" << format_number(max_thread_avg - min_thread_avg) << "\n";
    }

    if (!per_thread_max_cooldowns.empty()) {
        oss << "    Per-thread max cooldowns: min=" << min_thread_max_cooldown
            << " | max=" << max_thread_max_cooldown
            << " | Δ=" << (max_thread_max_cooldown - min_thread_max_cooldown) << "\n";
    }

    return oss.str();
}

string get_transition_metrics() {
    vector<double> all_EIF, all_DIF, all_FUF, all_FXD, all_FUF_abort, all_FUF_abort_delete, all_FXD_abort;
    uint64_t total_EIF = 0, total_DIF = 0, total_FUF = 0, total_FXD = 0, total_FUF_abort = 0, total_FUF_abort_delete = 0, total_FXD_abort = 0;

    for (const auto& tm : transition_metrics) {
        all_EIF.insert(all_EIF.end(), tm.EIF_times.begin(), tm.EIF_times.end());
        all_DIF.insert(all_DIF.end(), tm.DIF_times.begin(), tm.DIF_times.end());
        all_FUF.insert(all_FUF.end(), tm.FUF_times.begin(), tm.FUF_times.end());
        all_FXD.insert(all_FXD.end(), tm.FXD_times.begin(), tm.FXD_times.end());
        all_FUF_abort.insert(all_FUF_abort.end(), tm.FUF_abort_times.begin(), tm.FUF_abort_times.end());
        all_FUF_abort_delete.insert(all_FUF_abort_delete.end(), tm.FUF_abort_delete_times.begin(), tm.FUF_abort_delete_times.end());
        all_FXD_abort.insert(all_FXD_abort.end(), tm.FXD_abort_times.begin(), tm.FXD_abort_times.end());

        total_EIF += tm.EIF_count;
        total_DIF += tm.DIF_count;
        total_FUF += tm.FUF_count;
        total_FXD += tm.FXD_count;
        total_FUF_abort += tm.FUF_abort_count;
        total_FUF_abort_delete += tm.FUF_abort_delete_count;
        total_FXD_abort += tm.FXD_abort_count;
    }

    ostringstream oss;
    oss << std::fixed << std::setprecision(4);

    auto format_transition = [&](const string& name,
                                 const vector<double>& times,
                                 uint64_t count) -> string {
        if (times.empty()) {
            return "    " + name + ": count=0\n";
        }

        vector<double> sorted = times;
        std::sort(sorted.begin(), sorted.end());

        double min_t = sorted[0];
        double max_t = sorted.back();
        double total = 0;
        for (double t : sorted) total += t;
        double mean_t = total / sorted.size();

        double p50 = sorted[sorted.size() * 50 / 100];
        double p95 = sorted[sorted.size() * 95 / 100];
        double p99 = sorted[sorted.size() * 99 / 100];
        double p999 = sorted[sorted.size() * 999 / 1000];

        ostringstream line;
        line << std::fixed << std::setprecision(4);
        line << "    " << name << ": "
             << "count=" << format_number(count) << " | "
             << "min=" << min_t << "ms | "
             << "mean=" << mean_t << "ms | "
             << "p50=" << p50 << "ms | "
             << "p95=" << p95 << "ms | "
             << "p99=" << p99 << "ms | "
             << "p999=" << p999 << "ms | "
             << "max=" << max_t << "ms\n";
        return line.str();
    };

    oss << "\n    Transitions:\n";
    oss << format_transition("E→I→F (insert empty)    ", all_EIF, total_EIF);
    oss << format_transition("D→I→F (insert deleted)  ", all_DIF, total_DIF);
    oss << format_transition("F→U→F (update)          ", all_FUF, total_FUF);
    oss << format_transition("F→X→D (delete)          ", all_FXD, total_FXD);

    if (total_FUF_abort > 0) {
        oss << format_transition("F→U→F (abort swap)     ", all_FUF_abort, total_FUF_abort);
    }

    if (total_FUF_abort_delete > 0) {
        oss << format_transition("F→U→D (abort delete)   ", all_FUF_abort_delete, total_FUF_abort_delete);
    }

    if (total_FXD_abort > 0) {
        oss << format_transition("F→X→D (abort)          ", all_FXD_abort, total_FXD_abort);
    }

    uint64_t total_transitions = total_EIF + total_DIF + total_FUF + total_FXD + total_FUF_abort + total_FUF_abort_delete + total_FXD_abort;
    if (total_transitions > 0) {
        oss << std::setprecision(1);
        oss << "    Distribution: "
            << "EIF=" << (total_EIF * 100.0 / total_transitions) << "% | "
            << "DIF=" << (total_DIF * 100.0 / total_transitions) << "% | "
            << "FUF=" << (total_FUF * 100.0 / total_transitions) << "% | "
            << "FXD=" << (total_FXD * 100.0 / total_transitions) << "%";

        if (total_FUF_abort > 0) {
            oss << " | FUF_ABORT_SWAP=" << (total_FUF_abort * 100.0 / total_transitions) << "%";
        }

        if (total_FUF_abort_delete > 0) {
            oss << " | FUF_ABORT_DEL=" << (total_FUF_abort_delete * 100.0 / total_transitions) << "%";
        }

        if (total_FXD_abort > 0) {
            oss << " | FXD_ABORT=" << (total_FXD_abort * 100.0 / total_transitions) << "%";
        }

        oss << "\n";
    }

    return oss.str();
}