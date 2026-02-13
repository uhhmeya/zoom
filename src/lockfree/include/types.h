#pragma once

#include <atomic>
#include <string>
#include <vector>
#include <chrono>

using std::string;
using std::vector;
using std::atomic;
namespace chrono = std::chrono;
using TimePoint = chrono::high_resolution_clock::time_point;
using HRClock = chrono::high_resolution_clock;

constexpr int MAX_THREADS = 250;
constexpr int MAX_KEYS = 100;
constexpr int RETIRED_THRESHOLD = 100;
constexpr int COOLDOWN_THRES = 10'000;

constexpr auto acq_rel = std::memory_order_acq_rel;
constexpr auto release = std::memory_order_release;
constexpr auto acquire = std::memory_order_acquire;
constexpr auto relaxed = std::memory_order_relaxed;

enum HP_Index {
    K = 0,
    V = 1
};

enum TransitionType {
    EIF_TRANS,
    DIF_TRANS,
    FUF_TRANS,
    FUF_ABORT_TRANS,
    FUF_ABORT_DELETE_TRANS,
    FXD_TRANS,
    FXD_ABORT_TRANS
};

struct alignas(64) SpinMetrics {
    vector<int> spins_per_req;
    vector<int> cooldowns_per_req;
    vector<double> spin_time_ms_per_req;
    uint64_t reqs_that_spun{0};
    uint64_t successful_spins{0};
    uint64_t aborted_spins{0};

    SpinMetrics() {
        spins_per_req.reserve(100000);
        cooldowns_per_req.reserve(100000);
        spin_time_ms_per_req.reserve(100000);
    }
};

struct alignas(64) HP_Slot {
    atomic<void*> slot[2]{ nullptr, nullptr };
    atomic<bool> in_use{ false };
};

struct alignas(64) TB_slot {
    atomic<string*> k{nullptr};
    atomic<string*> v{nullptr};
    atomic<char> s{'E'};
};

struct alignas(64) TransitionMetrics {
    vector<double> EIF_times;   // insert into empty
    vector<double> DIF_times;   // insert into deleted
    vector<double> FUF_times;   // update
    vector<double> FXD_times;   // delete
    vector<double> FUF_abort_times; // abort (key swapped)
    vector<double> FUF_abort_delete_times; // abort (key deleted)
    vector<double> FXD_abort_times; // abort (job already done)

    // Counts
    uint64_t EIF_count{0};
    uint64_t DIF_count{0};
    uint64_t FUF_count{0};
    uint64_t FUF_abort_count{0}; // key swapped during set
    uint64_t FUF_abort_delete_count{0}; // key deleted during set
    uint64_t FXD_count{0};
    uint64_t FXD_abort_count{0}; // key deleted during del

    TransitionMetrics() {
        EIF_times.reserve(10000);
        DIF_times.reserve(10000);
        FUF_times.reserve(100000);
        FXD_times.reserve(10000);
        FUF_abort_times.reserve(1000);
        FUF_abort_delete_times.reserve(1000);
        FXD_abort_times.reserve(1000);
    }
};

extern vector<TransitionMetrics> transition_metrics;
extern vector<HP_Slot> hp;
extern vector<TB_slot> tb;
extern vector<SpinMetrics> spin_metrics;

extern thread_local vector<string*> retired_list;
extern thread_local int my_hp_index;