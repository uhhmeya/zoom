#include <vector>
#include <mutex>
#include <thread>
#include <chrono>
#include <algorithm>
#include <string>
#include <sstream>
#include <unistd.h>
#include <iomanip>
#include <atomic>
#include <iostream>


std::mutex S;
int _active = 0;
int _total = 0;
std::vector<int> _samples;

std::mutex L;
std::vector<double> _lats;

// 1 wr only
int expc = 0;
std::chrono::high_resolution_clock::time_point start_time;
double dur = 0;
int admin_fd = -1;
std::thread* bthread = nullptr;
std::atomic stop_bthread{false};

constexpr int sampling_interval_ms = 5;


void sample() {
    while (!stop_bthread.load()) {
        {
            std::lock_guard lock(S);
            _samples.push_back(_active);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sampling_interval_ms));
    }
}

void start(const int expected, const int admin_socket) {
    std::lock_guard _(S);
    std::lock_guard __(L);
    
    // Reset
    _active = 0;
    _total = 0;
    _samples.clear();
    _lats.clear();
    expc = expected;
    admin_fd = admin_socket;
    stop_bthread = false;
    start_time = std::chrono::high_resolution_clock::now();
    bthread = new std::thread(sample);
}

std::string get_metrics() {
    std::vector<int> sorted_samples;
    std::vector<double> sorted_lats;

    {
        std::lock_guard lock(S);
        sorted_samples = _samples;
    }

    {
        std::lock_guard lock(L);
        sorted_lats = _lats;
    }

    std::ranges::sort(sorted_samples);
    std::ranges::sort(sorted_lats);

    const int ns = sorted_samples.size();
    const int nl = sorted_lats.size();

    const int peak = *std::ranges::max_element(sorted_samples);
    const int minC = sorted_samples[0];
    double meanC = 0;
    for (const int s : sorted_samples) meanC += s;
    meanC /= ns;

    const int p50C = sorted_samples[static_cast<size_t>(ns) * 50 / 100];
    const int p95C = sorted_samples[static_cast<size_t>(ns) * 95 / 100];
    const int p99C = sorted_samples[static_cast<size_t>(ns) * 99 / 100];

    int contention_count = 0;
    for (const int s : sorted_samples) if (s > 1) contention_count++;
    const double conten = static_cast<double>(contention_count) / ns * 100;

    const double minL = sorted_lats[0];
    const double maxL = sorted_lats[nl - 1];
    double meanL = 0;
    for (double l : sorted_lats) meanL += l;
    meanL /= nl;

    const double p50L = sorted_lats[static_cast<size_t>(nl) * 50 / 100];
    const double p95L = sorted_lats[static_cast<size_t>(nl) * 95 / 100];
    const double p99L = sorted_lats[static_cast<size_t>(nl) * 99 / 100];

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3);
    oss << "    Latency (ms): min=" << minL << " | max=" << maxL << " | mean=" << meanL << " | p50=" << p50L << " | p95=" << p95L << " | p99=" << p99L << "\n";
    oss << std::setprecision(2);
    oss << "    Throughput:   requests=" << _total << " | duration=" << dur << "s | rate=" << (_total/dur/1'000'000.0) << "M req/s\n";
    oss << std::setprecision(1);
    oss << "    Concurrency:  peak=" << peak << " | min=" << minC << " | mean=" << meanC << " | p50=" << p50C << " | p95=" << p95C << " | p99=" << p99C << " | contention=" << conten << "%\n";

    return oss.str();
}

void inc_active() {
    std::lock_guard lock(S);
    _active++;
}

void dec_active_log_lat(const double latency_ms) {
    bool should_end_test = false;

    {
        std::lock_guard _(S);
        _active--;
        _total++;

        if (_total == expc) {
            should_end_test = true;
            stop_bthread.store(true);
        }
    }

    {
        std::lock_guard _(L);
        _lats.push_back(latency_ms);
    }


    if (should_end_test) {
        const auto end_time = std::chrono::high_resolution_clock::now();
        dur = std::chrono::duration<double>(end_time - start_time).count();

        if (bthread) {
            bthread->join();
            delete bthread;
            bthread = nullptr;
        }

        const std::string metrics = get_metrics();
        write(admin_fd, metrics.c_str(), metrics.size());
    }
}


