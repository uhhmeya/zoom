#include "include/ops.h"
#include "include/hp.h"
#include "include/metrics.h"
#include <thread>

size_t hash(const string& key) {
    size_t h = 0;
    for (const char c : key) h = h * 31 + c;
    return h;
}

size_t hash2(const string& key) {
    size_t h = 5381;
    for (const char c : key) h = ((h << 5) + h) ^ c;
    return h | 1;
}

void key_deleted_during_spin(bool did_spin, int spin_count, int cooldowns_hit, TimePoint spin_start) {
    // end spin
    if (did_spin) {
        auto spin_end = HRClock::now();
        double spin_time_ms = chrono::duration<double>(spin_end - spin_start).count() * 1000.0;
        log_spins(spin_count, cooldowns_hit, spin_time_ms, false);
    }
    clear_hp(K);
}

string* get(const string& kB) {
    const size_t y = hash(kB);
    const size_t step = hash2(kB);
    const size_t table_size = tb.size();

    for (size_t j = 0; j < table_size; j++) {
        const size_t i = (y + j * step) % table_size;
        auto& CPSi = tb[i].s;
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        const char Si = CPSi.load(acquire);

        if (Si == 'E') return nullptr;
        if (Si != 'F') continue;

        // protect
        string* ptr_ki = protect(CPKi, K);
        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        // wrong key
        if (*ptr_ki != kB) {
            clear_hp(K);
            continue;
        }

        // right key deleted
        string* ptr_vi = protect(CPVi, V);
        if (ptr_vi == nullptr) {
            clear_hp_both();
            continue;
        }

        // value not of key
        if (ptr_ki != CPKi.load(acquire)) {
            clear_hp_both();
            continue;
        }
        clear_hp_both();
        return ptr_vi;
    }
    return nullptr;
}

void set(const string& kA, const string& vA) {
    const size_t y = hash(kA);
    const size_t step = hash2(kA);
    const size_t table_size = tb.size();
    string* ptr_kA = nullptr;
    string* ptr_vA = nullptr;

    for (size_t j = 0; j < table_size; j++) {
        const size_t i = (y + j * step) % table_size;
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        auto& CPSi = tb[i].s;
        const char Si = CPSi.load(acquire);

        // EIF
        if (Si == 'E') {
            char expected = 'E';
            if (CPSi.compare_exchange_strong(expected, 'I', acq_rel, relaxed)) {
                auto trans_start = HRClock::now();

                ptr_kA = new string(kA);
                ptr_vA = new string(vA);
                CPKi.store(ptr_kA, relaxed);
                CPVi.store(ptr_vA, relaxed);
                CPSi.store('F', release);

                auto trans_end = HRClock::now();
                log_transition(EIF_TRANS, trans_start, trans_end);
                return;
            }
            continue;
        }

        // DIF
        if (Si == 'D') {
            char expected = 'D';
            if (CPSi.compare_exchange_strong(expected, 'I', acq_rel, relaxed)) {
                auto trans_start = HRClock::now();

                ptr_kA = new string(kA);
                ptr_vA = new string(vA);
                string* old_k = CPKi.exchange(ptr_kA, acq_rel);
                string* old_v = CPVi.exchange(ptr_vA, acq_rel);
                CPSi.store('F', release);
                retire(old_k);
                retire(old_v);

                auto trans_end = HRClock::now();
                log_transition(DIF_TRANS, trans_start, trans_end);
                return;
            }
            continue;
        }

        if (Si != 'F') continue;

        // protect
        string* ptr_ki = protect(CPKi, K);
        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        // bad key
        if (*ptr_ki != kA) {
            clear_hp(K);
            continue;
        }

        // right key deleted
        if (ptr_ki != CPKi.load(acquire)) {
            clear_hp(K);
            continue;
        }

        int spin_count = 0;
        int cooldowns_hit = 0;
        bool did_spin = false;
        TimePoint spin_start;

        char updated_Si = CPSi.load(acquire);

        while (true) {

            // spin
            if (updated_Si != 'F') {
                spin_count++;

                // start timer
                if (!did_spin) {
                    spin_start = HRClock::now();
                    did_spin = true;
                }

                if (updated_Si == 'D' ) {
                    key_deleted_during_spin(did_spin, spin_count, cooldowns_hit, spin_start);
                    break;
                }

                // cooldown
                if (spin_count % COOLDOWN_THRES == 0) {
                    cooldowns_hit++;
                    int sleep_ms;
                    if             (cooldowns_hit <= 30)         sleep_ms = 10;
                    else if     (cooldowns_hit <= 50)         sleep_ms = 20;
                    else if     (cooldowns_hit <= 70)         sleep_ms = 30;
                    else if     (cooldowns_hit <= 90)         sleep_ms = 50;
                    else if     (cooldowns_hit <= 100)       sleep_ms = 60;
                    else                                                    sleep_ms = 60;
                    std::this_thread::sleep_for(chrono::milliseconds(sleep_ms));
                }

                updated_Si = CPSi.load(acquire);
                continue;
            }

            // key deleted/swapped ; probe
            if (ptr_ki != CPKi.load(acquire)) {
                key_deleted_during_spin(did_spin, spin_count, cooldowns_hit, spin_start);
                break;
            }

            // send cas - FUF start
            if (CPSi.compare_exchange_strong(updated_Si, 'U', acq_rel, relaxed)) {
                auto trans_start = HRClock::now();

                // key deleted/swapped ; probe (ABORT CASE)
                if (ptr_ki != CPKi.load(acquire)) {
                    auto trans_end = HRClock::now();
                    string* current_ki = CPKi.load(acquire);

                    // key was deleted
                    if (current_ki == nullptr) {
                        CPSi.store('D', release);
                        log_transition(FUF_ABORT_DELETE_TRANS, trans_start, trans_end);
                    }

                    // key was swapped
                    else {
                        CPSi.store('F', release);
                        log_transition(FUF_ABORT_TRANS, trans_start, trans_end);
                    }

                    key_deleted_during_spin(did_spin, spin_count, cooldowns_hit, spin_start);
                    break;
                }

                // cas approved ~ FUF end
                ptr_vA = new string(vA);
                string *old_ptr_vi = CPVi.exchange(ptr_vA, acq_rel);
                CPSi.store('F', release);
                clear_hp(K);
                retire(old_ptr_vi);

                auto trans_end = HRClock::now();
                log_transition(FUF_TRANS, trans_start, trans_end);

                // end spin
                if (did_spin) {
                    auto spin_end = HRClock::now();
                    double spin_time_ms = chrono::duration<double>(spin_end - spin_start).count() * 1000.0;
                    log_spins(spin_count, cooldowns_hit, spin_time_ms, true);
                }
                return;
            }
            // cas failed : spin!
        }
        // key deleted : probe!
    }
}

void del(const string& kx) {
    const size_t y = hash(kx);
    const size_t step = hash2(kx);
    const size_t table_size = tb.size();

    for (size_t j = 0; j < table_size; j++) {
        const size_t i = (y + j * step) % table_size;
        auto& CPSi = tb[i].s;
        auto& CPKi = tb[i].k;
        auto& CPVi = tb[i].v;
        char Si = CPSi.load(acquire);

        if (Si == 'E') return;
        if (Si != 'F') continue;

        string* ptr_ki = protect(CPKi, K);

        // key deleted/swap
        if (ptr_ki == nullptr) {
            clear_hp(K);
            continue;
        }

        // wrong key
        if (*ptr_ki != kx) {
            clear_hp(K);
            continue;
        }

        // key deleted/swap
        if (ptr_ki != CPKi.load(acquire)) {
            clear_hp(K);
            continue;
        }

        char expected = 'F';
        if (CPSi.compare_exchange_strong(expected, 'X', acq_rel, relaxed)) {

            auto trans_start = HRClock::now();

            // key deleted / swapped
            if (ptr_ki != CPKi.load(acquire)) {
                CPSi.store('D', release); // FXD abort (job already done)
                clear_hp(K);
                auto trans_end = HRClock::now();
                log_transition(FXD_ABORT_TRANS, trans_start, trans_end);
                return;
            }

            string* ptr_k = CPKi.exchange(nullptr, acq_rel);
            string* ptr_v = CPVi.exchange(nullptr, acq_rel);
            CPSi.store('D', release);
            clear_hp_both();
            retire(ptr_k);
            retire(ptr_v);

            auto trans_end = HRClock::now();
            log_transition(FXD_TRANS, trans_start, trans_end);
            return;
        }
        clear_hp(K);
        continue;
    }
}