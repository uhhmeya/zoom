#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <chrono>
#include <atomic>
#include <vector>

enum HP_Index {
    K = 0,
    V = 1
};

struct HP_Slot {
    std::atomic<void*> slot[2]{ nullptr, nullptr };
    std::atomic<bool> in_use{ false };
};

extern std::vector<HP_Slot> hp;
extern thread_local int my_hp_index;
extern thread_local std::vector<std::string*> retired_list;

extern void inc_set_count();

extern int get_my_hp_index();
extern void get(const std::string& key);
extern void set(const std::string& key, const std::string& value);
extern void del(const std::string& key);

extern void start(int expected, int admin_socket);
extern void inc_active();
extern void dec_active_log_lat(double latency_ms);

void Hreq(const std::string& input) {
    const size_t sp0 = input.find(' ');
    const std::string cmd = input.substr(0, sp0);

    if (cmd == "GET") {
        const std::string key = input.substr(sp0 + 1);
        get(key);
    }
    else if (cmd == "SET") {
        const size_t sp1 = input.find(' ', sp0+1);
        const std::string key = input.substr(sp0+1, sp1-sp0-1);
        const std::string value = input.substr(sp1+1);
        inc_set_count();
        set(key, value);
    }
    else if (cmd == "DEL") {
        const std::string key = input.substr(sp0 + 1);
        del(key);
    }
}

[[noreturn]] int main() {
    const int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(8080);
    address.sin_addr.s_addr = INADDR_ANY;
    bind(server_socket, reinterpret_cast<sockaddr *>(&address), sizeof(address));
    listen(server_socket, 10);

    while (true) {
        int client_socket = accept(server_socket, nullptr, nullptr);

        std::thread([client_socket]() {
            get_my_hp_index();
            std::string data;
            char batch[1024];
            ssize_t bytes_read;
            size_t i;

            while ((bytes_read = read(client_socket, batch, 1024)) > 0) {
                data.append(batch, bytes_read);

                while ((i = data.find('\n')) != std::string::npos) {
                    std::string cmd = data.substr(0, i);
                    data.erase(0, i + 1);

                    if (cmd.substr(0, 5) == "START") {
                        const size_t sp = cmd.find(' ');
                        const int exp = std::stoi(cmd.substr(sp + 1));
                        start(exp, client_socket);
                        continue;
                    }

                    inc_active();
                    auto t1 = std::chrono::high_resolution_clock::now();
                    Hreq(cmd);
                    auto t2 = std::chrono::high_resolution_clock::now();
                    const double lat = std::chrono::duration<double>(t2 - t1).count() * 1000.0;
                    dec_active_log_lat(lat);
                }
            }

            hp[my_hp_index].slot[K].store(nullptr);
            hp[my_hp_index].slot[V].store(nullptr);
            for (auto* ptr : retired_list) delete ptr;
            retired_list.clear();
            hp[my_hp_index].in_use.store(false);

            close(client_socket);
        }).detach();
    }
}