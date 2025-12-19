#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unordered_map>
#include <string>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <chrono>

extern void start(int expected, int admin_socket);
extern void samp1();
extern void lat_samp2(double latency_ms);

std::unordered_map<std::string, std::string> db;
std::mutex lock;

void Hreq(const std::string& input) {
    const size_t sp0 = input.find(' ');
    const std::string cmd = input.substr(0, sp0);

    if (cmd == "GET") {
        std::string key = input.substr(sp0 + 1);
        std::lock_guard _(lock);
        std::string v = db.count(key) ? db[key] : "NULL";
    }

    else if (cmd == "SET") {
        size_t sp1 = input.find(' ', sp0+1);
        const std::string key = input.substr(sp0+1, sp1-sp0 -1);
        const std::string value = input.substr(sp1+1);
        std::lock_guard _(lock);
        db[key] = value;
    }

    else if (cmd == "DEL") {
        std::string key = input.substr(sp0 + 1);
        std::lock_guard _(lock);
        db.erase(key);
    }
}

[[noreturn]] int main() {

    // make server
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
            std::string data;
            char batch[1024];
            ssize_t bytes_read;
            size_t i;

            // buffer --> batch
            while ((bytes_read = read(client_socket, batch, 1024)) > 0) {

                // batch --> data
                data.append(batch, bytes_read);

                // data --> Hreq
                while ((i = data.find('\n')) != std::string::npos) {
                    std::string cmd = data.substr(0, i);

                    if (cmd.substr(0, 5) == "START") {
                        const size_t space = cmd.find(' ');
                        const int exp = std::stoi(cmd.substr(space + 1));
                        start(exp, client_socket);
                    }

                    else {
                        auto t1 = std::chrono::high_resolution_clock::now();
                        samp1(); // _active++
                        Hreq(cmd);
                        auto t2 = std::chrono::high_resolution_clock::now();
                        const double lat = std::chrono::duration<double>(t2 - t1).count() * 1000.0;
                        lat_samp2(lat); //_active-- & append latency
                    }
                    data.erase(0, i+1);
                }
            }
            close(client_socket); // runs after last batch
        }).detach();}
}




