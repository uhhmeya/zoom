#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <random>


int make_client() {
    const int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(8080);
    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (connect(sock, reinterpret_cast<sockaddr *>(&server), sizeof(server)) < 0) {
        std::cerr << "Connection failed\n";
        close(sock);
        return -1;
    }
    return sock;
}

void worker(const int num_requests, const double rate_per_second) {
    const int sock = make_client();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> op_dist(1, 3);
    std::uniform_int_distribution<> key_dist(0, 100);
    std::uniform_int_distribution<> val_dist(0, 1000);

    const auto delay = std::chrono::nanoseconds(static_cast<long long>(1.0 / rate_per_second * 1e9));

    for (int i = 0; i < num_requests; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        const int op = op_dist(gen);
        std::string cmd;

        if (op == 1) cmd = "SET key_"+std::to_string(key_dist(gen))+" value_"+ std::to_string(val_dist(gen)) + "\n";
        else if (op == 2) cmd = "GET key_" + std::to_string(key_dist(gen)) + "\n";
        else  cmd = "DEL key_" + std::to_string(key_dist(gen)) + "\n";

        write(sock, cmd.c_str(), cmd.size());

        // maintain rate
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = end - start;
        if (auto sleep_time = delay - elapsed; sleep_time.count() > 0)
            std::this_thread::sleep_for(sleep_time);
    }
    close(sock);
}



void run_test(const int rate, const double duration, const int num_threads) {
    std::cout << rate / 1'000'000 << "M r/s for " << duration << "s :\n";

    const int total_reqs = static_cast<int>(rate * duration);
    const int base_reqs = total_reqs / num_threads;
    const int remainder = total_reqs % num_threads;

    std::vector<std::thread> threads;

    const int admin_sock = make_client();
    const std::string cmd = "START " + std::to_string(total_reqs) + "\n";
    write(admin_sock, cmd.c_str(), cmd.size());

    for (int i = 0; i < num_threads; i++) {
        int thr_reqs = base_reqs + (i < remainder ? 1 : 0);
        double thr_rate = static_cast<double>(rate) / num_threads;
        threads.push_back(std::thread(worker, thr_reqs, thr_rate));
    }

    for (auto& t : threads)
        t.join();

    // display result
    char buffer[4096];
    if (const ssize_t bytes = read(admin_sock, buffer, sizeof(buffer)); bytes > 0)
        std::cout << std::string(buffer, bytes);
    std::cout << "\n";
}

int main() {
    std::cout << "\n";

    constexpr int thread_counts[] = {10,50,99};

    for (const int num_threads : thread_counts) {
        std::cout << "" << num_threads << " threads...\n\n";
        for (int rate = 10'000'000; rate <= 30'000'000; rate += 20'000'000) {
            run_test(rate, 1.0, num_threads);
            int sleep = 10 + (rate / 10'000'000);
            std::this_thread::sleep_for(std::chrono::seconds(sleep));
        }
    }
    return 0;
}