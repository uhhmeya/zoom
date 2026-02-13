cmake_minimum_required(VERSION 3.30)
project(zoom)

set(CMAKE_CXX_STANDARD 20)

add_executable(server
        src/server.cpp
        src/bench_metrics.cpp
        src/lockfree/globals.cpp
        src/lockfree/hp.cpp
        src/lockfree/metrics.cpp
        src/lockfree/ops.cpp
)

target_include_directories(server PRIVATE src/lockfree/include)

add_executable(test src/test.cpp)