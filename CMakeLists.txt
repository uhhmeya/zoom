cmake_minimum_required(VERSION 3.30)
project(zoom)

set(CMAKE_CXX_STANDARD 20)

add_executable(server src/server.cpp src/metrics.cpp
        src/lockfree.cpp)
add_executable(test src/test.cpp)