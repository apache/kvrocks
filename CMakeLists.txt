cmake_minimum_required(VERSION 3.10)
project(kvrocks)

set(CMAKE_CXX_STANDARD 98)

include_directories(./deps/glog/src/)
add_executable(kvrocks src/ev.cpp src/ev.h src/ev_kqueue.cpp src/kvrocks.cpp src/anet.cpp src/anet.h src/server.cpp src/server.h src/worker.cpp src/worker.h src/thread.h src/conn.cpp src/conn.h src/util.cpp src/util.h src/kvrocks.h)
target_link_libraries(kvrocks ${CMAKE_SOURCE_DIR}/deps/glog/.libs/libglog.a)
