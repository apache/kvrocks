cmake_minimum_required(VERSION 3.10)
project(ev
        VERSION 0.0.0
        DESCRIPTION "demo ev"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-Wall -Wpedantic -g -fsanitize=address")

#find_package(gflags REQUIRED)
#find_package(glog REQUIRED)

find_package(Git REQUIRED)
execute_process(COMMAND git rev-parse --short HEAD OUTPUT_VARIABLE GIT_SHA)
string(STRIP ${GIT_SHA} GIT_SHA)

configure_file(src/version.h.in src/version.h)
LINK_DIRECTORIES(/usr/local/lib)
include_directories(/usr/local/include)
include_directories(${PROJECT_BINARY_DIR})
add_executable(kvrocks
               ${PROJECT_BINARY_DIR}/version.h
               src/server.cc
               src/server.h
               src/main.cc
               src/redis_request.cc
               src/redis_request.h
               src/redis_cmd.cc
               src/redis_cmd.h
               src/string_util.cc
               src/string_util.h
               src/storage.cc
               src/storage.h
               src/status.h
               src/redis_reply.h
               src/redis_reply.cc
               src/redis_replication.cc
               src/redis_replication.h
               src/t_encoding.h
               src/t_encoding.cc
               src/t_metadata.h
               src/t_metadata.cc
               src/t_string.h
               src/t_string.cc
               src/t_hash.h
               src/t_hash.cc
               src/t_list.h
               src/t_list.cc
               src/t_set.h
               src/t_set.cc
               src/t_zset.cc
               src/t_zset.h
               src/sock_util.h
               src/sock_util.cc)
target_link_libraries(kvrocks rocksdb event event_pthreads gflags glog)

include_directories(src)
add_executable(unittest
        src/t_metadata.cc
        src/t_encoding.cc
        src/t_string.cc
        src/t_hash.cc
        src/t_list.cc
        src/t_set.cc
        src/t_zset.cc
        src/string_util.cc
        src/storage.cc
        tests/main.cc
        tests/test_base.h
        tests/t_string_test.cc
        tests/t_encoding_test.cc
        tests/t_list_test.cc
        tests/t_hash_test.cc
        tests/t_set_test.cc
        tests/t_zset_test.cc
        tests/t_metadata_test.cc
        tests/string_util_test.cc
        )
target_link_libraries(unittest gtest rocksdb event event_pthreads gflags glog)