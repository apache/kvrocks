cmake_minimum_required(VERSION 3.10)
project(kvrocks
        VERSION 0.1.13
        DESCRIPTION "Redis on rocksdb"
        LANGUAGES CXX)

# External Dependences
include(ExternalProject)

set(default_build_type "Release")
if (NOT CMAKE_BUILD_DIRECTORY)
    set(CMAKE_BUILD_DIRECTORY ${CMAKE_BINARY_DIR})
endif()
# GLIBC < 2.17 should explict specify the real time library when use clock_*
find_library(REALTIME_LIB rt)
if (REALTIME_LIB)
    list(APPEND EXTERNAL_LIBS PRIVATE rt)
endif()

include(cmake/gflags.cmake)
list(APPEND EXTERNAL_LIBS PRIVATE ${gflags_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${gflags_INCLUDE_DIRS})

include(cmake/glog.cmake)
list(APPEND EXTERNAL_LIBS PRIVATE ${glog_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${glog_INCLUDE_DIRS})

include(cmake/snappy.cmake)
list(APPEND EXTERNAL_LIBS PRIVATE ${snappy_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${snappy_INCLUDE_DIRS})

include(cmake/rocksdb.cmake)
list(APPEND EXTERNAL_LIBS PRIVATE ${rocksdb_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${rocksdb_INCLUDE_DIRS})

include(cmake/libevent.cmake)
list(APPEND EXTERNAL_LIBS PRIVATE ${libevent_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${libevent_INCLUDE_DIRS})

include(cmake/gtest.cmake)
list(APPEND EXTERNAL_LIBS PRIVATE ${gtest_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${gtest_INCLUDE_DIRS})
# End dependences

# Add git sha to version.h
find_package(Git REQUIRED)
execute_process(COMMAND git rev-parse --short HEAD OUTPUT_VARIABLE GIT_SHA)
string(STRIP ${GIT_SHA} GIT_SHA)
configure_file(src/version.h.in ${PROJECT_BINARY_DIR}/version.h)

# Main target
add_executable(kvrocks)
target_compile_features(kvrocks PRIVATE cxx_std_11)
target_compile_options(kvrocks PRIVATE -Wall -Wpedantic -g -Wsign-compare -Wreturn-type)
option(ENABLE_ASAN "enable ASAN santinizer" OFF)
if(ENBALE_ASAN)
    target_compile_options(kvrocks PRIVATE -fno-omit-frame-pointer -fsanitize=address)
    target_link_libraries(kvrocks PRIVATE -fno-omit-frame-pointer -fsanitize=address)
endif()
add_dependencies(kvrocks libevent gflags glog rocksdb gtest)
target_include_directories(kvrocks PRIVATE ${PROJECT_BINARY_DIR})
target_include_directories(kvrocks ${EXTERNAL_INCS})
target_link_libraries(kvrocks ${EXTERNAL_LIBS} -pthread)
target_sources(kvrocks PRIVATE
        src/compact_filter.cc
        src/compact_filter.h
        src/worker.cc
        src/worker.h
        src/main.cc
        src/redis_request.cc
        src/redis_request.h
        src/redis_cmd.cc
        src/redis_cmd.h
        src/util.cc
        src/util.h
        src/storage.cc
        src/storage.h
        src/status.h
        src/redis_reply.h
        src/redis_reply.cc
        src/replication.cc
        src/replication.h
        src/task_runner.cc
        src/task_runner.h
        src/redis_encoding.h
        src/redis_encoding.cc
        src/redis_metadata.h
        src/redis_metadata.cc
        src/redis_string.h
        src/redis_string.cc
        src/redis_hash.h
        src/redis_hash.cc
        src/redis_list.h
        src/redis_list.cc
        src/redis_set.h
        src/redis_set.cc
        src/redis_zset.cc
        src/redis_zset.h
        src/redis_bitmap.cc
        src/redis_bitmap.h
        src/lock_manager.cc
        src/rocksdb_crc32c.h
        src/config.cc
        src/config.h
        src/stats.cc
        src/stats.h
        src/server.cc
        src/server.h
        src/cron.cc
        src/cron.h
        src/event_listener.h
        src/event_listener.cc)

# kvrocks2redis sync tool
add_executable(kvrocks2redis)
target_compile_features(kvrocks2redis PRIVATE cxx_std_11)
target_compile_options(kvrocks2redis PRIVATE -Wall -Wpedantic -g -Wsign-compare -Wreturn-type)
option(ENABLE_ASAN "enable ASAN santinizer" OFF)
if(ENBALE_ASAN)
    target_compile_options(kvrocks2redis PRIVATE -fno-omit-frame-pointer -fsanitize=address)
    target_link_libraries(kvrocks2redis PRIVATE -fno-omit-frame-pointer -fsanitize=address)
endif()
add_dependencies(kvrocks2redis libevent gflags glog rocksdb gtest)
target_include_directories(kvrocks2redis PRIVATE ${PROJECT_BINARY_DIR})
target_include_directories(kvrocks2redis ${EXTERNAL_INCS})
target_link_libraries(kvrocks2redis ${EXTERNAL_LIBS} -pthread)
target_sources(kvrocks2redis PRIVATE
        src/compact_filter.cc
        src/compact_filter.h
        src/util.cc
        src/util.h
        src/storage.cc
        src/storage.h
        src/status.h
        src/redis_reply.h
        src/redis_reply.cc
        src/task_runner.cc
        src/task_runner.h
        src/redis_encoding.h
        src/redis_encoding.cc
        src/redis_metadata.h
        src/redis_metadata.cc
        src/redis_string.h
        src/redis_string.cc
        src/redis_hash.h
        src/redis_hash.cc
        src/redis_list.h
        src/redis_list.cc
        src/redis_set.h
        src/redis_set.cc
        src/redis_zset.cc
        src/redis_zset.h
        src/redis_bitmap.cc
        src/redis_bitmap.h
        src/replication.cc
        src/replication.h
        src/lock_manager.cc
        src/rocksdb_crc32c.h
        src/config.cc
        src/config.h
        src/stats.cc
        src/stats.h
        src/cron.cc
        src/cron.h
        src/event_listener.h
        src/event_listener.cc
        tools/kvrocks2redis/config.cc
        tools/kvrocks2redis/config.h
        tools/kvrocks2redis/main.cc
        tools/kvrocks2redis/sync.cc
        tools/kvrocks2redis/sync.h
        tools/kvrocks2redis/util.cc
        tools/kvrocks2redis/util.h
        tools/kvrocks2redis/redis_writer.cc
        tools/kvrocks2redis/redis_writer.h
        tools/kvrocks2redis/writer.cc
        tools/kvrocks2redis/writer.h
        tools/kvrocks2redis/parser.cc
        tools/kvrocks2redis/parser.h)

add_executable(unittest
        src/redis_bitmap.cc
        src/redis_bitmap.h
        src/redis_metadata.cc
        src/redis_encoding.cc
        src/redis_string.cc
        src/redis_hash.cc
        src/redis_list.cc
        src/redis_set.cc
        src/redis_zset.cc
        src/util.cc
        src/storage.cc
        src/lock_manager.cc
        src/stats.cc
        src/event_listener.cc
        src/task_runner.cc
        src/cron.cc
        src/compact_filter.cc
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
        tests/rwlock_test.cc
        tests/stats_test.cc
        tests/cron_test.cc
        tests/task_runner_test.cc tests/t_bitmap_test.cc tests/compact_test.cc)

add_dependencies(unittest gflags glog rocksdb gtest)
target_compile_features(unittest PRIVATE cxx_std_11)
target_link_libraries(unittest ${EXTERNAL_LIBS})
target_include_directories(unittest ${EXTERNAL_INCS})
target_include_directories(unittest PRIVATE src)
