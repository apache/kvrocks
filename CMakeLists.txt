cmake_minimum_required(VERSION 3.10)
project(kvrocks
        VERSION 0.0.0
        DESCRIPTION "Redis on rocksdb"
        LANGUAGES CXX)

# External Dependences
include(ExternalProject)

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
if(CMAKE_BUILD_TYPE STREQUAL Debug)
    target_compile_options(kvrocks PRIVATE -fno-omit-frame-pointer -fsanitize=address)
    target_link_libraries(kvrocks PRIVATE -fno-omit-frame-pointer -fsanitize=address)
endif()
add_dependencies(kvrocks libevent gflags glog rocksdb)
target_include_directories(kvrocks PRIVATE ${PROJECT_BINARY_DIR})
target_include_directories(kvrocks ${EXTERNAL_INCS})
target_link_libraries(kvrocks ${EXTERNAL_LIBS})
target_sources(kvrocks PRIVATE
               src/worker.cc
               src/worker.h
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
               src/replication.cc
               src/replication.h
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
               src/sock_util.cc
               src/rwlock.cc
               src/rocksdb_crc32c.h
               src/config.cc
               src/config.h
               src/stats.cc
               src/stats.h
               src/server.cc
               src/server.h
               src/event_listener.h
               src/event_listener.cc)

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
               src/rwlock.cc
               src/stats.cc
               src/event_listener.cc
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
               tests/stats_test.cc)
add_dependencies(unittest gflags glog rocksdb gtest)
target_compile_features(unittest PRIVATE cxx_std_11)
target_link_libraries(unittest ${EXTERNAL_LIBS})
target_include_directories(unittest ${EXTERNAL_INCS})
target_include_directories(unittest PRIVATE src)
