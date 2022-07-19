# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.13)
project(kvrocks
        DESCRIPTION "NoSQL which based on rocksdb and compatible with the Redis protocol"
        LANGUAGES CXX)

option(DISABLE_JEMALLOC "disable use of the jemalloc library" OFF)
option(ENABLE_ASAN "enable address santinizer" OFF)
option(ENABLE_TSAN "enable thread santinizer" OFF)
option(ASAN_WITH_LSAN "enable leak santinizer while address santinizer is enabled" ON)
option(ENABLE_STATIC_LIBSTDCXX "link kvrocks with static library of libstd++ instead of shared library" ON)
option(USE_LUAJIT "use luaJIT instead of lua" ON)

set(DEPS_FETCH_PROXY "" CACHE STRING
    "a template URL to proxy the traffic for fetching dependencies, e.g. with DEPS_FETCH_PROXY = https://some-proxy/,
     https://example/some-dep.zip -> https://some-proxy/https://example/some-dep.zip")

if(ENABLE_ASAN AND ENABLE_TSAN)
    message(FATAL_ERROR "ASan and TSan cannot be used at the same time")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# GLIBC < 2.17 should explict specify the real time library when use clock_*
find_library(REALTIME_LIB rt)
if (REALTIME_LIB)
    list(APPEND EXTERNAL_LIBS PRIVATE rt)
endif()

if (CMAKE_HOST_APPLE)
    set(DISABLE_JEMALLOC ON)
endif ()

if(NOT DISABLE_JEMALLOC)
    include(cmake/jemalloc.cmake)
    list(APPEND EXTERNAL_LIBS PRIVATE jemalloc)
endif()

set(BUILD_SHARED_LIBS OFF CACHE BOOL "do not build shared libs by default")

include(cmake/gtest.cmake)
include(cmake/glog.cmake)
include(cmake/snappy.cmake)
include(cmake/lz4.cmake)
include(cmake/rocksdb.cmake)
include(cmake/libevent.cmake)

if (USE_LUAJIT)
include(cmake/luajit.cmake)
else()
include(cmake/lua.cmake)
endif()

find_package(Threads REQUIRED)

list(APPEND EXTERNAL_LIBS PRIVATE glog)
list(APPEND EXTERNAL_LIBS PRIVATE snappy)
list(APPEND EXTERNAL_LIBS PRIVATE rocksdb_with_headers)
list(APPEND EXTERNAL_LIBS PRIVATE event_with_headers)
list(APPEND EXTERNAL_LIBS PRIVATE lz4)
if (USE_LUAJIT)
list(APPEND EXTERNAL_LIBS PRIVATE luajit)
else()
list(APPEND EXTERNAL_LIBS PRIVATE lua)
endif()


list(APPEND EXTERNAL_LIBS PRIVATE Threads::Threads)

# Add git sha to version.h
find_package(Git REQUIRED)
execute_process(COMMAND sh -c "cat VERSION"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} OUTPUT_VARIABLE PROJECT_VERSION)
execute_process(COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} OUTPUT_VARIABLE GIT_SHA)
string(STRIP "${GIT_SHA}" GIT_SHA)
configure_file(src/version.h.in ${PROJECT_BINARY_DIR}/version.h)

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc")

    if(ENABLE_STATIC_LIBSTDCXX)
        try_compile(FOUND_STATIC_LIBSTDCXX ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/cmake/checks/static_libstdcxx.cc
            LINK_OPTIONS -static-libstdc++ CXX_STANDARD 11)

        if(NOT FOUND_STATIC_LIBSTDCXX)
            message(FATAL_ERROR "cannot find static library of libstdc++, please add ENABLE_STATIC_LIBSTDCXX=OFF to disable")
        endif()

        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
    endif()
endif()

find_library(FOUND_UNWIND_LIB unwind)

set(WARNING_FLAGS -Wall -Wpedantic -Wsign-compare -Wreturn-type)

# kvrocks objects target
file(GLOB KVROCKS_SRCS src/*.cc)
list(FILTER KVROCKS_SRCS EXCLUDE REGEX src/main.cc)

add_library(kvrocks_objs OBJECT ${KVROCKS_SRCS})

target_include_directories(kvrocks_objs PUBLIC src ${PROJECT_BINARY_DIR})
target_compile_features(kvrocks_objs PUBLIC cxx_std_11)
target_compile_options(kvrocks_objs PUBLIC ${WARNING_FLAGS} -fno-omit-frame-pointer)
target_link_libraries(kvrocks_objs PUBLIC -fno-omit-frame-pointer)
if(ENABLE_ASAN)
    if(ASAN_WITH_LSAN)
        if((CMAKE_CXX_COMPILER_ID STREQUAL "GNU") AND (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5"))
            message(FATAL_ERROR "leak sanitizer is not supported until gcc 5")
        endif()
        target_compile_options(kvrocks_objs PUBLIC -fsanitize=leak)
        target_link_libraries(kvrocks_objs PUBLIC -fsanitize=leak)
    endif()
    target_compile_options(kvrocks_objs PUBLIC -fsanitize=address)
    target_link_libraries(kvrocks_objs PUBLIC -fsanitize=address)
endif()
if(ENABLE_TSAN)
    target_compile_options(kvrocks_objs PUBLIC -fsanitize=thread)
    target_link_libraries(kvrocks_objs PUBLIC -fsanitize=thread)
endif()
target_link_libraries(kvrocks_objs PUBLIC ${EXTERNAL_LIBS})
if(FOUND_UNWIND_LIB)
    target_link_libraries(kvrocks_objs PUBLIC ${FOUND_UNWIND_LIB})
endif()

# kvrocks main target
add_executable(kvrocks src/main.cc)
target_link_libraries(kvrocks PRIVATE kvrocks_objs ${EXTERNAL_LIBS})

# kvrocks2redis sync tool
file(GLOB KVROCKS2REDIS_SRCS tools/kvrocks2redis/*.cc)
add_executable(kvrocks2redis ${KVROCKS2REDIS_SRCS})

target_link_libraries(kvrocks2redis PRIVATE kvrocks_objs ${EXTERNAL_LIBS})

# kvrocks unit tests
file(GLOB TESTS_SRCS tests/cppunit/*.cc)
add_executable(unittest ${TESTS_SRCS})

target_link_libraries(unittest PRIVATE kvrocks_objs gtest_main ${EXTERNAL_LIBS})
