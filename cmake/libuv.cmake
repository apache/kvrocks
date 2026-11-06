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

include_guard()

# Prefer a system-installed libuv (e.g. apt-get install libuv1-dev / brew install libuv).
# Fall back to FetchContent if pkg-config / CMake config files are not available.
find_package(libuv CONFIG QUIET)

if(NOT libuv_FOUND)
  find_package(libuv QUIET)
endif()

if(NOT libuv_FOUND)
  message(STATUS "libuv not found via find_package, fetching from GitHub")
  include(FetchContent)

  FetchContent_Declare(libuv
    GIT_REPOSITORY https://github.com/libuv/libuv.git
    GIT_TAG        v1.49.0
    GIT_SHALLOW    TRUE
  )

  set(LIBUV_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(LIBUV_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  set(LIBUV_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(libuv)
endif()

add_library(uv_with_headers INTERFACE)
target_include_directories(uv_with_headers INTERFACE ${libuv_INCLUDE_DIRS})

if(TARGET libuv::libuv)
  target_link_libraries(uv_with_headers INTERFACE libuv::libuv)
elseif(TARGET uv_a)
  target_link_libraries(uv_with_headers INTERFACE uv_a)
elseif(TARGET uv)
  target_link_libraries(uv_with_headers INTERFACE uv)
else()
  # last resort: link directly to the discovered library variable
  target_link_libraries(uv_with_headers INTERFACE ${libuv_LIBRARIES})
endif()
