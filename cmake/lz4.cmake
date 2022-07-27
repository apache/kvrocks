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

include(cmake/utils.cmake)

FetchContent_DeclareGitHubWithMirror(lz4
  lz4/lz4 v1.9.3
  MD5=72defe037b2c3db7a69affe7fe4bffd6
)

FetchContent_GetProperties(lz4)
if(NOT lz4_POPULATED)
  FetchContent_Populate(lz4)

  set(LZ4_CFLAGS "")
  if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    set(LZ4_CFLAGS "${LZ4_CFLAGS} -isysroot ${CMAKE_OSX_SYSROOT}")
  endif()
  
  add_custom_target(make_lz4 COMMAND make CC=${CMAKE_C_COMPILER} CFLAGS=${LZ4_CFLAGS} liblz4.a
    WORKING_DIRECTORY ${lz4_SOURCE_DIR}/lib
    BYPRODUCTS ${lz4_SOURCE_DIR}/lib/liblz4.a
  )
endif()

add_library(lz4 INTERFACE)
target_include_directories(lz4 INTERFACE $<BUILD_INTERFACE:${lz4_SOURCE_DIR}/lib>)
target_link_libraries(lz4 INTERFACE $<BUILD_INTERFACE:${lz4_SOURCE_DIR}/lib/liblz4.a>)
add_dependencies(lz4 make_lz4)
