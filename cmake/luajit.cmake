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

if((${CMAKE_SYSTEM_NAME} MATCHES "Darwin") AND (NOT CMAKE_OSX_DEPLOYMENT_TARGET))
  message(FATAL_ERROR "The CMake option `CMAKE_OSX_DEPLOYMENT_TARGET` need to be specified, e.g. `-DCMAKE_OSX_DEPLOYMENT_TARGET=10.3`")
endif()

FetchContent_DeclareGitHubWithMirror(luajit
  KvrocksLabs/LuaJIT b80ea0e44bd259646d988324619612f645e4b637
  MD5=f9566c424fb57b226066e3a39a10ec8d
)

FetchContent_GetProperties(luajit)
if(NOT lua_POPULATED)
  FetchContent_Populate(luajit)

  set(LUA_CFLAGS "-DLUA_ANSI -DENABLE_CJSON_GLOBAL -DREDIS_STATIC= -DLUA_USE_MKSTEMP")
  if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    set(LUA_CFLAGS "${LUA_CFLAGS} -isysroot ${CMAKE_OSX_SYSROOT}")
  endif()

  if (CMAKE_HOST_APPLE)
    set(MACOSX_TARGET "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}")
  endif()

  add_custom_target(make_luajit COMMAND make libluajit.a
    "CFLAGS=${LUA_CFLAGS}" ${MACOSX_TARGET}
    WORKING_DIRECTORY ${luajit_SOURCE_DIR}/src
    BYPRODUCTS ${luajit_SOURCE_DIR}/src/libluajit.a
  )

  file(GLOB LUA_PUBLIC_HEADERS "${luajit_SOURCE_DIR}/src/*.hpp" "${luajit_SOURCE_DIR}/src/*.h")
  file(COPY ${LUA_PUBLIC_HEADERS} DESTINATION ${luajit_BINARY_DIR}/include)
endif()

add_library(luajit INTERFACE)
target_include_directories(luajit INTERFACE ${luajit_BINARY_DIR}/include)
target_link_libraries(luajit INTERFACE ${luajit_SOURCE_DIR}/src/libluajit.a dl)
add_dependencies(luajit make_luajit)
