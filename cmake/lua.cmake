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

include(FetchContent)

FetchContent_Declare(lua
  GIT_REPOSITORY https://github.com/KvrocksLabs/lua
  GIT_TAG 6f73d72d45c2e3915ee961e41705f35526608735
)

FetchContent_GetProperties(lua)
if(NOT lua_POPULATED)
  FetchContent_Populate(lua)

  set(LUA_CFLAGS "${CMAKE_C_FLAGS} -DLUA_ANSI -DENABLE_CJSON_GLOBAL -DREDIS_STATIC= -DLUA_USE_MKSTEMP")

  add_custom_target(make_lua COMMAND make "CFLAGS=${LUA_CFLAGS}" liblua.a
    WORKING_DIRECTORY ${lua_SOURCE_DIR}/src
  )
  
  file(GLOB LUA_PUBLIC_HEADERS "${lua_SOURCE_DIR}/src/*.h")
  file(COPY ${LUA_PUBLIC_HEADERS} DESTINATION ${lua_BINARY_DIR}/include)
endif()

add_library(lua INTERFACE)
target_include_directories(lua INTERFACE ${lua_BINARY_DIR}/include)
target_link_libraries(lua INTERFACE ${lua_SOURCE_DIR}/src/liblua.a)
add_dependencies(lua make_lua)
