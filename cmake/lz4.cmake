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

FetchContent_Declare(
  lz4
  URL https://github.com/lz4/lz4/archive/v1.9.3.tar.gz
  URL_HASH MD5=3a1ab1684e14fc1afc66228ce61b2db3
)

FetchContent_GetProperties(lz4)
if(NOT lz4_POPULATED)
  FetchContent_Populate(lz4)
  add_custom_target(make_lz4 COMMAND make liblz4.a
    WORKING_DIRECTORY ${lz4_SOURCE_DIR}/lib)
endif()

add_library(lz4 INTERFACE)
target_include_directories(lz4 INTERFACE ${lz4_SOURCE_DIR}/lib)
target_link_libraries(lz4 INTERFACE ${lz4_SOURCE_DIR}/lib/liblz4.a)
add_dependencies(lz4 make_lz4)
