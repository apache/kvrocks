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

FetchContent_DeclareGitHubWithMirror(libevent
  libevent/libevent release-2.1.12-stable
  MD5=041edf4f20251f429d1674759ab6882c
)

set(libevent_disable_ssl ON)
if(ENABLE_OPENSSL)
  set(libevent_disable_ssl OFF)
endif()

FetchContent_MakeAvailableWithArgs(libevent
  EVENT__DISABLE_TESTS=ON
  EVENT__DISABLE_REGRESS=ON
  EVENT__DISABLE_SAMPLES=ON
  EVENT__DISABLE_OPENSSL=${libevent_disable_ssl}
  EVENT__LIBRARY_TYPE=STATIC
  EVENT__DISABLE_BENCHMARK=ON
)

add_library(event_with_headers INTERFACE)
target_include_directories(event_with_headers INTERFACE ${libevent_SOURCE_DIR}/include ${libevent_BINARY_DIR}/include)
target_link_libraries(event_with_headers INTERFACE event event_pthreads)
if(ENABLE_OPENSSL)
  target_link_libraries(event_with_headers INTERFACE event_openssl)
endif()
