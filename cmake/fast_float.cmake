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

FetchContent_DeclareGitHubWithMirror(fast_float
  fastfloat/fast_float v8.2.7
  MD5=d2bdc4e0af1755f6fe0e58fd8c9d8a3c
)

FetchContent_MakeAvailableWithArgs(fast_float
  FASTFLOAT_TEST=OFF
  FASTFLOAT_BENCHMARKS=OFF
  FASTFLOAT_INSTALL=OFF
)
