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

FetchContent_DeclareGitHubWithMirror(rangev3
  ericniebler/range-v3 0.12.0
  MD5=e220e3f545fdf46241b4f139822d73a1
)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(WITH_DEBUG_INFO ON)
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(WITH_DEBUG_INFO OFF)
elseif(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(WITH_DEBUG_INFO ON)
elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
    set(WITH_DEBUG_INFO OFF)
endif()

if (PORTABLE STREQUAL 0)
    set(ARG_RANGES_NATIVE ON)
else()
    set(ARG_RANGES_NATIVE OFF)
endif()

FetchContent_MakeAvailableWithArgs(rangev3
  RANGES_CXX_STD=17
  RANGES_BUILD_CALENDAR_EXAMPLE=OFF
  RANGES_DEBUG_INFO=${WITH_DEBUG_INFO}
  RANGES_NATIVE=${ARG_RANGES_NATIVE}
)
