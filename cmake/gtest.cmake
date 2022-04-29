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

if (NOT __GTEST_INCLUDED) # guard against multiple includes
    set(__GTEST_INCLUDED TRUE)

    # gtest will use pthreads if it's available in the system, so we must link with it
    find_package(Threads)

    # build directory
    set(gtest_PREFIX ${CMAKE_BUILD_DIRECTORY}/external/gtest-prefix)
    # install directory
    set(gtest_INSTALL ${CMAKE_BUILD_DIRECTORY}/external/gtest-install)

    if (UNIX)
        set(GTEST_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(GTEST_CXX_FLAGS ${CMAKE_CXX_FLAGS} ${GTEST_EXTRA_COMPILER_FLAGS})
    set(GTEST_C_FLAGS ${CMAKE_C_FLAGS} ${GTEST_EXTRA_COMPILER_FLAGS})

    ExternalProject_Add(gtest
        PREFIX ${gtest_PREFIX}
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/external/googletest
        INSTALL_DIR ${gtest_INSTALL}
        CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
                   -DCMAKE_INSTALL_PREFIX=${gtest_INSTALL}
                   -DCMAKE_C_FLAGS=${GTEST_C_FLAGS}
                   -DCMAKE_CXX_FLAGS=${GTEST_CXX_FLAGS}
                   -DBUILD_GMOCK=OFF
        LOG_DOWNLOAD 1
        LOG_INSTALL 1
        )

    set(gtest_FOUND TRUE)
    set(gtest_INCLUDE_DIRS ${gtest_INSTALL}/include)
    set(gtest_LIBRARIES ${gtest_INSTALL}/${CMAKE_INSTALL_LIBDIR}/libgtest.a ${CMAKE_THREAD_LIBS_INIT})
endif()
