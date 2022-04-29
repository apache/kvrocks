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

if (NOT __JEMALLOC_INCLUDED) # guard against multiple includes
    set(__JEMALLOC_INCLUDED TRUE)

    find_package(Threads)
    # build directory
    set(jemalloc_PREFIX ${CMAKE_BUILD_DIRECTORY}/external/jemalloc-prefix)
    # install directory
    set(jemalloc_INSTALL ${CMAKE_BUILD_DIRECTORY}/external/jemalloc-install)
    set(JEMALLOC_SOURCE_DIR "${PROJECT_SOURCE_DIR}/external/jemalloc")

    if (UNIX)
        set(JEMALLOC_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(JEMALLOC_CXX_FLAGS ${CMAKE_CXX_FLAGS} ${JEMALLOC_EXTRA_COMPILER_FLAGS})
    set(JEMALLOC_C_FLAGS ${CMAKE_C_FLAGS} ${JEMALLOC_EXTRA_COMPILER_FLAGS})

    ExternalProject_Add(jemalloc
        SOURCE_DIR ${JEMALLOC_SOURCE_DIR}
        PREFIX ${jemalloc_PREFIX}
        INSTALL_DIR ${jemalloc_INSTALL}
        CONFIGURE_COMMAND ${JEMALLOC_SOURCE_DIR}/configure --enable-autogen --disable-libdl --with-jemalloc-prefix=""
            --prefix=${jemalloc_INSTALL}
        BUILD_COMMAND make
        INSTALL_COMMAND make dist COMMAND make install
    )
    ExternalProject_Add_Step(jemalloc autoconf
        COMMAND autoconf
        WORKING_DIRECTORY ${JEMALLOC_SOURCE_DIR}
        COMMENT  "Jemalloc autoconf"
        LOG 1
    )
    set(jemalloc_FOUND TRUE)
    set(jemalloc_INCLUDE_DIRS ${jemalloc_INSTALL}/include)
    set(jemalloc_LIBRARIES ${jemalloc_INSTALL}/lib/libjemalloc.a ${CMAKE_THREAD_LIBS_INIT})
endif()
