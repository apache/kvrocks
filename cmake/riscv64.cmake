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

set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR riscv64)
set(CMAKE_CROSSCOMPILING TRUE)

set(CMAKE_C_COMPILER "riscv64-unknown-linux-gnu-gcc")
set(CMAKE_CXX_COMPILER "riscv64-unknown-linux-gnu-g++")

add_compile_options("-march=rv64gc")

set(JEMALLOC_CROSS_FLAGS
    "--host=riscv64-unknown-linux-gnu"
    "--build=${CMAKE_HOST_SYSTEM_PROCESSOR}-pc-linux-gnu"
    "--with-lg-vaddr=48"
    CACHE STRING "jemalloc flags for RISC-V cross-compilation"
)
set(ENABLE_LUAJIT OFF CACHE BOOL "Disable LuaJIT on RISC-V" FORCE)
