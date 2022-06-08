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

# lib name
set(LZ4_LIB_NAME "lz4")
# set lz4
set(LZ4_SOURCE "lz4-1.9.3")
# set download dir
set(LZ4_ZIP_DL_DIR ${CMAKE_CURRENT_BINARY_DIR}/_deps)
# set zip file name
set(LZ4_ZIP_NAME "lz4-1.9.3.tar.gz")
#
set(LZ4_ZIP_ABSOLUT_PATH "${LZ4_ZIP_DL_DIR}/lz4-1.9.3.tar.gz")
# url
set(LZ4_DOWNLOAD_URL "https://github.com/lz4/lz4/archive/v1.9.3.tar.gz")
# md5sum
set(LZ4_MD5SUM "3a1ab1684e14fc1afc66228ce61b2db3")
# set source dir
set(LZ4_SOURCE_DIR "${LZ4_ZIP_DL_DIR}/${LZ4_SOURCE}")
# include dir
set(LZ4_LIB_INCLUDE_DIR ${LZ4_SOURCE_DIR}/lib)
# src dir
set(LZ4_LIB_SOURCE_DIR ${LZ4_SOURCE_DIR}/lib)
# download timeout
set(DOWNLOAD_LZ4_TIMEOUT 600 CACHE STRING "Timeout in seconds when downloading LZ4.")

if (EXISTS ${LZ4_ZIP_ABSOLUT_PATH})
	file(MD5 ${LZ4_ZIP_ABSOLUT_PATH} LZ4_MD5SUM_)
	if (NOT ${LZ4_MD5SUM} STREQUAL ${LZ4_MD5SUM_})
		message(STATUS "remove -f ${LZ4_ZIP_ABSOLUT_PATH}")
		execute_process(COMMAND sh -c "rm -f ${LZ4_ZIP_ABSOLUT_PATH}")
	endif()
endif()

if (NOT EXISTS ${LZ4_ZIP_ABSOLUT_PATH})
	# download
	message(STATUS "Downloading ${LZ4_ZIP_NAME} to ${LZ4_ZIP_ABSOLUT_PATH}")
	file(DOWNLOAD ${LZ4_DOWNLOAD_URL}
		${LZ4_ZIP_ABSOLUT_PATH}
		TIMEOUT ${DOWNLOAD_LZ4_TIMEOUT}
		STATUS ERR SHOW_PROGRESS)
endif()

if (EXISTS ${LZ4_ZIP_ABSOLUT_PATH})
	file(MD5 ${LZ4_ZIP_ABSOLUT_PATH} LZ4_MD5SUM_)
	if (NOT ${LZ4_MD5SUM} STREQUAL ${LZ4_MD5SUM_})
		message(FATAL_ERROR "${LZ4_ZIP_ABSOLUT_PATH} seems be something wrong, please check")
	endif()
else()
	message(FATAL_ERROR "${LZ4_ZIP_ABSOLUT_PATH} seems be something wrong, please check")
endif()

message(STATUS "remove ${LZ4_SOURCE_DIR}")
execute_process(COMMAND sh -c "rm -rf ${LZ4_SOURCE_DIR}")
message(STATUS "tar xf ${LZ4_ZIP_ABSOLUT_PATH}")
execute_process(COMMAND ${CMAKE_COMMAND} -E tar xf "${LZ4_ZIP_ABSOLUT_PATH}"
    WORKING_DIRECTORY "${LZ4_ZIP_DL_DIR}"
    RESULT_VARIABLE tar_result)

add_custom_target(make_lz4 COMMAND make liblz4.a
	WORKING_DIRECTORY ${LZ4_LIB_SOURCE_DIR})

add_library(lz4 INTERFACE)
target_include_directories(lz4 INTERFACE ${LZ4_LIB_INCLUDE_DIR})
target_link_libraries(lz4 INTERFACE ${LZ4_LIB_SOURCE_DIR}/liblz4.a)
add_dependencies(lz4 make_lz4)
