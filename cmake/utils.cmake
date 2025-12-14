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

macro(parse_var arg key value)
  string(REGEX REPLACE "^(.+)=(.+)$" "\\1;\\2" REGEX_RESULT ${arg})
  list(GET REGEX_RESULT 0 ${key})
  list(GET REGEX_RESULT 1 ${value})
endmacro()

function(FetchContent_MakeAvailableWithArgs dep)
  if(NOT ${dep}_POPULATED)
    message("Fetching ${dep}...")
    FetchContent_Populate(${dep})

    foreach(arg IN LISTS ARGN)
      parse_var(${arg} key value)
      set(${key}_OLD ${${key}})
      set(${key} ${value} CACHE INTERNAL "")
      message("In ${dep}: ${key} set to ${value}")
    endforeach()

    add_subdirectory(${${dep}_SOURCE_DIR} ${${dep}_BINARY_DIR} EXCLUDE_FROM_ALL)

    foreach(arg IN LISTS ARGN)
      parse_var(${arg} key value)
      set(${key} ${${key}_OLD} CACHE INTERNAL "")
    endforeach()
  endif()
endfunction()

function(FetchContent_DeclareGitHubWithMirror dep repo tag hash)
  set(_dep_fetch_url "${DEPS_FETCH_PROXY}https://github.com/${repo}/archive/${tag}.zip")
  if(DEPS_FETCH_DIR)
    get_filename_component(_deps_fetch_dir ${DEPS_FETCH_DIR} ABSOLUTE)
    set(_dep_fetch_dst ${_deps_fetch_dir}/${dep}-${tag}.zip)

    if(NOT EXISTS ${_dep_fetch_dst})
      message("Downloading ${_dep_fetch_url} to ${_dep_fetch_dst}...")
      file(DOWNLOAD ${_dep_fetch_url} ${_dep_fetch_dst} STATUS _dep_download_status)
      list(GET _dep_download_status 0 _dep_download_status_code)
      if(NOT _dep_download_status_code EQUAL 0)
        message(FATAL_ERROR "Failed to download ${_dep_fetch_url} to ${_dep_fetch_dst}")
      endif()
    endif()

    FetchContent_Declare(${dep}
      URL ${_dep_fetch_dst}
      URL_HASH ${hash}
    )
  else()
    FetchContent_Declare(${dep}
      URL ${_dep_fetch_url}
      URL_HASH ${hash}
    )
  endif()
endfunction()
