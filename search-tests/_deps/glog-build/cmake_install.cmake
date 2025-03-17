# Install script for directory: /Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "RelWithDebInfo")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set path to fallback-tool for dependency-resolution.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/Library/Developer/CommandLineTools/usr/bin/objdump")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/libglog.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libglog.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libglog.a")
    execute_process(COMMAND "/Library/Developer/CommandLineTools/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libglog.a")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/glog" TYPE FILE FILES
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/glog/export.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/src/glog/log_severity.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/src/glog/logging.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/src/glog/platform.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/src/glog/raw_logging.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/src/glog/stl_logging.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/src/glog/types.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/src/glog/flags.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/src/glog/vlog_is_on.h"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Development" OR NOT CMAKE_INSTALL_COMPONENT)
  
set (glog_FULL_CMake_DATADIR "\${CMAKE_CURRENT_LIST_DIR}/../../../share/glog/cmake")
set (glog_DATADIR_DESTINATION lib/cmake/glog)

if (NOT IS_ABSOLUTE lib/cmake/glog)
  set (glog_DATADIR_DESTINATION "${CMAKE_INSTALL_PREFIX}/${glog_DATADIR_DESTINATION}")
endif (NOT IS_ABSOLUTE lib/cmake/glog)

configure_file ("/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-src/glog-modules.cmake.in"
  "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/CMakeFiles/CMakeTmp/glog-modules.cmake" @ONLY)
file (INSTALL
  "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/CMakeFiles/CMakeTmp/glog-modules.cmake"
  DESTINATION
  "${glog_DATADIR_DESTINATION}")

endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/glog" TYPE FILE FILES
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/glog-config.cmake"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/glog-config-version.cmake"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Development" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/share/glog" TYPE DIRECTORY FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/share/glog/cmake" FILES_MATCHING REGEX "/[^/]*\\.cmake$")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/glog/glog-targets.cmake")
    file(DIFFERENT _cmake_export_file_changed FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/glog/glog-targets.cmake"
         "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/CMakeFiles/Export/3b58dfe2d8c90c3a489d71c8991c4dd3/glog-targets.cmake")
    if(_cmake_export_file_changed)
      file(GLOB _cmake_old_config_files "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/glog/glog-targets-*.cmake")
      if(_cmake_old_config_files)
        string(REPLACE ";" ", " _cmake_old_config_files_text "${_cmake_old_config_files}")
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/glog/glog-targets.cmake\" will be replaced.  Removing files [${_cmake_old_config_files_text}].")
        unset(_cmake_old_config_files_text)
        file(REMOVE ${_cmake_old_config_files})
      endif()
      unset(_cmake_old_config_files)
    endif()
    unset(_cmake_export_file_changed)
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/glog" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/CMakeFiles/Export/3b58dfe2d8c90c3a489d71c8991c4dd3/glog-targets.cmake")
  if(CMAKE_INSTALL_CONFIG_NAME MATCHES "^([Rr][Ee][Ll][Ww][Ii][Tt][Hh][Dd][Ee][Bb][Ii][Nn][Ff][Oo])$")
    file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/glog" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/CMakeFiles/Export/3b58dfe2d8c90c3a489d71c8991c4dd3/glog-targets-relwithdebinfo.cmake")
  endif()
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/glog-build/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
