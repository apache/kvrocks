# Install script for directory: /Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src

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

if(CMAKE_INSTALL_COMPONENT STREQUAL "lib" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/lib/libevent_core.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_core.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_core.a")
    execute_process(COMMAND "/Library/Developer/CommandLineTools/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_core.a")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/pkgconfig/libevent_core.pc")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/usr/local/lib/pkgconfig" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/libevent_core.pc")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "lib" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/lib/libevent_extra.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_extra.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_extra.a")
    execute_process(COMMAND "/Library/Developer/CommandLineTools/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_extra.a")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/pkgconfig/libevent_extra.pc")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/usr/local/lib/pkgconfig" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/libevent_extra.pc")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "lib" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/lib/libevent_pthreads.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_pthreads.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_pthreads.a")
    execute_process(COMMAND "/Library/Developer/CommandLineTools/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent_pthreads.a")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/pkgconfig/libevent_pthreads.pc")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/usr/local/lib/pkgconfig" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/libevent_pthreads.pc")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "lib" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/lib/libevent.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent.a")
    execute_process(COMMAND "/Library/Developer/CommandLineTools/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libevent.a")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/pkgconfig/libevent.pc")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/usr/local/lib/pkgconfig" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/libevent.pc")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include" TYPE FILE FILES
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/evdns.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/evrpc.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/evhttp.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/evutil.h"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/event2" TYPE FILE FILES
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/buffer.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/bufferevent.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/bufferevent_compat.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/bufferevent_struct.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/buffer_compat.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/dns.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/dns_compat.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/dns_struct.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/event.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/event_compat.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/event_struct.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/http.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/http_compat.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/http_struct.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/keyvalq_struct.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/listener.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/rpc.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/rpc_compat.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/rpc_struct.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/tag.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/tag_compat.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/thread.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/util.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/include/event2/visibility.h"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/include/event2/event-config.h"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/cmake/libevent/LibeventConfig.cmake;/usr/local/lib/cmake/libevent/LibeventConfigVersion.cmake")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/usr/local/lib/cmake/libevent" TYPE FILE FILES
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build//CMakeFiles/LibeventConfig.cmake"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/LibeventConfigVersion.cmake"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}/usr/local/lib/cmake/libevent/LibeventTargets-static.cmake")
    file(DIFFERENT _cmake_export_file_changed FILES
         "$ENV{DESTDIR}/usr/local/lib/cmake/libevent/LibeventTargets-static.cmake"
         "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/CMakeFiles/Export/d258eb479df84f13e2e6af122d3f910d/LibeventTargets-static.cmake")
    if(_cmake_export_file_changed)
      file(GLOB _cmake_old_config_files "$ENV{DESTDIR}/usr/local/lib/cmake/libevent/LibeventTargets-static-*.cmake")
      if(_cmake_old_config_files)
        string(REPLACE ";" ", " _cmake_old_config_files_text "${_cmake_old_config_files}")
        message(STATUS "Old export file \"$ENV{DESTDIR}/usr/local/lib/cmake/libevent/LibeventTargets-static.cmake\" will be replaced.  Removing files [${_cmake_old_config_files_text}].")
        unset(_cmake_old_config_files_text)
        file(REMOVE ${_cmake_old_config_files})
      endif()
      unset(_cmake_old_config_files)
    endif()
    unset(_cmake_export_file_changed)
  endif()
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/cmake/libevent/LibeventTargets-static.cmake")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/usr/local/lib/cmake/libevent" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/CMakeFiles/Export/d258eb479df84f13e2e6af122d3f910d/LibeventTargets-static.cmake")
  if(CMAKE_INSTALL_CONFIG_NAME MATCHES "^([Rr][Ee][Ll][Ww][Ii][Tt][Hh][Dd][Ee][Bb][Ii][Nn][Ff][Oo])$")
    list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
     "/usr/local/lib/cmake/libevent/LibeventTargets-static-relwithdebinfo.cmake")
    if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
      message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
    endif()
    if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
      message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
    endif()
    file(INSTALL DESTINATION "/usr/local/lib/cmake/libevent" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/CMakeFiles/Export/d258eb479df84f13e2e6af122d3f910d/LibeventTargets-static-relwithdebinfo.cmake")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "runtime" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-src/event_rpcgen.py")
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/libevent-build/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
