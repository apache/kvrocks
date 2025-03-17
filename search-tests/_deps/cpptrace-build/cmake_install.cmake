# Install script for directory: /Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-src

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

if(CMAKE_INSTALL_COMPONENT STREQUAL "cpptrace_development" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include" TYPE DIRECTORY FILES
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-src/include/"
    "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-build/include/"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "cpptrace_development" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-build/libcpptrace.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libcpptrace.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libcpptrace.a")
    execute_process(COMMAND "/Library/Developer/CommandLineTools/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libcpptrace.a")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "cpptrace_development" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-build/cmake/cpptrace-config.cmake")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "cpptrace_development" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-build/cpptrace-config-version.cmake")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "cpptrace_development" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace/cpptrace-targets.cmake")
    file(DIFFERENT _cmake_export_file_changed FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace/cpptrace-targets.cmake"
         "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-build/CMakeFiles/Export/feca40dfcffcc17837fe53a02ceb18d3/cpptrace-targets.cmake")
    if(_cmake_export_file_changed)
      file(GLOB _cmake_old_config_files "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace/cpptrace-targets-*.cmake")
      if(_cmake_old_config_files)
        string(REPLACE ";" ", " _cmake_old_config_files_text "${_cmake_old_config_files}")
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace/cpptrace-targets.cmake\" will be replaced.  Removing files [${_cmake_old_config_files_text}].")
        unset(_cmake_old_config_files_text)
        file(REMOVE ${_cmake_old_config_files})
      endif()
      unset(_cmake_old_config_files)
    endif()
    unset(_cmake_export_file_changed)
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-build/CMakeFiles/Export/feca40dfcffcc17837fe53a02ceb18d3/cpptrace-targets.cmake")
  if(CMAKE_INSTALL_CONFIG_NAME MATCHES "^([Rr][Ee][Ll][Ww][Ii][Tt][Hh][Dd][Ee][Bb][Ii][Nn][Ff][Oo])$")
    file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-build/CMakeFiles/Export/feca40dfcffcc17837fe53a02ceb18d3/cpptrace-targets-relwithdebinfo.cmake")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/cpptrace" TYPE FILE FILES "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-src/cmake/Findzstd.cmake")
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/cpptrace-build/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
