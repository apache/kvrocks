# Copyright 2019 Eric Niebler
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.md or copy at http://boost.org/LICENSE_1_0.txt)

# Download and unpack googletest at configure time
configure_file(cmake/GoogleTest.cmake.in google-test-download/CMakeLists.txt)

execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/google-test-download )

if(result)
  message(FATAL_ERROR "CMake step for google-test failed: ${result}")
endif()

execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/google-test-download )

if(result)
  message(FATAL_ERROR "Build step for google-test failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add google-test directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/google-test-src
                 ${CMAKE_CURRENT_BINARY_DIR}/google-test-build
                 EXCLUDE_FROM_ALL)
