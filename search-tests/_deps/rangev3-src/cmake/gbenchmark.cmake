# Download and unpack googletest at configure time
configure_file(
    cmake/GoogleBenchmark.cmake.in
    google-benchmark-download/CMakeLists.txt)

execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/google-benchmark-download)

if(result)
  message(FATAL_ERROR "CMake step for google-benchmark failed: ${result}")
endif()

execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/google-benchmark-download)

if(result)
  message(FATAL_ERROR "Build step for google-benchmark failed: ${result}")
endif()

# Add google-benchmark directly to our build. This defines
# the benchmark and benchmark_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/google-benchmark-src
                 ${CMAKE_CURRENT_BINARY_DIR}/google-benchmark-build
                 EXCLUDE_FROM_ALL)
