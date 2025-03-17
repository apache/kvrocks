# Copyright Gonzalo Brito Gadeschi 2015
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.md or copy at http://boost.org/LICENSE_1_0.txt)
#
# CMake options

include(CMakeDependentOption)

set(RANGES_CXX_STD default CACHE STRING "C++ standard version.")
option(RANGES_BUILD_CALENDAR_EXAMPLE "Builds the calendar example." ON)
option(RANGES_ASAN "Run the tests using AddressSanitizer." OFF)
option(RANGES_MSAN "Run the tests using MemorySanitizer." OFF)
option(RANGES_ASSERTIONS "Enable assertions." ON)
option(RANGES_DEBUG_INFO "Include debug information in the binaries." ON)
option(RANGES_MODULES "Enables use of Clang modules (experimental)." OFF)
option(RANGES_NATIVE "Enables -march/-mtune=native." ON)
option(RANGES_VERBOSE_BUILD "Enables debug output from CMake." OFF)
option(RANGES_LLVM_POLLY "Enables LLVM Polly." OFF)
option(RANGES_ENABLE_WERROR
  "Enables -Werror. Only effective if compiler is not clang-cl or MSVC. ON by default"
  ON)
option(RANGES_PREFER_REAL_CONCEPTS
  "Use real concepts instead of emulation if the compiler supports it"
  ON)
option(RANGES_DEEP_STL_INTEGRATION
  "Hijacks the primary std::iterator_traits template to emulate the C++20 std::ranges:: behavior."
  OFF)
option(RANGE_V3_HEADER_CHECKS
  "Build the Range-v3 header checks and integrate with ctest"
  OFF)
set(RANGES_INLINE_THRESHOLD -1 CACHE STRING "Force a specific inlining threshold.")

# Enable verbose configure when passing -Wdev to CMake
if (DEFINED CMAKE_SUPPRESS_DEVELOPER_WARNINGS AND
    NOT CMAKE_SUPPRESS_DEVELOPER_WARNINGS)
  set(RANGES_VERBOSE_BUILD ON)
endif()

if (RANGES_VERBOSE_BUILD)
  message(STATUS "[range-v3]: verbose build enabled.")
endif()

CMAKE_DEPENDENT_OPTION(RANGE_V3_TESTS
  "Build the Range-v3 tests and integrate with ctest"
  ON "${is_standalone}" OFF)

CMAKE_DEPENDENT_OPTION(RANGE_V3_EXAMPLES
  "Build the Range-v3 examples and integrate with ctest"
  ON "${is_standalone}" OFF)

option(RANGE_V3_PERF
  "Build the Range-v3 performance benchmarks"
  OFF)

CMAKE_DEPENDENT_OPTION(RANGE_V3_DOCS
  "Build the Range-v3 documentation"
  ON "${is_standalone}" OFF)

mark_as_advanced(RANGE_V3_PERF)
