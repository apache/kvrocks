# Copyright Louis Dionne 2015
# Copyright Gonzalo Brito Gadeschi 2015
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.md or copy at http://boost.org/LICENSE_1_0.txt)
#
# Setup compiler flags (more can be set on a per-target basis or in
# subdirectories)

# Compilation flags
include(CheckCXXCompilerFlag)
macro(ranges_append_flag testname flag)
  # As -Wno-* flags do not lead to build failure when there are no other
  # diagnostics, we check positive option to determine their applicability.
  # Of course, we set the original flag that is requested in the parameters.
  string(REGEX REPLACE "^-Wno-" "-W" alt ${flag})
  check_cxx_compiler_flag(${alt} ${testname})
  if (${testname})
    add_compile_options(${flag})
  endif()
endmacro()

function(cxx_standard_normalize cxx_standard return_value)
  if("x${cxx_standard}" STREQUAL "x1y")
    set( ${return_value} "14" PARENT_SCOPE )
  elseif("x${cxx_standard}" STREQUAL "x1z")
    set( ${return_value} "17" PARENT_SCOPE )
  elseif("x${cxx_standard}" STREQUAL "xlatest" OR "x${cxx_standard}" STREQUAL "x2a")
    set( ${return_value} "20" PARENT_SCOPE )
  else()
    set( ${return_value} "${cxx_standard}" PARENT_SCOPE )
  endif()
endfunction()

function(cxx_standard_denormalize cxx_standard return_value)
  if("x${cxx_standard}" STREQUAL "x17")
    if (RANGES_CXX_COMPILER_CLANGCL OR RANGES_CXX_COMPILER_MSVC)
      set( ${return_value} 17 PARENT_SCOPE )
    else()
      set( ${return_value} 1z PARENT_SCOPE )
    endif()
  elseif("x${cxx_standard}" STREQUAL "x20")
    if (RANGES_CXX_COMPILER_CLANGCL OR RANGES_CXX_COMPILER_MSVC)
      set( ${return_value} latest PARENT_SCOPE )
    else()
      set( ${return_value} 2a PARENT_SCOPE )
    endif()
  else()
    set( ${return_value} ${cxx_standard} PARENT_SCOPE )
  endif()
endfunction()

if(CMAKE_CXX_STANDARD)
  if(NOT "x${RANGES_CXX_STD}" STREQUAL "xdefault")
    # Normalize RANGES_CXX_STD
    cxx_standard_normalize( ${RANGES_CXX_STD} ranges_cxx_std )
    if(NOT "x${ranges_cxx_std}" STREQUAL "x${CMAKE_CXX_STANDARD}")
      message(FATAL_ERROR "[range-v3]: Cannot specify both CMAKE_CXX_STANDARD and RANGES_CXX_STD, or they must match.")
    endif()
  else()
    cxx_standard_denormalize(${CMAKE_CXX_STANDARD} RANGES_CXX_STD)
  endif()
elseif("x${RANGES_CXX_STD}" STREQUAL "xdefault")
  if (RANGES_CXX_COMPILER_CLANGCL OR RANGES_CXX_COMPILER_MSVC)
    set(RANGES_CXX_STD 17)
  else()
    set(RANGES_CXX_STD 14)
  endif()
endif()

# All compilation flags
# Language flag: version of the C++ standard to use
message(STATUS "[range-v3]: C++ std=${RANGES_CXX_STD}")
if (RANGES_CXX_COMPILER_CLANGCL OR RANGES_CXX_COMPILER_MSVC)
  ranges_append_flag(RANGES_HAS_CXXSTDCOLON "/std:c++${RANGES_CXX_STD}")
  set(RANGES_STD_FLAG "/std:c++${RANGES_CXX_STD}")
  if (RANGES_CXX_COMPILER_CLANGCL)
    # The MSVC STL before VS 2019v16.6 with Clang 10 requires -fms-compatibility in C++17 mode, and
    # doesn't support C++20 mode at all. Let's drop this flag until AppVeyor updates to VS2016v16.6.
    # ranges_append_flag(RANGES_HAS_FNO_MS_COMPATIBIILITY "-fno-ms-compatibility")
    ranges_append_flag(RANGES_HAS_FNO_DELAYED_TEMPLATE_PARSING "-fno-delayed-template-parsing")
  endif()
  # Enable "normal" warnings and make them errors:
  ranges_append_flag(RANGES_HAS_W3 /W3)
  ranges_append_flag(RANGES_HAS_WX /WX)
else()
  ranges_append_flag(RANGES_HAS_CXXSTD "-std=c++${RANGES_CXX_STD}")
  set(RANGES_STD_FLAG "-std=c++${RANGES_CXX_STD}")
  # Enable "normal" warnings and make them errors:
  ranges_append_flag(RANGES_HAS_WALL -Wall)
  ranges_append_flag(RANGES_HAS_WEXTRA -Wextra)
  if (RANGES_ENABLE_WERROR)
    ranges_append_flag(RANGES_HAS_WERROR -Werror)
  endif()
endif()

if (RANGES_ENV_LINUX AND RANGES_CXX_COMPILER_CLANG)
  # On linux libc++ re-exports the system math headers. The ones from libstdc++
  # use the GCC __extern_always_inline intrinsic which is not supported by clang
  # versions 3.6, 3.7, 3.8, 3.9, 4.0, and current trunk 5.0 (as of 2017.04.13).
  #
  # This works around it by replacing __extern_always_inline with inline using a
  # macro:
  ranges_append_flag(RANGES_HAS_D__EXTERN_ALWAYS_INLINE -D__extern_always_inline=inline)
endif()

# Deep integration support
if (RANGES_DEEP_STL_INTEGRATION)
  if (RANGES_CXX_COMPILER_MSVC)
    add_compile_options(/I "${PROJECT_SOURCE_DIR}/include/std")
    add_compile_options(/I "${PROJECT_SOURCE_DIR}/include")
  else()
    add_compile_options(-isystem "${PROJECT_SOURCE_DIR}/include/std")
    add_compile_options(-I "${PROJECT_SOURCE_DIR}/include")
  endif()
  add_compile_options(-DRANGES_DEEP_STL_INTEGRATION=1)
endif()

# Template diagnostic flags
ranges_append_flag(RANGES_HAS_FDIAGNOSTIC_SHOW_TEMPLATE_TREE -fdiagnostics-show-template-tree)
ranges_append_flag(RANGES_HAS_FTEMPLATE_BACKTRACE_LIMIT "-ftemplate-backtrace-limit=0")
ranges_append_flag(RANGES_HAS_FMACRO_BACKTRACE_LIMIT "-fmacro-backtrace-limit=1")

# Clang modules support
if (RANGES_MODULES)
  ranges_append_flag(RANGES_HAS_MODULES -fmodules)
  ranges_append_flag(RANGES_HAS_MODULE_MAP_FILE "-fmodule-map-file=${PROJECT_SOURCE_DIR}/include/module.modulemap")
  ranges_append_flag(RANGES_HAS_MODULE_CACHE_PATH "-fmodules-cache-path=${PROJECT_BINARY_DIR}/module.cache")
  if (RANGES_LIBCXX_MODULE)
    ranges_append_flag(RANGES_HAS_LIBCXX_MODULE_MAP_FILE "-fmodule-map-file=${RANGES_LIBCXX_MODULE}")
  endif()
  if (RANGES_ENV_MACOSX)
    ranges_append_flag(RANGES_HAS_NO_IMPLICIT_MODULE_MAPS -fno-implicit-module-maps)
  endif()
  if (RANGES_DEBUG_BUILD)
    ranges_append_flag(RANGES_HAS_GMODULES -gmodules)
  endif()
endif()

# Sanitizer support: detect incompatible sanitizer combinations
if (RANGES_ASAN AND RANGES_MSAN)
  message(FATAL_ERROR "[range-v3 error]: AddressSanitizer and MemorySanitizer are both enabled at the same time!")
endif()

if (RANGES_MSAN AND RANGES_ENV_MACOSX)
  message(FATAL_ERROR "[range-v3 error]: MemorySanitizer is not supported on MacOSX!")
endif()

# AddressSanitizer support
if (RANGES_ASAN)
  # This policy enables passing the linker flags to the linker when trying to
  # test the features, which is required to successfully link ASan binaries
  cmake_policy(SET CMP0056 NEW)
  set (ASAN_FLAGS "")
  if (RANGES_ENV_MACOSX) # LeakSanitizer not supported on MacOSX
    set (ASAN_FLAGS "-fsanitize=address,signed-integer-overflow,shift,integer-divide-by-zero,implicit-signed-integer-truncation,implicit-integer-sign-change,undefined,nullability")
  else()
    if (RANGES_CXX_COMPILER_CLANG AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.0")
      set (ASAN_FLAGS "-fsanitize=address")
    else()
      set (ASAN_FLAGS "-fsanitize=address,signed-integer-overflow,shift,integer-divide-by-zero,implicit-signed-integer-truncation,implicit-integer-sign-change,leak,nullability")
    endif()
  endif()
  ranges_append_flag(RANGES_HAS_ASAN "${ASAN_FLAGS}")
  if (RANGES_HAS_ASAN) #ASAN flags must be passed to the linker:
    set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} ${ASAN_FLAGS}")
  endif()
  ranges_append_flag(RANGES_HAS_SANITIZE_NO_RECOVER "-fno-sanitize-recover=all")
  ranges_append_flag(RANGES_HAS_NO_OMIT_FRAME_POINTER -fno-omit-frame-pointer)
endif()

# MemorySanitizer support
if (RANGES_MSAN)
  # This policy enables passing the linker flags to the linker when trying to
  # compile the examples, which is required to successfully link MSan binaries
  cmake_policy(SET CMP0056 NEW)
  ranges_append_flag(RANGES_HAS_MSAN "-fsanitize=memory")
  ranges_append_flag(RANGES_HAS_MSAN_TRACK_ORIGINS -fsanitize-memory-track-origins)
  ranges_append_flag(RANGES_HAS_SANITIZE_RECOVER_ALL "-fno-sanitize-recover=all")
  ranges_append_flag(RANGES_HAS_NO_OMIT_FRAME_POINTER -fno-omit-frame-pointer)
endif()

# Build types:
if (RANGES_DEBUG_BUILD AND RANGES_RELEASE_BUILD)
  message(FATAL_ERROR "[range-v3 error] Cannot simultaneously generate debug and release builds!")
endif()

if (RANGES_DEBUG_BUILD)
  ranges_append_flag(RANGES_HAS_NO_INLINE -fno-inline)
  ranges_append_flag(RANGES_HAS_STACK_PROTECTOR_ALL -fstack-protector-all)
  ranges_append_flag(RANGES_HAS_G3 -g3)
  # Clang can generate debug info tuned for LLDB or GDB
  if (RANGES_CXX_COMPILER_CLANG)
    if (RANGES_ENV_MACOSX)
      ranges_append_flag(RANGES_HAS_GLLDB -glldb)
    elseif(RANGES_ENV_LINUX OR RANGES_ENV_OPENBSD)
      ranges_append_flag(RANGES_HAS_GGDB -ggdb)
    endif()
  endif()
endif()

if (RANGES_RELEASE_BUILD)
  if (NOT RANGES_ASSERTIONS)
    ranges_append_flag(RANGES_HAS_DNDEBUG -DNDEBUG)
  endif()
  if (NOT RANGES_ASAN AND NOT RANGES_MSAN)
    # The quality of ASan and MSan error messages suffers if we disable the
    # frame pointer, so leave it enabled when compiling with either of them:
    ranges_append_flag(RANGES_HAS_OMIT_FRAME_POINTER -fomit-frame-pointer)
  endif()

  ranges_append_flag(RANGES_HAS_OFAST -Ofast)
  if (NOT RANGES_HAS_OFAST)
    ranges_append_flag(RANGES_HAS_O2 -O2)
  endif()
  ranges_append_flag(RANGES_HAS_STRICT_ALIASING -fstrict-aliasing)
  ranges_append_flag(RANGES_HAS_STRICT_VTABLE_POINTERS -fstrict-vtable-pointers)
  ranges_append_flag(RANGES_HAS_FAST_MATH -ffast-math)
  ranges_append_flag(RANGES_HAS_VECTORIZE -fvectorize)

  if (NOT RANGES_ENV_MACOSX)
    # Sized deallocation is not available in MacOSX:
    ranges_append_flag(RANGES_HAS_SIZED_DEALLOCATION -fsized-deallocation)
  endif()

  if (RANGES_LLVM_POLLY)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mllvm -polly -mllvm -polly-vectorizer=stripmine")
  endif()

  if (RANGES_CXX_COMPILER_CLANG AND (NOT (RANGES_INLINE_THRESHOLD EQUAL -1)))
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mllvm -inline-threshold=${RANGES_INLINE_THRESHOLD}")
  endif()
endif()

if (RANGES_NATIVE)
  ranges_append_flag(RANGES_HAS_MARCH_NATIVE "-march=native")
  ranges_append_flag(RANGES_HAS_MTUNE_NATIVE "-mtune=native")
endif()

include(CheckCXXSourceCompiles)

set(CMAKE_REQUIRED_FLAGS ${RANGES_STD_FLAG})
# Probe for library and compiler support for aligned new
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/cmake/aligned_new_probe.cpp" RANGE_V3_PROBE_CODE)
check_cxx_source_compiles("${RANGE_V3_PROBE_CODE}" RANGE_V3_ALIGNED_NEW_PROBE)
unset(RANGE_V3_PROBE_CODE)
unset(CMAKE_REQUIRED_FLAGS)
if (NOT RANGE_V3_ALIGNED_NEW_PROBE)
  add_compile_options("-DRANGES_CXX_ALIGNED_NEW=0")
endif()

# Probe for coroutine TS support
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/cmake/coro_test_code.cpp" RANGE_V3_PROBE_CODE)
if(RANGES_CXX_COMPILER_MSVC)
  set(CMAKE_REQUIRED_FLAGS "/await")
  check_cxx_source_compiles("${RANGE_V3_PROBE_CODE}" RANGES_HAS_AWAIT)
  if(RANGES_HAS_AWAIT)
    set(RANGE_V3_COROUTINE_FLAGS "/await")
  endif()
elseif(RANGES_CXX_COMPILER_CLANG)
  set(CMAKE_REQUIRED_FLAGS "-fcoroutines-ts ${RANGES_STD_FLAG}")
  check_cxx_source_compiles("${RANGE_V3_PROBE_CODE}" RANGES_HAS_FCOROUTINES_TS)
  if(RANGES_HAS_FCOROUTINES_TS)
    set(RANGE_V3_COROUTINE_FLAGS "-fcoroutines-ts")
  endif()
endif()
unset(CMAKE_REQUIRED_FLAGS)
unset(RANGE_V3_PROBE_CODE)
if (RANGE_V3_COROUTINE_FLAGS)
  add_compile_options(${RANGE_V3_COROUTINE_FLAGS})
endif()

# Test for concepts support
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/cmake/concepts_test_code.cpp" RANGE_V3_PROBE_CODE)
if(RANGES_CXX_COMPILER_GCC OR RANGES_CXX_COMPILER_CLANG)
  set(CMAKE_REQUIRED_FLAGS "-fconcepts ${RANGES_STD_FLAG}")
  check_cxx_source_compiles("${RANGE_V3_PROBE_CODE}" RANGE_V3_HAS_FCONCEPTS)
  if(RANGE_V3_HAS_FCONCEPTS)
    set(RANGE_V3_CONCEPTS_FLAGS "-fconcepts")
  endif()
endif()
unset(CMAKE_REQUIRED_FLAGS)
unset(RANGE_V3_PROBE_CODE)
if (RANGE_V3_CONCEPTS_FLAGS AND RANGES_PREFER_REAL_CONCEPTS)
  add_compile_options(${RANGE_V3_CONCEPTS_FLAGS})
endif()

if (RANGES_VERBOSE_BUILD)
  get_directory_property(RANGES_COMPILE_OPTIONS COMPILE_OPTIONS)
  message(STATUS "[range-v3]: C++ flags: ${CMAKE_CXX_FLAGS}")
  message(STATUS "[range-v3]: C++ debug flags: ${CMAKE_CXX_FLAGS_DEBUG}")
  message(STATUS "[range-v3]: C++ Release Flags: ${CMAKE_CXX_FLAGS_RELEASE}")
  message(STATUS "[range-v3]: C++ Compile Flags: ${CMAKE_CXX_COMPILE_FLAGS}")
  message(STATUS "[range-v3]: Compile options: ${RANGES_COMPILE_OPTIONS}")
  message(STATUS "[range-v3]: C Flags: ${CMAKE_C_FLAGS}")
  message(STATUS "[range-v3]: C Compile Flags: ${CMAKE_C_COMPILE_FLAGS}")
  message(STATUS "[range-v3]: EXE Linker flags: ${CMAKE_EXE_LINKER_FLAGS}")
  message(STATUS "[range-v3]: C++ Linker flags: ${CMAKE_CXX_LINK_FLAGS}")
  message(STATUS "[range-v3]: MODULE Linker flags: ${CMAKE_MODULE_LINKER_FLAGS}")
  get_directory_property(CMakeCompDirDefs COMPILE_DEFINITIONS)
  message(STATUS "[range-v3]: Compile Definitions: ${CmakeCompDirDefs}")
endif()
