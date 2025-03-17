# Copyright Eric Niebler 2014
# Copyright Gonzalo Brito Gadeschi 2014, 2017
# Copyright Louis Dionne 2015
# Copyright Casey Carter 2016
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.md or copy at http://boost.org/LICENSE_1_0.txt)

cmake_minimum_required(VERSION 3.6)
get_directory_property(is_subproject PARENT_DIRECTORY)

if(NOT is_subproject)
    set(is_standalone YES)
else()
    set(is_standalone NO)
endif()

project(Range-v3 CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Export compilation data-base
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

include(ranges_options)
include(ranges_env)
include(ranges_flags)

add_library(range-v3-meta INTERFACE)
add_library(range-v3::meta ALIAS range-v3-meta)
target_include_directories(range-v3-meta INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/>)
target_include_directories(range-v3-meta SYSTEM INTERFACE $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>)
target_compile_options(range-v3-meta INTERFACE $<$<COMPILE_LANG_AND_ID:CXX,MSVC>:/permissive->
                                               $<$<COMPILE_LANG_AND_ID:CUDA,MSVC>:-Xcompiler=/permissive->)

add_library(range-v3-concepts INTERFACE)
add_library(range-v3::concepts ALIAS range-v3-concepts)
target_include_directories(range-v3-concepts INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/>)
target_include_directories(range-v3-concepts SYSTEM INTERFACE $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>)
target_compile_options(range-v3-concepts INTERFACE $<$<COMPILE_LANG_AND_ID:CXX,MSVC>:/permissive->
                                                   $<$<COMPILE_LANG_AND_ID:CUDA,MSVC>:-Xcompiler=/permissive->)
target_link_libraries(range-v3-concepts INTERFACE range-v3::meta)

add_library(range-v3 INTERFACE)
add_library(range-v3::range-v3 ALIAS range-v3)
target_include_directories(range-v3 INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/>)
target_include_directories(range-v3 SYSTEM INTERFACE $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>)
target_compile_options(range-v3 INTERFACE $<$<COMPILE_LANG_AND_ID:CXX,MSVC>:/permissive->
                                          $<$<COMPILE_LANG_AND_ID:CUDA,MSVC>:-Xcompiler=/permissive->)
target_link_libraries(range-v3 INTERFACE range-v3::concepts range-v3::meta)

function(rv3_add_test TESTNAME EXENAME FIRSTSOURCE)
  add_executable(range.v3.${EXENAME} ${FIRSTSOURCE} ${ARGN})
  target_link_libraries(range.v3.${EXENAME} range-v3)
  add_test(range.v3.${TESTNAME} range.v3.${EXENAME})
endfunction(rv3_add_test)

if(RANGE_V3_DOCS)
  add_subdirectory(doc)
endif()

if(RANGE_V3_TESTS)
  include(CTest)
  add_subdirectory(test)
endif()

if(RANGE_V3_EXAMPLES)
  add_subdirectory(example)
endif()

if(RANGE_V3_PERF)
  include(gtest)
  include(gbenchmark)
  add_subdirectory(perf)
endif()

# Add header files as sources to fix MSVS 2017 not finding source during debugging
file(GLOB_RECURSE RANGE_V3_PUBLIC_HEADERS_ABSOLUTE
                  "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp")
add_custom_target(range.v3.headers SOURCES ${RANGE_V3_PUBLIC_HEADERS_ABSOLUTE})
set_target_properties(range.v3.headers PROPERTIES FOLDER "header")

# Test all headers
if(RANGE_V3_HEADER_CHECKS)
  include(TestHeaders)

  file(GLOB_RECURSE RANGE_V3_PUBLIC_HEADERS
                    RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}/include"
                    "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp")
  # These headers are not meant to be included directly:
  list(REMOVE_ITEM RANGE_V3_PUBLIC_HEADERS std/detail/associated_types.hpp)
  list(REMOVE_ITEM RANGE_V3_PUBLIC_HEADERS range/v3/detail/epilogue.hpp)
  # Deprecated headers
  if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
    foreach(header ${RANGE_V3_PUBLIC_HEADERS})
      file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/include/${header}" is_deprecated
           LIMIT_COUNT 1
           REGEX ".*RANGES_DEPRECATED_HEADER.*")
      if(is_deprecated)
        list(APPEND RANGE_V3_DEPRECATED_PUBLIC_HEADERS "${header}")
      endif()
    endforeach()
  endif()

  if(NOT RANGE_V3_COROUTINE_FLAGS)
      # Can't test this header if we can't even compile it
      list(APPEND RANGE_V3_DEPRECATED_PUBLIC_HEADERS range/v3/experimental/utility/generator.hpp)
  endif()

  add_header_test(test.range.v3.headers
    EXCLUDE ${RANGE_V3_DEPRECATED_PUBLIC_HEADERS}
    HEADERS ${RANGE_V3_PUBLIC_HEADERS})
  target_link_libraries(test.range.v3.headers PRIVATE range-v3)
endif()

# Grab the range-v3 version numbers:
include(${CMAKE_CURRENT_SOURCE_DIR}/Version.cmake)
set(RANGE_V3_VERSION ${RANGE_V3_MAJOR}.${RANGE_V3_MINOR}.${RANGE_V3_PATCHLEVEL})

# Try to build a new version.hpp
configure_file(version.hpp.in include/range/v3/version.hpp @ONLY)
file(STRINGS ${CMAKE_CURRENT_BINARY_DIR}/include/range/v3/version.hpp RANGE_V3_OLD_VERSION_HPP)
file(STRINGS ${CMAKE_CURRENT_SOURCE_DIR}/include/range/v3/version.hpp RANGE_V3_NEW_VERSION_HPP)

# If the new version.hpp is materially different from the one in the source
# directory, update it, commit, and tag.
if(NOT RANGE_V3_NEW_VERSION_HPP STREQUAL RANGE_V3_OLD_VERSION_HPP)
  # Check that doc/release_notes.md and Version.cmake are the only changed file:
  execute_process(
    COMMAND ${GIT_EXECUTABLE} -C "${CMAKE_CURRENT_SOURCE_DIR}" status --porcelain -uno
    OUTPUT_VARIABLE RANGE_V3_GIT_STATUS
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  string(REPLACE "\n" ";"  RANGE_V3_GIT_STATUS ${RANGE_V3_GIT_STATUS})
  if (NOT "x${RANGE_V3_GIT_STATUS}" STREQUAL "x M Version.cmake; M doc/release_notes.md")
    message(FATAL_ERROR "Cannot update version.hpp: range-v3 source directory has a dirty status")
  endif()
  file(
    COPY ${CMAKE_CURRENT_BINARY_DIR}/include/range/v3/version.hpp
    DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/include/range/v3
  )
  execute_process(
    COMMAND ${GIT_EXECUTABLE} -C "${CMAKE_CURRENT_SOURCE_DIR}" add -u
  )
  execute_process(
    COMMAND ${GIT_EXECUTABLE} -C "${CMAKE_CURRENT_SOURCE_DIR}" commit -m "${RANGE_V3_VERSION}"
  )
  execute_process(
    COMMAND ${GIT_EXECUTABLE} -C "${CMAKE_CURRENT_SOURCE_DIR}" tag -f -a "${RANGE_V3_VERSION}" -m "${RANGE_V3_VERSION}"
  )
  find_program(CONAN_EXECUTABLE NAMES conan conan.exe)
  if (NOT "x${CONAN_EXECUTABLE}" STREQUAL "xCONAN_EXECUTABLE-NOTFOUND")
    message(STATUS "Exporting conanfile for new version")
    execute_process(
      COMMAND ${CONAN_EXECUTABLE} create . range-v3/${RANGE_V3_VERSION}@ericniebler/stable
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    )
  endif()
  message(STATUS "Version updated to ${RANGE_V3_VERSION}. Don't forget to:")
  message(STATUS "  git push origin <feature-branch>")
  message(STATUS "and (after that is merged to master) then:")
  message(STATUS "  conan remote add range-v3 https://api.bintray.com/conan/range-v3/range-v3")
  message(STATUS "  conan create ${CMAKE_CURRENT_SOURCE_DIR} range-v3/${RANGE_V3_VERSION}@ericniebler/stable")
  message(STATUS "  conan upload --all range-v3/${RANGE_V3_VERSION}@ericniebler/stable -r=range-v3")
endif()

include(CMakePackageConfigHelpers)

# write_basic_package_version_file(...) gained ARCH_INDEPENDENT in CMake 3.14.
# For CMake 3.6, this workaround makes the version file ARCH_INDEPENDENT
# by making CMAKE_SIZEOF_VOID_P empty.
set(OLD_CMAKE_SIZEOF_VOID_P ${CMAKE_SIZEOF_VOID_P})
set(CMAKE_SIZEOF_VOID_P "")
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/range-v3-config-version.cmake
  VERSION ${RANGE_V3_VERSION}
  COMPATIBILITY ExactVersion
)
set(CMAKE_SIZEOF_VOID_P ${OLD_CMAKE_SIZEOF_VOID_P})

include(GNUInstallDirs)
install(TARGETS range-v3-concepts range-v3-meta range-v3 EXPORT range-v3-targets DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(EXPORT range-v3-targets FILE range-v3-targets.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/range-v3)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/range-v3-config-version.cmake
  cmake/range-v3-config.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/range-v3)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*")

export(EXPORT range-v3-targets FILE range-v3-config.cmake)
