cmake_minimum_required(VERSION 3.5)

project(jsoncons CXX)

set(JSONCONS_PROJECT_DIR ${PROJECT_SOURCE_DIR})
set(JSONCONS_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Versioning
# ==========

file(STRINGS "${JSONCONS_INCLUDE_DIR}/jsoncons/config/version.hpp" jsoncons_version_defines
     REGEX "#define JSONCONS_VERSION_(MAJOR|MINOR|PATCH)")
foreach(ver ${jsoncons_version_defines})
    if(ver MATCHES "#define JSONCONS_VERSION_(MAJOR|MINOR|PATCH) +([^ ]+)$")
        set(JSONCONS_VERSION_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" CACHE INTERNAL "")
    endif()
endforeach()
set(${PROJECT_NAME}_VERSION
    ${JSONCONS_VERSION_MAJOR}.${JSONCONS_VERSION_MINOR}.${JSONCONS_VERSION_PATCH})
message(STATUS "jsoncons v${${PROJECT_NAME}_VERSION}")

# Build
# =====

add_library(jsoncons INTERFACE)
target_include_directories(jsoncons INTERFACE $<BUILD_INTERFACE:${JSONCONS_INCLUDE_DIR}>
                                           $<INSTALL_INTERFACE:include>)

OPTION(JSONCONS_BUILD_TESTS "jsoncons test suite" ON)

if(JSONCONS_BUILD_TESTS)
    include(CTest)
    enable_testing()
    add_subdirectory(test)
endif()

# Installation
# ============

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS jsoncons
        EXPORT ${PROJECT_NAME}-targets)

# Makes the project importable from the build directory
export(EXPORT ${PROJECT_NAME}-targets
       FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Targets.cmake")

install(DIRECTORY ${JSONCONS_INCLUDE_DIR}/jsoncons
                  ${JSONCONS_INCLUDE_DIR}/jsoncons_ext
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# GNUInstallDirs "DATADIR" wrong here; CMake search path wants "share".
set(JSONCONS_CMAKECONFIG_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}" CACHE STRING "install path for jsonconsConfig.cmake")

configure_package_config_file(cmake/Config.cmake
                              "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
                              INSTALL_DESTINATION ${JSONCONS_CMAKECONFIG_INSTALL_DIR})

# jsoncons is header-only and does not depend on the architecture.

if (${CMAKE_VERSION} VERSION_LESS "3.14.0")
   write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
                                    VERSION ${${PROJECT_NAME}_VERSION}
                                    COMPATIBILITY AnyNewerVersion)
else ()
   write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
                                    VERSION ${${PROJECT_NAME}_VERSION}
                                    COMPATIBILITY AnyNewerVersion
                                    ARCH_INDEPENDENT)
endif ()

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION ${JSONCONS_CMAKECONFIG_INSTALL_DIR})
install(EXPORT ${PROJECT_NAME}-targets
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION ${JSONCONS_CMAKECONFIG_INSTALL_DIR})

