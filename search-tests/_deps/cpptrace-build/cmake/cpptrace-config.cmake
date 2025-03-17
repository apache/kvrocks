# Init @ variables before doing anything else


# Dependencies
if(OFF)
  include(CMakeFindDependencyMacro)
  # we don't go the Findzstd.cmake route on vcpkg
  if(OFF)
    find_dependency(zstd CONFIG REQUIRED)
  else()
    set(CMAKE_MODULE_PATH_OLD "${CMAKE_MODULE_PATH}")
    set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_LIST_DIR}")
    find_dependency(zstd)
    set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH_OLD}")
    unset(CMAKE_MODULE_PATH_OLD)
  endif()
  if(NOT OFF)
    find_dependency(libdwarf REQUIRED)
  endif()
endif()

# We cannot modify an existing IMPORT target
if(NOT TARGET cpptrace::cpptrace)

  # import targets
  include("${CMAKE_CURRENT_LIST_DIR}/cpptrace-targets.cmake")

endif()

if(TRUE)
  target_compile_definitions(cpptrace::cpptrace INTERFACE CPPTRACE_STATIC_DEFINE)
endif()
