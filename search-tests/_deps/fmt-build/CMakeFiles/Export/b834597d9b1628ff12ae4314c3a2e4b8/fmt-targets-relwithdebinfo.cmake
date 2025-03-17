#----------------------------------------------------------------
# Generated CMake target import file for configuration "RelWithDebInfo".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "fmt::fmt" for configuration "RelWithDebInfo"
set_property(TARGET fmt::fmt APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(fmt::fmt PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELWITHDEBINFO "CXX"
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libfmt.a"
  )

list(APPEND _cmake_import_check_targets fmt::fmt )
list(APPEND _cmake_import_check_files_for_fmt::fmt "${_IMPORT_PREFIX}/lib/libfmt.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
