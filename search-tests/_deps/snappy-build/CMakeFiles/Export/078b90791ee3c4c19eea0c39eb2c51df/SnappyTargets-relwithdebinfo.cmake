#----------------------------------------------------------------
# Generated CMake target import file for configuration "RelWithDebInfo".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "Snappy::snappy" for configuration "RelWithDebInfo"
set_property(TARGET Snappy::snappy APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(Snappy::snappy PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELWITHDEBINFO "CXX"
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libsnappy.a"
  )

list(APPEND _cmake_import_check_targets Snappy::snappy )
list(APPEND _cmake_import_check_files_for_Snappy::snappy "${_IMPORT_PREFIX}/lib/libsnappy.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
