#----------------------------------------------------------------
# Generated CMake target import file for configuration "RelWithDebInfo".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "RocksDB::zlibstatic" for configuration "RelWithDebInfo"
set_property(TARGET RocksDB::zlibstatic APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(RocksDB::zlibstatic PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELWITHDEBINFO "C"
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libz.a"
  )

list(APPEND _cmake_import_check_targets RocksDB::zlibstatic )
list(APPEND _cmake_import_check_files_for_RocksDB::zlibstatic "${_IMPORT_PREFIX}/lib/libz.a" )

# Import target "RocksDB::rocksdb" for configuration "RelWithDebInfo"
set_property(TARGET RocksDB::rocksdb APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(RocksDB::rocksdb PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELWITHDEBINFO "CXX"
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/librocksdb.a"
  )

list(APPEND _cmake_import_check_targets RocksDB::rocksdb )
list(APPEND _cmake_import_check_files_for_RocksDB::rocksdb "${_IMPORT_PREFIX}/lib/librocksdb.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
