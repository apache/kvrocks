if (NOT __ROCKSDB_INCLUDED)
  set(__ROCKSDB_INCLUDED TRUE)

  find_package(Threads)
  set(CMAKE_INSTALL_LIBDIR lib)
  # build directory
  set(rocksdb_PREFIX ${CMAKE_BUILD_DIRECTORY}/external/rocksdb-prefix)
  # install directory
  set(rocksdb_INSTALL ${CMAKE_BUILD_DIRECTORY}/external/rocksdb-install)

  if (UNIX)
      set(ROCKSDB_EXTRA_COMPILER_FLAGS "-fPIC")
  endif()

  set(ROCKSDB_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ROCKSDB_EXTRA_COMPILER_FLAGS}")
  set(ROCKSDB_C_FLAGS "${CMAKE_C_FLAGS} ${ROCKSDB_EXTRA_COMPILER_FLAGS}")
  set(JEMALLOC_ROOT_DIR ${jemalloc_INSTALL})
  ExternalProject_Add(rocksdb
      DEPENDS jemalloc snappy
      PREFIX ${rocksdb_PREFIX}
      #GIT_REPOSITORY "https://github.com/facebook/rocksdb"
      #GIT_TAG "v5.15.10"
      SOURCE_DIR ${PROJECT_SOURCE_DIR}/external/rocksdb
      INSTALL_DIR ${rocksdb_INSTALL}
      CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
		 -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
                 -DCMAKE_INSTALL_PREFIX=${rocksdb_INSTALL}
                 -DBUILD_SHARED_LIBS=OFF
                 -DBUILD_STATIC_LIBS=ON
                 -DBUILD_PACKAGING=OFF
                 -DBUILD_TESTING=OFF
                 -DBUILD_NC_TESTS=OFF
                 -DBUILD_CONFIG_TESTS=OFF
                 -DINSTALL_HEADERS=ON
                 -DCMAKE_C_FLAGS=${ROCKSDB_C_FLAGS}
                 -DCMAKE_CXX_FLAGS=${ROCKSDB_CXX_FLAGS}
                 -DCMAKE_PREFIX_PATH=${snappy_INSTALL}
                 -DJEMALLOC_ROOT_DIR=${JEMALLOC_ROOT_DIR}
                 -DFAIL_ON_WARNINGS=OFF
                 -DWITH_TESTS=OFF
                 -DWITH_SNAPPY=ON
                 -DWITH_TOOLS=OFF
                 -DWITH_GFLAGS=OFF
                 -DUSE_RTTI=ON
                 -DWITH_JEMALLOC=ON
      LOG_DOWNLOAD 1
      LOG_CONFIGURE 1
      LOG_INSTALL 1
      )

  include(GNUInstallDirs)
  set(rocksdb_FOUND TRUE)
  set(rocksdb_INCLUDE_DIRS ${rocksdb_INSTALL}/include)
  set(rocksdb_LIBRARIES ${rocksdb_INSTALL}/${CMAKE_INSTALL_LIBDIR}/librocksdb.a)
endif()

