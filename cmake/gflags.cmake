if (NOT __GFLAGS_INCLUDED) # guard against multiple includes
    set(__GFLAGS_INCLUDED TRUE)

    # gflags will use pthreads if it's available in the system, so we must link with it
    find_package(Threads)

    # build directory
    set(gflags_PREFIX ${CMAKE_BUILD_DIRECTORY}/external/gflags-prefix)
    # install directory
    set(gflags_INSTALL ${CMAKE_BUILD_DIRECTORY}/external/gflags-install)

    if (UNIX)
        set(GFLAGS_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(GFLAGS_CXX_FLAGS ${CMAKE_CXX_FLAGS} ${GFLAGS_EXTRA_COMPILER_FLAGS})
    set(GFLAGS_C_FLAGS ${CMAKE_C_FLAGS} ${GFLAGS_EXTRA_COMPILER_FLAGS})

    ExternalProject_Add(gflags
        PREFIX ${gflags_PREFIX}
        #GIT_REPOSITORY "https://github.com/gflags/gflags.git"
        #GIT_TAG "v2.2.1"
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/external/gflags
        INSTALL_DIR ${gflags_INSTALL}
        CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
                   -DCMAKE_INSTALL_PREFIX=${gflags_INSTALL}
                   -DBUILD_SHARED_LIBS=OFF
                   -DBUILD_STATIC_LIBS=ON
                   -DBUILD_PACKAGING=OFF
                   -DBUILD_TESTING=OFF
                   -DBUILD_NC_TESTS=OFF
                   -DBUILD_CONFIG_TESTS=OFF
                   -DINSTALL_HEADERS=ON
                   -DCMAKE_C_FLAGS=${GFLAGS_C_FLAGS}
                   -DCMAKE_CXX_FLAGS=${GFLAGS_CXX_FLAGS}
        LOG_DOWNLOAD 1
        LOG_INSTALL 1
        )

    set(gflags_FOUND TRUE)
    set(gflags_INCLUDE_DIRS ${gflags_INSTALL}/include)
    set(gflags_LIBRARIES ${gflags_INSTALL}/lib/libgflags.a ${CMAKE_THREAD_LIBS_INIT})
endif()
