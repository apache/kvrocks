if (NOT __LIBEVENT_INCLUDED)
    set(__LIBEVENT_INCLUDED TRUE)
    # build directory
    set(libevent_PREFIX ${CMAKE_BUILD_DIRECTORY}/external/libevent-prefix)
    # install directory
    set(libevent_INSTALL ${CMAKE_BUILD_DIRECTORY}/external/libevent-install)

    if (UNIX)
        set(LIBEVENT_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(LIBEVENT_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LIBEVENT_EXTRA_COMPILER_FLAGS}")
    set(LIBEVENT_C_FLAGS "${CMAKE_C_FLAGS} ${LIBEVENT_EXTRA_COMPILER_FLAGS}")

    ExternalProject_Add(libevent
        PREFIX ${libevent_PREFIX}
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/external/libevent
        INSTALL_DIR ${libevent_INSTALL}
        CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                   -DCMAKE_INSTALL_PREFIX=${libevent_INSTALL}
                   -DCMAKE_C_FLAGS=${LIBEVENT_C_FLAGS}
                   -DCMAKE_CXX_FLAGS=${LIBEVENT_CXX_FLAGS}
                   -DEVENT__DISABLE_TESTS=ON
                   -DEVENT__DISABLE_REGRESS=ON
                   -DEVENT__DISABLE_SAMPLES=ON
                   -DEVENT__DISABLE_OPENSSL=ON
                   -DEVENT__LIBRARY_TYPE=STATIC
        LOG_DOWNLOAD 1
        LOG_CONFIGURE 1
        LOG_INSTALL 1
        )

    set(libevent_FOUND TRUE)
    set(libevent_INCLUDE_DIRS ${libevent_INSTALL}/include)
    set(libevent_LIBRARIES ${libevent_INSTALL}/lib/libevent.a ${libevent_INSTALL}/lib/libevent_pthreads.a)
endif()
