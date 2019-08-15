if (NOT __GTEST_INCLUDED) # guard against multiple includes
    set(__GTEST_INCLUDED TRUE)

    # gtest will use pthreads if it's available in the system, so we must link with it
    find_package(Threads)

    # build directory
    set(gtest_PREFIX ${CMAKE_BUILD_DIRECTORY}/external/gtest-prefix)
    # install directory
    set(gtest_INSTALL ${CMAKE_BUILD_DIRECTORY}/external/gtest-install)

    if (UNIX)
        set(GTEST_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(GTEST_CXX_FLAGS ${CMAKE_CXX_FLAGS} ${GTEST_EXTRA_COMPILER_FLAGS})
    set(GTEST_C_FLAGS ${CMAKE_C_FLAGS} ${GTEST_EXTRA_COMPILER_FLAGS})

    ExternalProject_Add(gtest
        PREFIX ${gtest_PREFIX}
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/external/googletest
        INSTALL_DIR ${gtest_INSTALL}
        CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
                   -DCMAKE_INSTALL_PREFIX=${gtest_INSTALL}
                   -DCMAKE_C_FLAGS=${GTEST_C_FLAGS}
                   -DCMAKE_CXX_FLAGS=${GTEST_CXX_FLAGS}
                   -DBUILD_GMOCK=OFF
        LOG_DOWNLOAD 1
        LOG_INSTALL 1
        )

    set(gtest_FOUND TRUE)
    set(gtest_INCLUDE_DIRS ${gtest_INSTALL}/include)
    set(gtest_LIBRARIES ${gtest_INSTALL}/${CMAKE_INSTALL_LIBDIR}/libgtest.a ${CMAKE_THREAD_LIBS_INIT})
endif()
