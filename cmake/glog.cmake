# glog depends on gflags
include("cmake/gflags.cmake")

if (NOT __GLOG_INCLUDED)
    set(__GLOG_INCLUDED TRUE)
    # build directory
    set(glog_PREFIX ${CMAKE_BINARY_DIR}/external/glog-prefix)
    # install directory
    set(glog_INSTALL ${CMAKE_BINARY_DIR}/external/glog-install)

    if (UNIX)
        set(GLOG_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(GLOG_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GLOG_EXTRA_COMPILER_FLAGS}")
    set(GLOG_C_FLAGS "${CMAKE_C_FLAGS} ${GLOG_EXTRA_COMPILER_FLAGS}")

    ExternalProject_Add(glog
        DEPENDS gflags
        PREFIX ${glog_PREFIX}
        #GIT_REPOSITORY "https://github.com/google/glog"
        #GIT_TAG "v0.3.5"
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/external/glog
        INSTALL_DIR ${glog_INSTALL}
        CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
                   -DCMAKE_INSTALL_PREFIX=${glog_INSTALL}
                   -DBUILD_SHARED_LIBS=OFF
                   -DBUILD_STATIC_LIBS=ON
                   -DBUILD_PACKAGING=OFF
                   -DBUILD_TESTING=OFF
                   -DBUILD_NC_TESTS=OFF
                   -DBUILD_CONFIG_TESTS=OFF
                   -DINSTALL_HEADERS=ON
                   -DCMAKE_C_FLAGS=${GLOG_C_FLAGS}
                   -DCMAKE_CXX_FLAGS=${GLOG_CXX_FLAGS}
                   -DCMAKE_PREFIX_PATH=${gflags_INSTALL}
        LOG_DOWNLOAD 1
        LOG_CONFIGURE 1
        LOG_INSTALL 1
        )

    set(glog_FOUND TRUE)
    set(glog_INCLUDE_DIRS ${glog_INSTALL}/include)
    set(glog_LIBRARIES ${glog_INSTALL}/glog/libglog.a)
endif()

