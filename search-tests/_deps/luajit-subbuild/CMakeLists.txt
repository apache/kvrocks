# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.31.6)

# Reject any attempt to use a toolchain file. We must not use one because
# we could be downloading it here. If the CMAKE_TOOLCHAIN_FILE environment
# variable is set, the cache variable will have been initialized from it.
unset(CMAKE_TOOLCHAIN_FILE CACHE)
unset(ENV{CMAKE_TOOLCHAIN_FILE})

# We name the project and the target for the ExternalProject_Add() call
# to something that will highlight to the user what we are working on if
# something goes wrong and an error message is produced.

project(luajit-populate NONE)



include(ExternalProject)
ExternalProject_Add(luajit-populate
                     "UPDATE_DISCONNECTED" "False" "DOWNLOAD_EXTRACT_TIMESTAMP" "FALSE" "EXTERNALPROJECT_INTERNAL_ARGUMENT_SEPARATOR" "URL" "https://github.com/RocksLabs/LuaJIT/archive/c0a8e68325ec261a77bde1c8eabad398168ffe74.zip" "URL_HASH" "MD5=7ff3e5ca4ddec59be2c2f97c5ff881d0"
                    SOURCE_DIR          "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/luajit-src"
                    BINARY_DIR          "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/luajit-build"
                    CONFIGURE_COMMAND   ""
                    BUILD_COMMAND       ""
                    INSTALL_COMMAND     ""
                    TEST_COMMAND        ""
                    USES_TERMINAL_DOWNLOAD  YES
                    USES_TERMINAL_UPDATE    YES
                    USES_TERMINAL_PATCH     YES
)


