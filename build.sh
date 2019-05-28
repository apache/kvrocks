#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 BUILD_DIR" >&2
  exit 1
fi

BUILD_DIR=$1
WORKING_DIR=$(pwd)
CMAKE_INSTALL_DIR=$WORKING_DIR/$BUILD_DIR/cmake
CMAKE_REQUIRE_VERSION="3.13.2"

if [ -x "$(command -v cmake)" ]; then
    CMAKE_BIN=$(command -v cmake)
fi

if [ -x "$CMAKE_INSTALL_DIR/bin/cmake" ]; then
    CMAKE_BIN=$CMAKE_INSTALL_DIR/bin/cmake
fi

if [ -f "$CMAKE_BIN" ]; then
    CMAKE_VERSION=`$CMAKE_BIN -version | head -n 1 | sed 's/[^0-9.]*//g'`
else
    CMAKE_VERSION=0
fi

if [ "$(printf '%s\n' "$CMAKE_REQUIRE_VERSION" "$CMAKE_VERSION" | sort -V | head -n1)" != "$CMAKE_REQUIRE_VERSION" ]; then
    echo "CMake $CMAKE_REQUIRE_VERSION or higher is required. Trying to install CMake $CMAKE_REQUIRE_VERSION ..."
    mkdir -p $BUILD_DIR/cmake
    cd $BUILD_DIR
    curl -O -L https://github.com/Kitware/CMake/releases/download/v3.13.2/cmake-3.13.2.tar.gz
    tar -zxf cmake-3.13.2.tar.gz && cd cmake-3.13.2
    ./bootstrap --prefix=$CMAKE_INSTALL_DIR && make && make install && cd ../..
    CMAKE_BIN=$CMAKE_INSTALL_DIR/bin/cmake
fi

cd $BUILD_DIR && $CMAKE_BIN .. && make -j4