# Range v3 library
#
#  Copyright Luis Martinez de Bartolome Izquierdo 2016
#
#  Use, modification and distribution is subject to the
#  Boost Software License, Version 1.0. (See accompanying
#  file LICENSE_1_0.txt or copy at
#  http://www.boost.org/LICENSE_1_0.txt)
#
# Project home: https://github.com/ericniebler/range-v3
#

PROJECT(PackageTest)
cmake_minimum_required(VERSION 3.1.0 FATAL_ERROR)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

add_executable(range-v3-example example.cpp)

target_link_libraries(range-v3-example ${CONAN_LIBS})

set_property(TARGET range-v3-example PROPERTY CXX_STANDARD 17)
