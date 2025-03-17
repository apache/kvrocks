# Copyright 2018-2021 by Martin Moene
#
# https://github.com/martinmoene/span-lite
#
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

if( NOT DEFINED CMAKE_MINIMUM_REQUIRED_VERSION )
    cmake_minimum_required( VERSION 3.8 FATAL_ERROR )
endif()

project( example LANGUAGES CXX )

# unit_name provided by toplevel CMakeLists.txt
set( PACKAGE   ${unit_name}-lite )
set( PROGRAM   ${unit_name}-lite )

message( STATUS "Subproject '${PROJECT_NAME}', examples '${PROGRAM}-*'")

# Target default options and definitions:

set( OPTIONS "" )
#set( DEFINITIONS "" )

# Sources (.cpp), normal and no-exception, and their base names:

set( SOURCES
    01-basic.cpp
    02-span.cpp
)

set( SOURCES_NE
)

string( REPLACE ".cpp" "" BASENAMES    "${SOURCES}" )
string( REPLACE ".cpp" "" BASENAMES_NE "${SOURCES_NE}" )

# Determine options:

if( MSVC )
    message( STATUS "Matched: MSVC")

    set( BASE_OPTIONS -W3 )
    set( EXCEPTIONS_OPTIONS ${BASE_OPTIONS} -EHsc )
    set( NO_EXCEPTIONS_OPTIONS ${BASE_OPTIONS} )

elseif( CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang" )
    message( STATUS "CompilerId: '${CMAKE_CXX_COMPILER_ID}'")

    set( BASE_OPTIONS -Wall -Wextra -Wconversion -Wsign-conversion -Wno-missing-braces -fno-elide-constructors )
    set( EXCEPTIONS_OPTIONS ${BASE_OPTIONS} )
    set( NO_EXCEPTIONS_OPTIONS -fno-exceptions )

elseif( CMAKE_CXX_COMPILER_ID MATCHES "Intel" )
    # as is
    message( STATUS "Matched: Intel")
else()
    # as is
    message( STATUS "Matched: nothing")
endif()

# Function to emulate ternary operation `result = b ? x : y`:

macro( ternary var boolean value1 value2 )
    if( ${boolean} )
        set( ${var} ${value1} )
    else()
        set( ${var} ${value2} )
    endif()
endmacro()

# Function to create a target:

function( make_target name no_exceptions )
    ternary( ne no_exceptions "-ne" "" )

    add_executable             ( ${PROGRAM}-${name}${ne} ${name}.cpp )
    target_include_directories ( ${PROGRAM}-${name}${ne} PRIVATE ../include )
    target_link_libraries      ( ${PROGRAM}-${name}${ne} PRIVATE ${PACKAGE} )
    if ( no_exceptions )
        target_compile_options ( ${PROGRAM}-${name}${ne} PRIVATE ${NO_EXCEPTIONS_OPTIONS} )
    else()
        target_compile_options ( ${PROGRAM}-${name}${ne} PRIVATE ${EXCEPTIONS_OPTIONS} )
    endif()

endfunction()

# Create targets:

foreach( target ${BASENAMES} )
    make_target( ${target} FALSE )
endforeach()

foreach( target ${BASENAMES_NE} )
    make_target( ${target} TRUE )
endforeach()

# end of file
