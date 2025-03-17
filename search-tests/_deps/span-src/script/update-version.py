#!/usr/bin/env python
#
# Copyright 2017-2019 by Martin Moene
#
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
#
# script/update-version.py
#

from __future__ import print_function

import argparse
import os
import re
import sys

# Configuration:

table = (
    # path, substitute find, substitute format
    ( 'CMakeLists.txt'
        , r'\W{2,4}VERSION\W+([0-9]+\.[0-9]+\.[0-9]+)\W*$'
        , '    VERSION {major}.{minor}.{patch}' )

    , ( 'CMakeLists.txt'
        , r'set\W+span_lite_version\W+"([0-9]+\.[0-9]+\.[0-9]+)"\W+$'
        , 'set( span_lite_version "{major}.{minor}.{patch}" )\n' )

#    , ( 'example/cmake-pkg/CMakeLists.txt'
#        , r'set\W+span_lite_version\W+"([0-9]+\.[0-9]+(\.[0-9]+)?)"\W+$'
#        , 'set( span_lite_version "{major}.{minor}" )\n' )
#
#    , ( 'script/install-xxx-pkg.py'
#        , r'\span_lite_version\s+=\s+"([0-9]+\.[0-9]+\.[0-9]+)"\s*$'
#        , 'span_lite_version = "{major}.{minor}.{patch}"\n' )

    , ( 'conanfile.py'
        , r'version\s+=\s+"([0-9]+\.[0-9]+\.[0-9]+)"\s*$'
        , 'version = "{major}.{minor}.{patch}"' )

    , ( 'include/nonstd/span.hpp'
        , r'\#define\s+span_lite_MAJOR\s+[0-9]+\s*$'
        , '#define span_lite_MAJOR  {major}' )

    , ( 'include/nonstd/span.hpp'
        , r'\#define\s+span_lite_MINOR\s+[0-9]+\s*$'
        , '#define span_lite_MINOR  {minor}' )

    , ( 'include/nonstd/span.hpp'
        , r'\#define\s+span_lite_PATCH\s+[0-9]+\s*$'
        , '#define span_lite_PATCH  {patch}\n' )
)

# End configuration.

def readFile( in_path ):
    """Return content of file at given path"""
    with open( in_path, 'r' ) as in_file:
        contents = in_file.read()
    return contents

def writeFile( out_path, contents ):
    """Write contents to file at given path"""
    with open( out_path, 'w' ) as out_file:
        out_file.write( contents )

def replaceFile( output_path, input_path ):
    # prevent race-condition (Python 3.3):
    if sys.version_info >= (3, 3):
        os.replace( output_path, input_path )
    else:
        os.remove( input_path )
        os.rename( output_path, input_path )

def editFileToVersion( version, info, verbose ):
    """Update version given file path, version regexp and new version format in info"""
    major, minor, patch = version.split('.')
    in_path, ver_re, ver_fmt = info
    out_path = in_path + '.tmp'
    new_text = ver_fmt.format( major=major, minor=minor, patch=patch )

    if verbose:
        print( "- {path} => '{text}':".format( path=in_path, text=new_text.strip('\n') ) )

    writeFile(
        out_path,
        re.sub(
            ver_re, new_text, readFile( in_path )
            , count=0, flags=re.MULTILINE
        )
    )
    replaceFile( out_path, in_path )

def editFilesToVersion( version, table, verbose ):
    if verbose:
        print( "Editing files to version {v}:".format(v=version) )
    for item in table:
        editFileToVersion( version, item, verbose )

def editFilesToVersionFromCommandLine():
    """Update version number given on command line in paths from configuration table."""

    parser = argparse.ArgumentParser(
        description='Update version number in files.',
        epilog="""""",
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        'version',
        metavar='version',
        type=str,
        nargs=1,
        help='new version number, like 1.2.3')

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='report the name of the file being processed')

    args = parser.parse_args()

    editFilesToVersion( args.version[0], table, args.verbose )


if __name__ == '__main__':
    editFilesToVersionFromCommandLine()

# end of file
