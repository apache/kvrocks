#!/usr/bin/env python
#
# Copyright 2019-2019 by Martin Moene
#
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
#
# script/upload-conan.py, Python 3.4 and later
#

import argparse
import os
import re
import sys
import subprocess

# Configuration:

cfg_github_project = 'span-lite'
cfg_github_user    = 'martinmoene'
cfg_description    = '(unused)'

cfg_cmakelists     = 'CMakeLists.txt'
cfg_readme         = 'Readme.md'
cfg_license        = 'LICENSE.txt'
cfg_ref_prefix     = 'v'

cfg_sha512            = 'dadeda'
cfg_vcpkg_description = '(no description found)'
cfg_vcpkg_root        = os.environ['VCPKG_ROOT']

cfg_cmake_optpfx      = "SPAN_LITE"

# End configuration.

# vcpkg  control and port templates:

tpl_path_vcpkg_control  = '{vcpkg}/ports/{prj}/CONTROL'
tpl_path_vcpkg_portfile = '{vcpkg}/ports/{prj}/portfile.cmake'

tpl_vcpkg_control =\
"""Source: {prj}
Version: {ver}
Description: {desc}"""

tpl_vcpkg_portfile =\
"""include(vcpkg_common_functions)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO {usr}/{prj}
    REF {ref}
    SHA512 {sha}
)

vcpkg_configure_cmake(
    SOURCE_PATH ${{SOURCE_PATH}}
    PREFER_NINJA
    OPTIONS
        -D{optpfx}_OPT_BUILD_TESTS=OFF
        -D{optpfx}_OPT_BUILD_EXAMPLES=OFF
)

vcpkg_install_cmake()

vcpkg_fixup_cmake_targets(
    CONFIG_PATH lib/cmake/${{PORT}}
)

file(REMOVE_RECURSE
    ${{CURRENT_PACKAGES_DIR}}/debug
    ${{CURRENT_PACKAGES_DIR}}/lib
)

file(INSTALL
    ${{SOURCE_PATH}}/{lic} DESTINATION ${{CURRENT_PACKAGES_DIR}}/share/${{PORT}} RENAME copyright
)"""

tpl_vcpkg_note_sha =\
"""
Next actions:
- Obtain package SHA: 'vcpkg install {prj}', copy SHA mentioned in 'Actual hash: [...]'
- Add SHA to package: 'script\create-vcpkg --sha={sha}'
- Install package   : 'vcpkg install {prj}'"""

tpl_vcpkg_note_install =\
"""
Next actions:
- Install package: 'vcpkg install {prj}'"""

# End of vcpkg templates

def versionFrom( filename ):
    """Obtain version from CMakeLists.txt"""
    with open( filename, 'r' ) as f:
        content = f.read()
        version = re.search(r'VERSION\s(\d+\.\d+\.\d+)', content).group(1)
    return version

def descriptionFrom( filename ):
    """Obtain description from CMakeLists.txt"""
    with open( filename, 'r' ) as f:
        content = f.read()
        description = re.search(r'DESCRIPTION\s"(.*)"', content).group(1)
    return description if description else cfg_vcpkg_description

def vcpkgRootFrom( path ):
    return path if path else './vcpkg'

def to_ref( version ):
    """Add prefix to version/tag, like v1.2.3"""
    return cfg_ref_prefix + version

def control_path( args ):
    """Create path like vcpks/ports/_project_/CONTROL"""
    return tpl_path_vcpkg_control.format( vcpkg=args.vcpkg_root, prj=args.project )

def portfile_path( args ):
    """Create path like vcpks/ports/_project_/portfile.cmake"""
    return tpl_path_vcpkg_portfile.format( vcpkg=args.vcpkg_root, prj=args.project )

def createControl( args ):
    """Create vcpkg CONTROL file"""
    output = tpl_vcpkg_control.format(
        prj=args.project, ver=args.version, desc=args.description )
    if args.verbose:
        print( "Creating control file '{f}':".format( f=control_path( args ) ) )
    if args.verbose > 1:
         print( output )
    os.makedirs( os.path.dirname( control_path( args ) ), exist_ok=True )
    with open( control_path( args ), 'w') as f:
        print( output, file=f )

def createPortfile( args ):
    """Create vcpkg portfile"""
    output = tpl_vcpkg_portfile.format(
        optpfx=cfg_cmake_optpfx, usr=args.user, prj=args.project, ref=to_ref(args.version), sha=args.sha, lic=cfg_license )
    if args.verbose:
        print( "Creating portfile '{f}':".format( f=portfile_path( args ) ) )
    if args.verbose > 1:
        print( output )
    os.makedirs( os.path.dirname( portfile_path( args ) ), exist_ok=True )
    with open( portfile_path( args ), 'w') as f:
        print( output, file=f )

def printNotes( args ):
    if args.sha == cfg_sha512:
        print( tpl_vcpkg_note_sha.
            format( prj=args.project, sha='...' ) )
    else:
        print( tpl_vcpkg_note_install.
            format( prj=args.project ) )

def createVcpkg( args ):
    print( "Creating vcpkg for '{usr}/{prj}', version '{ver}' in folder '{vcpkg}':".
        format( usr=args.user, prj=args.project, ver=args.version, vcpkg=args.vcpkg_root, ) )
    createControl( args )
    createPortfile( args )
    printNotes( args )

def createVcpkgFromCommandLine():
    """Collect arguments from the commandline and create vcpkg."""

    parser = argparse.ArgumentParser(
        description='Create microsoft vcpkg.',
        epilog="""""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='level of progress reporting')

    parser.add_argument(
        '--user',
        metavar='u',
        type=str,
        default=cfg_github_user,
        help='github user name')

    parser.add_argument(
        '--project',
        metavar='p',
        type=str,
        default=cfg_github_project,
        help='github project name')

    parser.add_argument(
        '--description',
        metavar='d',
        type=str,
#       default=cfg_description,
        default=descriptionFrom( cfg_cmakelists ),
        help='vcpkg description [from ' + cfg_cmakelists + ']')

    parser.add_argument(
        '--version',
        metavar='v',
        type=str,
        default=versionFrom( cfg_cmakelists ),
        help='version number [from ' + cfg_cmakelists + ']')

    parser.add_argument(
        '--sha',
        metavar='s',
        type=str,
        default=cfg_sha512,
        help='sha of package')

    parser.add_argument(
        '--vcpkg-root',
        metavar='r',
        type=str,
        default=vcpkgRootFrom( cfg_vcpkg_root ),
        help='parent folder containing ports to write files to')

    createVcpkg( parser.parse_args() )

if __name__ == '__main__':
    createVcpkgFromCommandLine()

# end of file
