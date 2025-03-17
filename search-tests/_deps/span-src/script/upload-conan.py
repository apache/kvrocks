#!/usr/bin/env python
#
# Copyright 2019-2019 by Martin Moene
#
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
#
# script/upload-conan.py
#

from __future__ import print_function

import argparse
import os
import re
import sys
import subprocess

# Configuration:

def_conan_project = 'span-lite'
def_conan_user    = 'nonstd-lite'
def_conan_channel = 'stable'
cfg_conanfile     = 'conanfile.py'

tpl_conan_create  = 'conan create . {usr}/{chn}'
tpl_conan_upload  = 'conan upload --remote {usr} {prj}/{ver}@{usr}/{chn}'

# End configuration.

def versionFrom( filename ):
    """Obtain version from  conanfile.py"""
    with open( filename ) as f:
        content = f.read()
        version = re.search(r'version\s=\s"(.*)"', content).group(1)
    return version

def createConanPackage( args ):
    """Create conan package and upload it."""
    cmd = tpl_conan_create.format(usr=args.user, chn=args.channel)
    if args.verbose:
        print( "> {}".format(cmd) )
    if not args.dry_run:
        subprocess.call( cmd, shell=False )

def uploadConanPackage( args ):
    """Create conan package and upload it."""
    cmd = tpl_conan_upload.format(prj=args.project, usr=args.user, chn=args.channel, ver=args.version)
    if args.verbose:
        print( "> {}".format(cmd) )
    if not args.dry_run:
        subprocess.call( cmd, shell=False )

def uploadToConan( args ):
    """Create conan package and upload it."""
    print( "Updating project '{prj}' to user '{usr}', channel '{chn}', version {ver}:".
        format(prj=args.project, usr=args.user, chn=args.channel, ver=args.version) )
    createConanPackage( args )
    uploadConanPackage( args )

def uploadToConanFromCommandLine():
    """Collect arguments from the commandline and create conan package and upload it."""

    parser = argparse.ArgumentParser(
        description='Create conan package and upload it to conan.',
        epilog="""""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-n', '--dry-run',
        action='store_true',
        help='do not execute conan commands')

    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='level of progress reporting')

    parser.add_argument(
        '--project',
        metavar='p',
        type=str,
        default=def_conan_project,
        help='conan project')

    parser.add_argument(
        '--user',
        metavar='u',
        type=str,
        default=def_conan_user,
        help='conan user')

    parser.add_argument(
        '--channel',
        metavar='c',
        type=str,
        default=def_conan_channel,
        help='conan channel')

    parser.add_argument(
        '--version',
        metavar='v',
        type=str,
        default=versionFrom( cfg_conanfile ),
        help='version number [from conanfile.py]')

    uploadToConan( parser.parse_args() )


if __name__ == '__main__':
    uploadToConanFromCommandLine()

# end of file
