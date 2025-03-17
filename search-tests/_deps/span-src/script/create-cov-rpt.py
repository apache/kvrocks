#!/usr/bin/env python
#
# Copyright 2019-2019 by Martin Moene
#
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
#
# script/create-cov-rpt.py, Python 3.4 and later
#

import argparse
import os
import re
import sys
import subprocess

# Configuration:

cfg_github_project   = 'span-lite'
cfg_github_user      = 'martinmoene'
cfg_prj_folder_level = 3

tpl_coverage_cmd     = 'opencppcoverage --no_aggregate_by_file --sources {src} -- {exe}'

# End configuration.

def project_folder( f, args ):
    """Project root"""
    if args.prj_folder:
        return args.prj_folder
    return os.path.normpath( os.path.join( os.path.dirname( os.path.abspath(f) ), '../' * args.prj_folder_level ) )

def executable_folder( f ):
    """Folder where the xecutable is"""
    return  os.path.dirname( os.path.abspath(f) )

def executable_name( f ):
    """Folder where the executable is"""
    return  os.path.basename( f )

def createCoverageReport( f, args ):
    print( "Creating coverage report for project '{usr}/{prj}', '{file}':".
        format( usr=args.user, prj=args.project, file=f ) )
    cmd = tpl_coverage_cmd.format( folder=executable_folder(f), src=project_folder(f, args), exe=executable_name(f) )
    if args.verbose:
        print( "> {}".format(cmd) )
    if not args.dry_run:
        os.chdir( executable_folder(f) )
        subprocess.call( cmd, shell=False )
        os.chdir( project_folder(f, args) )

def createCoverageReports( args ):
    for f in args.executable:
        createCoverageReport( f, args )

def createCoverageReportFromCommandLine():
    """Collect arguments from the commandline and create coverage report."""
    parser = argparse.ArgumentParser(
        description='Create coverage report.',
        epilog="""""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        'executable',
        metavar='executable',
        type=str,
        nargs=1,
        help='executable to report on')

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
        '--prj-folder',
        metavar='f',
        type=str,
        default=None,
        help='project root folder')

    parser.add_argument(
        '--prj-folder-level',
        metavar='n',
        type=int,
        default=cfg_prj_folder_level,
        help='project root folder level from executable')

    createCoverageReports( parser.parse_args() )

if __name__ == '__main__':
    createCoverageReportFromCommandLine()

# end of file
