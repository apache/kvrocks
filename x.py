#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import glob
import os
import pathlib
import re
import subprocess
import sys

CMAKE_REQUIRE_VERSION = (3, 13, 0)
SEMVER_REGEX = re.compile(
    r"""
        ^
        (?P<major>0|[1-9]\d*)
        \.
        (?P<minor>0|[1-9]\d*)
        \.
        (?P<patch>0|[1-9]\d*)
        (?:-(?P<prerelease>
            (?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)
            (?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*
        ))?
        (?:\+(?P<build>
            [0-9a-zA-Z-]+
            (?:\.[0-9a-zA-Z-]+)*
        ))?
        $
    """,
    re.VERBOSE,
)

def run(args, msg=None, **kwargs):
    sys.stdout.flush()
    p = subprocess.Popen(args, **kwargs)
    code = p.wait()
    if code != 0:
        err = f"""
failed to run: {args}
exit with code: {code}
error message: {msg}
"""
        raise RuntimeError(err)
    else:
        return p.stdout

def find_command(command, msg=None):
    output = run(["bash", "-c", f"command -v {command}"], stdout=subprocess.PIPE)
    path = output.read().decode().strip()
    run(["test", "-x", path], msg=msg)
    return path

def build(args):
    (dir, jobs, ghproxy, ninja, unittest, compiler, d) = (args.dir, args.jobs, args.ghproxy, args.ninja, args.unittest, args.compiler, args.D)

    basedir = pathlib.Path(__file__).parent.absolute()

    find_command("autoconf", msg="autoconf is required to build jemalloc")
    cmake = find_command("cmake", msg="CMake is required")

    output = run([cmake, "-version"], stdout=subprocess.PIPE)
    output = run(["head", "-n", "1"], stdin=output, stdout=subprocess.PIPE)
    output = run(["sed", "s/[^0-9.]*//g"], stdin=output, stdout=subprocess.PIPE)
    cmake_version = output.read().decode().strip()
    cmake_require_version = '.'.join(map(str, CMAKE_REQUIRE_VERSION))
    cmake_semver = SEMVER_REGEX.match(cmake_version)
    if cmake_semver is None:
        raise RuntimeError(f"CMake {cmake_require_version} or higher is required, got: {cmake_version}")
    cmake_semver = cmake_semver.groupdict()
    cmake_semver = (int(cmake_semver["major"]), int(cmake_semver["minor"]), int(cmake_semver["patch"]))
    if cmake_semver < CMAKE_REQUIRE_VERSION:
        raise RuntimeError(f"CMake {cmake_require_version} or higher is required, got: {cmake_version}")

    os.makedirs(dir, exist_ok=True)
    os.chdir(dir)

    cmake_options = ["-DCMAKE_BUILD_TYPE=RelWithDebInfo"]
    if ghproxy:
        cmake_options.append("-DDEPS_FETCH_PROXY=https://ghproxy.com/")
    if ninja:
        cmake_options.append("-G Ninja")
    if compiler == 'gcc':
        cmake_options += ["-DCMAKE_C_COMPILER=gcc", "-DCMAKE_CXX_COMPILER=g++"]
    elif compiler == 'clang':
        cmake_options += ["-DCMAKE_C_COMPILER=clang", "-DCMAKE_CXX_COMPILER=clang++"]
    if d:
        cmake_options += [f"-D{o}" for o in d]
    run([cmake, basedir, *cmake_options])

    target = ["kvrocks", "kvrocks2redis"]
    if unittest:
        target.append("unittest")
    run([cmake, "--build", ".", f"-j{jobs}", "-t", *target])

def cpplint(args):
    command = find_command("cpplint", msg="cpplint is required")
    options = ["--linelength=120", "--filter=-build/include_subdir,-legal/copyright,-build/c++11"]
    sources = [*glob.glob("src/*.h"), *glob.glob("src/*.cc")]
    run([command, *options, *sources])

def cppcheck(args):
    command = find_command("cppcheck", msg="cppcheck is required")

    options = ["-x", "c++"]
    options.append("-U__GNUC__")
    options.append("--force")
    options.append("--std=c++11")
    # we should run cmake configuration to fetch deps if we want to enable missingInclude
    options.append("--enable=warning,portability,information")
    options.append("--error-exitcode=1")
    options.append("--inline-suppr")

    sources = ["src"]

    run([command, *options, *sources])

def package_source(args):
    version = args.release_version.strip()
    if SEMVER_REGEX.match(version) is None:
        raise RuntimeError(f"Kvrocks version should follow semver spec, got: {version}")
    
    # 0. Write input version to VERSION file
    with open('VERSION', 'w+') as f:
        f.write(version)

    # 1. Git commit and tag
    git = find_command('git', msg='git is required for source packaging')
    run([git, 'commit', '-a', '-m', f'[source-release] prepare release apache-kvrocks-{version}'], stdout=subprocess.PIPE)
    run([git, 'tag', '-a', f'v{version}', '-m', '[source-release] copy for tag v{version}'], stdout=subprocess.PIPE)

    tarball = f'apache-kvrocks-{version}-incubating-src.tar.gz'
    # 2. Create the source tarball
    output = run([git, 'ls-files'], stdout=subprocess.PIPE)
    run(['xargs', 'tar', '-czf', tarball], stdin=output, stdout=subprocess.PIPE)

    # 3. GPG Sign
    gpg = find_command('gpg', msg='gpg is required for source packaging')
    run([gpg, '--detach-sign', '--armor', tarball], stdout=subprocess.PIPE)

    # 4. Generate sha512 checksum
    sha512sum = find_command('sha512sum', msg='sha512sum is required for source packaging')
    output = run([sha512sum, tarball], stdout=subprocess.PIPE)
    payload = output.read().decode().strip()
    with open(f'{tarball}.sha512', 'w+') as f:
        f.write(payload)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.set_defaults(func=lambda _: parser.print_help())

    subparsers = parser.add_subparsers()

    parser_check = subparsers.add_parser(
        'check',
        description="Check code with cpplint or cppcheck",
        help="Check code with cpplint or cppcheck")
    parser_check.set_defaults(func=lambda _: parser_check.print_help())
    parser_check_subparsers = parser_check.add_subparsers()
    parser_check_cpplint = parser_check_subparsers.add_parser(
        'cpplint',
        description="Lint code with cpplint (https://github.com/cpplint/cpplint)",
        help="Lint code with cpplint (https://github.com/cpplint/cpplint)")
    parser_check_cpplint.set_defaults(func=cpplint)
    parser_check_cppcheck = parser_check_subparsers.add_parser(
        'cppcheck',
        description="Check code with cppcheck (https://github.com/danmar/cppcheck)",
        help="Check code with cppcheck (https://github.com/danmar/cppcheck)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_check_cppcheck.set_defaults(func=cppcheck)

    parser_build = subparsers.add_parser(
        'build',
        description="Build executables to BUILD_DIR [default: build]",
        help="Build executables to BUILD_DIR [default: build]",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_build.add_argument('dir', metavar='BUILD_DIR', nargs='?', default='build', help="directory to store cmake-generated and build files")
    parser_build.add_argument('-j', '--jobs', default=4, metavar='N', help='execute N build jobs concurrently')
    parser_build.add_argument('--ghproxy', default=False, action='store_true', help='use https://ghproxy.com to fetch dependencies')
    parser_build.add_argument('--ninja', default=False, action='store_true', help='use Ninja to build kvrocks')
    parser_build.add_argument('--unittest', default=False, action='store_true', help='build unittest target')
    parser_build.add_argument('--compiler', default='auto', choices=('auto', 'gcc', 'clang'), help="compiler used to build kvrocks")
    parser_build.add_argument('-D', nargs='*', metavar='key=value', help='extra CMake definitions')
    parser_build.set_defaults(func=build)

    parser_package = subparsers.add_parser(
        'package',
        description="Package the source tarball or binary installer",
        help="Package the source tarball or binary installer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_package.set_defaults(func=lambda _: parser_package.print_help())
    parser_package_subparsers = parser_package.add_subparsers()
    parser_package_source = parser_package_subparsers.add_parser(
        'source',
        description="Package the source tarball",
        help="Package the source tarball",
    )
    parser_package_source.add_argument('-v', '--release-version', required=True, metavar='VERSION', help='current releasing version')
    parser_package_source.set_defaults(func=package_source)

    args = parser.parse_args()
    args.func(args)
