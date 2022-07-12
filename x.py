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

import glob
import os
import pathlib
import re
import click
import subprocess
import sys

CMAKE_REQUIRE_VERSION = (3, 13, 0)
CONTEXT_SETTINGS = {
    "help_option_names": ['-h', '--help'],
}
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

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@click.command()
@click.argument('dir', default='build', metavar='BUILD_DIR')
@click.option('-j', '--jobs', default=4, show_default=True, metavar='N', help='execute N build jobs concurrently')
@click.option('--ghproxy', default=False, help='use https://ghproxy.com to fetch dependencies', is_flag=True)
@click.option('--ninja', default=False, help='use Ninja to build kvrocks', is_flag=True)
@click.option('--unittest', default=False, help='build unittest target', is_flag=True)
@click.option('--compiler', default='auto', show_default=True, type=click.Choice(('auto', 'gcc', 'clang')), help='compiler used to build kvrocks')
@click.option('-D', multiple=True, metavar='key=value', help='extra CMake definitions')
def build(dir, jobs, ghproxy, ninja, unittest, compiler, d):
    """
    Build executables to BUILD_DIR [default: build]
    """

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

@click.group()
def check():
    """
    Check code with cpplint or cppcheck
    """
    pass

@click.command()
def cpplint():
    """
    Lint code with cpplint (https://github.com/cpplint/cpplint)
    """
    cpplint = find_command("cpplint", msg="cpplint is required")
    options = ["--linelength=120", "--filter=-build/include_subdir,-legal/copyright,-build/c++11"]
    sources = [*glob.glob("src/*.h"), *glob.glob("src/*.cc")]
    run([cpplint, *options, *sources])

@click.command()
def cppcheck():
    """
    Check code with cppcheck (https://github.com/danmar/cppcheck)
    """
    cppcheck = find_command("cppcheck", msg="cppcheck is required")

    options = ["-x", "c++"]
    options.append("-U__GNUC__")
    options.append("--force")
    options.append("--std=c++11")
    # we should run cmake configuration to fetch deps if we want to enable missingInclude
    options.append("--enable=warning,portability,information")
    options.append("--error-exitcode=1")
    options.append("--inline-suppr")

    sources = ["src"]

    run([cppcheck, *options, *sources])

cli.add_command(build)
cli.add_command(check)
check.add_command(cpplint)
check.add_command(cppcheck)

if __name__ == '__main__':
    cli()
