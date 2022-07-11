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

import os
import pathlib
import click
import semver
import subprocess
import sys

CMAKE_REQUIRE_VERSION = "3.13.0"
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

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

@click.group()
def cli():
    pass

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('dir', default='build', metavar='BUILD_DIR')
@click.option('-j', '--jobs', default=4, show_default=True, metavar='N', help='execute N build jobs concurrently')
@click.option('--ghproxy', default=False, help='use https://ghproxy.com to fetch dependencies', is_flag=True)
@click.option('--ninja', default=False, help='use Ninja to build kvrocks', is_flag=True)
@click.option('--unittest', default=False, help='build unittest target', is_flag=True)
@click.option('-D', multiple=True, metavar='key=value', help='extra CMake definitions')
def build(dir, jobs, ghproxy, ninja, unittest, d):
    """
    BUILD_DIR: directory to store build files [default: build]
    """

    basedir = pathlib.Path(__file__).parent

    output = run(["command", "-v", "autoconf"], stdout=subprocess.PIPE)
    autoconf = output.read().decode().strip()
    run(["test", "-x", autoconf], msg="autoconf is required to build jemalloc")

    output = run(["command", "-v", "cmake"], stdout=subprocess.PIPE)
    cmake = output.read().decode().strip()
    run(["test", "-x", cmake], msg="cmake is required to build kvrocks")

    output = run([cmake, "-version"], stdout=subprocess.PIPE)
    output = run(["head", "-n", "1"], stdin=output, stdout=subprocess.PIPE)
    output = run(["sed", "s/[^0-9.]*//g"], stdin=output, stdout=subprocess.PIPE)
    cmake_version = output.read().decode().strip()
    if semver.compare(cmake_version, CMAKE_REQUIRE_VERSION) < 0:
        raise RuntimeError(f"CMake {CMAKE_REQUIRE_VERSION} or higher is required. Got: {cmake_version}")

    os.makedirs(dir, exist_ok=True)
    os.chdir(dir)

    cmake_options = ["-DCMAKE_BUILD_TYPE=RelWithDebInfo"]
    if ghproxy:
        cmake_options.append("-DDEPS_FETCH_PROXY=https://ghproxy.com/")
    if ninja:
        cmake_options.append("-G Ninja")
    if d:
        cmake_options += [f"-D{o}" for o in d]
    run([cmake, basedir, *cmake_options])

    target = ["kvrocks", "kvrocks2redis"]
    if unittest:
        target.append("unittest")
    run([cmake, "--build", ".", f"-j{jobs}", "-t", *target])
    click.echo(f'Building {dir} {jobs} {ghproxy} {ninja} {unittest}...\n{basedir}')

@click.command(context_settings=CONTEXT_SETTINGS)
def check():
    click.echo("Checking...")

cli.add_command(build)
cli.add_command(check)

if __name__ == '__main__':
    cli()
