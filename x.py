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

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, REMAINDER
from glob import glob
from os import makedirs
import os
from pathlib import Path
import re
from subprocess import Popen, PIPE
import sys
from typing import List, Any, Optional, TextIO, Tuple
from shutil import copyfile

CMAKE_REQUIRE_VERSION = (3, 13, 0)
TCL_REQUIRE_VERSION = (8, 5, 0)

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

def run(*args: str, msg: Optional[str]=None, verbose: bool=False, **kwargs: Any) -> Popen:
    sys.stdout.flush()
    if verbose:
        print(f"$ {' '.join(args)}")

    p = Popen(args, **kwargs)
    code = p.wait()
    if code != 0:
        err = f"\nfailed to run: {args}\nexit with code: {code}\n"
        if msg:
            err += f"error message: {msg}\n"
        raise RuntimeError(err)

    return p

def run_pipe(*args: str, msg: Optional[str]=None, verbose: bool=False, **kwargs: Any) -> TextIO:
    p = run(*args, msg=msg, verbose=verbose, stdout=PIPE, universal_newlines=True, **kwargs)
    return p.stdout # type: ignore

def find_command(command: str, msg: Optional[str]=None) -> str:
    return run_pipe("which", command, msg=msg).read().strip()

def check_version(current: str, required: Tuple[int, int, int], prog_name: Optional[str] = None) -> None:
    require_version = '.'.join(map(str, required))
    semver_match = SEMVER_REGEX.match(current)
    if semver_match is None:
        raise RuntimeError(f"{prog_name} {require_version} or higher is required, got: {current}")
    semver_dict = semver_match.groupdict()
    semver = (int(semver_dict["major"]), int(semver_dict["minor"]), int(semver_dict["patch"]))
    if semver < required:
        raise RuntimeError(f"{prog_name} {require_version} or higher is required, got: {current}")

def build(dir: str, jobs: int, ghproxy: bool, ninja: bool, unittest: bool, compiler: str, cmake_path: str, D: List[str]) -> None:
    basedir = Path(__file__).parent.absolute()

    find_command("autoconf", msg="autoconf is required to build jemalloc")
    cmake = find_command(cmake_path, msg="CMake is required")

    output = run_pipe(cmake, "-version")
    output = run_pipe("head", "-n", "1", stdin=output)
    output = run_pipe("sed", "s/[^0-9.]*//g", stdin=output)
    cmake_version = output.read().strip()
    check_version(cmake_version, CMAKE_REQUIRE_VERSION, "CMake")

    makedirs(dir, exist_ok=True)

    cmake_options = ["-DCMAKE_BUILD_TYPE=RelWithDebInfo"]
    if ghproxy:
        cmake_options.append("-DDEPS_FETCH_PROXY=https://ghproxy.com/")
    if ninja:
        cmake_options.append("-G Ninja")
    if compiler == 'gcc':
        cmake_options += ["-DCMAKE_C_COMPILER=gcc", "-DCMAKE_CXX_COMPILER=g++"]
    elif compiler == 'clang':
        cmake_options += ["-DCMAKE_C_COMPILER=clang", "-DCMAKE_CXX_COMPILER=clang++"]
    if D:
        cmake_options += [f"-D{o}" for o in D]
    run(cmake, str(basedir), *cmake_options, verbose=True, cwd=dir)

    target = ["kvrocks", "kvrocks2redis"]
    if unittest:
        target.append("unittest")
    run(cmake, "--build", ".", f"-j{jobs}", "-t", *target, verbose=True, cwd=dir)

def cpplint() -> None:
    command = find_command("cpplint", msg="cpplint is required")
    options = ["--linelength=120", "--filter=-build/include_subdir,-legal/copyright,-build/c++11"]
    sources = [*glob("src/*.h"), *glob("src/*.cc")]
    run(command, *options, *sources, verbose=True)

def cppcheck() -> None:
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

    run(command, *options, *sources, verbose=True)

def golangci_lint() -> None:
    go = find_command('go', msg='go is required for testing')
    gopath = run_pipe(go, 'env', 'GOPATH').read().strip()
    bindir = Path(gopath).absolute() / 'bin'
    binpath = bindir / 'golangci-lint'
    if not binpath.exists():
        output = run_pipe('curl', '-sfL', 'https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh', verbose=True)
        run('sh', '-s', '--', '-b', str(bindir), 'v1.49.0', verbose=True, stdin=output)
    basedir = Path(__file__).parent.absolute() / 'tests' / 'gocase'
    run(str(binpath), 'run', '-v', './...', cwd=str(basedir), verbose=True)

def write_version(release_version: str) -> str:
    version = release_version.strip()
    if SEMVER_REGEX.match(version) is None:
        raise RuntimeError(f"Kvrocks version should follow semver spec, got: {version}")

    with open('VERSION', 'w+') as f:
        f.write(version)

    return version

def package_source(release_version: str) -> None:
    # 0. Write input version to VERSION file
    version = write_version(release_version)

    # 1. Git commit and tag
    git = find_command('git', msg='git is required for source packaging')
    run(git, 'commit', '-a', '-m', f'[source-release] prepare release apache-kvrocks-{version}')
    run(git, 'tag', '-a', f'v{version}', '-m', f'[source-release] copy for tag v{version}')

    tarball = f'apache-kvrocks-{version}-incubating-src.tar.gz'
    # 2. Create the source tarball
    output = run_pipe(git, 'ls-files')
    run('xargs', 'tar', '-czf', tarball, stdin=output)

    # 3. GPG Sign
    gpg = find_command('gpg', msg='gpg is required for source packaging')
    run(gpg, '--detach-sign', '--armor', tarball)

    # 4. Generate sha512 checksum
    sha512sum = find_command('sha512sum', msg='sha512sum is required for source packaging')
    output = run_pipe(sha512sum, tarball)
    payload = output.read().strip()
    with open(f'{tarball}.sha512', 'w+') as f:
        f.write(payload)

def package_fpm(package_type: str, release_version: str, dir: str, jobs: int) -> None:
    fpm = find_command('fpm', msg=f'fpm is required for {package_type} packaging')

    version = write_version(release_version)

    build(dir=dir, jobs=jobs, ghproxy=False, ninja=False, unittest=False, compiler='auto', cmake_path='cmake', D=[])

    package_dir = Path(dir) / 'package-fpm'
    makedirs(str(package_dir), exist_ok=False)
    makedirs(str(package_dir / 'bin'))
    makedirs(str(package_dir / 'conf'))

    basedir = Path(__file__).parent.absolute()

    copyfile(str(Path(dir) / 'kvrocks'), str(package_dir / 'bin' / 'kvrocks'))
    copyfile(str(Path(dir) / 'kvrocks2redis'), str(package_dir / 'bin' / 'kvrocks2redis'))
    copyfile(str(basedir / 'kvrocks.conf'), str(package_dir / 'conf' / 'kvrocks.conf'))

    fpm_opts = [
        '-t', package_type,
        '-v', version,
        '-C', str(package_dir),
        '-s', 'dir',
        '--prefix', '/www/kvrocks',
        '-n', 'kvrocks',
        '--epoch', '7',
        '--config-files', '/www/kvrocks/conf/kvrocks.conf',
        '--iteration', 'release',
        '--verbose',
        '--category', 'kvrocks/projects',
        '--description', 'kvrocks',
        '--url', 'https://github.com/apache/incubator-kvrocks',
        '--license', 'Apache-2.0'
    ]

    run(fpm, *fpm_opts, verbose=True)

def test_tcl(dir: str, rest: List[str]) -> None:
    tclsh = find_command('tclsh', msg='TCL is required for testing')

    output = run_pipe('echo', 'puts [info patchlevel];exit 0')
    output = run_pipe(tclsh, stdin=output)
    tcl_version = output.read().strip()
    check_version(tcl_version, TCL_REQUIRE_VERSION, "tclsh")

    tcldir = Path(__file__).parent.absolute() / 'tests' / 'tcl'
    run(tclsh, 'tests/test_helper.tcl', '--server-path', str(Path(dir).absolute() / 'kvrocks'), *rest,
        cwd=str(tcldir), verbose=True
    )

def test_go(dir: str, rest: List[str]) -> None:
    go = find_command('go', msg='go is required for testing')

    binpath = Path(dir).absolute() / 'kvrocks'
    basedir = Path(__file__).parent.absolute() / 'tests' / 'gocase'
    worksapce = basedir / 'workspace'
    goenv = {
        'KVROCKS_BIN_PATH': str(binpath),
        'GO_CASE_WORKSPACE': str(worksapce),
    }
    goenv = {**os.environ, **goenv}
    run(go, 'test', '-v', '-bench=.', './...', *rest,
        env=goenv, cwd=str(basedir), verbose=True
    )

if __name__ == '__main__':
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.set_defaults(func=parser.print_help)

    subparsers = parser.add_subparsers()

    parser_check = subparsers.add_parser(
        'check',
        description="Check or lint source code",
        help="Check or lint source code")
    parser_check.set_defaults(func=parser_check.print_help)
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
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser_check_cppcheck.set_defaults(func=cppcheck)
    parser_check_golangci_lint = parser_check_subparsers.add_parser(
        'golangci-lint',
        description="Check code with golangci-lint (https://golangci-lint.run/)",
        help="Check code with golangci-lint (https://golangci-lint.run/)",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser_check_golangci_lint.set_defaults(func=golangci_lint)

    parser_build = subparsers.add_parser(
        'build',
        description="Build executables to BUILD_DIR [default: build]",
        help="Build executables to BUILD_DIR [default: build]",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser_build.add_argument('dir', metavar='BUILD_DIR', nargs='?', default='build', help="directory to store cmake-generated and build files")
    parser_build.add_argument('-j', '--jobs', default=4, metavar='N', help='execute N build jobs concurrently')
    parser_build.add_argument('--ghproxy', default=False, action='store_true', help='use https://ghproxy.com to fetch dependencies')
    parser_build.add_argument('--ninja', default=False, action='store_true', help='use Ninja to build kvrocks')
    parser_build.add_argument('--unittest', default=False, action='store_true', help='build unittest target')
    parser_build.add_argument('--compiler', default='auto', choices=('auto', 'gcc', 'clang'), help="compiler used to build kvrocks")
    parser_build.add_argument('--cmake-path', default='cmake', help="path of cmake binary used to build kvrocks")
    parser_build.add_argument('-D', nargs='*', metavar='key=value', help='extra CMake definitions')
    parser_build.set_defaults(func=build)

    parser_package = subparsers.add_parser(
        'package',
        description="Package the source tarball or binary installer",
        help="Package the source tarball or binary installer",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser_package.set_defaults(func=parser_package.print_help)
    parser_package_subparsers = parser_package.add_subparsers()
    parser_package_source = parser_package_subparsers.add_parser(
        'source',
        description="Package the source tarball",
        help="Package the source tarball",
    )
    parser_package_source.add_argument('-v', '--release-version', required=True, metavar='VERSION', help='current releasing version')
    parser_package_source.set_defaults(func=package_source)
    parser_package_fpm = parser_package_subparsers.add_parser(
        'fpm',
        description="Package built binaries to an rpm/deb package",
        help="Package built binaries to an rpm/deb package",
    )
    parser_package_fpm.add_argument('-v', '--release-version', required=True, metavar='VERSION', help='current releasing version')
    parser_package_fpm.add_argument('-t', '--package-type', required=True, choices=('rpm', 'deb'), help='package type for fpm to build')
    parser_package_fpm.add_argument('dir', metavar='BUILD_DIR', help="directory to store cmake-generated and build files")
    parser_package_fpm.add_argument('-j', '--jobs', default=4, metavar='N', help='execute N build jobs concurrently')
    parser_package_fpm.set_defaults(func=package_fpm)

    parser_test = subparsers.add_parser(
        'test',
        description="Test against a specific kvrocks build",
        help="Test against a specific kvrocks build",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser_test.set_defaults(func=parser_test.print_help)
    parser_test_subparsers = parser_test.add_subparsers()
    parser_test_tcl = parser_test_subparsers.add_parser(
        'tcl',
        description="Test kvrocks via TCL scripts",
        help="Test kvrocks via TCL scripts",
    )
    parser_test_tcl.add_argument('dir', metavar='BUILD_DIR', nargs='?', default='build', help="directory including kvrocks build files")
    parser_test_tcl.add_argument('rest', nargs=REMAINDER, help="the rest of arguments to forward to TCL scripts")
    parser_test_tcl.set_defaults(func=test_tcl)

    parser_test_go = parser_test_subparsers.add_parser(
        'go',
        description="Test kvrocks via go test cases",
        help="Test kvrocks via go test cases",
    )
    parser_test_go.add_argument('dir', metavar='BUILD_DIR', nargs='?', default='build', help="directory including kvrocks build files")
    parser_test_go.add_argument('rest', nargs=REMAINDER, help="the rest of arguments to forward to go test")
    parser_test_go.set_defaults(func=test_go)

    args = parser.parse_args()

    arg_dict = dict(vars(args))
    del arg_dict['func']
    args.func(**arg_dict)
