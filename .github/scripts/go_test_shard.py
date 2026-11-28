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
from pathlib import Path
import subprocess

PACKAGE_SECONDS = {
    "./unit/geo": 156,
    "./integration/slotmigrate": 134,
    "./unit/type/hash": 129,
    "./unit/wait": 114,
    "./unit/type/list": 72,
    "./integration/replication": 48,
    "./unit/disk": 38,
    "./unit/type/set": 38,
    "./unit/type/strings": 29,
    "./unit/type/stream": 27,
    "./unit/type/zset": 26,
    "./unit/expire": 19,
}
DEFAULT_SECONDS = 5


def list_packages(gocase_dir: Path) -> list[str]:
    gocase_dir = gocase_dir.resolve()
    directories = subprocess.check_output(["go", "list", "-f", "{{.Dir}}", "./..."], cwd=gocase_dir, text=True)
    return [f"./{Path(directory).relative_to(gocase_dir).as_posix()}" for directory in directories.splitlines()]


def partition_packages(packages: list[str], shard_total: int) -> list[list[str]]:
    weights = {package: PACKAGE_SECONDS.get(package, DEFAULT_SECONDS) for package in packages}
    shards: list[list[str]] = [[] for _ in range(shard_total)]
    totals = [0] * shard_total
    for package in sorted(weights, key=lambda path: (-weights[path], path)):
        index = min(range(shard_total), key=totals.__getitem__)
        shards[index].append(package)
        totals[index] += weights[package]
    return shards


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List the Go packages assigned to a test shard.")
    parser.add_argument("--shard-index", type=int, required=True, help="1-based shard index")
    parser.add_argument("--shard-total", type=int, required=True, help="number of shards")
    parser.add_argument("--gocase-dir", type=Path, required=True, help="Go test module directory")
    args = parser.parse_args(argv)
    if not 1 <= args.shard_index <= args.shard_total:
        parser.error("--shard-index must be between 1 and --shard-total")

    packages = list_packages(args.gocase_dir)
    shards = partition_packages(packages, args.shard_total)
    for package in shards[args.shard_index - 1]:
        print(package)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
