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

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import unittest
from unittest.mock import patch

import go_test_shard
import x


class TestGoTestShard(unittest.TestCase):
    def test_shards_cover_all_packages_with_balanced_weights(self) -> None:
        packages = [*go_test_shard.PACKAGE_SECONDS, "./unit/new_feature"]
        shards = go_test_shard.partition_packages(packages, 2)
        self.assertCountEqual([package for shard in shards for package in shard], packages)
        self.assertTrue(set(shards[0]).isdisjoint(shards[1]))
        totals = [
            sum(go_test_shard.PACKAGE_SECONDS.get(package, go_test_shard.DEFAULT_SECONDS) for package in shard)
            for shard in shards
        ]
        self.assertLessEqual(abs(totals[0] - totals[1]), go_test_shard.DEFAULT_SECONDS)

    def test_duration_and_shard_ties_are_deterministic(self) -> None:
        packages = ["./unit/type/set", "./unit/new_feature", "./unit/disk", "./unit/another_feature"]
        expected = [["./unit/disk", "./unit/another_feature"], ["./unit/type/set", "./unit/new_feature"]]
        self.assertEqual(go_test_shard.partition_packages(packages, 2), expected)
        self.assertEqual(go_test_shard.partition_packages(packages[::-1], 2), expected)
        self.assertEqual(go_test_shard.partition_packages(packages[:1], 3), [[packages[0]], [], []])

    def test_cli_discovers_packages_and_prints_requested_shard(self) -> None:
        gocase_dir = Path("tests/gocase").resolve()
        directories = "\n".join(str(gocase_dir / path) for path in ("unit/geo", "unit/new_feature", "."))
        with (
            patch.object(go_test_shard.subprocess, "check_output", return_value=directories) as go_list,
            redirect_stdout(StringIO()) as output,
        ):
            self.assertEqual(
                go_test_shard.main(["--shard-index", "2", "--shard-total", "2", "--gocase-dir", str(gocase_dir)]),
                0,
            )
        go_list.assert_called_once_with(["go", "list", "-f", "{{.Dir}}", "./..."], cwd=gocase_dir, text=True)
        self.assertEqual(output.getvalue().splitlines(), ["./.", "./unit/new_feature"])

    def test_cli_rejects_out_of_range_shard_indices(self) -> None:
        for index, total in (("0", "2"), ("3", "2"), ("1", "0")):
            with self.subTest(index=index, total=total), redirect_stderr(StringIO()), self.assertRaises(SystemExit) as error:
                go_test_shard.main(["--shard-index", index, "--shard-total", total, "--gocase-dir", "tests/gocase"])
            self.assertEqual(error.exception.code, 2)

    def test_go_test_forwards_explicit_packages_before_binary_flags(self) -> None:
        flags = ["-p", "2", "-parallel", "1", "-run", "./subtest"]
        for packages in ([], ["./unit/geo", "./unit/type/hash"]):
            with self.subTest(packages=packages), patch.object(x, "find_command"), patch.object(x, "run") as run:
                x.test_go("build", "redis-cli", packages + flags)
                args = run.call_args.args[1:]
                expected_packages = packages or ["./..."]
                self.assertEqual(args[2:2 + len(expected_packages)], tuple(expected_packages))
                self.assertTrue(args[2 + len(expected_packages)].startswith("-binPath="))
                self.assertEqual(args[-len(flags):], tuple(flags))
