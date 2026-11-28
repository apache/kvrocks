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

from contextlib import redirect_stdout
import http.client
from io import StringIO
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import ci_plan


class TestCIPlan(unittest.TestCase):
    def test_docs_only_skips_all_heavy_jobs(self) -> None:
        plan = ci_plan.build_plan(
            event_name="pull_request",
            changed_files=["README.md", "docs/design.md", ".asf.yaml", "NOTICE"],
            base_ref="unstable",
        )
        self.assertTrue(plan["docs_only"])
        self.assertEqual(plan["tier"], "skip")
        self.assertFalse(plan["run_lint"])
        self.assertFalse(plan["has_build_matrix"])
        self.assertEqual(plan["build_matrix"], [])
        self.assertFalse(plan["has_container_matrix"])
        self.assertFalse(plan["run_docker"])
        self.assertFalse(plan["has_compat_matrix"])

    def test_docs_plus_code_is_not_docs_only(self) -> None:
        plan = ci_plan.build_plan(
            event_name="pull_request",
            changed_files=["README.md", "src/commands/cmd_server.cc"],
            base_ref="unstable",
        )
        self.assertFalse(plan["docs_only"])
        self.assertEqual(plan["tier"], "t1")
        self.assertEqual(plan["tidy_mode"], "incremental")
        self.assertEqual(len(plan["build_matrix"]), 8)
        self.assertFalse(plan["has_container_matrix"])
        self.assertFalse(plan["run_docker"])
        self.assertFalse(plan["has_compat_matrix"])

    def test_tier1_orthogonal_coverage_invariants(self) -> None:
        plan = ci_plan.build_plan(
            event_name="pull_request",
            changed_files=["src/commands/cmd_string.cc"],
            base_ref="unstable",
        )
        matrix = plan["build_matrix"]
        self.assertEqual(len(matrix), 8)
        self.assertEqual(len({job["cache_key"] for job in matrix}), 6)
        jobs = {job["name"]: job for job in matrix}
        keys = {
            "Ubuntu 24 GCC": "ubuntu24-gcc",
            "Ubuntu 24 Clang with OpenSSL": "ubuntu24-clang-openssl",
            "Ubuntu 24 GCC (Old Encoding + Lua)": "ubuntu24-gcc-oldenc-lua",
            "Darwin Clang arm64 with OpenSSL (shard 1/2)": "macos15-clang-openssl",
            "Darwin Clang arm64 with OpenSSL (shard 2/2)": "macos15-clang-openssl",
            "Ubuntu ARM Clang": "ubuntu24-arm64-clang",
            "Ubuntu Clang ASan + UBSan (shard 1/2)": "ubuntu22-clang-asan-ubsan",
            "Ubuntu Clang ASan + UBSan (shard 2/2)": "ubuntu22-clang-asan-ubsan",
        }
        self.assertEqual({name: job["cache_key"] for name, job in jobs.items()}, keys)
        for job in matrix:
            with self.subTest(job=job["name"]):
                self.assertTrue(job["use_ccache"])
                self.assertNotIn("cache_fallback_key", job)
                self.assertEqual(job["with_ninja"], "" if job["name"] == "Ubuntu 24 GCC" else "--ninja")
                uses_lld = job["with_ninja"] and job["os"].startswith("ubuntu")
                self.assertEqual(job["with_lld"], "-DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld" if uses_lld else "")

        oldenc_lua = jobs["Ubuntu 24 GCC (Old Encoding + Lua)"]
        self.assertEqual(oldenc_lua["new_encoding"], "-DENABLE_NEW_ENCODING=FALSE")
        self.assertEqual(oldenc_lua["without_luajit"], "-DENABLE_LUAJIT=OFF")

        # Combined ASan + UBSan job must pass comma-separated Go build tags.
        asan_ubsan = jobs["Ubuntu Clang ASan + UBSan (shard 1/2)"]
        self.assertIn("-DENABLE_ASAN=ON", asan_ubsan["with_sanitizer"])
        self.assertIn("-DENABLE_UBSAN=ON", asan_ubsan["with_sanitizer"])
        self.assertEqual(asan_ubsan["go_tags"], "-tags=ignore_when_asan,ignore_when_ubsan")

    def test_tier1_shards_only_darwin_openssl_and_combined_sanitizers(self) -> None:
        jobs = ci_plan.tier1_build_matrix()
        sharded_keys = {"macos15-clang-openssl", "ubuntu22-clang-asan-ubsan"}
        for key in {job["cache_key"] for job in jobs}:
            with self.subTest(cache_key=key):
                shards = [job for job in jobs if job["cache_key"] == key]
                total = 2 if key in sharded_keys else 1
                self.assertEqual(len(shards), total)
                for index, job in enumerate(shards, start=1):
                    self.assertEqual(job["shard_total"], total)
                    self.assertEqual(job["shard_index"], index)
                    self.assertIs(job["run_cpp_test"], index == 1)
                if total == 2:
                    first, second = shards
                    self.assertEqual(dict(second, name=first["name"], shard_index=1, run_cpp_test=True), first)

    def test_tier1_escalates_on_cmake_and_storage_changes(self) -> None:
        plan = ci_plan.build_plan(
            event_name="pull_request",
            changed_files=["CMakeLists.txt", "src/storage/redis_metadata.cc"],
            base_ref="unstable",
        )
        self.assertEqual(plan["tier"], "t1")
        self.assertEqual(plan["tidy_mode"], "full")
        self.assertTrue(plan["run_docker"])
        self.assertTrue(plan["has_container_matrix"])
        self.assertEqual([c["name"] for c in plan["container_matrix"]], ["Alpine 3"])
        self.assertFalse(plan["container_matrix"][0]["use_ccache"])
        self.assertTrue(plan["has_compat_matrix"])
        self.assertEqual(plan["compat_matrix"], ["v2.3.0", "v2.15.0"])

    def test_compatibility_trigger_paths(self) -> None:
        for path in (
            "tests/gocase/go.mod",
            "tests/gocase/go.sum",
            ".github/scripts/ci_plan.py",
            "src/common/encoding.h",
            "src/common/encoding.cc",
            "src/search/search_encoding.h",
        ):
            with self.subTest(path=path):
                plan = ci_plan.build_plan(event_name="pull_request", changed_files=[path])
                self.assertTrue(plan["has_compat_matrix"])
                self.assertEqual(plan["compat_matrix"], ci_plan.SMOKE_COMPAT_VERSIONS)

    def test_ci_full_label_escalates_pr_to_tier2(self) -> None:
        plan = ci_plan.build_plan(
            event_name="pull_request",
            changed_files=["README.md"],
            labels=["enhancement", "ci/full"],
            base_ref="unstable",
        )
        self.assertFalse(plan["docs_only"])
        self.assertEqual(plan["tier"], "t2")
        self.assertEqual(plan["tidy_mode"], "full")
        self.assertEqual(len(plan["build_matrix"]), 20)
        self.assertEqual(len(plan["container_matrix"]), 6)
        self.assertTrue(plan["run_docker"])
        self.assertEqual(len(plan["compat_matrix"]), 6)

    def test_pr_label_sources_ignore_body_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            event = Path(tmp) / "event.json"
            for event_labels, live_labels, tier in (
                ([], None, "t1"),
                ([], ["ci/full"], "t2"),
                (["ci/full"], None, "t2"),
                (["ci/full"], [], "t1"),
            ):
                with self.subTest(event_labels=event_labels, live_labels=live_labels):
                    payload = {
                        "pull_request": {
                            "number": 42,
                            "body": "Documented the [ci/full] marker.",
                            "base": {"ref": "unstable"},
                            "labels": [{"name": name} for name in event_labels],
                        },
                    }
                    event.write_text(json.dumps(payload), encoding="utf-8")
                    with (
                        patch.object(ci_plan, "fetch_live_pr_labels", return_value=live_labels),
                        redirect_stdout(StringIO()) as out,
                    ):
                        ci_plan.main(
                            [
                                "--event-name", "pull_request",
                                "--event-path", str(event),
                                "--base-ref", "unstable",
                                "--changed-file", "src/server/server.cc",
                                "--github-output", "",
                                "--summary-file", "",
                            ]
                        )
                    self.assertEqual(json.loads(out.getvalue())["tier"], tier)

    def test_fetch_live_pr_labels_handles_http_exception(self) -> None:
        with patch("urllib.request.urlopen", side_effect=http.client.HTTPException):
            self.assertIsNone(ci_plan.fetch_live_pr_labels("apache/kvrocks", 42, "test-token"))

    def test_git_changed_files_reports_both_rename_paths_and_preserves_filenames(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            def git(*args: str) -> str:
                return subprocess.check_output(["git", "-c", "commit.gpgsign=false", *args], cwd=tmp, text=True).strip()

            git("init", "-q", "--template=")
            git("config", "user.name", "CI planner test")
            git("config", "user.email", "ci@example.org")
            git("config", "diff.renames", "true")
            (Path(tmp) / "CMakeLists.txt").write_text("project(kvrocks)\n", encoding="utf-8")
            git("add", "CMakeLists.txt")
            git("commit", "-qm", "Add build configuration")
            base = git("rev-parse", "HEAD")

            git("mv", "CMakeLists.txt", "CMakeLists.md")
            doc_path = " docs café\nnotes.md"
            (Path(tmp) / doc_path).touch()
            git("add", "--", doc_path)
            git("commit", "-qm", "Rename build configuration and add documentation")
            head = git("rev-parse", "HEAD")

            changed_files = ci_plan.git_changed_files(base, head, cwd=tmp)
            self.assertCountEqual(changed_files, ["CMakeLists.txt", "CMakeLists.md", doc_path])
            plan = ci_plan.build_plan(event_name="pull_request", changed_files=changed_files)
            self.assertFalse(plan["docs_only"])
            self.assertEqual(plan["tidy_mode"], "full")

    def test_obsolete_changes_config_is_not_docs_only(self) -> None:
        self.assertFalse(ci_plan.is_docs_only([".github/config/changes.yml"]))

    def test_host_disk_space_and_go_parallelism(self) -> None:
        for tier, jobs in (
            ("t1", ci_plan.tier1_build_matrix()),
            ("t2", ci_plan.tier2_build_matrix()),
        ):
            for job in jobs:
                with self.subTest(tier=tier, job=job["name"]):
                    if job["with_sanitizer"]:
                        self.assertEqual(job["go_parallel"], 1)
                        self.assertEqual(job["go_pkg_parallel"], 2)
                    else:
                        self.assertEqual(job["go_parallel"], 2)
                        self.assertEqual(job["go_pkg_parallel"], 3 if job["os"].startswith("macos") else 4)
                    if job["os"] == "ubuntu-22.04" or job["with_sanitizer"]:
                        self.assertTrue(job["maximize_disk"])

    def test_release_branch_pr_escalates_to_tier2(self) -> None:
        plan = ci_plan.build_plan(
            event_name="pull_request",
            changed_files=["src/commands/cmd_server.cc"],
            base_ref="2.17",
        )
        self.assertEqual(plan["tier"], "t2")
        self.assertEqual(len(plan["container_matrix"]), 6)

    def test_tier2_keeps_all_distro_containers_without_ccache(self) -> None:
        jobs = ci_plan.tier2_container_matrix()
        self.assertCountEqual(
            [job["image"] for job in jobs],
            ["opensuse/leap:15", "archlinux:base", "rockylinux:8", "rockylinux:9", "debian:12", "alpine:3.22"],
        )
        for job in jobs:
            with self.subTest(job=job["name"]):
                self.assertFalse(job["use_ccache"])

    def test_tier2_keeps_canonical_host_jobs_without_ccache(self) -> None:
        jobs = ci_plan.tier2_build_matrix()
        self.assertEqual(len(jobs), 20)
        self.assertEqual(len({job["cache_key"] for job in jobs}), 20)
        names = [job["name"] for job in jobs]
        self.assertIn("Ubuntu Clang TSan", names)
        self.assertNotIn("SonarCloud with Coverage", names)
        self.assertNotIn("Darwin Clang arm64 without Jemalloc", names)
        for job in jobs:
            with self.subTest(job=job["name"]):
                self.assertFalse(job["use_ccache"])
                self.assertNotIn("cache_fallback_key", job)
                self.assertEqual(job["shard_index"], 1)
                self.assertEqual(job["shard_total"], 1)
                self.assertIs(job["run_cpp_test"], True)
        # Only the dedicated Ninja check job should pass --ninja in Tier 2.
        ninja_jobs = [job["name"] for job in jobs if job["with_ninja"]]
        self.assertEqual(ninja_jobs, ["Ubuntu GCC Ninja"])

    def test_full_ci_events_use_canonical_tier2_matrices(self) -> None:
        for event, labels in (("merge_group", []), ("pull_request", ["ci/full"]), ("push", [])):
            with self.subTest(event=event):
                plan = ci_plan.build_plan(
                    event_name=event,
                    changed_files=["src/commands/cmd_server.cc"],
                    labels=labels,
                    base_ref="unstable",
                )
                self.assertEqual(plan["tier"], "t2")
                self.assertEqual(plan["build_matrix"], ci_plan.tier2_build_matrix())
                self.assertEqual(plan["container_matrix"], ci_plan.tier2_container_matrix())

    def test_emit_warmup_matrix_matches_tier1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "output"
            for env in ({}, {"GITHUB_OUTPUT": str(output)}):
                with (
                    self.subTest(env=env),
                    patch.dict("os.environ", env, clear=True),
                    redirect_stdout(StringIO()) as stdout,
                ):
                    self.assertEqual(ci_plan.main(["--emit-warmup-matrix"]), 0)
                    if env:
                        self.assertEqual(stdout.getvalue(), "")
                        result = output.read_text(encoding="utf-8")
                    else:
                        result = stdout.getvalue()
                    line, = result.splitlines()
                    key, value = line.split("=", 1)
                    self.assertEqual(key, "warmup_matrix")
                    jobs = json.loads(value)
                    self.assertEqual(len(jobs), 6)
                    self.assertEqual(len({job["cache_key"] for job in jobs}), 6)
                    self.assertEqual(jobs, [job for job in ci_plan.tier1_build_matrix() if job["shard_index"] == 1])

    def test_extract_event_metadata_and_github_outputs(self) -> None:
        mg_payload = {
            "merge_group": {
                "base_ref": "refs/heads/unstable",
                "base_sha": "1111111",
                "head_sha": "2222222",
            }
        }
        labels, base_ref, diff_range, pr_num = ci_plan.extract_event_metadata("merge_group", mg_payload)
        self.assertEqual(pr_num, 0)
        self.assertEqual(labels, [])
        self.assertEqual(base_ref, "unstable")
        self.assertEqual(diff_range, ("1111111", "2222222"))

        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as out_f, redirect_stdout(StringIO()):
            rc = ci_plan.main(
                [
                    "--event-name",
                    "pull_request",
                    "--base-ref",
                    "unstable",
                    "--changed-file",
                    "src/server/server.cc",
                    "--github-output",
                    out_f.name,
                    "--summary-file",
                    "",
                ]
            )
            self.assertEqual(rc, 0)
            out_f.seek(0)
            content = out_f.read()
            self.assertIn("tier=t1\n", content)
            for line in content.strip().splitlines():
                key, val = line.split("=", 1)
                if key in ("build_matrix", "container_matrix", "compat_matrix"):
                    parsed = json.loads(val)
                    self.assertIsInstance(parsed, list)


if __name__ == "__main__":
    unittest.main()
