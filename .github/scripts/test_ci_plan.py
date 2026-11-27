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

import json
import tempfile
import unittest

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
        jobs = {job["name"]: job for job in plan["build_matrix"]}
        self.assertEqual(len(jobs), 8)

        # Canonical baseline must use native Makefiles and system linker.
        canonical = jobs["Ubuntu 24 GCC"]
        self.assertEqual(canonical["with_ninja"], "")
        self.assertEqual(canonical["with_lld"], "")
        self.assertTrue(canonical["use_ccache"])

        # Combined ASan + UBSan job must pass comma-separated Go build tags.
        asan_ubsan = jobs["Ubuntu Clang ASan + UBSan"]
        self.assertIn("-DENABLE_ASAN=ON", asan_ubsan["with_sanitizer"])
        self.assertIn("-DENABLE_UBSAN=ON", asan_ubsan["with_sanitizer"])
        self.assertEqual(asan_ubsan["go_tags"], "-tags=ignore_when_asan,ignore_when_ubsan")

        # SonarCloud must keep standard build wrapper output without ccache launcher.
        sonar = jobs["SonarCloud with Coverage"]
        self.assertFalse(sonar["use_ccache"])
        self.assertEqual(sonar["with_ninja"], "")

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
        self.assertTrue(plan["has_compat_matrix"])
        self.assertEqual(plan["compat_matrix"], ["v2.3.0", "v2.15.0"])

    def test_ci_full_label_escalates_pr_to_tier2(self) -> None:
        plan = ci_plan.build_plan(
            event_name="pull_request",
            changed_files=["src/commands/cmd_server.cc"],
            labels=["enhancement", "ci/full"],
            base_ref="unstable",
        )
        self.assertEqual(plan["tier"], "t2")
        self.assertEqual(plan["tidy_mode"], "full")
        self.assertEqual(len(plan["build_matrix"]), 21)
        self.assertEqual(len(plan["container_matrix"]), 6)
        self.assertTrue(plan["run_docker"])
        self.assertEqual(len(plan["compat_matrix"]), 6)

    def test_release_branch_pr_escalates_to_tier2(self) -> None:
        plan = ci_plan.build_plan(
            event_name="pull_request",
            changed_files=["src/commands/cmd_server.cc"],
            base_ref="2.17",
        )
        self.assertEqual(plan["tier"], "t2")
        self.assertEqual(len(plan["container_matrix"]), 6)

    def test_merge_group_runs_canonical_tier2_without_sonarcloud(self) -> None:
        plan = ci_plan.build_plan(
            event_name="merge_group",
            changed_files=["src/commands/cmd_server.cc"],
            base_ref="unstable",
        )
        self.assertEqual(plan["tier"], "t2")
        names = [job["name"] for job in plan["build_matrix"]]
        self.assertNotIn("SonarCloud with Coverage", names)
        self.assertNotIn("Darwin Clang arm64 without Jemalloc", names)
        self.assertEqual(len(plan["build_matrix"]), 20)
        self.assertEqual(len(plan["container_matrix"]), 6)
        # Only the dedicated Ninja check job should pass --ninja in Tier 2.
        ninja_jobs = [job["name"] for job in plan["build_matrix"] if job["with_ninja"]]
        self.assertEqual(ninja_jobs, ["Ubuntu GCC Ninja"])

    def test_extract_event_metadata_and_github_outputs(self) -> None:
        mg_payload = {
            "merge_group": {
                "base_ref": "refs/heads/unstable",
                "base_sha": "1111111",
                "head_sha": "2222222",
            }
        }
        labels, base_ref, diff_range = ci_plan.extract_event_metadata("merge_group", mg_payload)
        self.assertEqual(labels, [])
        self.assertEqual(base_ref, "unstable")
        self.assertEqual(diff_range, ("1111111", "2222222"))

        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as out_f:
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
