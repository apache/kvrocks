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

from typing import Any, Dict, List, Optional
import unittest

import nightly_report


class FakeRepoClient:
    def __init__(self, jobs: List[Dict[str, Any]], issues: Optional[List[Dict[str, Any]]] = None) -> None:
        self.jobs = jobs
        self.issues = list(issues or [])
        self.labels_ensured: List[str] = []
        self.comments: List[Dict[str, Any]] = []
        self.next_number = 4000

    def list_run_jobs(self, run_id: str) -> List[Dict[str, Any]]:
        return self.jobs

    def ensure_label(self, name: str, color: str, description: str) -> None:
        self.labels_ensured.append(name)

    def list_open_nightly_issues(self, label: str) -> List[Dict[str, Any]]:
        return [item for item in self.issues if item.get("state", "open") == "open"]

    def create_issue(self, title: str, body: str, labels: List[str]) -> Dict[str, Any]:
        issue = {"number": self.next_number, "title": title, "body": body, "labels": labels, "state": "open"}
        self.next_number += 1
        self.issues.append(issue)
        return issue

    def update_issue(
        self, number: int, *, title: Optional[str] = None, body: Optional[str] = None, state: Optional[str] = None
    ) -> Dict[str, Any]:
        for issue in self.issues:
            if issue["number"] == number:
                if title is not None:
                    issue["title"] = title
                if body is not None:
                    issue["body"] = body
                if state is not None:
                    issue["state"] = state
                return issue
        raise AssertionError(f"Issue #{number} not found")

    def create_comment(self, number: int, body: str) -> Dict[str, Any]:
        entry = {"number": number, "body": body}
        self.comments.append(entry)
        return entry


class TestNightlyReport(unittest.TestCase):
    def test_creates_issue_when_job_fails(self) -> None:
        client = FakeRepoClient(
            jobs=[
                {
                    "name": "Ubuntu GCC TSan",
                    "conclusion": "failure",
                    "html_url": "https://github.com/apache/kvrocks/actions/runs/1/job/10",
                    "steps": [
                        {"name": "Build Kvrocks", "conclusion": "success"},
                        {"name": "Run Go Integration Cases", "conclusion": "failure"},
                    ],
                },
                {
                    "name": "Pure Cold-Start Release Build",
                    "conclusion": "success",
                    "html_url": "https://github.com/apache/kvrocks/actions/runs/1/job/11",
                    "steps": [],
                },
                {
                    "name": "Report Nightly Status",
                    "conclusion": "unknown",
                    "html_url": "",
                    "steps": [],
                },
            ]
        )
        outcome = nightly_report.reconcile_nightly_status(
            client,
            repository="apache/kvrocks",
            run_id="12345",
            head_sha="abcdef1234567",
            self_job_name="Report Nightly Status",
            apply_changes=True,
        )
        self.assertEqual(outcome["action"], "created_issue")
        self.assertEqual(outcome["issue_number"], 4000)
        self.assertEqual(len(client.issues), 1)
        self.assertIn("Run Go Integration Cases", client.issues[0]["body"])
        self.assertIn(nightly_report.NIGHTLY_MARKER, client.issues[0]["body"])

    def test_updates_existing_issue_without_creating_duplicate(self) -> None:
        existing = {
            "number": 3999,
            "title": nightly_report.NIGHTLY_TITLE,
            "body": f"{nightly_report.NIGHTLY_MARKER}\nold body",
            "state": "open",
        }
        client = FakeRepoClient(
            jobs=[
                {
                    "name": "Ubuntu RISC-V GCC without luaJIT (QEMU)",
                    "conclusion": "timed_out",
                    "html_url": "https://github.com/apache/kvrocks/actions/runs/2/job/20",
                    "steps": [{"name": "Run Go Integration Cases", "conclusion": "timed_out"}],
                }
            ],
            issues=[existing],
        )
        outcome = nightly_report.reconcile_nightly_status(
            client,
            repository="apache/kvrocks",
            run_id="22222",
            head_sha="9876543210abc",
            self_job_name="Report Nightly Status",
            apply_changes=True,
        )
        self.assertEqual(outcome["action"], "updated_issue")
        self.assertEqual(outcome["issue_number"], 3999)
        self.assertEqual(len(client.issues), 1)
        self.assertEqual(len(client.comments), 1)

    def test_closes_open_issue_on_all_green_recovery(self) -> None:
        existing = {
            "number": 3999,
            "title": nightly_report.NIGHTLY_TITLE,
            "body": f"{nightly_report.NIGHTLY_MARKER}\nold body",
            "state": "open",
        }
        client = FakeRepoClient(
            jobs=[
                {"name": "Ubuntu GCC TSan", "conclusion": "success", "html_url": "", "steps": []},
                {"name": "Pure Cold-Start Release Build", "conclusion": "success", "html_url": "", "steps": []},
            ],
            issues=[existing],
        )
        outcome = nightly_report.reconcile_nightly_status(
            client,
            repository="apache/kvrocks",
            run_id="33333",
            head_sha="1122334455667",
            self_job_name="Report Nightly Status",
            apply_changes=True,
        )
        self.assertEqual(outcome["action"], "closed_issue")
        self.assertEqual(client.issues[0]["state"], "closed")
        self.assertEqual(len(client.comments), 1)

    def test_dry_run_never_mutates_issues(self) -> None:
        client = FakeRepoClient(
            jobs=[{"name": "Ubuntu GCC TSan", "conclusion": "failure", "html_url": "", "steps": []}]
        )
        outcome = nightly_report.reconcile_nightly_status(
            client,
            repository="apache/kvrocks",
            run_id="44444",
            head_sha="abcdef0000000",
            self_job_name="Report Nightly Status",
            apply_changes=False,
        )
        self.assertEqual(outcome["action"], "dry_run_failure")
        self.assertEqual(len(client.issues), 0)


if __name__ == "__main__":
    unittest.main()
