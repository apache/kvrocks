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

import http.client
import unittest
import urllib.error
from email.message import Message
from io import BytesIO
from typing import Any, Dict, List, Optional, Sequence
from unittest.mock import call, patch

import nightly_report

EXPECTED_JOBS = (
    "Validate Nightly Reporter",
    "Ubuntu RISC-V GCC without luaJIT (QEMU)",
    "Ubuntu GCC TSan",
    "Pure Cold-Start Release Build",
    "SonarCloud with Coverage",
)


def passing_jobs() -> List[Dict[str, Any]]:
    return [{"name": name, "conclusion": "success"} for name in EXPECTED_JOBS]


def nightly_issue(number: int, run_id: int) -> Dict[str, Any]:
    return {
        "number": number,
        "body": f"{nightly_report.NIGHTLY_MARKER}\n{nightly_report.LAST_RUN_ID_MARKER.format(run_id)}\n",
        "state": "open",
    }


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
    def reconcile(
        self, client: FakeRepoClient, run_id: str = "12345", expected_jobs: Sequence[str] = EXPECTED_JOBS
    ) -> Dict[str, Any]:
        return nightly_report.reconcile_nightly_status(
            client,
            repository="apache/kvrocks",
            run_id=run_id,
            head_sha="abcdef1234567",
            self_job_name="Report Nightly Status",
            apply_changes=True,
            expected_jobs=expected_jobs,
        )

    def test_creates_issue_when_job_fails(self) -> None:
        client = FakeRepoClient(
            jobs=[
                {
                    "name": "SonarCloud with Coverage",
                    "conclusion": "failure",
                    "html_url": "https://github.com/apache/kvrocks/actions/runs/1/job/10",
                    "steps": [
                        {"name": "Build Kvrocks (SonarCloud)", "conclusion": "success"},
                        {"name": "SonarQube Scan", "conclusion": "failure"},
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
        outcome = self.reconcile(client)
        self.assertEqual(outcome["action"], "created_issue")
        self.assertEqual(outcome["issue_number"], 4000)
        self.assertEqual(len(client.issues), 1)
        self.assertIn("ci/nightly", client.issues[0]["labels"])
        self.assertIn("SonarCloud with Coverage", client.issues[0]["body"])
        self.assertIn("SonarQube Scan", client.issues[0]["body"])
        self.assertIn(nightly_report.NIGHTLY_MARKER, client.issues[0]["body"])
        self.assertIn("<!-- kvrocks-nightly-last-run-id: 12345 -->", client.issues[0]["body"])

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
        outcome = self.reconcile(client, "22222")
        self.assertEqual(outcome["action"], "updated_issue")
        self.assertEqual(outcome["issue_number"], 3999)
        self.assertEqual(len(client.issues), 1)
        self.assertEqual(len(client.comments), 1)
        self.assertIn("<!-- kvrocks-nightly-last-run-id: 22222 -->", existing["body"])

    def test_closes_all_matching_open_issues_on_complete_recovery(self) -> None:
        older = nightly_issue(3998, 9999)
        legacy = {"number": 3999, "body": nightly_report.NIGHTLY_MARKER, "state": "open"}
        unrelated = {"number": 3997, "body": "", "state": "open"}
        client = FakeRepoClient(jobs=passing_jobs(), issues=[older, legacy, unrelated])
        outcome = self.reconcile(client)
        self.assertEqual(outcome["action"], "closed_issue")
        self.assertEqual(outcome["closed_issues"], [3998, 3999])
        self.assertEqual(older["state"], "closed")
        self.assertEqual(legacy["state"], "closed")
        self.assertEqual(unrelated["state"], "open")
        self.assertEqual([comment["number"] for comment in client.comments], [3998, 3999])

    def test_missing_expected_job_does_not_close_issue(self) -> None:
        existing = nightly_issue(3999, 12344)
        client = FakeRepoClient(jobs=passing_jobs()[:-1], issues=[existing])
        self.assertFalse(self.reconcile(client)["all_passed"])
        self.assertEqual(existing["state"], "open")
        self.assertEqual(client.comments, [])

    def test_incomplete_expected_job_does_not_close_issue(self) -> None:
        for conclusion in (None, "skipped"):
            with self.subTest(conclusion=conclusion):
                jobs = passing_jobs()
                jobs[-1]["conclusion"] = conclusion
                existing = nightly_issue(3999, 12344)
                client = FakeRepoClient(jobs=jobs, issues=[existing])
                self.assertFalse(self.reconcile(client)["all_passed"])
                self.assertEqual(existing["state"], "open")
                self.assertEqual(client.comments, [])

    def test_no_expected_jobs_does_not_close_issue(self) -> None:
        existing = nightly_issue(3999, 12344)
        client = FakeRepoClient(jobs=passing_jobs(), issues=[existing])
        self.assertFalse(self.reconcile(client, expected_jobs=())["all_passed"])
        self.assertEqual(existing["state"], "open")

    def test_stale_recovery_leaves_newer_issue_open(self) -> None:
        newer = nightly_issue(3999, 10000)
        older = nightly_issue(3998, 9998)
        client = FakeRepoClient(jobs=passing_jobs(), issues=[newer, older])
        with self.assertLogs(level="WARNING") as logs:
            outcome = self.reconcile(client, "9999")
        self.assertIn("Skipping stale recovery run 9999 for issue #3999", logs.output[0])
        self.assertEqual(outcome["closed_issues"], [3998])
        self.assertEqual(newer["state"], "open")
        self.assertEqual([comment["number"] for comment in client.comments], [3998])

    def test_cancelled_only_run_neither_opens_nor_closes_issues(self) -> None:
        jobs = [{"name": name, "conclusion": "cancelled"} for name in EXPECTED_JOBS]
        for issues in ([], [nightly_issue(3999, 12344)]):
            with self.subTest(existing_issue=bool(issues)):
                client = FakeRepoClient(jobs=jobs, issues=issues)
                outcome = self.reconcile(client)
                self.assertEqual(outcome["action"], "noop")
                self.assertEqual(outcome["failed_jobs"], [])
                self.assertEqual(client.list_open_nightly_issues(nightly_report.NIGHTLY_LABEL), issues)
                self.assertEqual(client.comments, [])

    def test_repeated_failure_skips_duplicate_comment(self) -> None:
        existing = nightly_issue(3999, 12344)
        client = FakeRepoClient(
            jobs=[{"name": "Ubuntu GCC TSan", "conclusion": "failure"}],
            issues=[existing],
        )
        self.assertEqual(self.reconcile(client)["action"], "updated_issue")
        self.assertEqual(self.reconcile(client)["action"], "noop")
        self.assertEqual(len(client.comments), 1)

    def test_stale_failure_keeps_newer_recorded_run_id(self) -> None:
        existing = nightly_issue(3999, 12346)
        client = FakeRepoClient(jobs=[{"name": "Ubuntu GCC TSan", "conclusion": "failure"}], issues=[existing])
        self.assertEqual(self.reconcile(client)["action"], "noop")
        self.assertIn("<!-- kvrocks-nightly-last-run-id: 12346 -->", existing["body"])
        self.assertEqual(client.comments, [])

    def test_main_returns_zero_after_successfully_reporting_failure(self) -> None:
        client = FakeRepoClient(jobs=[{"name": "Ubuntu GCC TSan", "conclusion": "failure"}])
        args = [
            "--repository",
            "apache/kvrocks",
            "--run-id",
            "12345",
            "--head-sha",
            "abcdef1234567",
            "--expected-jobs",
            ", ".join(EXPECTED_JOBS[:2]),
            "--expected-jobs",
            ", ".join(EXPECTED_JOBS[2:]),
            "--apply",
        ]
        with (
            patch.dict("os.environ", {"GH_TOKEN": "test-token"}),
            patch.object(nightly_report, "GitHubRESTClient", return_value=client),
            patch("builtins.print"),
        ):
            self.assertEqual(nightly_report.main(args), 0)
            self.assertEqual(len(client.issues), 1)
            client.jobs = passing_jobs()
            self.assertEqual(nightly_report.main(args), 0)
            self.assertEqual(client.issues[0]["state"], "closed")

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


class TestGitHubRESTClient(unittest.TestCase):
    def setUp(self) -> None:
        self.client = nightly_report.GitHubRESTClient("apache/kvrocks", "test-token")

    def test_lists_jobs_across_multiple_pages(self) -> None:
        jobs = passing_jobs()[:3]
        with (
            patch.object(self.client, "PAGE_SIZE", 2),
            patch.object(self.client, "_request", side_effect=[{"jobs": jobs[:2]}, {"jobs": jobs[2:]}]) as request,
        ):
            self.assertEqual(self.client.list_run_jobs("12345"), jobs)
        self.assertEqual(
            request.call_args_list,
            [call(f"/actions/runs/12345/jobs?per_page=2&page={page}") for page in (1, 2)],
        )

    def test_lists_issues_across_multiple_pages_and_excludes_pull_requests(self) -> None:
        issues = [nightly_issue(3998, 12343), nightly_issue(3999, 12344)]
        with (
            patch.object(self.client, "PAGE_SIZE", 2),
            patch.object(
                self.client, "_request", side_effect=[[issues[0], {"pull_request": {}}], issues[1:]]
            ) as request,
        ):
            self.assertEqual(self.client.list_open_nightly_issues(nightly_report.NIGHTLY_LABEL), issues)
        self.assertEqual(
            request.call_args_list,
            [call(f"/issues?state=open&labels=ci%2Fnightly&per_page=2&page={page}") for page in (1, 2)],
        )

    def test_pagination_limit_rejects_partial_results(self) -> None:
        with (
            patch.object(self.client, "PAGE_SIZE", 1),
            patch.object(self.client, "MAX_PAGES", 2),
            patch.object(self.client, "_request", return_value={"jobs": passing_jobs()[:1]}) as request,
            self.assertRaisesRegex(RuntimeError, "Pagination limit reached"),
        ):
            self.client.list_run_jobs("12345")
        self.assertEqual(request.call_count, 2)

    def test_retries_transient_network_errors(self) -> None:
        for error in (urllib.error.URLError("temporary"), TimeoutError("timeout"), http.client.IncompleteRead(b"")):
            with self.subTest(error=error):
                with (
                    patch("urllib.request.urlopen", side_effect=[error, BytesIO(b'{"jobs": []}')]) as urlopen,
                    patch("time.sleep") as sleep,
                ):
                    self.assertEqual(self.client.list_run_jobs("12345"), [])
                self.assertEqual(urlopen.call_count, 2)
                sleep.assert_called_once_with(1)

    def test_retries_rate_limited_403_with_bounded_retry_after(self) -> None:
        for header, value, expected_delay in (
            ("Retry-After", "8", 8),
            ("Retry-After", "120", 30),
            ("X-RateLimit-Remaining", "0", 1),
        ):
            with self.subTest(header=header, value=value):
                headers = Message()
                headers[header] = value
                error = urllib.error.HTTPError("https://api.github.com", 403, "Forbidden", headers, BytesIO(b""))
                with (
                    patch("urllib.request.urlopen", side_effect=[error, BytesIO(b"[]")]) as urlopen,
                    patch("time.sleep") as sleep,
                ):
                    self.assertEqual(self.client.list_open_nightly_issues(nightly_report.NIGHTLY_LABEL), [])
                self.assertEqual(urlopen.call_count, 2)
                sleep.assert_called_once_with(expected_delay)

    def test_does_not_retry_other_403(self) -> None:
        error = urllib.error.HTTPError("https://api.github.com", 403, "Forbidden", Message(), BytesIO(b"denied"))
        with (
            patch("urllib.request.urlopen", side_effect=error) as urlopen,
            patch("time.sleep") as sleep,
            self.assertRaisesRegex(RuntimeError, "HTTP 403: denied"),
        ):
            self.client.list_run_jobs("12345")
        urlopen.assert_called_once()
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
