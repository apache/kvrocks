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

"""Reconcile Apache Kvrocks nightly test outcomes with a deduplicated GitHub Issue."""

import http.client
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from argparse import ArgumentParser
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Protocol, Sequence

NIGHTLY_MARKER = "<!-- kvrocks-nightly-key: nightly-tests -->"
LAST_RUN_ID_MARKER = "<!-- kvrocks-nightly-last-run-id: {} -->"
NIGHTLY_LABEL = "ci/nightly"
NIGHTLY_TITLE = "Nightly CI failure: Nightly Tests"
FAILURE_CONCLUSIONS = {"failure", "timed_out", "startup_failure"}


@dataclass(frozen=True)
class JobResult:
    name: str
    conclusion: str
    html_url: str
    failed_step: Optional[str] = None


class RepoClient(Protocol):
    def list_run_jobs(self, run_id: str) -> List[Dict[str, Any]]: ...

    def ensure_label(self, name: str, color: str, description: str) -> None: ...

    def list_open_nightly_issues(self, label: str) -> List[Dict[str, Any]]: ...

    def create_issue(self, title: str, body: str, labels: List[str]) -> Dict[str, Any]: ...

    def update_issue(
        self, number: int, *, title: Optional[str] = None, body: Optional[str] = None, state: Optional[str] = None
    ) -> Dict[str, Any]: ...

    def create_comment(self, number: int, body: str) -> Dict[str, Any]: ...


class GitHubRESTClient:
    PAGE_SIZE = 100
    MAX_PAGES = 100

    def __init__(self, repository: str, token: str) -> None:
        self.repository = repository
        self.base_url = f"https://api.github.com/repos/{repository}"
        self.token = token

    def _request(self, path: str, method: str = "GET", payload: Optional[Dict[str, Any]] = None) -> Any:
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "User-Agent": "apache-kvrocks-nightly-reporter",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        for attempt in range(3):
            delay: float = 2**attempt
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                    return json.loads(raw.decode("utf-8")) if raw else None
            except urllib.error.HTTPError as err:
                with err:
                    if err.code == 422 and method == "POST" and path == "/labels":
                        return None
                    retry_after = err.headers.get("Retry-After")
                    rate_limited = err.code == 403 and (
                        retry_after is not None or err.headers.get("X-RateLimit-Remaining") == "0"
                    )
                    if attempt == 2 or (not rate_limited and err.code not in (429, 500, 502, 503, 504)):
                        detail = err.read(500).decode("utf-8", errors="replace")
                        raise RuntimeError(f"{method} {url} failed with HTTP {err.code}: {detail}") from None
                    if retry_after is not None:
                        delay = self._retry_delay(retry_after, delay)
            except (urllib.error.URLError, TimeoutError, http.client.HTTPException) as err:
                if attempt == 2:
                    raise RuntimeError(f"{method} {url} failed: {err}") from err
            time.sleep(delay)

    @staticmethod
    def _retry_delay(retry_after: str, fallback: float) -> float:
        try:
            delay = int(retry_after)
        except ValueError:
            try:
                delay = parsedate_to_datetime(retry_after).timestamp() - time.time()
            except (ValueError, OverflowError, OSError):
                return fallback
        return min(30, max(0, delay))

    def _paginate(self, path: str, *, items_key: Optional[str] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        separator = "&" if "?" in path else "?"
        for page in range(1, self.MAX_PAGES + 1):
            response = self._request(f"{path}{separator}per_page={self.PAGE_SIZE}&page={page}")
            batch = response
            if items_key is not None:
                batch = response.get(items_key) if isinstance(response, dict) else None
            if not isinstance(batch, list):
                raise RuntimeError(f"Unexpected response for {path} on page {page}")
            items.extend(batch)
            if len(batch) < self.PAGE_SIZE:
                return items
        raise RuntimeError(f"Pagination limit reached for {path} after {self.MAX_PAGES} pages")

    def list_run_jobs(self, run_id: str) -> List[Dict[str, Any]]:
        return self._paginate(f"/actions/runs/{run_id}/jobs", items_key="jobs")

    def ensure_label(self, name: str, color: str, description: str) -> None:
        encoded = urllib.parse.quote(name, safe="")
        try:
            self._request(f"/labels/{encoded}", method="GET")
        except RuntimeError as err:
            if "HTTP 404" in str(err):
                self._request(
                    "/labels",
                    method="POST",
                    payload={"name": name, "color": color, "description": description},
                )
            else:
                raise

    def list_open_nightly_issues(self, label: str) -> List[Dict[str, Any]]:
        encoded = urllib.parse.quote(label, safe="")
        issues = self._paginate(f"/issues?state=open&labels={encoded}")
        return [item for item in issues if "pull_request" not in item]

    def create_issue(self, title: str, body: str, labels: List[str]) -> Dict[str, Any]:
        return self._request("/issues", method="POST", payload={"title": title, "body": body, "labels": labels})

    def update_issue(
        self, number: int, *, title: Optional[str] = None, body: Optional[str] = None, state: Optional[str] = None
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if title is not None:
            payload["title"] = title
        if body is not None:
            payload["body"] = body
        if state is not None:
            payload["state"] = state
        return self._request(f"/issues/{number}", method="PATCH", payload=payload)

    def create_comment(self, number: int, body: str) -> Dict[str, Any]:
        return self._request(f"/issues/{number}/comments", method="POST", payload={"body": body})


def collect_job_results(raw_jobs: Sequence[Dict[str, Any]], self_job_name: str) -> List[JobResult]:
    results: List[JobResult] = []
    for job in raw_jobs:
        name = str(job.get("name") or "")
        if not name or name == self_job_name:
            continue
        conclusion = str(job.get("conclusion") or "unknown")
        failed_step = None
        for step in job.get("steps") or ():
            if str(step.get("conclusion") or "") in FAILURE_CONCLUSIONS:
                failed_step = str(step.get("name") or "")
                break
        results.append(
            JobResult(
                name=name,
                conclusion=conclusion,
                html_url=str(job.get("html_url") or ""),
                failed_step=failed_step,
            )
        )
    return results


def render_issue_body(
    *,
    repository: str,
    run_id: str,
    head_sha: str,
    results: Sequence[JobResult],
) -> str:
    run_url = f"https://github.com/{repository}/actions/runs/{run_id}"
    short_sha = head_sha[:7] if head_sha else "unknown"
    lines = [
        NIGHTLY_MARKER,
        LAST_RUN_ID_MARKER.format(run_id),
        "### Nightly Test Failure Report",
        "",
        f"- **Workflow Run**: [{run_id}]({run_url})",
        f"- **Commit**: `{short_sha}` ({head_sha})",
        "",
        "| Job | Status | Failed Step | Logs |",
        "| :--- | :--- | :--- | :--- |",
    ]
    for item in results:
        step_text = f"`{item.failed_step}`" if item.failed_step else "-"
        link_text = f"[View logs]({item.html_url})" if item.html_url else "-"
        lines.append(f"| `{item.name}` | `{item.conclusion}` | {step_text} | {link_text} |")
    lines.append("")
    lines.append("_This issue is automatically maintained by `.github/workflows/nightly-tests.yaml`._")
    return "\n".join(lines) + "\n"


def recorded_run_id(issue: Dict[str, Any]) -> int:
    pattern = LAST_RUN_ID_MARKER.format(r"([0-9]+)")
    match = re.search(rf"^{pattern}\r?$", issue.get("body") or "", re.MULTILINE)
    return int(match[1]) if match else 0


def reconcile_nightly_status(
    client: RepoClient,
    *,
    repository: str,
    run_id: str,
    head_sha: str,
    self_job_name: str,
    apply_changes: bool,
    expected_jobs: Sequence[str] = (),
) -> Dict[str, Any]:
    raw_jobs = client.list_run_jobs(run_id)
    results = collect_job_results(raw_jobs, self_job_name=self_job_name)
    failed = [item for item in results if item.conclusion in FAILURE_CONCLUSIONS]
    all_passed = (
        bool(expected_jobs)
        and set(expected_jobs).issubset(item.name for item in results)
        and all(item.conclusion == "success" for item in results)
    )

    summary: Dict[str, Any] = {
        "total_jobs": len(results),
        "failed_jobs": [item.name for item in failed],
        "all_passed": all_passed,
        "apply_changes": apply_changes,
        "action": "noop",
    }

    if not apply_changes:
        if failed:
            summary["action"] = "dry_run_failure"
        elif all_passed:
            summary["action"] = "dry_run_pass"
        else:
            summary["action"] = "dry_run_incomplete"
        return summary
    if not failed and not all_passed:
        return summary

    current_run_id = int(run_id)
    open_issues = [
        issue
        for issue in client.list_open_nightly_issues(NIGHTLY_LABEL)
        if NIGHTLY_MARKER in str(issue.get("body") or "")
    ]
    if failed:
        existing = max(open_issues, key=recorded_run_id, default=None)
        if existing is not None and current_run_id <= recorded_run_id(existing):
            logging.info("Skipping already recorded or stale failure run %s for issue #%s", run_id, existing["number"])
            return summary
        body = render_issue_body(
            repository=repository,
            run_id=run_id,
            head_sha=head_sha,
            results=results,
        )
        if existing is None:
            client.ensure_label(
                NIGHTLY_LABEL,
                "b60205",
                "Automated nightly CI regression report",
            )
            created = client.create_issue(
                NIGHTLY_TITLE,
                body,
                [NIGHTLY_LABEL, "A-ci"],
            )
            summary["action"] = "created_issue"
            summary["issue_number"] = created.get("number")
        else:
            num = int(existing["number"])
            client.update_issue(num, title=NIGHTLY_TITLE, body=body)
            run_url = f"https://github.com/{repository}/actions/runs/{run_id}"
            failed_list = ", ".join(f"`{item.name}`" for item in failed)
            client.create_comment(
                num,
                f"Nightly run [{run_id}]({run_url}) at `{head_sha[:7]}` still failing: {failed_list}.",
            )
            summary["action"] = "updated_issue"
            summary["issue_number"] = num
    else:
        run_url = f"https://github.com/{repository}/actions/runs/{run_id}"
        closed_issues = []
        for issue in open_issues:
            num = int(issue["number"])
            if current_run_id < recorded_run_id(issue):
                logging.warning("Skipping stale recovery run %s for issue #%s", run_id, num)
                continue
            client.create_comment(
                num,
                f"Resolved: all nightly jobs passed in run [{run_id}]({run_url}) at `{head_sha[:7]}`. Closing automatically.",
            )
            client.update_issue(num, state="closed")
            closed_issues.append(num)
        if closed_issues:
            summary["action"] = "closed_issue"
            summary["issue_number"] = closed_issues[0]
            summary["closed_issues"] = closed_issues

    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = ArgumentParser(description="Reconcile Apache Kvrocks nightly test outcomes with GitHub Issues.")
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", "apache/kvrocks"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--self-job-name", default="Report Nightly Status")
    parser.add_argument(
        "--expected-jobs",
        action="append",
        default=[],
        help="Job names required for recovery, as a comma-separated list or repeated option.",
    )
    parser.add_argument("--apply", action="store_true", help="Create, update, or close GitHub issues.")
    args = parser.parse_args(argv)

    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
    if not token:
        raise RuntimeError("GH_TOKEN or GITHUB_TOKEN environment variable is required.")

    client = GitHubRESTClient(args.repository, token)
    outcome = reconcile_nightly_status(
        client,
        repository=args.repository,
        run_id=args.run_id,
        head_sha=args.head_sha,
        self_job_name=args.self_job_name,
        apply_changes=args.apply,
        expected_jobs=[name.strip() for value in args.expected_jobs for name in value.split(",") if name.strip()],
    )
    print(json.dumps(outcome, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
