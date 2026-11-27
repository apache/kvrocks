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

from argparse import ArgumentParser
from dataclasses import dataclass
import json
import os
import time
from typing import Any, Dict, List, Optional, Protocol, Sequence
import urllib.error
import urllib.parse
import urllib.request

NIGHTLY_MARKER = "<!-- kvrocks-nightly-key: nightly-tests -->"
NIGHTLY_LABEL = "ci/nightly"
NIGHTLY_TITLE = "Nightly CI failure: Nightly Tests"
FAILURE_CONCLUSIONS = {"failure", "timed_out", "startup_failure", "cancelled"}


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
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, data=data, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                    return json.loads(raw.decode("utf-8")) if raw else None
            except urllib.error.HTTPError as err:
                if err.code == 422 and method == "POST" and path == "/labels":
                    return None
                if err.code not in (429, 500, 502, 503, 504) or attempt == 2:
                    detail = err.read().decode("utf-8", errors="replace")[:500]
                    raise RuntimeError(f"{method} {url} failed with HTTP {err.code}: {detail}") from None
            time.sleep(2**attempt)
        return None

    def list_run_jobs(self, run_id: str) -> List[Dict[str, Any]]:
        jobs: List[Dict[str, Any]] = []
        for page in range(1, 10):
            resp = self._request(f"/actions/runs/{run_id}/jobs?per_page=100&page={page}")
            batch = resp.get("jobs", []) if isinstance(resp, dict) else []
            jobs.extend(batch)
            if len(batch) < 100:
                break
        return jobs

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
        issues = self._request(f"/issues?state=open&labels={encoded}&per_page=50")
        if not isinstance(issues, list):
            return []
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


def reconcile_nightly_status(
    client: RepoClient,
    *,
    repository: str,
    run_id: str,
    head_sha: str,
    self_job_name: str,
    apply_changes: bool,
) -> Dict[str, Any]:
    raw_jobs = client.list_run_jobs(run_id)
    results = collect_job_results(raw_jobs, self_job_name=self_job_name)
    failed = [item for item in results if item.conclusion in FAILURE_CONCLUSIONS]
    all_passed = len(results) > 0 and all(item.conclusion == "success" for item in results)

    summary: Dict[str, Any] = {
        "total_jobs": len(results),
        "failed_jobs": [item.name for item in failed],
        "all_passed": all_passed,
        "apply_changes": apply_changes,
        "action": "noop",
    }

    if not apply_changes:
        summary["action"] = "dry_run_failure" if failed else "dry_run_pass"
        return summary

    open_issues = [
        issue
        for issue in client.list_open_nightly_issues(NIGHTLY_LABEL)
        if NIGHTLY_MARKER in str(issue.get("body") or "")
    ]
    existing = open_issues[0] if open_issues else None

    if failed:
        client.ensure_label(
            NIGHTLY_LABEL,
            "b60205",
            "Automated nightly CI regression report",
        )
        body = render_issue_body(
            repository=repository,
            run_id=run_id,
            head_sha=head_sha,
            results=results,
        )
        if existing is None:
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
    elif all_passed and existing is not None:
        num = int(existing["number"])
        run_url = f"https://github.com/{repository}/actions/runs/{run_id}"
        client.create_comment(
            num,
            f"Resolved: all nightly jobs passed in run [{run_id}]({run_url}) at `{head_sha[:7]}`. Closing automatically.",
        )
        client.update_issue(num, state="closed")
        summary["action"] = "closed_issue"
        summary["issue_number"] = num

    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = ArgumentParser(description="Reconcile Apache Kvrocks nightly test outcomes with GitHub Issues.")
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", "apache/kvrocks"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--self-job-name", default="Report Nightly Status")
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
    )
    print(json.dumps(outcome, indent=2))
    return 1 if outcome["failed_jobs"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
