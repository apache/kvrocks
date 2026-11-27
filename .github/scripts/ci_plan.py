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

from argparse import ArgumentParser
from fnmatch import fnmatchcase
import json
import os
from pathlib import Path
import subprocess
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

FULL_CI_LABEL = "ci/full"
DEFAULT_BRANCH = "unstable"

ALL_COMPAT_VERSIONS = [
    "v2.3.0",
    "v2.6.0",
    "v2.7.0",
    "v2.10.0",
    "v2.14.0",
    "v2.15.0",
]
SMOKE_COMPAT_VERSIONS = ["v2.3.0", "v2.15.0"]

DOCS_ONLY_EXACT = {
    ".asf.yaml",
    ".github/config/changes.yml",
    ".gitignore",
    "NOTICE",
}
DOCS_ONLY_PREFIXES = (
    ".github/ISSUE_TEMPLATE/",
    "assets/",
    "licenses/",
)

BUILD_INFRA_EXACT = {
    ".dockerignore",
    ".github/scripts/ci_plan.py",
    ".github/scripts/test_ci_plan.py",
    ".github/workflows/kvrocks.yaml",
    "CMakeLists.txt",
    "Dockerfile",
    "x.py",
}
BUILD_INFRA_PREFIXES = ("cmake/",)

CMAKE_PREFIXES = ("cmake/",)
CMAKE_EXACT = {"CMakeLists.txt", "x.py"}

COMPAT_EXACT = {
    ".github/workflows/compatibility.yml",
    "CMakeLists.txt",
    "Dockerfile",
    "kvrocks.conf",
    "x.py",
}
COMPAT_PREFIXES = (
    "cmake/",
    "src/cluster/",
    "src/storage/",
    "src/types/",
    "tests/gocase/integration/compatibility/",
)


def _job(
    name: str,
    os_name: str,
    compiler: str,
    cache_key: str,
    *,
    arm_linux: bool = False,
    with_ninja: str = "",
    with_lld: str = "",
    without_jemalloc: str = "",
    without_luajit: str = "",
    with_sanitizer: str = "",
    with_openssl: str = "",
    new_encoding: str = "",
    sonarcloud: str = "",
    go_tags: str = "",
    use_ccache: bool = True,
    maximize_disk: bool = False,
    skip_kvrocks2redis: bool = False,
) -> Dict[str, Any]:
    return {
        "name": name,
        "os": os_name,
        "compiler": compiler,
        "cache_key": cache_key,
        "arm_linux": arm_linux,
        "with_ninja": with_ninja,
        "with_lld": with_lld,
        "without_jemalloc": without_jemalloc,
        "without_luajit": without_luajit,
        "with_sanitizer": with_sanitizer,
        "with_openssl": with_openssl,
        "new_encoding": new_encoding,
        "sonarcloud": sonarcloud,
        "go_tags": go_tags,
        "use_ccache": use_ccache,
        "maximize_disk": maximize_disk,
        "skip_kvrocks2redis": skip_kvrocks2redis,
    }


def _container_job(
    name: str,
    image: str,
    compiler: str = "gcc",
    *,
    disable_jemalloc: str = "",
    container_options: str = "",
    use_ccache: bool = False,
) -> Dict[str, Any]:
    return {
        "name": name,
        "image": image,
        "compiler": compiler,
        "disable_jemalloc": disable_jemalloc,
        "container_options": container_options,
        "use_ccache": use_ccache,
    }


def tier1_build_matrix() -> List[Dict[str, Any]]:
    return [
        # Canonical Makefiles + GNU ld baseline guarding real user source builds.
        _job(
            "Ubuntu 24 GCC",
            "ubuntu-24.04",
            "gcc",
            "ubuntu24-gcc",
        ),
        _job(
            "Ubuntu 24 Clang with OpenSSL",
            "ubuntu-24.04",
            "clang",
            "ubuntu24-clang-openssl",
            with_ninja="--ninja",
            with_lld="-DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld",
            with_openssl="-DENABLE_OPENSSL=ON",
        ),
        _job(
            "Ubuntu 24 GCC (Old Encoding + Lua)",
            "ubuntu-24.04",
            "gcc",
            "ubuntu24-gcc-oldenc-lua",
            with_ninja="--ninja",
            without_luajit="-DENABLE_LUAJIT=OFF",
            new_encoding="-DENABLE_NEW_ENCODING=FALSE",
        ),
        # CMakeLists.txt forces DISABLE_JEMALLOC=ON on Apple hosts, covering no-jemalloc too.
        _job(
            "Darwin Clang arm64 with OpenSSL",
            "macos-15",
            "auto",
            "macos15-clang-openssl",
            with_ninja="--ninja",
            with_openssl="-DENABLE_OPENSSL=ON",
            skip_kvrocks2redis=True,
        ),
        _job(
            "Ubuntu ARM Clang",
            "ubuntu-24.04-arm",
            "clang",
            "ubuntu24-arm64-clang",
            arm_linux=True,
            with_ninja="--ninja",
            with_lld="-DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld",
        ),
        _job(
            "Ubuntu Clang ASan + UBSan",
            "ubuntu-22.04",
            "clang",
            "ubuntu22-clang-asan-ubsan",
            with_ninja="--ninja",
            with_lld="-DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld",
            without_jemalloc="-DDISABLE_JEMALLOC=ON",
            with_sanitizer="-DENABLE_ASAN=ON -DENABLE_UBSAN=ON",
            go_tags="-tags=ignore_when_asan,ignore_when_ubsan",
            skip_kvrocks2redis=True,
        ),
        _job(
            "Ubuntu Clang TSan",
            "ubuntu-22.04",
            "clang",
            "ubuntu22-clang-tsan",
            with_ninja="--ninja",
            with_lld="-DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld",
            without_jemalloc="-DDISABLE_JEMALLOC=ON",
            with_sanitizer="-DENABLE_TSAN=ON",
            go_tags="-tags=ignore_when_tsan",
            skip_kvrocks2redis=True,
        ),
        _job(
            "SonarCloud with Coverage",
            "ubuntu-22.04",
            "gcc",
            "ubuntu22-gcc-sonar",
            sonarcloud="-DCMAKE_CXX_FLAGS=--coverage",
            use_ccache=False,
            maximize_disk=True,
            skip_kvrocks2redis=True,
        ),
    ]


def tier2_build_matrix(*, include_sonarcloud: bool) -> List[Dict[str, Any]]:
    jobs = [
        _job("Darwin Clang arm64", "macos-15", "auto", "macos15-clang", skip_kvrocks2redis=True),
        _job(
            "Darwin Clang arm64 with OpenSSL",
            "macos-15",
            "auto",
            "macos15-clang-openssl",
            with_openssl="-DENABLE_OPENSSL=ON",
            skip_kvrocks2redis=True,
        ),
        _job(
            "Darwin Clang arm64 without luaJIT",
            "macos-15",
            "auto",
            "macos15-clang-lua",
            without_luajit="-DENABLE_LUAJIT=OFF",
            skip_kvrocks2redis=True,
        ),
        _job("Ubuntu GCC", "ubuntu-22.04", "gcc", "ubuntu22-gcc"),
        _job("Ubuntu Clang", "ubuntu-22.04", "clang", "ubuntu22-clang"),
        _job("Ubuntu 24 GCC", "ubuntu-24.04", "gcc", "ubuntu24-gcc"),
        _job("Ubuntu 24 Clang", "ubuntu-24.04", "clang", "ubuntu24-clang"),
        _job(
            "Ubuntu GCC ASan",
            "ubuntu-22.04",
            "gcc",
            "ubuntu22-gcc-asan",
            without_jemalloc="-DDISABLE_JEMALLOC=ON",
            with_sanitizer="-DENABLE_ASAN=ON",
            go_tags="-tags=ignore_when_asan",
            skip_kvrocks2redis=True,
        ),
        _job(
            "Ubuntu Clang ASan",
            "ubuntu-22.04",
            "clang",
            "ubuntu22-clang-asan",
            without_jemalloc="-DDISABLE_JEMALLOC=ON",
            with_sanitizer="-DENABLE_ASAN=ON",
            go_tags="-tags=ignore_when_asan",
            skip_kvrocks2redis=True,
        ),
        _job(
            "Ubuntu Clang TSan",
            "ubuntu-22.04",
            "clang",
            "ubuntu22-clang-tsan",
            without_jemalloc="-DDISABLE_JEMALLOC=ON",
            with_sanitizer="-DENABLE_TSAN=ON",
            go_tags="-tags=ignore_when_tsan",
            skip_kvrocks2redis=True,
        ),
        _job(
            "Ubuntu Clang UBSAN",
            "ubuntu-22.04",
            "clang",
            "ubuntu22-clang-ubsan",
            without_jemalloc="-DDISABLE_JEMALLOC=ON",
            with_sanitizer="-DENABLE_UBSAN=ON",
            go_tags="-tags=ignore_when_ubsan",
        ),
        _job(
            "Ubuntu GCC Ninja",
            "ubuntu-22.04",
            "gcc",
            "ubuntu22-gcc",
            with_ninja="--ninja",
        ),
        _job(
            "Ubuntu GCC with OpenSSL",
            "ubuntu-22.04",
            "gcc",
            "ubuntu22-gcc-openssl",
            with_openssl="-DENABLE_OPENSSL=ON",
        ),
        _job(
            "Ubuntu Clang with OpenSSL",
            "ubuntu-22.04",
            "clang",
            "ubuntu22-clang-openssl",
            with_openssl="-DENABLE_OPENSSL=ON",
        ),
        _job(
            "Ubuntu GCC without luaJIT",
            "ubuntu-22.04",
            "gcc",
            "ubuntu22-gcc-lua",
            without_luajit="-DENABLE_LUAJIT=OFF",
        ),
        _job(
            "Ubuntu Clang without luaJIT",
            "ubuntu-22.04",
            "clang",
            "ubuntu22-clang-lua",
            without_luajit="-DENABLE_LUAJIT=OFF",
        ),
        _job(
            "Ubuntu GCC with old encoding",
            "ubuntu-22.04",
            "gcc",
            "ubuntu22-gcc-oldenc",
            new_encoding="-DENABLE_NEW_ENCODING=FALSE",
        ),
        _job(
            "Ubuntu Clang with old encoding",
            "ubuntu-22.04",
            "clang",
            "ubuntu22-clang-oldenc",
            new_encoding="-DENABLE_NEW_ENCODING=FALSE",
        ),
        _job(
            "Ubuntu ARM GCC",
            "ubuntu-24.04-arm",
            "gcc",
            "ubuntu24-arm64-gcc",
            arm_linux=True,
        ),
        _job(
            "Ubuntu ARM Clang",
            "ubuntu-24.04-arm",
            "clang",
            "ubuntu24-arm64-clang",
            arm_linux=True,
        ),
    ]
    if include_sonarcloud:
        jobs.insert(
            4,
            _job(
                "SonarCloud with Coverage",
                "ubuntu-22.04",
                "gcc",
                "ubuntu22-gcc-sonar",
                sonarcloud="-DCMAKE_CXX_FLAGS=--coverage",
                use_ccache=False,
                maximize_disk=True,
                skip_kvrocks2redis=True,
            ),
        )
    return jobs


def tier2_container_matrix() -> List[Dict[str, Any]]:
    return [
        _container_job("openSUSE Leap 15", "opensuse/leap:15"),
        _container_job("ArchLinux", "archlinux:base"),
        _container_job("Rocky Linux 8", "rockylinux:8"),
        _container_job(
            "Rocky Linux 9",
            "rockylinux:9",
            container_options="--env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        ),
        _container_job("Debian 12", "debian:12"),
        _container_job(
            "Alpine 3",
            "alpine:3.22",
            disable_jemalloc="-DDISABLE_JEMALLOC=ON",
            use_ccache=True,
        ),
    ]


def alpine_smoke_container_matrix() -> List[Dict[str, Any]]:
    return [
        _container_job(
            "Alpine 3",
            "alpine:3.22",
            disable_jemalloc="-DDISABLE_JEMALLOC=ON",
            use_ccache=True,
        )
    ]


def is_doc_file(path: str) -> bool:
    normalized = path.strip("/")
    if not normalized:
        return False
    if normalized in DOCS_ONLY_EXACT:
        return True
    if normalized.startswith(DOCS_ONLY_PREFIXES):
        return True
    return fnmatchcase(normalized, "*.md") or fnmatchcase(normalized, "**/*.md") or normalized.endswith(".md")


def is_docs_only(changed_files: Sequence[str]) -> bool:
    return len(changed_files) > 0 and all(is_doc_file(path) for path in changed_files)


def _matches_any(paths: Iterable[str], exact: set, prefixes: Tuple[str, ...]) -> bool:
    for raw in paths:
        path = raw.strip("/")
        if path in exact or path.startswith(prefixes):
            return True
    return False


def build_plan(
    *,
    event_name: str,
    changed_files: Optional[Sequence[str]],
    labels: Sequence[str] = (),
    base_ref: str = "",
) -> Dict[str, Any]:
    notes: List[str] = []

    if changed_files is not None and is_docs_only(changed_files):
        notes.append("All changed files match documentation/non-code filters; skipping build and test jobs.")
        return {
            "docs_only": True,
            "tier": "skip",
            "tidy_mode": "skip",
            "run_lint": False,
            "has_build_matrix": False,
            "build_matrix": [],
            "has_container_matrix": False,
            "container_matrix": [],
            "run_docker": False,
            "has_compat_matrix": False,
            "compat_matrix": [],
            "notes": notes,
        }

    has_full_label = FULL_CI_LABEL in set(labels)
    non_default_pr_base = bool(base_ref) and base_ref != DEFAULT_BRANCH

    if event_name == "merge_group":
        tier = "t2"
        notes.append("Merge Queue event detected: running full canonical Tier 2 matrix (SonarCloud skipped on synthetic queue ref).")
    elif event_name in ("push", "workflow_dispatch", "release"):
        tier = "t2"
        notes.append(f"Event '{event_name}' detected: running full canonical Tier 2 matrix.")
    elif event_name == "pull_request" and has_full_label:
        tier = "t2"
        notes.append(f"Label '{FULL_CI_LABEL}' present: escalating PR to full canonical Tier 2 matrix.")
    elif event_name == "pull_request" and non_default_pr_base:
        tier = "t2"
        notes.append(f"Target branch '{base_ref}' is not '{DEFAULT_BRANCH}': running full canonical Tier 2 matrix.")
    else:
        tier = "t1"
        notes.append("Routine pull request: running fast orthogonal Tier 1 matrix.")

    if tier == "t2":
        include_sonar = event_name != "merge_group"
        build_matrix = tier2_build_matrix(include_sonarcloud=include_sonar)
        container_matrix = tier2_container_matrix()
        return {
            "docs_only": False,
            "tier": "t2",
            "tidy_mode": "full",
            "run_lint": True,
            "has_build_matrix": len(build_matrix) > 0,
            "build_matrix": build_matrix,
            "has_container_matrix": len(container_matrix) > 0,
            "container_matrix": container_matrix,
            "run_docker": True,
            "has_compat_matrix": True,
            "compat_matrix": list(ALL_COMPAT_VERSIONS),
            "notes": notes,
        }

    # Tier 1 path-aware escalations for pull_request.
    files = list(changed_files) if changed_files is not None else []
    diff_unavailable = changed_files is None

    touch_build_infra = diff_unavailable or _matches_any(files, BUILD_INFRA_EXACT, BUILD_INFRA_PREFIXES)
    touch_cmake = diff_unavailable or _matches_any(files, CMAKE_EXACT, CMAKE_PREFIXES)
    touch_compat = diff_unavailable or _matches_any(files, COMPAT_EXACT, COMPAT_PREFIXES)

    tidy_mode = "full" if touch_cmake else "incremental"
    if touch_cmake:
        notes.append("CMake or build script changes detected: running full clang-tidy scan.")

    container_matrix: List[Dict[str, Any]] = []
    run_docker = False
    if touch_build_infra:
        container_matrix = alpine_smoke_container_matrix()
        run_docker = True
        notes.append("Build/Docker/CI infrastructure changes detected: adding Alpine (musl) container and Docker image checks.")

    compat_matrix: List[str] = []
    if touch_compat:
        compat_matrix = list(SMOKE_COMPAT_VERSIONS)
        notes.append(f"Storage/types/build changes detected: running boundary compatibility versions {compat_matrix}.")

    build_matrix = tier1_build_matrix()
    return {
        "docs_only": False,
        "tier": "t1",
        "tidy_mode": tidy_mode,
        "run_lint": True,
        "has_build_matrix": len(build_matrix) > 0,
        "build_matrix": build_matrix,
        "has_container_matrix": len(container_matrix) > 0,
        "container_matrix": container_matrix,
        "run_docker": run_docker,
        "has_compat_matrix": len(compat_matrix) > 0,
        "compat_matrix": compat_matrix,
        "notes": notes,
    }


def extract_event_metadata(event_name: str, payload: Dict[str, Any]) -> Tuple[List[str], str, Optional[Tuple[str, str]]]:
    labels: List[str] = []
    base_ref = ""
    diff_range: Optional[Tuple[str, str]] = None

    if event_name == "pull_request":
        pr = payload.get("pull_request") or {}
        labels = [lbl.get("name", "") for lbl in (pr.get("labels") or []) if isinstance(lbl, dict) and lbl.get("name")]
        base = pr.get("base") or {}
        head = pr.get("head") or {}
        base_ref = str(base.get("ref") or "")
        base_sha = str(base.get("sha") or "")
        head_sha = str(head.get("sha") or "")
        if base_sha and head_sha:
            diff_range = (base_sha, head_sha)
    elif event_name == "merge_group":
        mg = payload.get("merge_group") or {}
        base_ref = str(mg.get("base_ref") or "").removeprefix("refs/heads/")
        base_sha = str(mg.get("base_sha") or "")
        head_sha = str(mg.get("head_sha") or "")
        if base_sha and head_sha:
            diff_range = (base_sha, head_sha)
    elif event_name == "push":
        before = str(payload.get("before") or "")
        after = str(payload.get("after") or "")
        if before and after and set(before) != {"0"}:
            diff_range = (before, after)

    return labels, base_ref, diff_range


def git_changed_files(base_sha: str, head_sha: str, cwd: Optional[str] = None) -> Optional[List[str]]:
    # Use two-dot diff for push/merge_group exact range, or three-dot merge-base diff for PRs when available.
    for rev_expr in (f"{base_sha}...{head_sha}", f"{base_sha}..{head_sha}"):
        proc = subprocess.run(
            ["git", "diff", "--name-only", rev_expr],
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    return None


def format_github_outputs(plan: Dict[str, Any]) -> str:
    def _bool_str(val: bool) -> str:
        return "true" if val else "false"

    lines = [
        f"docs_only={_bool_str(plan['docs_only'])}",
        f"tier={plan['tier']}",
        f"tidy_mode={plan['tidy_mode']}",
        f"run_lint={_bool_str(plan['run_lint'])}",
        f"has_build_matrix={_bool_str(plan['has_build_matrix'])}",
        f"build_matrix={json.dumps(plan['build_matrix'], separators=(',', ':'))}",
        f"has_container_matrix={_bool_str(plan['has_container_matrix'])}",
        f"container_matrix={json.dumps(plan['container_matrix'], separators=(',', ':'))}",
        f"run_docker={_bool_str(plan['run_docker'])}",
        f"has_compat_matrix={_bool_str(plan['has_compat_matrix'])}",
        f"compat_matrix={json.dumps(plan['compat_matrix'], separators=(',', ':'))}",
    ]
    return "\n".join(lines) + "\n"


def format_step_summary(plan: Dict[str, Any]) -> str:
    lines = [
        "### Kvrocks CI Execution Plan",
        "",
        f"- **Selected Tier**: `{plan['tier']}`",
        f"- **Docs Only**: `{plan['docs_only']}`",
        f"- **clang-tidy Mode**: `{plan['tidy_mode']}`",
        f"- **Host Build Jobs**: `{len(plan['build_matrix'])}`",
        f"- **Distro Container Jobs**: `{len(plan['container_matrix'])}`",
        f"- **Docker Health Check**: `{plan['run_docker']}`",
        f"- **Compatibility Versions**: `{', '.join(plan['compat_matrix']) if plan['compat_matrix'] else 'skipped'}`",
    ]
    if plan.get("notes"):
        lines.append("")
        lines.append("#### Planner Notes")
        for note in plan["notes"]:
            lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = ArgumentParser(description="Generate tiered GitHub Actions execution plan for Apache Kvrocks.")
    parser.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME", "pull_request"))
    parser.add_argument("--event-path", default=os.environ.get("GITHUB_EVENT_PATH", ""))
    parser.add_argument("--base-ref", default=os.environ.get("GITHUB_BASE_REF", ""))
    parser.add_argument("--changed-files-from-git", action="store_true", help="Resolve changed files from git diff.")
    parser.add_argument("--changed-file", action="append", default=None, help="Explicit changed file path (repeatable).")
    parser.add_argument("--label", action="append", default=None, help="Explicit PR label override (repeatable).")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    parser.add_argument("--summary-file", default=os.environ.get("GITHUB_STEP_SUMMARY", ""))
    args = parser.parse_args(argv)

    payload: Dict[str, Any] = {}
    if args.event_path and Path(args.event_path).is_file():
        payload = json.loads(Path(args.event_path).read_text(encoding="utf-8"))

    event_labels, payload_base_ref, diff_range = extract_event_metadata(args.event_name, payload)
    labels = args.label if args.label is not None else event_labels
    base_ref = args.base_ref or payload_base_ref

    changed_files: Optional[Sequence[str]] = args.changed_file
    if changed_files is None and args.changed_files_from_git and diff_range is not None:
        changed_files = git_changed_files(diff_range[0], diff_range[1])

    plan = build_plan(
        event_name=args.event_name,
        changed_files=changed_files,
        labels=labels,
        base_ref=base_ref,
    )

    print(json.dumps(plan, indent=2))

    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as out_file:
            out_file.write(format_github_outputs(plan))

    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as summary_file:
            summary_file.write(format_step_summary(plan))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
