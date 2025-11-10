#!/usr/bin/env python3

import os, sys, subprocess, re, json
from pathlib import Path


def parse_cmake_file(path):
    content = path.read_text()
    # Try FetchContent_DeclareGitHubWithMirror first
    match = re.search(
        r"FetchContent_DeclareGitHubWithMirror\s*\(\s*(\S+)\s+([\w.-]+)/([\w.-]+)\s+([\w.-]+)",
        content,
    )
    if match:
        _, owner, repo, rev = match.groups()
        key = path.stem
        return key, {"owner": owner, "repo": repo, "rev": rev}

    # Try FetchContent_DeclareGitHubTarWithMirror
    match = re.search(
        r"FetchContent_DeclareGitHubTarWithMirror\s*\(\s*(\S+)\s+([\w.-]+)/([\w.-]+)\s+([\w.-]+)",
        content,
    )
    if match:
        _, owner, repo, rev = match.groups()
        key = path.stem
        return key, {"owner": owner, "repo": repo, "rev": rev}

    return None, None


def generate_dep_json(cmake_dir, output_path):
    deps = {}
    ignore_list = {"riscv64.cmake"}
    print(f"Scanning {cmake_dir}...")
    for fname in sorted(os.listdir(cmake_dir)):
        if fname.endswith(".cmake") and fname not in ignore_list:
            path = Path(cmake_dir) / fname
            key, dep_info = parse_cmake_file(path)
            if key and dep_info:
                print(f"  -> Found {key}")
                deps[key] = dep_info
    with open(output_path, "w") as f:
        json.dump(deps, f, indent=2, sort_keys=True)
    print(f"Generated {output_path}")
    return deps


def generate_sha_json(deps, output_path):
    # Load existing sha.json if it exists
    existing_sha = {}
    if output_path.exists():
        try:
            with open(output_path, "r") as f:
                existing_sha = json.load(f)
            print(f"Loaded existing {output_path}")
        except (json.JSONDecodeError, IOError):
            print(f"Could not load existing {output_path}, will regenerate all")

    sha = {}
    print("\nFetching commit sha and prefetching sources...")
    for name, info in deps.items():
        owner, repo, rev = info["owner"], info["repo"], info["rev"]

        # Check if we can reuse existing sha
        if name in existing_sha and existing_sha[name].get("rev") == rev:
            print(f"  -> Reusing cached {name} @ {rev}")
            sha[name] = existing_sha[name]
            continue

        commit_hash = rev
        if not re.fullmatch(r"[0-9a-f]{40}", rev):
            print(f"  -> Resolving {name} {owner}/{repo} @ {rev}...")
            url = f"https://github.com/{owner}/{repo}"
            try:
                result = subprocess.run(
                    ["git", "ls-remote", url, rev],
                    capture_output=True,
                    text=True,
                    check=True,
                    encoding="utf-8",
                )
                commit_hash = result.stdout.split()[0]
            except (subprocess.CalledProcessError, IndexError) as e:
                print(f"    !> Failed to resolve {name}: {e}", file=sys.stderr)
                continue

        # Check again with resolved commit hash
        if name in existing_sha and existing_sha[name].get("rev") == commit_hash:
            print(f"  -> Reusing cached {name} @ {commit_hash[:7]}")
            sha[name] = existing_sha[name]
            continue

        print(f"  -> Fetching {name} : {owner}/{repo} {commit_hash[:7]}...")
        url = f"https://github.com/{owner}/{repo}"
        try:
            # Use nix-prefetch-git to get the source tree hash, which is what fetchFromGitHub expects.
            # The output is a JSON object containing the hash.
            result = subprocess.run(
                [
                    "nix-shell",
                    "-p",
                    "nix-prefetch-git",
                    "--run",
                    f"nix-prefetch-git --url {url} --rev {commit_hash}",
                ],
                capture_output=True,
                text=True,
                check=True,
                encoding="utf-8",
            )
            prefetch_data = json.loads(result.stdout)
            sha[name] = {"rev": commit_hash, "sha256": prefetch_data["hash"]}
        except (subprocess.CalledProcessError, IndexError, json.JSONDecodeError) as e:
            print(f"    !> Failed to prefetch {name}: {e}", file=sys.stderr)

    with open(output_path, "w") as f:
        json.dump(sha, f, indent=2, sort_keys=True)
    print(f"Generated {output_path}")


def main():
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent
    cmake_dir = project_root / "cmake"
    dep_json_path = script_dir / "dep.json"
    sha_json_path = script_dir / "sha.json"

    print("Generating nix/dep.json")
    deps = generate_dep_json(cmake_dir, dep_json_path)

    print(f"\nGenerating {sha_json_path}")
    generate_sha_json(deps, sha_json_path)

    print("\n✅ Done")


if __name__ == "__main__":
    main()
