#!/usr/bin/env bash
# Check 'format' and 'golangci-lint' before 'git push',
# Copy this script to .git/hooks to activate,
# and remove it from .git/hooks to deactivate.

set -Euo pipefail

unset GIT_DIR
ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

run_check() {
    local check_name=$1
    echo "Running pre-push script $ROOT_DIR/x.py $check_name"
    ./x.py check "$check_name"

    if [ $? -ne 0 ]; then
        echo "You may use \`git push --no-verify\` to skip this check."
        exit 1
    fi
}

run_check format
run_check golangci-lint
