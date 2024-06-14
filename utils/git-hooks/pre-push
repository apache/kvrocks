#!/usr/bin/env bash
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
