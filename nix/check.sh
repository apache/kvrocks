#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

./update_dep.py
export NIX_CONFIG="extra-experimental-features = nix-command flakes"
cd ..
exec nix flake check path:. --no-build --all-systems
