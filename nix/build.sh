#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

cd ..
if ! command -v nix 2>/dev/null; then
  if command -v apt-get 2>/dev/null; then
    apt-get install -y nix
  elif command -v brew 2>/dev/null; then
    brew install nix
  else
    echo "Please install nix"
    exit 1
  fi
fi

nix build
