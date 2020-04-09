#!/bin/bash
set -e

./scripts/assembly.sh
./scripts/docker.sh
./scripts/save_to_cache.sh
