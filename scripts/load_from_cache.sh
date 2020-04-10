#!/bin/bash
set -e

echo "Loading metorikku JAR"
cp -r $TARGET_CACHE/target .

#./scripts/load_docker_from_cache.sh
./scripts/docker.sh
