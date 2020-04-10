#!/bin/bash
set -e

echo "Loading metorikku JAR"
cp -r $TARGET_CACHE/target .

echo "Loading docker cache"
docker load -i $DOCKER_CACHE/images.tar
