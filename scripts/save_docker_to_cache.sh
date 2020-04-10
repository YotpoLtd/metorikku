#!/bin/bash
set -e

echo "Saving docker images to cache"
docker save -o $DOCKER_CACHE/images.tar $(docker images -a -q)
docker images | grep -v '<none>' | sed '1d' | awk '{print $1 " " $2 " " $3}' > $DOCKER_CACHE/images.list
