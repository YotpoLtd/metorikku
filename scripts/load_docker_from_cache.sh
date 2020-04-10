#!/bin/bash
set -e

echo "Loading docker cache"
docker load -i $DOCKER_CACHE/images.tar

while read REPOSITORY TAG IMAGE_ID
do
        echo "== Tagging $REPOSITORY $TAG $IMAGE_ID =="
        docker tag "$IMAGE_ID" "$REPOSITORY:$TAG"
done < $DOCKER_CACHE/images.list
