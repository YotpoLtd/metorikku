#!/bin/bash
set -e

echo "Loading metorikku JAR"
cp -r $TARGET_CACHE/target .

echo "Loading docker cache"
docker load -i $DOCKER_CACHE/images.tar

docker images

while read REPOSITORY TAG IMAGE_ID
do
        echo "== Tagging $REPOSITORY $TAG $IMAGE_ID =="
        docker tag "$IMAGE_ID" "$REPOSITORY:$TAG"
done < $DOCKER_CACHE/images.list

docker images