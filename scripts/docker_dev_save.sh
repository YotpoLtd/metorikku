#!/bin/bash
set -e

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker tag metorikku/spark metorikku/dev:spark-"$TRAVIS_COMMIT"
docker tag metorikku/metorikku metorikku/dev:metorikku-"$TRAVIS_COMMIT"
docker tag metorikku/hive metorikku/dev:hive-"$TRAVIS_COMMIT"
docker tag metorikku/hive:1 metorikku/dev:hive1-"$TRAVIS_COMMIT"

docker push metorikku/dev:spark-"$TRAVIS_COMMIT"
docker push metorikku/dev:metorikku-"$TRAVIS_COMMIT"
docker push metorikku/dev:hive-"$TRAVIS_COMMIT"
docker push metorikku/dev:hive1-"$TRAVIS_COMMIT"
