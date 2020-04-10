#!/bin/bash
set -e

find $HOME/.sbt -name "*.lock" | xargs rm
find $HOME/.ivy2 -name "ivydata-*.properties" | xargs rm
rm -f $HOME/.ivy2/.sbt.ivy.lock

echo "Saving metorikku JAR to cache"
rm -rf target/streams/\$global/assembly target/streams/\$global/assemblyOption target/streams/test/assemblyOption target/streams/test/assembly
cp -r target $TARGET_CACHE

echo "Saving docker images to cache"
docker save -o $DOCKER_CACHE/images.tar $(docker images -a -q)
