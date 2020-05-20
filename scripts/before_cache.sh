#!/bin/bash
set -e

find $HOME/.sbt -name "*.lock" | xargs rm
find $HOME/.ivy2 -name "ivydata-*.properties" | xargs rm
rm -f $HOME/.ivy2/.sbt.ivy.lock
rm -f $HOME/.ivy2/cache/org.apache.hbase/hbase-server/ivy-1.1.1.xml

echo "Saving metorikku JAR to cache"
rm -rf target/streams/\$global/assembly target/streams/\$global/assemblyOption target/streams/test/assemblyOption target/streams/test/assembly
cp -r target $TARGET_CACHE

#./scripts/save_docker_to_cache.sh
