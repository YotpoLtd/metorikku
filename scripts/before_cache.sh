#!/bin/bash
set -e

./scripts/clear_from_cache.sh

echo "Saving metorikku JAR to cache"
rm -rf target/streams/\$global/assembly target/streams/\$global/assemblyOption target/streams/test/assemblyOption target/streams/test/assembly
cp -r target $TARGET_CACHE
