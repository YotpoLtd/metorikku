#!/bin/bash
set -e

travis_fold start "build"
echo "Building metorikku JAR"
sbt -DsparkVersion=$SPARK_VERSION clean scalastyle assembly "set test in (Test, assembly) := {}" test:assembly
travis_fold end "build"

travis_fold start "docker"
./scripts/docker.sh
travis_fold end "docker"
