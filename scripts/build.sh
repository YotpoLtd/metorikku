#!/bin/bash
set -e

echo "Building metorikku JAR"
sbt -DsparkVersion=$SPARK_VERSION clean scalastyle assembly "set test in (Test, assembly) := {}" test:assembly

#./scripts/docker.sh
