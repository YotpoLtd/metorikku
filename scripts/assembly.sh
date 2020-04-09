#!/bin/bash
set -e

sbt -DsparkVersion=$SPARK_VERSION clean scalastyle assembly "set test in (Test, assembly) := {}" test:assembly
