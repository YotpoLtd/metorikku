#!/bin/bash
set -e

sbt -DsparkVersion=$SPARK_VERSION clean scalastyle "set test in (assembly) := {}" assembly "set test in (Test, assembly) := {}" test:assembly
