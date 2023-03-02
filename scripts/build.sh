#!/bin/bash
set -e

MAIN_DIR="$( cd "$( dirname "$0" )" && pwd )/.."

TMP_TARGET="/tmp/target"
FINAL_TARGET="$MAIN_DIR/target"

rm -Rf $TMP_TARGET

sbt 'set target := file("'$TMP_TARGET'")' +clean scalastyle "set assembly / test := {}" +assembly

mkdir -p $FINAL_TARGET && rm -Rf $FINAL_TARGET/*.jar && cp -Rf $TMP_TARGET/scala-${SCALA_BINARY_VERSION}/*.jar $FINAL_TARGET/