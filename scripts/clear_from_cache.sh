#!/bin/bash
set -e

find $HOME/.sbt -name "*.lock" | xargs rm
find $HOME/.ivy2 -name "ivydata-*.properties" | xargs rm
rm -f $HOME/.ivy2/.sbt.ivy.lock
rm -f $HOME/.ivy2/cache/org.apache.hbase/hbase-server/ivy-1.1.1.xml
