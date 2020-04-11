#!/bin/bash
set -e

./scripts/build.sh

travis_fold start "tests"
  travis_time_start
    ./scripts/test.sh
  travis_time_finish
travis_fold end "tests"

travis_fold start "influxdb"
  travis_time_start
    (cd e2e/influxdb && ./test.sh)
  travis_time_finish
travis_fold end "influxdb"


travis_fold start "kafka"
(cd e2e/kafka && ./test.sh)
travis_fold end "kafka"

travis_fold start "elasticsearch"
(cd e2e/elasticsearch && ./test.sh)
travis_fold end "elasticsearch"

travis_fold start "hive1"
(cd e2e/hive1 && ./test.sh)
travis_fold end "hive1"

travis_fold start "hive"
(cd e2e/hive && ./test.sh)
travis_fold end "hive"

travis_fold start "hudi"
(cd e2e/hudi && ./test.sh)
travis_fold end "hudi"

travis_fold start "cdc"
(cd e2e/cdc && ./test.sh)
travis_fold end "cdc"
