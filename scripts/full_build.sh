#!/bin/bash
set -e

./scripts/build.sh

travis_fold start "test"
./scripts/test.sh
travis_fold end "tests"

travis_fold start "influxdb e2e"
(cd e2e/influxdb && ./test.sh)
travis_fold end "influxdb e2e"

travis_fold start "kafka e2e"
(cd e2e/kafka && ./test.sh)
travis_fold end "kafka e2e"

travis_fold start "elasticsearch e2e"
(cd e2e/elasticsearch && ./test.sh)
travis_fold end "elasticsearch e2e"

travis_fold start "hive 1 e2e"
(cd e2e/hive1 && ./test.sh)
travis_fold end "hive 1 e2e"

travis_fold start "hive e2e"
(cd e2e/hive && ./test.sh)
travis_fold end "hive e2e"

travis_fold start "hudi e2e"
(cd e2e/hudi && ./test.sh)
travis_fold end "hudi e2e"

travis_fold start "cdc e2e"
(cd e2e/cdc && ./test.sh)
travis_fold end "cdc e2e"
