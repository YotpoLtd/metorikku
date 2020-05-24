#!/bin/bash
set -e

travis_fold start "build"
  travis_time_start
    ./scripts/build.sh
  travis_time_finish
travis_fold end "build"

travis_fold start "docker"
  travis_time_start
    ./scripts/docker.sh
  travis_time_finish
travis_fold end "docker"

travis_fold start "docker_publish_dev"
  travis_time_start
    ./scripts/docker_publish_dev.sh
  travis_time_finish
travis_fold end "docker_publish_dev"
