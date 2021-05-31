#!/bin/bash
set -e

echo "Executing SBT publish"
sbt +publishSigned sonatypeReleaseAll > /dev/null
