#!/bin/bash
set -e

echo "Executing SBT publish"
sbt --warn +publishSigned sonatypeReleaseAll
