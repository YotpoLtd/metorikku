#!/bin/bash
set -e

sbt +publishSigned sonatypeReleaseAll
