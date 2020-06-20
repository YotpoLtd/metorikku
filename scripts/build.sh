#!/bin/bash
set -e

sbt +clean scalastyle "set test in (assembly) := {}" +assembly "set test in (Test, assembly) := {}" +test:assembly
