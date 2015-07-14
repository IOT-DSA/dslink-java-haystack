#!/usr/bin/env bash
set -e
rm -rf build
./gradlew clean distZip --refresh-dependencies
cp build/distributions/*.zip ../../files/haystack.zip
