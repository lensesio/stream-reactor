#!/usr/bin/env bash

set -x

MODIFIED_MODULES=$(git show --name-only --oneline HEAD | tail -n +2 | cut -d/ -f1 | sort | uniq )

BUILD_ALL=false

for module in ${MODIFIED_MODULES}; do
  if [[ ${module} != kafka-connect-* || ${module} == kafka-connect-common ]]; then
      BUILD_ALL=true
      break
  fi
done

if $BUILD_ALL; then
    ./gradlew clean test --parallel
else
    GRADLE_TASKS=""
    for module in ${MODIFIED_MODULES}; do
        GRADLE_TASKS="${GRADLE_TASKS} :${module}:test"
    done

    ./gradlew clean ${GRADLE_TASKS} --parallel
fi
