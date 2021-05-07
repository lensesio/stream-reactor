#!/usr/bin/env bash

set -x

env | sort

if [[ -v "${GITHUB_HEAD_REF}" ]]; then
  git fetch
  MODIFIED_MODULES=$(git diff --name-only "remotes/origin/${GITHUB_BASE_REF}" "remotes/origin/${GITHUB_HEAD_REF}" | tail -n +2 | cut -d/ -f1 | sort | uniq)
else
  MODIFIED_MODULES=$(git show --name-only --oneline HEAD | tail -n +2 | cut -d/ -f1 | sort | uniq)
fi

BUILD_ALL=false

for module in ${MODIFIED_MODULES}; do
  if [[ ${module} != kafka-connect-* || ${module} == kafka-connect-common ]]; then
      BUILD_ALL=true
      break
  fi
done

sleep 3m

if $BUILD_ALL; then
    ./gradlew clean test
else
    GRADLE_TASKS=""
    for module in ${MODIFIED_MODULES}; do
        GRADLE_TASKS="${GRADLE_TASKS} :${module}:test"
    done

    ./gradlew clean ${GRADLE_TASKS}
fi
