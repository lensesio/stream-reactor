#!/usr/bin/env bash
# Build the kafka-connect-azure-datalake fat-jar using JDK 17 and copy it to ./connectors/.
# Usage:
#   ./01-build-connector.sh          # skip if jar already present
#   REBUILD=1 ./01-build-connector.sh  # always rebuild

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
CONNECTOR_DIR="${SCRIPT_DIR}/../connectors"
SBT_MODULE="kafka-connect-azure-datalake"
JAR_GLOB="${REPO_ROOT}/${SBT_MODULE}/target/libs/*.jar"

echo "==> [01-build-connector] repo root: ${REPO_ROOT}"

# ── JDK 17 detection ────────────────────────────────────────────────────────
detect_java17() {
  # 1. Honour explicit JAVA_HOME if it points at Java 17
  if [[ -n "${JAVA_HOME:-}" ]]; then
    local ver
    ver=$("${JAVA_HOME}/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
    if [[ "${ver}" == "17" ]]; then
      echo "${JAVA_HOME}"
      return 0
    fi
  fi

  # 2. macOS: /usr/libexec/java_home
  if command -v /usr/libexec/java_home &>/dev/null; then
    local jh
    jh=$(/usr/libexec/java_home -v 17 2>/dev/null || true)
    if [[ -n "${jh}" ]]; then
      echo "${jh}"
      return 0
    fi
  fi

  # 3. SDKMAN
  local sdk_dir="${HOME}/.sdkman/candidates/java"
  if [[ -d "${sdk_dir}" ]]; then
    local candidate
    candidate=$(find "${sdk_dir}" -maxdepth 1 -name "17*" -type d | sort | tail -1)
    if [[ -n "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  fi

  # 4. Common Linux paths
  for dir in /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk /usr/lib/jvm/temurin-17; do
    if [[ -d "${dir}" ]]; then
      echo "${dir}"
      return 0
    fi
  done

  return 1
}

JAVA17_HOME=$(detect_java17 || true)
if [[ -z "${JAVA17_HOME}" ]]; then
  echo "ERROR: JDK 17 not found."
  echo "  macOS:  brew install --cask temurin@17"
  echo "  Linux:  sudo apt-get install -y temurin-17-jdk  (or equivalent)"
  echo "  Manual: set JAVA_HOME to your JDK 17 installation."
  exit 1
fi

export JAVA_HOME="${JAVA17_HOME}"
export PATH="${JAVA_HOME}/bin:${PATH}"

JAVA_REPORTED=$("${JAVA_HOME}/bin/java" -version 2>&1 | head -1)
echo "==> [01-build-connector] using Java: ${JAVA_REPORTED} (JAVA_HOME=${JAVA_HOME})"

# ── Skip if already built ────────────────────────────────────────────────────
if [[ "${REBUILD:-0}" != "1" ]]; then
  existing=$(ls "${CONNECTOR_DIR}"/*.jar 2>/dev/null | head -1 || true)
  if [[ -n "${existing}" ]]; then
    echo "==> [01-build-connector] connector jar already present: ${existing}"
    echo "    Set REBUILD=1 to force a rebuild."
    exit 0
  fi
fi

# ── Build ────────────────────────────────────────────────────────────────────
echo "==> [01-build-connector] running sbt assembly for ${SBT_MODULE} ..."
cd "${REPO_ROOT}"
sbt "project azure-datalake" assembly

# ── Copy jar ─────────────────────────────────────────────────────────────────
JAR=$(ls ${JAR_GLOB} 2>/dev/null | head -1 || true)
if [[ -z "${JAR}" ]]; then
  echo "ERROR: no jar found at ${JAR_GLOB} after build."
  exit 1
fi

mkdir -p "${CONNECTOR_DIR}"
cp -v "${JAR}" "${CONNECTOR_DIR}/"
echo "==> [01-build-connector] connector jar ready: ${CONNECTOR_DIR}/$(basename "${JAR}")"
