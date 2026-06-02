#!/usr/bin/env bash
# Full end-to-end setup for the ADLS local dev environment.
#
# Usage:
#   export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
#   export ADLS_FILESYSTEM="adls-dev"          # optional, default: adls-dev
#
#   ./00-setup.sh              # normal run (skips connector build if jar exists)
#   ./00-setup.sh --clean      # tear down first, then rebuild everything from scratch
#   REBUILD=1 ./00-setup.sh   # force connector rebuild even if jar exists
#
# What it does (in order):
#   1. (optional) tear down existing stack if --clean
#   2. Validate required env vars
#   3. Build the connector fat-jar (01-build-connector.sh)
#   4. Start Docker Compose (kafka + connect)
#   5. Create Kafka topics (02-create-topics.sh)
#   6. Deploy the connector (03-deploy-connector.sh)
#   7. Produce sample records (04-produce-data.sh)
#   8. Verify files in ADLS (05-verify.sh)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="${SCRIPT_DIR}/.."
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"

step() { echo ""; echo "════════════════════════════════════════════════════"; echo "  STEP: $*"; echo "════════════════════════════════════════════════════"; }
info() { echo "  >> $*"; }

# ── Arg parsing ───────────────────────────────────────────────────────────────
CLEAN=0
for arg in "$@"; do
  case "${arg}" in
    --clean) CLEAN=1 ;;
    *) echo "Unknown argument: ${arg}"; exit 1 ;;
  esac
done

# ── Validate required env vars ────────────────────────────────────────────────
if [[ -z "${AZURE_STORAGE_CONNECTION_STRING:-}" ]]; then
  echo "ERROR: AZURE_STORAGE_CONNECTION_STRING is not set."
  echo ""
  echo "  See the README for step-by-step instructions to obtain it:"
  echo "    dev-scripts/adls-local/README.md"
  echo ""
  echo "  Quick CLI path (requires az login):"
  echo "    RG=streamreactor-dev"
  echo "    LOC=westeurope"
  echo "    ACC=srdev\$RANDOM"
  echo "    az group create -n \"\$RG\" -l \"\$LOC\""
  echo "    az storage account create -n \"\$ACC\" -g \"\$RG\" -l \"\$LOC\" --sku Standard_LRS --kind StorageV2 --hierarchical-namespace true"
  echo "    CONN=\$(az storage account show-connection-string -n \"\$ACC\" -g \"\$RG\" -o tsv)"
  echo "    az storage fs create -n adls-dev --connection-string \"\$CONN\""
  echo "    export AZURE_STORAGE_CONNECTION_STRING=\"\$CONN\""
  exit 1
fi

ADLS_FILESYSTEM="${ADLS_FILESYSTEM:-adls-dev}"
info "Using ADLS filesystem: ${ADLS_FILESYSTEM}"

# ── Clean / teardown ──────────────────────────────────────────────────────────
if [[ "${CLEAN}" -eq 1 ]]; then
  step "CLEAN — tearing down existing stack"
  bash "${SCRIPT_DIR}/99-teardown.sh" || true
fi

# ── Step 1: build connector jar ───────────────────────────────────────────────
step "1/5  Build connector fat-jar"
bash "${SCRIPT_DIR}/01-build-connector.sh"

# ── Step 2: start Kafka + Connect ─────────────────────────────────────────────
step "2/5  Start Kafka + Kafka Connect"
info "Bringing up kafka-adls and connect-adls containers ..."
docker compose -f "${COMPOSE_FILE}" up -d

info "Waiting for Kafka to become healthy ..."
for i in {1..20}; do
  STATUS=$(docker inspect -f '{{.State.Health.Status}}' kafka-adls 2>/dev/null || echo "starting")
  echo "    kafka health: ${STATUS} (attempt ${i}/20)"
  if [[ "${STATUS}" == "healthy" ]]; then
    break
  fi
  sleep 5
done
STATUS=$(docker inspect -f '{{.State.Health.Status}}' kafka-adls 2>/dev/null || echo "unknown")
if [[ "${STATUS}" != "healthy" ]]; then
  echo "ERROR: Kafka container did not become healthy. Last 30 log lines:"
  docker logs kafka-adls 2>&1 | tail -30
  exit 1
fi

info "Waiting for Kafka Connect to pass its healthcheck ..."
for i in {1..30}; do
  STATUS=$(docker inspect -f '{{.State.Health.Status}}' connect-adls 2>/dev/null || echo "starting")
  echo "    connect health: ${STATUS} (attempt ${i}/30)"
  if [[ "${STATUS}" == "healthy" ]]; then
    break
  fi
  sleep 10
done
STATUS=$(docker inspect -f '{{.State.Health.Status}}' connect-adls 2>/dev/null || echo "unknown")
if [[ "${STATUS}" != "healthy" ]]; then
  echo "ERROR: Kafka Connect container did not become healthy. Check logs:"
  echo "  docker logs connect-adls"
  exit 1
fi

# ── Step 3: create topics ─────────────────────────────────────────────────────
step "3/5  Create Kafka topics"
bash "${SCRIPT_DIR}/02-create-topics.sh"

# ── Step 4: deploy connector ──────────────────────────────────────────────────
step "4/5  Deploy adls-dev connector"
bash "${SCRIPT_DIR}/03-deploy-connector.sh"

# ── Step 5: produce data ──────────────────────────────────────────────────────
step "5/5  Produce sample records"
bash "${SCRIPT_DIR}/04-produce-data.sh"

# ── Verify ────────────────────────────────────────────────────────────────────
step "VERIFY"
bash "${SCRIPT_DIR}/05-verify.sh"

echo ""
echo "══════════════════════════════════════════════════════"
echo "  Setup complete!"
echo ""
echo "  Kafka Connect REST:  http://localhost:8083"
echo "  Connector status:    http://localhost:8083/connectors/adls-dev/status"
echo "  ADLS filesystem:     ${ADLS_FILESYSTEM}"
echo ""
echo "  Teardown:            ./dev-scripts/adls-local/scripts/99-teardown.sh"
echo "══════════════════════════════════════════════════════"
