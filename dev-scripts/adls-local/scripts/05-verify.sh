#!/usr/bin/env bash
# Verify that files have landed in ADLS after the connector processed the
# records produced by 04-produce-data.sh.
#
# Checks:
#   - Connector state is RUNNING
#   - All 3 connector tasks are RUNNING
#   - >= 6 JSON files appear in the ADLS filesystem (via az CLI if available,
#     otherwise falls back to printing the Connect offset REST endpoint and
#     instructing the user to check the Azure Portal).
#
# Required env vars (same as 03-deploy-connector.sh):
#   AZURE_STORAGE_CONNECTION_STRING
#
# Optional:
#   ADLS_FILESYSTEM   (default: adls-dev)

set -euo pipefail

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="adls-dev"
ADLS_FILESYSTEM="${ADLS_FILESYSTEM:-adls-dev}"
EXPECTED_FILES=6

FAIL=0

pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*"; FAIL=1; }

# ── Connector + task status ────────────────────────────────────────────────────
echo "==> [05-verify] checking connector and task status ..."
STATUS_JSON=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)

if command -v jq &>/dev/null; then
  CONN_STATE=$(echo "${STATUS_JSON}" | jq -r '.connector.state // "unknown"')
  TASK_COUNT=$(echo "${STATUS_JSON}" | jq '.tasks | length')
  FAILED_TASKS=$(echo "${STATUS_JSON}" | jq '[.tasks[] | select(.state != "RUNNING")] | length')
else
  CONN_STATE=$(echo "${STATUS_JSON}" | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4 || true)
  TASK_COUNT=0
  FAILED_TASKS=0
fi

if [[ "${CONN_STATE}" == "RUNNING" ]]; then
  pass "connector '${CONNECTOR_NAME}' is RUNNING"
else
  fail "connector '${CONNECTOR_NAME}' state is '${CONN_STATE:-unknown}' (expected RUNNING)"
fi

if command -v jq &>/dev/null; then
  if [[ "${TASK_COUNT}" -eq 0 ]]; then
    fail "connector has no tasks (deployment may not have started)"
  elif [[ "${FAILED_TASKS}" -gt 0 ]]; then
    fail "${FAILED_TASKS}/${TASK_COUNT} task(s) are not RUNNING:"
    echo "${STATUS_JSON}" | jq '.tasks[] | select(.state != "RUNNING") | {id: .id, state: .state, trace: .trace}'
  else
    pass "all ${TASK_COUNT} task(s) are RUNNING"
  fi
else
  echo "    (jq not available — skipping detailed task check)"
fi

# Dump Connect logs on task failure
if [[ "${FAIL}" -eq 1 ]]; then
  echo ""
  echo "==> [05-verify] Connect worker logs (last 40 lines):"
  echo "──────────────────────────────────────────────────"
  docker logs connect-adls 2>&1 | tail -40
  echo "──────────────────────────────────────────────────"
fi

# ── Wait for connector to flush ───────────────────────────────────────────────
echo ""
echo "==> [05-verify] waiting 15 s for connector to flush to ADLS ..."
sleep 15

# ── ADLS file count check ─────────────────────────────────────────────────────
echo ""
echo "==> [05-verify] checking files in ADLS filesystem '${ADLS_FILESYSTEM}' ..."

if [[ -z "${AZURE_STORAGE_CONNECTION_STRING:-}" ]]; then
  echo "  WARNING: AZURE_STORAGE_CONNECTION_STRING is not set — skipping az CLI check."
  echo "  Please verify manually in the Azure Portal:"
  echo "    Storage account → Data storage → Containers → ${ADLS_FILESYSTEM}"
  echo "    Expect at least ${EXPECTED_FILES} JSON files."
elif command -v az &>/dev/null; then
  echo "  Using az CLI to count files ..."

  FILE_COUNT=$(az storage fs file list \
    --file-system "${ADLS_FILESYSTEM}" \
    --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" \
    --recursive \
    --output tsv \
    --query "[?ends_with(name, '.json')] | length(@)" 2>/dev/null || echo "0")

  # Fallback: az may output a table; count non-header lines
  if [[ -z "${FILE_COUNT}" || "${FILE_COUNT}" == "0" ]]; then
    FILE_COUNT=$(az storage fs file list \
      --file-system "${ADLS_FILESYSTEM}" \
      --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" \
      --recursive \
      --output json 2>/dev/null \
      | (command -v jq &>/dev/null && jq '[.[] | select(.name | endswith(".json"))] | length' || echo "0"))
  fi

  echo "==> [05-verify] files found in '${ADLS_FILESYSTEM}': ${FILE_COUNT} (expected >= ${EXPECTED_FILES})"

  if [[ "${FILE_COUNT:-0}" -ge "${EXPECTED_FILES}" ]]; then
    pass "filesystem '${ADLS_FILESYSTEM}' has ${FILE_COUNT} JSON file(s) (>= ${EXPECTED_FILES})"
    echo ""
    echo "  File listing:"
    az storage fs file list \
      --file-system "${ADLS_FILESYSTEM}" \
      --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" \
      --recursive \
      --output table 2>/dev/null || true
  else
    fail "filesystem '${ADLS_FILESYSTEM}' has ${FILE_COUNT:-0} JSON file(s) (expected >= ${EXPECTED_FILES})"
    echo ""
    echo "  Diagnostics:"
    echo "    docker logs connect-adls 2>&1 | tail -50"
    echo "    curl http://localhost:8083/connectors/${CONNECTOR_NAME}/status | jq ."
    echo "    az storage fs file list --file-system ${ADLS_FILESYSTEM} --connection-string \"\$AZURE_STORAGE_CONNECTION_STRING\" --recursive --output table"
  fi
else
  echo "  az CLI not found — skipping remote file count check."
  echo "  Install: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
  echo ""
  echo "  Falling back to connector offset check via REST API ..."
  OFFSETS_JSON=$(curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/offsets" 2>/dev/null || true)
  echo "  Connector offsets:"
  echo "${OFFSETS_JSON}" | (command -v jq &>/dev/null && jq . || cat)
  echo ""
  echo "  Verify manually in the Azure Portal:"
  echo "    Storage account → Data storage → Containers → ${ADLS_FILESYSTEM}"
  echo "    Expect at least ${EXPECTED_FILES} JSON files."
fi

# ── Result ────────────────────────────────────────────────────────────────────
echo ""
if [[ "${FAIL}" -eq 0 ]]; then
  echo "==> [05-verify] ALL CHECKS PASSED"
  exit 0
else
  echo ""
  echo "==> [05-verify] SOME CHECKS FAILED — see [FAIL] lines above"
  echo "    Useful diagnostics:"
  echo "      docker logs connect-adls 2>&1 | tail -50"
  echo "      curl http://localhost:8083/connectors/${CONNECTOR_NAME}/status | jq ."
  exit 1
fi
