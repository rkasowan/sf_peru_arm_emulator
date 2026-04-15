#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ROOT_ENV_FILE="${ROOT_ENV_FILE:-$ROOT_DIR/.env}"

env_value() {
  local key="$1"
  awk -F= -v wanted="$key" '
    {
      current = $1
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", current)
    }
    current == wanted {
      value = substr($0, index($0, "=") + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^"/, "", value)
      gsub(/"$/, "", value)
      print value
      exit
    }
  ' "$ROOT_ENV_FILE"
}

if [[ ! -f "$ROOT_ENV_FILE" ]]; then
  echo "Missing root env file: $ROOT_ENV_FILE" >&2
  exit 1
fi

SN_INSTANCE_URL="${SN_INSTANCE_URL:-$(env_value SN_INSTANCE_URL)}"
SN_INSTANCE_URL="${SN_INSTANCE_URL:-$(env_value instance)}"
SN_USERNAME="${SN_USERNAME:-$(env_value SN_USERNAME)}"
SN_USERNAME="${SN_USERNAME:-$(env_value user)}"
SN_PASSWORD="${SN_PASSWORD:-$(env_value SN_PASSWORD)}"
SN_PASSWORD="${SN_PASSWORD:-$(env_value password)}"

if [[ -z "$SN_INSTANCE_URL" || -z "$SN_USERNAME" || -z "$SN_PASSWORD" ]]; then
  echo "Root env file must define SN_INSTANCE_URL/SN_USERNAME/SN_PASSWORD or instance/user/password." >&2
  exit 1
fi

if [[ ! -x "$SCRIPT_DIR/.venv/bin/python" ]]; then
  python3 -m venv "$SCRIPT_DIR/.venv"
  "$SCRIPT_DIR/.venv/bin/pip" install --upgrade pip wheel
  "$SCRIPT_DIR/.venv/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"
fi

export SN_INSTANCE_URL
export SN_USERNAME
export SN_PASSWORD
export SN_VERIFY_SSL="${SN_VERIFY_SSL:-true}"
export SN_REQUEST_TIMEOUT_SECONDS="${SN_REQUEST_TIMEOUT_SECONDS:-30}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export PUSH_CONNECTOR_URL="${PUSH_CONNECTOR_URL:-${SN_INSTANCE_URL%/}/api/sn_em_connector/em/inbound_event?source=genericJsonV2}"
export PUSH_CONNECTOR_METHOD="${PUSH_CONNECTOR_METHOD:-POST}"
export PUSH_CONNECTOR_HEADER_NAME="${PUSH_CONNECTOR_HEADER_NAME:-user-agent}"
export PUSH_CONNECTOR_HEADER_VALUE="${PUSH_CONNECTOR_HEADER_VALUE:-genericendpoint}"
export PUSH_CONNECTOR_TIMEOUT_SECONDS="${PUSH_CONNECTOR_TIMEOUT_SECONDS:-30}"
export PUSH_CONNECTOR_PAYLOAD_MODE="${PUSH_CONNECTOR_PAYLOAD_MODE:-single_event}"
export PUSH_CONNECTOR_WAIT_SECONDS="${PUSH_CONNECTOR_WAIT_SECONDS:-30}"
export PUSH_CONNECTOR_WAIT_POLL_SECONDS="${PUSH_CONNECTOR_WAIT_POLL_SECONDS:-2}"
export WEBHOOK_BIND_HOST="${WEBHOOK_BIND_HOST:-0.0.0.0}"
export WEBHOOK_PORT="${WEBHOOK_PORT:-8090}"
export WEBHOOK_PATH="${WEBHOOK_PATH:-/salesforce/peru}"
export WEBHOOK_AUTH_TOKEN="${WEBHOOK_AUTH_TOKEN:-}"
export WEBHOOK_EVENT_SOURCE="${WEBHOOK_EVENT_SOURCE:-salesforce}"
export WEBHOOK_EVENT_TYPE="${WEBHOOK_EVENT_TYPE:-Amazon}"
export WEBHOOK_EVENT_CLASS="${WEBHOOK_EVENT_CLASS:-salesforce}"
export WEBHOOK_METRIC_NAME="${WEBHOOK_METRIC_NAME:-salesforce_case}"

exec "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/salesforce_peru_bridge.py" "$@"
