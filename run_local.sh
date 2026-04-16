#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ROOT_ENV_FILE="${ROOT_ENV_FILE:-$ROOT_DIR/.env}"
STATE_FILE_DEFAULT="${SCRIPT_DIR}/state.local.json"

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
HTTPS_PROXY_VALUE="${HTTPS_PROXY:-${https_proxy:-$(env_value HTTPS_PROXY)}}"
HTTP_PROXY_VALUE="${HTTP_PROXY:-${http_proxy:-$(env_value HTTP_PROXY)}}"
NO_PROXY_VALUE="${NO_PROXY:-${no_proxy:-$(env_value NO_PROXY)}}"

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
if [[ -n "$HTTPS_PROXY_VALUE" ]]; then
  export HTTPS_PROXY="$HTTPS_PROXY_VALUE"
  export https_proxy="$HTTPS_PROXY_VALUE"
fi
if [[ -n "$HTTP_PROXY_VALUE" ]]; then
  export HTTP_PROXY="$HTTP_PROXY_VALUE"
  export http_proxy="$HTTP_PROXY_VALUE"
fi
if [[ -n "$NO_PROXY_VALUE" ]]; then
  export NO_PROXY="$NO_PROXY_VALUE"
  export no_proxy="$NO_PROXY_VALUE"
fi
export SN_VERIFY_SSL="${SN_VERIFY_SSL:-true}"
export SN_REQUEST_TIMEOUT_SECONDS="${SN_REQUEST_TIMEOUT_SECONDS:-30}"
export POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-3}"
export QUERY_OVERLAP_SECONDS="${QUERY_OVERLAP_SECONDS:-45}"
export INITIAL_LOOKBACK_SECONDS="${INITIAL_LOOKBACK_SECONDS:-3600}"
export ALERT_QUERY_LIMIT="${ALERT_QUERY_LIMIT:-100}"
export ALERT_BOOTSTRAP_LIMIT="${ALERT_BOOTSTRAP_LIMIT:-25}"
export ALERT_DISCOVERY_HYDRATE_LIMIT="${ALERT_DISCOVERY_HYDRATE_LIMIT:-25}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export STATE_FILE="${STATE_FILE:-$STATE_FILE_DEFAULT}"
export ALERT_SOURCE="${ALERT_SOURCE:-GenericJSON}"
export ALERT_TYPE_CONTAINS="${ALERT_TYPE_CONTAINS:-Amazon,peru}"
export ALERT_QUERY_EXTRA="${ALERT_QUERY_EXTRA:-}"
export ALERT_OPEN_STATE_TOKENS="${ALERT_OPEN_STATE_TOKENS:-open,reopen,reopened,new,active}"
export ALERT_CLOSED_STATE_TOKENS="${ALERT_CLOSED_STATE_TOKENS:-closed,resolved,clear,cleared}"
export PUSH_CONNECTOR_URL="${PUSH_CONNECTOR_URL:-${SN_INSTANCE_URL%/}/api/sn_em_connector/em/inbound_event?source=genericJsonV2}"
export PUSH_CONNECTOR_METHOD="${PUSH_CONNECTOR_METHOD:-POST}"
export PUSH_CONNECTOR_HEADER_NAME="${PUSH_CONNECTOR_HEADER_NAME:-user-agent}"
export PUSH_CONNECTOR_HEADER_VALUE="${PUSH_CONNECTOR_HEADER_VALUE:-genericendpoint}"
export PUSH_CONNECTOR_TIMEOUT_SECONDS="${PUSH_CONNECTOR_TIMEOUT_SECONDS:-90}"
export PUSH_CONNECTOR_PAYLOAD_MODE="${PUSH_CONNECTOR_PAYLOAD_MODE:-single_event}"
export PUSH_CONNECTOR_WAIT_SECONDS="${PUSH_CONNECTOR_WAIT_SECONDS:-30}"
export PUSH_CONNECTOR_WAIT_POLL_SECONDS="${PUSH_CONNECTOR_WAIT_POLL_SECONDS:-2}"
export PINC_TAG_SYS_ID="${PINC_TAG_SYS_ID:-}"
export DEFAULT_CMDB_CI_SYS_ID="${DEFAULT_CMDB_CI_SYS_ID:-}"
export DEFAULT_CMDB_CI_NAME="${DEFAULT_CMDB_CI_NAME:-}"
export SALESFORCE_USER_SYS_ID="${SALESFORCE_USER_SYS_ID:-}"
export INCIDENT_EXTERNAL_CASE_FIELD="${INCIDENT_EXTERNAL_CASE_FIELD:-u_external_salesforce_case_id}"
export INCIDENT_GENERATING_ALERT_FIELD="${INCIDENT_GENERATING_ALERT_FIELD:-u_generating_alert}"
export SET_ASSIGNED_TO="${SET_ASSIGNED_TO:-false}"
export SET_CALLER_ID="${SET_CALLER_ID:-false}"
export ENABLE_TAGGING="${ENABLE_TAGGING:-false}"
export ENABLE_DTI_FALLBACK="${ENABLE_DTI_FALLBACK:-false}"
export ENABLE_CI_LOOKUP="${ENABLE_CI_LOOKUP:-true}"
export DEFAULT_ASSIGNMENT_GROUP_SYS_ID="${DEFAULT_ASSIGNMENT_GROUP_SYS_ID:-}"
export INCIDENT_EXTRA_STATIC_FIELDS_JSON="${INCIDENT_EXTRA_STATIC_FIELDS_JSON:-}"
export SALESFORCE_OFFERING_SERVICE_NAME="${SALESFORCE_OFFERING_SERVICE_NAME:-Salesforce PERU Services}"
export AUTO_CREATE_SERVICE_OFFERINGS="${AUTO_CREATE_SERVICE_OFFERINGS:-false}"
export DRY_RUN="${DRY_RUN:-false}"

if [[ "${1:-}" == "--continuous" ]]; then
  shift
fi

if [[ $# -eq 0 ]]; then
  set -- --once
fi

exec "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/salesforce_peru_arm_emulator.py" "$@"
