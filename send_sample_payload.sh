#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ROOT_ENV_FILE="${ROOT_ENV_FILE:-$ROOT_DIR/.env}"
PAYLOAD_FILE="${1:-$ROOT_DIR/peru_example_paylaod.json}"

env_value() {
  local key="$1"
  local raw_value=""
  raw_value="$(
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
  )"
  if [[ "$raw_value" == OBFMD5:* ]]; then
    python3 "$SCRIPT_DIR/secret_codec.py" decode "$raw_value"
    return 0
  fi
  printf '%s' "$raw_value"
}

if [[ ! -f "$ROOT_ENV_FILE" ]]; then
  echo "Missing root env file: $ROOT_ENV_FILE" >&2
  exit 1
fi

if [[ ! -f "$PAYLOAD_FILE" ]]; then
  echo "Payload file not found: $PAYLOAD_FILE" >&2
  exit 1
fi

INSTANCE_URL="${SN_INSTANCE_URL:-$(env_value SN_INSTANCE_URL)}"
INSTANCE_URL="${INSTANCE_URL:-$(env_value instance)}"
USERNAME="${SN_USERNAME:-$(env_value SN_USERNAME)}"
USERNAME="${USERNAME:-$(env_value user)}"
PASSWORD="${SN_PASSWORD:-$(env_value SN_PASSWORD)}"
PASSWORD="${PASSWORD:-$(env_value password)}"
PUSH_CONNECTOR_URL="${PUSH_CONNECTOR_URL:-${INSTANCE_URL%/}/api/sn_em_connector/em/inbound_event?source=genericJsonV2}"
PUSH_CONNECTOR_HEADER_NAME="${PUSH_CONNECTOR_HEADER_NAME:-user-agent}"
PUSH_CONNECTOR_HEADER_VALUE="${PUSH_CONNECTOR_HEADER_VALUE:-genericendpoint}"
HTTPS_PROXY_VALUE="${HTTPS_PROXY:-${https_proxy:-$(env_value HTTPS_PROXY)}}"
HTTP_PROXY_VALUE="${HTTP_PROXY:-${http_proxy:-$(env_value HTTP_PROXY)}}"
NO_PROXY_VALUE="${NO_PROXY:-${no_proxy:-$(env_value NO_PROXY)}}"

if [[ -z "$INSTANCE_URL" || -z "$USERNAME" || -z "$PASSWORD" ]]; then
  echo "Root env file must define SN_INSTANCE_URL/SN_USERNAME/SN_PASSWORD or instance/user/password." >&2
  exit 1
fi

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

exec curl -sS \
  -u "$USERNAME:$PASSWORD" \
  -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -H "$PUSH_CONNECTOR_HEADER_NAME: $PUSH_CONNECTOR_HEADER_VALUE" \
  --data @"$PAYLOAD_FILE" \
  "$PUSH_CONNECTOR_URL"
