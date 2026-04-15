#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ROOT_ENV_FILE="${ROOT_ENV_FILE:-$ROOT_DIR/.env}"
PATCH_FILE="${PATCH_FILE:-$ROOT_DIR/servicenow_generic_mapped_json_bundle_v8/src/USBEM_Core.genericJsonV2.salesforce_peru.js}"
USBEM_CORE_API_NAME="${USBEM_CORE_API_NAME:-x_usbna_usb_event.USBEM_Core}"
ALLOW_PLATFORM_PATCHES="${ALLOW_PLATFORM_PATCHES:-}"

if [[ "$ALLOW_PLATFORM_PATCHES" != "PDI_ONLY_I_UNDERSTAND" ]]; then
  echo "Refusing to patch ServiceNow platform records." >&2
  echo "This script is for explicit PDI/lab use only." >&2
  echo "If you really intend to patch sys_script_include, rerun with:" >&2
  echo "  ALLOW_PLATFORM_PATCHES=PDI_ONLY_I_UNDERSTAND ./apply_usb_core_patch.sh" >&2
  exit 1
fi

env_value() {
  local key="$1"
  awk -F= -v wanted="$key" '
    $1 == wanted {
      gsub(/^"/, "", $2)
      gsub(/"$/, "", $2)
      print $2
      exit
    }
  ' "$ROOT_ENV_FILE"
}

if [[ ! -f "$ROOT_ENV_FILE" ]]; then
  echo "Missing root env file: $ROOT_ENV_FILE" >&2
  exit 1
fi

if [[ ! -f "$PATCH_FILE" ]]; then
  echo "Patch file not found: $PATCH_FILE" >&2
  exit 1
fi

INSTANCE_URL="$(env_value instance)"
USERNAME="$(env_value user)"
PASSWORD="$(env_value password)"

if [[ -z "$INSTANCE_URL" || -z "$USERNAME" || -z "$PASSWORD" ]]; then
  echo "Root env file must define instance, user, and password." >&2
  exit 1
fi

script_include_sys_id="$(
  curl -sS -u "$USERNAME:$PASSWORD" \
    "${INSTANCE_URL%/}/api/now/table/sys_script_include?sysparm_query=api_name=${USBEM_CORE_API_NAME}&sysparm_fields=sys_id,api_name&sysparm_limit=1" \
    | jq -r '.result[0].sys_id // empty'
)"

if [[ -z "$script_include_sys_id" ]]; then
  echo "Could not find sys_script_include for api_name=${USBEM_CORE_API_NAME}" >&2
  exit 1
fi

payload_file="$(mktemp)"
trap 'rm -f "$payload_file"' EXIT
jq -Rs '{script: .}' "$PATCH_FILE" > "$payload_file"

curl -sS -u "$USERNAME:$PASSWORD" \
  -X PATCH \
  -H 'Content-Type: application/json' \
  --data @"$payload_file" \
  "${INSTANCE_URL%/}/api/now/table/sys_script_include/${script_include_sys_id}?sysparm_fields=sys_id,sys_updated_on,sys_mod_count" \
  | jq .
