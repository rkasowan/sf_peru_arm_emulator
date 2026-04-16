#!/usr/bin/env bash
set -euo pipefail

SERVICE_PREFIX="${SERVICE_PREFIX:-sf-peru-incident-management-process}"
INSTANCE_PROFILE="${INSTANCE_PROFILE:-}"
SERVICE_NAME="${SERVICE_NAME:-}"
APP_DIR="${APP_DIR:-}"
CONFIG_DIR="${CONFIG_DIR:-}"
CONFIG_FILE="${CONFIG_FILE:-}"
STATE_DIR="${STATE_DIR:-}"
STATE_FILE="${STATE_FILE:-}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_ENV_FILE="${SOURCE_ENV_FILE:-}"
AUTO_SOURCE_ENV_FILE=0
PYTHON_BIN="${PYTHON_BIN:-python3}"
RUN_AS_USER="${RUN_AS_USER:-${SUDO_USER:-$(id -un)}}"
RUN_AS_GROUP="${RUN_AS_GROUP:-$(id -gn "$RUN_AS_USER" 2>/dev/null || id -gn)}"
PROMPT_FOR_CONFIG="${PROMPT_FOR_CONFIG:-auto}"
ENABLE_SERVICE=0
RUN_PROBE=1
PROBE_ONLY=0
LEGACY_DAEMON_RELOAD=0

usage() {
  cat <<USAGE
Usage:
  ./install.sh                  Prompt for an instance profile and config, then deploy it disabled
  ./install.sh --enable-now     Install/update everything, then enable and start the service
  ./install.sh --no-probe       Install/update everything but skip the probe
  ./install.sh --probe-only     Deploy/update everything, then just run the alert probe
  ./install.sh --non-interactive  Skip prompts and use the env file as-is
  ./install.sh --prompt         Force prompts even if stdin is not a TTY

Optional environment overrides:
  INSTANCE_PROFILE=uat
  SERVICE_PREFIX=sf-peru-incident-management-process
  SERVICE_NAME=sf-peru-incident-management-process-uat
  APP_DIR=/opt/sf-peru-incident-management-process-uat
  CONFIG_DIR=/etc/sf-peru-incident-management-process-uat
  CONFIG_FILE=/etc/sf-peru-incident-management-process-uat/.env
  STATE_DIR=/var/lib/sf-peru-incident-management-process-uat
  STATE_FILE=/var/lib/sf-peru-incident-management-process-uat/state.json
  SOURCE_ENV_FILE=/path/to/uat.env   # optional persistent staging file
  RUN_AS_USER=$(id -un)
  RUN_AS_GROUP=$(id -gn)
  PYTHON_BIN=python3
  PROMPT_FOR_CONFIG=auto|1|0
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --enable-now)
      ENABLE_SERVICE=1
      shift
      ;;
    --no-probe)
      RUN_PROBE=0
      shift
      ;;
    --probe-only)
      PROBE_ONLY=1
      ENABLE_SERVICE=0
      RUN_PROBE=1
      shift
      ;;
    --non-interactive)
      PROMPT_FOR_CONFIG=0
      shift
      ;;
    --prompt)
      PROMPT_FOR_CONFIG=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

cleanup() {
  if [[ "${AUTO_SOURCE_ENV_FILE:-0}" -eq 1 && -n "${SOURCE_ENV_FILE:-}" && -f "${SOURCE_ENV_FILE:-}" ]]; then
    rm -f "$SOURCE_ENV_FILE"
  fi
}

trap cleanup EXIT

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

need_cmd "$PYTHON_BIN"
need_cmd systemctl

SUDO=""
if [[ $EUID -ne 0 ]]; then
  need_cmd sudo
  SUDO="sudo"
fi

slugify() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//; s/-+/-/g'
}

env_value_from_file() {
  local file="$1"
  local key="$2"
  [[ -f "$file" ]] || return 0
  awk -v wanted="$key" '
    $0 ~ ("^" wanted "[[:space:]]*[:=]") {
      value = $0
      sub(/^[^:=]*[:=][[:space:]]*/, "", value)
      gsub(/^"/, "", value)
      gsub(/"$/, "", value)
      print value
      exit
    }
  ' "$file"
}

env_file_value() {
  env_value_from_file "$SOURCE_ENV_FILE" "$1"
}

seed_value_is_placeholder() {
  local key="$1"
  local value="$2"

  case "$key" in
    SN_INSTANCE_URL)
      [[ -z "$value" || "$value" == "https://your-instance.service-now.com" ]]
      ;;
    SN_USERNAME)
      [[ -z "$value" || "$value" == your_* ]]
      ;;
    SN_PASSWORD)
      [[ -z "$value" || "$value" == your_* ]]
      ;;
    PUSH_CONNECTOR_URL)
      [[ -z "$value" || "$value" == "https://your-instance.service-now.com/api/sn_em_connector/em/inbound_event?source=genericJsonV2" ]]
      ;;
    *)
      [[ -z "$value" ]]
      ;;
  esac
}

seed_env_value() {
  local key="$1"
  local value=""
  local fallback=""
  local candidate_file=""

  for candidate_file in "$SOURCE_ENV_FILE" "$CONFIG_FILE" "$SCRIPT_DIR/.env.example"; do
    [[ -n "$candidate_file" && -f "$candidate_file" ]] || continue
    value="$(env_value_from_file "$candidate_file" "$key")"
    [[ -n "$value" ]] || continue
    if [[ -z "$fallback" ]]; then
      fallback="$value"
    fi
    if ! seed_value_is_placeholder "$key" "$value"; then
      printf '%s' "$value"
      return 0
    fi
  done

  printf '%s' "$fallback"
}

set_env_file_value() {
  local file="$1"
  local key="$2"
  local value="$3"
  local tmp

  value="${value//$'\r'/}"
  value="${value//$'\n'/}"
  tmp="$(mktemp)"
  awk -v key="$key" -v value="$value" '
    function quoted(text, safe) {
      safe = text
      gsub(/\\/, "\\\\", safe)
      gsub(/"/, "\\\"", safe)
      return "\"" safe "\""
    }
    BEGIN {
      updated = 0
      skip_broken_continuation = 0
      rendered = (value == "" ? key "=" : key "=" quoted(value))
    }
    skip_broken_continuation == 1 {
      if ($0 ~ /^[[:space:]]*$/ || $0 ~ /^[[:space:]]*#/ || $0 ~ /^[A-Za-z_][A-Za-z0-9_]*[[:space:]]*[:=]/) {
        skip_broken_continuation = 0
      } else {
        next
      }
    }
    $0 ~ ("^" key "[[:space:]]*[:=]") {
      print rendered
      updated = 1
      skip_broken_continuation = 1
      next
    }
    {
      print
    }
    END {
      if (!updated) {
        print rendered
      }
    }
  ' "$file" > "$tmp"
  mv "$tmp" "$file"
}

ensure_source_env_file() {
  local source_dir

  source_dir="$(dirname "$SOURCE_ENV_FILE")"
  mkdir -p "$source_dir"

  if [[ ! -f "$SOURCE_ENV_FILE" ]]; then
    cp "$SCRIPT_DIR/.env.example" "$SOURCE_ENV_FILE"
    chmod 600 "$SOURCE_ENV_FILE" 2>/dev/null || true
    echo "Created $SOURCE_ENV_FILE from $SCRIPT_DIR/.env.example"
  fi
}

infer_instance_profile_seed() {
  local seed=""
  local base=""

  if [[ -n "$INSTANCE_PROFILE" ]]; then
    seed="$INSTANCE_PROFILE"
  elif [[ -n "$SERVICE_NAME" && "$SERVICE_NAME" == "${SERVICE_PREFIX}-"* ]]; then
    seed="${SERVICE_NAME#${SERVICE_PREFIX}-}"
  elif [[ -n "$SOURCE_ENV_FILE" ]]; then
    base="$(basename "$SOURCE_ENV_FILE")"
    if [[ "$base" != ".env" ]]; then
      seed="${base%.*}"
    fi
  fi

  printf '%s' "$(slugify "$seed")"
}

prompt_instance_profile() {
  local seed=""
  local response=""
  local slug=""

  if [[ -n "$INSTANCE_PROFILE" ]]; then
    INSTANCE_PROFILE="$(slugify "$INSTANCE_PROFILE")"
    return 0
  fi

  seed="$(infer_instance_profile_seed)"
  if [[ ! -z "$seed" && ! -t 0 ]]; then
    INSTANCE_PROFILE="$seed"
    return 0
  fi

  if ! prompt_enabled; then
    INSTANCE_PROFILE="$seed"
    return 0
  fi

  while true; do
    if [[ -n "$seed" ]]; then
      read -r -p "Install label for this instance (dev/it/uat/prod) [$seed]: " response
      [[ -z "$response" ]] && response="$seed"
    else
      read -r -p "Install label for this instance (dev/it/uat/prod): " response
    fi
    slug="$(slugify "$response")"
    if [[ -z "$slug" ]]; then
      echo "Please enter a short label like dev, it, uat, or prod."
      continue
    fi
    INSTANCE_PROFILE="$slug"
    return 0
  done
}

apply_instance_defaults() {
  local suffix=""

  if [[ -n "$INSTANCE_PROFILE" ]]; then
    suffix="-$INSTANCE_PROFILE"
  fi

  if [[ -z "$SERVICE_NAME" ]]; then
    SERVICE_NAME="${SERVICE_PREFIX}${suffix}"
  fi
  if [[ -z "$APP_DIR" ]]; then
    APP_DIR="/opt/${SERVICE_NAME}"
  fi
  if [[ -z "$CONFIG_DIR" ]]; then
    CONFIG_DIR="/etc/${SERVICE_NAME}"
  fi
  if [[ -z "$CONFIG_FILE" ]]; then
    CONFIG_FILE="${CONFIG_DIR}/.env"
  fi
  if [[ -z "$STATE_DIR" ]]; then
    STATE_DIR="/var/lib/${SERVICE_NAME}"
  fi
  if [[ -z "$STATE_FILE" ]]; then
    STATE_FILE="${STATE_DIR}/state.json"
  fi
  if [[ -z "$SOURCE_ENV_FILE" ]]; then
    SOURCE_ENV_FILE="$(mktemp "${TMPDIR:-/tmp}/${SERVICE_NAME}.XXXXXX.env")"
    AUTO_SOURCE_ENV_FILE=1
    rm -f "$SOURCE_ENV_FILE"
  fi
}

prompt_enabled() {
  case "$(printf '%s' "$PROMPT_FOR_CONFIG" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|y|on)
      return 0
      ;;
    0|false|no|n|off)
      return 1
      ;;
    auto)
      [[ -t 0 && -t 1 ]]
      return
      ;;
    *)
      [[ -t 0 && -t 1 ]]
      return
      ;;
  esac
}

path_exists() {
  [[ -e "$1" || -L "$1" ]]
}

prompt_yes_no() {
  local label="$1"
  local default="${2:-N}"
  local response=""
  local suffix="[y/N]"

  case "$(printf '%s' "$default" | tr '[:upper:]' '[:lower:]')" in
    y|yes)
      suffix="[Y/n]"
      ;;
  esac

  while true; do
    read -r -p "$label $suffix: " response
    if [[ -z "$response" ]]; then
      case "$(printf '%s' "$default" | tr '[:upper:]' '[:lower:]')" in
        y|yes)
          return 0
          ;;
        *)
          return 1
          ;;
      esac
    fi
    case "$(printf '%s' "$response" | tr '[:upper:]' '[:lower:]')" in
      y|yes)
        return 0
        ;;
      n|no)
        return 1
        ;;
    esac
    echo "Please answer y or n."
  done
}

looks_like_placeholder() {
  seed_value_is_placeholder "$1" "$2"
}

default_push_connector_url() {
  local instance_url="$1"

  if [[ -z "$instance_url" ]]; then
    printf '%s' "https://your-instance.service-now.com/api/sn_em_connector/em/inbound_event?source=genericJsonV2"
    return 0
  fi
  printf '%s' "${instance_url%/}/api/sn_em_connector/em/inbound_event?source=genericJsonV2"
}

prompt_value() {
  local label="$1"
  local current="$2"
  local required="${3:-0}"
  local response=""

  while true; do
    if [[ -n "$current" ]]; then
      read -r -p "$label [$current]: " response
      [[ -z "$response" ]] && response="$current"
    else
      read -r -p "$label: " response
    fi

    if [[ "$required" -eq 1 && -z "$response" ]]; then
      echo "This value is required."
      continue
    fi

    printf '%s' "$response"
    return 0
  done
}

prompt_secret_value() {
  local label="$1"
  local current="$2"
  local required="${3:-0}"
  local response=""

  while true; do
    if [[ -n "$current" ]]; then
      read -r -s -p "$label [press Enter to keep current]: " response
      printf '\n' >&2
      [[ -z "$response" ]] && response="$current"
    else
      read -r -s -p "$label: " response
      printf '\n' >&2
    fi

    if [[ "$required" -eq 1 && -z "$response" ]]; then
      echo "This value is required."
      continue
    fi

    printf '%s' "$response"
    return 0
  done
}

maybe_prompt_for_config() {
  local sn_instance_url=""
  local sn_username=""
  local sn_password=""
  local push_connector_url=""
  local header_name=""
  local header_value=""
  local default_cmdb_ci_sys_id=""
  local default_cmdb_ci_name=""
  local default_assignment_group_sys_id=""
  local salesforce_user_sys_id=""
  local pinc_tag_sys_id=""
  local https_proxy_value=""
  local http_proxy_value=""
  local no_proxy_value=""

  if ! prompt_enabled; then
    return 0
  fi

  echo
  if [[ -n "$INSTANCE_PROFILE" ]]; then
    echo "[config] Instance profile: $INSTANCE_PROFILE"
  fi
  echo "[config] Service: $SERVICE_NAME"
  echo "[config] Answer a few prompts and the installer will write $SOURCE_ENV_FILE for you."
  echo "[config] Press Enter to keep the current value."

  sn_instance_url="$(seed_env_value SN_INSTANCE_URL)"
  if looks_like_placeholder SN_INSTANCE_URL "$sn_instance_url"; then
    sn_instance_url=""
  fi
  sn_instance_url="$(prompt_value "ServiceNow instance URL" "$sn_instance_url" 1)"
  set_env_file_value "$SOURCE_ENV_FILE" "SN_INSTANCE_URL" "$sn_instance_url"

  sn_username="$(seed_env_value SN_USERNAME)"
  if looks_like_placeholder SN_USERNAME "$sn_username"; then
    sn_username=""
  fi
  sn_username="$(prompt_value "ServiceNow username" "$sn_username" 1)"
  set_env_file_value "$SOURCE_ENV_FILE" "SN_USERNAME" "$sn_username"

  sn_password="$(seed_env_value SN_PASSWORD)"
  if looks_like_placeholder SN_PASSWORD "$sn_password"; then
    sn_password=""
  fi
  sn_password="$(prompt_secret_value "ServiceNow password" "$sn_password" 1)"
  set_env_file_value "$SOURCE_ENV_FILE" "SN_PASSWORD" "$sn_password"

  push_connector_url="$(seed_env_value PUSH_CONNECTOR_URL)"
  if looks_like_placeholder PUSH_CONNECTOR_URL "$push_connector_url"; then
    push_connector_url=""
  fi
  if [[ -z "$push_connector_url" ]]; then
    push_connector_url="$(default_push_connector_url "$sn_instance_url")"
  fi
  push_connector_url="$(prompt_value "genericJsonV2 endpoint URL" "$push_connector_url" 1)"
  set_env_file_value "$SOURCE_ENV_FILE" "PUSH_CONNECTOR_URL" "$push_connector_url"

  header_name="$(seed_env_value PUSH_CONNECTOR_HEADER_NAME)"
  [[ -z "$header_name" ]] && header_name="user-agent"
  header_name="$(prompt_value "Required connector header name" "$header_name" 1)"
  set_env_file_value "$SOURCE_ENV_FILE" "PUSH_CONNECTOR_HEADER_NAME" "$header_name"

  header_value="$(seed_env_value PUSH_CONNECTOR_HEADER_VALUE)"
  [[ -z "$header_value" ]] && header_value="genericendpoint"
  header_value="$(prompt_value "Required connector header value" "$header_value" 1)"
  set_env_file_value "$SOURCE_ENV_FILE" "PUSH_CONNECTOR_HEADER_VALUE" "$header_value"

  default_cmdb_ci_sys_id="$(seed_env_value DEFAULT_CMDB_CI_SYS_ID)"
  default_cmdb_ci_sys_id="$(prompt_value "Dummy/default CMDB CI sys_id" "$default_cmdb_ci_sys_id" 0)"
  set_env_file_value "$SOURCE_ENV_FILE" "DEFAULT_CMDB_CI_SYS_ID" "$default_cmdb_ci_sys_id"

  default_cmdb_ci_name="$(seed_env_value DEFAULT_CMDB_CI_NAME)"
  default_cmdb_ci_name="$(prompt_value "Dummy/default CMDB CI name" "$default_cmdb_ci_name" 0)"
  set_env_file_value "$SOURCE_ENV_FILE" "DEFAULT_CMDB_CI_NAME" "$default_cmdb_ci_name"

  default_assignment_group_sys_id="$(seed_env_value DEFAULT_ASSIGNMENT_GROUP_SYS_ID)"
  default_assignment_group_sys_id="$(prompt_value "Dummy fallback assignment group sys_id" "$default_assignment_group_sys_id" 0)"
  set_env_file_value "$SOURCE_ENV_FILE" "DEFAULT_ASSIGNMENT_GROUP_SYS_ID" "$default_assignment_group_sys_id"

  salesforce_user_sys_id="$(seed_env_value SALESFORCE_USER_SYS_ID)"
  salesforce_user_sys_id="$(prompt_value "Salesforce user sys_id for assigned_to/caller_id" "$salesforce_user_sys_id" 0)"
  set_env_file_value "$SOURCE_ENV_FILE" "SALESFORCE_USER_SYS_ID" "$salesforce_user_sys_id"

  pinc_tag_sys_id="$(seed_env_value PINC_TAG_SYS_ID)"
  pinc_tag_sys_id="$(prompt_value "PINC tag sys_id" "$pinc_tag_sys_id" 0)"
  set_env_file_value "$SOURCE_ENV_FILE" "PINC_TAG_SYS_ID" "$pinc_tag_sys_id"

  https_proxy_value="$(seed_env_value HTTPS_PROXY)"
  https_proxy_value="$(prompt_value "HTTPS proxy URL" "$https_proxy_value" 0)"
  set_env_file_value "$SOURCE_ENV_FILE" "HTTPS_PROXY" "$https_proxy_value"

  http_proxy_value="$(seed_env_value HTTP_PROXY)"
  if [[ -z "$http_proxy_value" && -n "$https_proxy_value" ]]; then
    http_proxy_value="$https_proxy_value"
  fi
  http_proxy_value="$(prompt_value "HTTP proxy URL" "$http_proxy_value" 0)"
  set_env_file_value "$SOURCE_ENV_FILE" "HTTP_PROXY" "$http_proxy_value"

  no_proxy_value="$(seed_env_value NO_PROXY)"
  [[ -z "$no_proxy_value" ]] && no_proxy_value="localhost,127.0.0.1"
  no_proxy_value="$(prompt_value "NO_PROXY" "$no_proxy_value" 0)"
  set_env_file_value "$SOURCE_ENV_FILE" "NO_PROXY" "$no_proxy_value"

  echo "[config] Saved answers to $SOURCE_ENV_FILE"
}

sync_derived_env_values() {
  set_env_file_value "$SOURCE_ENV_FILE" "STATE_FILE" "$STATE_FILE"
  set_env_file_value "$SOURCE_ENV_FILE" "AUTO_CREATE_SERVICE_OFFERINGS" "false"
  set_env_file_value "$SOURCE_ENV_FILE" "ENABLE_TAGGING" "false"
  set_env_file_value "$SOURCE_ENV_FILE" "ENABLE_DTI_FALLBACK" "false"
}

cleanup_legacy_source_env_file() {
  :
}

remove_legacy_candidate() {
  local path="$1"
  local label="$2"
  local mode="${3:-file}"
  local unit_name="${4:-}"

  path_exists "$path" || return 0

  echo
  echo "[cleanup] Found $label"
  echo "[cleanup]   $path"
  $SUDO ls -ld "$path" 2>/dev/null || true

  if ! prompt_yes_no "Delete this?" "N"; then
    echo "[cleanup] Keeping $path"
    return 0
  fi

  case "$mode" in
    file|state)
      $SUDO rm -f "$path"
      ;;
    dir)
      $SUDO rm -rf "$path"
      ;;
    service_unit)
      if [[ -z "$unit_name" ]]; then
        unit_name="$(basename "${path%.service}")"
      fi
      $SUDO systemctl stop "${unit_name}.service" >/dev/null 2>&1 || true
      $SUDO systemctl disable "${unit_name}.service" >/dev/null 2>&1 || true
      $SUDO rm -f "$path"
      LEGACY_DAEMON_RELOAD=1
      ;;
    *)
      echo "[cleanup] Unknown cleanup mode for $path: $mode" >&2
      return 1
      ;;
  esac

  echo "[cleanup] Deleted $path"
}

maybe_prompt_for_legacy_cleanup() {
  local legacy_service_prefix="${LEGACY_SERVICE_PREFIX:-sf-peru-arm-emulator}"
  local legacy_source_env=""
  local legacy_names=()
  local legacy_name=""
  local found_any=0

  if ! prompt_enabled; then
    return 0
  fi

  if path_exists "$CONFIG_FILE"; then
    found_any=1
  fi
  if path_exists "$STATE_FILE"; then
    found_any=1
  fi

  if [[ $AUTO_SOURCE_ENV_FILE -eq 1 && -n "$INSTANCE_PROFILE" ]]; then
    legacy_source_env="${SCRIPT_DIR}/${INSTANCE_PROFILE}.env"
    if path_exists "$legacy_source_env"; then
      found_any=1
    fi
  fi

  legacy_names+=("$legacy_service_prefix")
  if [[ -n "$INSTANCE_PROFILE" ]]; then
    legacy_names+=("${legacy_service_prefix}-${INSTANCE_PROFILE}")
  fi

  for legacy_name in "${legacy_names[@]}"; do
    [[ "$legacy_name" == "$SERVICE_NAME" ]] && continue
    if path_exists "/etc/systemd/system/${legacy_name}.service" \
      || path_exists "/opt/${legacy_name}" \
      || path_exists "/etc/${legacy_name}" \
      || path_exists "/var/lib/${legacy_name}"; then
      found_any=1
      break
    fi
  done

  if [[ $found_any -eq 0 ]]; then
    return 0
  fi

  echo
  echo "[cleanup] Existing install files can affect reinstall defaults and leave old services behind."
  echo "[cleanup] This step lets you review legacy files before anything is deleted."
  if ! prompt_yes_no "Review optional legacy cleanup items for ${INSTANCE_PROFILE:-this install} now?" "N"; then
    return 0
  fi

  remove_legacy_candidate "$CONFIG_FILE" "current deployed config for this install (removing it resets prompt defaults)" "file"
  remove_legacy_candidate "$STATE_FILE" "current state file for this install" "state"

  if [[ $AUTO_SOURCE_ENV_FILE -eq 1 && -n "$INSTANCE_PROFILE" ]]; then
    legacy_source_env="${SCRIPT_DIR}/${INSTANCE_PROFILE}.env"
    remove_legacy_candidate "$legacy_source_env" "old staging env file in the project directory" "file"
  fi

  for legacy_name in "${legacy_names[@]}"; do
    [[ "$legacy_name" == "$SERVICE_NAME" ]] && continue
    remove_legacy_candidate "/etc/systemd/system/${legacy_name}.service" "legacy systemd service unit" "service_unit" "$legacy_name"
    remove_legacy_candidate "/opt/${legacy_name}" "legacy app directory" "dir"
    remove_legacy_candidate "/etc/${legacy_name}" "legacy config directory" "dir"
    remove_legacy_candidate "/var/lib/${legacy_name}" "legacy state directory" "dir"
  done

  if [[ $LEGACY_DAEMON_RELOAD -eq 1 ]]; then
    echo "[cleanup] Reloading systemd after service unit cleanup ..."
    $SUDO systemctl daemon-reload
  fi
}

load_proxy_settings() {
  local https_proxy_value="${HTTPS_PROXY:-${https_proxy:-$(env_file_value HTTPS_PROXY)}}"
  local http_proxy_value="${HTTP_PROXY:-${http_proxy:-$(env_file_value HTTP_PROXY)}}"
  local no_proxy_value="${NO_PROXY:-${no_proxy:-$(env_file_value NO_PROXY)}}"

  if [[ -n "$https_proxy_value" ]]; then
    export HTTPS_PROXY="$https_proxy_value"
    export https_proxy="$https_proxy_value"
  fi
  if [[ -n "$http_proxy_value" ]]; then
    export HTTP_PROXY="$http_proxy_value"
    export http_proxy="$http_proxy_value"
  fi
  if [[ -n "$no_proxy_value" ]]; then
    export NO_PROXY="$no_proxy_value"
    export no_proxy="$no_proxy_value"
  fi
}

copy_runtime_files() {
  echo "[1/7] Deploying runtime files to $APP_DIR ..."
  $SUDO install -d -m 0755 "$APP_DIR"
  $SUDO install -m 0644 "$SCRIPT_DIR/salesforce_peru_arm_emulator.py" "$APP_DIR/salesforce_peru_arm_emulator.py"
  $SUDO install -m 0644 "$SCRIPT_DIR/requirements.txt" "$APP_DIR/requirements.txt"
  $SUDO install -m 0644 "$SCRIPT_DIR/README.md" "$APP_DIR/README.md"
  $SUDO rm -f "$APP_DIR/.env.example"
  $SUDO chown -R "$RUN_AS_USER:$RUN_AS_GROUP" "$APP_DIR"
}

install_python_env() {
  echo "[2/7] Creating/updating virtualenv in $APP_DIR/.venv ..."
  if [[ ! -d "$APP_DIR/.venv" ]]; then
    $SUDO "$PYTHON_BIN" -m venv "$APP_DIR/.venv"
  fi
  $SUDO "$APP_DIR/.venv/bin/pip" install --upgrade pip wheel
  $SUDO "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"
  $SUDO chown -R "$RUN_AS_USER:$RUN_AS_GROUP" "$APP_DIR/.venv"
}

prepare_env_file() {
  echo "[3/7] Preparing deployed config in $CONFIG_FILE ..."
  $SUDO install -d -m 0755 "$CONFIG_DIR"
  $SUDO install -m 0600 "$SOURCE_ENV_FILE" "$CONFIG_FILE"
  $SUDO chown "$RUN_AS_USER:$RUN_AS_GROUP" "$CONFIG_FILE"
}

env_looks_ready() {
  local instance_url=""
  local username=""
  local password=""
  local push_connector_url=""

  [[ -f "$CONFIG_FILE" ]] || return 1

  instance_url="$(env_value_from_file "$CONFIG_FILE" "SN_INSTANCE_URL")"
  username="$(env_value_from_file "$CONFIG_FILE" "SN_USERNAME")"
  password="$(env_value_from_file "$CONFIG_FILE" "SN_PASSWORD")"
  push_connector_url="$(env_value_from_file "$CONFIG_FILE" "PUSH_CONNECTOR_URL")"

  looks_like_placeholder SN_INSTANCE_URL "$instance_url" && return 1
  looks_like_placeholder SN_USERNAME "$username" && return 1
  looks_like_placeholder SN_PASSWORD "$password" && return 1
  looks_like_placeholder PUSH_CONNECTOR_URL "$push_connector_url" && return 1
  return 0
}

install_service() {
  echo "[4/7] Installing systemd service ..."
  $SUDO install -d -m 0755 "$STATE_DIR"
  $SUDO chown "$RUN_AS_USER:$RUN_AS_GROUP" "$STATE_DIR" || true

  local tmp_service
  tmp_service="$(mktemp)"
  sed \
    -e "s#__APP_DIR__#$APP_DIR#g" \
    -e "s#__CONFIG_FILE__#$CONFIG_FILE#g" \
    -e "s#__RUN_USER__#$RUN_AS_USER#g" \
    -e "s#__RUN_GROUP__#$RUN_AS_GROUP#g" \
    -e "s#__STATE_DIR__#$STATE_DIR#g" \
    "$SCRIPT_DIR/sf-peru-arm-emulator.service" > "$tmp_service"

  $SUDO install -m 0644 "$tmp_service" "/etc/systemd/system/${SERVICE_NAME}.service"
  rm -f "$tmp_service"

  $SUDO systemctl daemon-reload
  $SUDO systemctl disable "${SERVICE_NAME}.service" >/dev/null 2>&1 || true
}

stop_existing_service() {
  echo "[5/7] Stopping existing service if it is running ..."
  $SUDO systemctl stop "${SERVICE_NAME}.service" >/dev/null 2>&1 || true
}

run_probe() {
  echo "[6/7] Running alert probe from deployed runtime ..."
  "$APP_DIR/.venv/bin/python" "$APP_DIR/salesforce_peru_arm_emulator.py" --env "$CONFIG_FILE" --probe-alerts --probe-limit 10
}

enable_and_start_service() {
  echo "[7/7] Enabling and starting service ..."
  $SUDO systemctl enable --now "${SERVICE_NAME}.service"
  $SUDO systemctl --no-pager --full status "${SERVICE_NAME}.service" || true
}

prompt_enable_approval() {
  local response=""

  if [[ ! -t 0 || ! -t 1 ]]; then
    return 1
  fi

  printf '\nTests passed. Enable and start %s now? [y/N]: ' "$SERVICE_NAME" >&2
  read -r response || return 1
  case "$(printf '%s' "$response" | tr '[:upper:]' '[:lower:]')" in
    y|yes)
      return 0
      ;;
  esac
  return 1
}

print_next_steps() {
  local source_env_display="$SOURCE_ENV_FILE"
  if [[ $AUTO_SOURCE_ENV_FILE -eq 1 ]]; then
    source_env_display="temporary installer staging file (removed on exit)"
  fi
  cat <<MSG

Done.

Instance profile:
  ${INSTANCE_PROFILE:-default}

Service:
  $SERVICE_NAME

Source env:
  $source_env_display

Deployed paths:
  App dir:    $APP_DIR
  Config dir: $CONFIG_DIR
  Config:     $CONFIG_FILE
  State dir:  $STATE_DIR
  State file: $STATE_FILE

Useful commands:
  journalctl -u ${SERVICE_NAME}.service -f
  sudo systemctl enable --now ${SERVICE_NAME}.service
  sudo systemctl restart ${SERVICE_NAME}.service
  sudo systemctl stop ${SERVICE_NAME}.service
  sudo systemctl disable --now ${SERVICE_NAME}.service
  $APP_DIR/.venv/bin/python $APP_DIR/salesforce_peru_arm_emulator.py --env $CONFIG_FILE --probe-alerts --probe-limit 10
  $APP_DIR/.venv/bin/python $APP_DIR/salesforce_peru_arm_emulator.py --env $CONFIG_FILE --once --dry-run
MSG
}

prompt_instance_profile
apply_instance_defaults
maybe_prompt_for_legacy_cleanup
ensure_source_env_file
maybe_prompt_for_config
sync_derived_env_values
load_proxy_settings
copy_runtime_files
install_python_env
prepare_env_file
stop_existing_service
install_service

if [[ $PROBE_ONLY -eq 1 ]]; then
  if ! env_looks_ready; then
    echo "Config still contains placeholders. Rerun ./install.sh and answer the prompts, or update $SOURCE_ENV_FILE manually." >&2
    exit 1
  fi
  run_probe
  exit 0
fi

if ! env_looks_ready; then
  echo
  echo "Config still contains placeholder values. Rerun ./install.sh and answer the prompts, or update $SOURCE_ENV_FILE manually." >&2
  echo "The current source config has already been copied to $CONFIG_FILE." >&2
  print_next_steps
  exit 0
fi

probe_passed=1
if [[ $RUN_PROBE -eq 1 ]]; then
  if ! run_probe; then
    probe_passed=0
    echo
    echo "Probe failed. The service has been installed but left disabled." >&2
    print_next_steps
    exit 1
  fi
fi

if [[ $ENABLE_SERVICE -eq 1 ]]; then
  enable_and_start_service
elif [[ $RUN_PROBE -eq 1 && $probe_passed -eq 1 ]] && prompt_enable_approval; then
  enable_and_start_service
else
  echo "Service installed but left disabled by default."
fi

print_next_steps
