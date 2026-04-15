#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-sf-peru-webhook}"
APP_DIR="${APP_DIR:-/opt/sf-peru-arm-emulator}"
CONFIG_DIR="${CONFIG_DIR:-/etc/sf-peru-arm-emulator}"
CONFIG_FILE="${CONFIG_FILE:-${CONFIG_DIR}/.env}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RUN_AS_USER="${RUN_AS_USER:-${SUDO_USER:-$(id -un)}}"
RUN_AS_GROUP="${RUN_AS_GROUP:-$(id -gn "$RUN_AS_USER" 2>/dev/null || id -gn)}"
START_SERVICE=1

usage() {
  cat <<USAGE
Usage:
  ./install_bridge.sh            Deploy bridge runtime and install/update the webhook service
  ./install_bridge.sh --no-start Install/update everything but do not start the service
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-start)
      START_SERVICE=0
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

copy_runtime_files() {
  echo "[1/5] Deploying bridge files to $APP_DIR ..."
  $SUDO install -d -m 0755 "$APP_DIR"
  $SUDO install -m 0644 "$SCRIPT_DIR/salesforce_peru_bridge.py" "$APP_DIR/salesforce_peru_bridge.py"
  $SUDO install -m 0644 "$SCRIPT_DIR/salesforce_peru_arm_emulator.py" "$APP_DIR/salesforce_peru_arm_emulator.py"
  $SUDO install -m 0644 "$SCRIPT_DIR/requirements.txt" "$APP_DIR/requirements.txt"
  $SUDO install -m 0644 "$SCRIPT_DIR/README.md" "$APP_DIR/README.md"
  $SUDO install -m 0644 "$SCRIPT_DIR/.env.example" "$APP_DIR/.env.example"
  $SUDO chown -R "$RUN_AS_USER:$RUN_AS_GROUP" "$APP_DIR"
}

install_python_env() {
  echo "[2/5] Creating/updating virtualenv in $APP_DIR/.venv ..."
  if [[ ! -d "$APP_DIR/.venv" ]]; then
    $SUDO "$PYTHON_BIN" -m venv "$APP_DIR/.venv"
  fi
  $SUDO "$APP_DIR/.venv/bin/pip" install --upgrade pip wheel
  $SUDO "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"
  $SUDO chown -R "$RUN_AS_USER:$RUN_AS_GROUP" "$APP_DIR/.venv"
}

prepare_env_file() {
  echo "[3/5] Preparing deployed config in $CONFIG_FILE ..."
  if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
    cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
    echo "Copied $SCRIPT_DIR/.env.example -> $SCRIPT_DIR/.env"
  fi

  $SUDO install -d -m 0755 "$CONFIG_DIR"
  $SUDO install -m 0600 "$SCRIPT_DIR/.env" "$CONFIG_FILE"
  $SUDO chown "$RUN_AS_USER:$RUN_AS_GROUP" "$CONFIG_FILE"
}

install_service() {
  echo "[4/5] Installing webhook bridge service ..."
  local tmp_service
  tmp_service="$(mktemp)"
  sed \
    -e "s#__APP_DIR__#$APP_DIR#g" \
    -e "s#__CONFIG_FILE__#$CONFIG_FILE#g" \
    -e "s#__RUN_USER__#$RUN_AS_USER#g" \
    -e "s#__RUN_GROUP__#$RUN_AS_GROUP#g" \
    "$SCRIPT_DIR/sf-peru-webhook.service" > "$tmp_service"

  $SUDO install -m 0644 "$tmp_service" "/etc/systemd/system/${SERVICE_NAME}.service"
  rm -f "$tmp_service"

  $SUDO systemctl daemon-reload
  $SUDO systemctl enable "${SERVICE_NAME}.service"
}

start_service() {
  echo "[5/5] Starting webhook bridge service ..."
  $SUDO systemctl restart "${SERVICE_NAME}.service"
  $SUDO systemctl --no-pager --full status "${SERVICE_NAME}.service" || true
}

copy_runtime_files
install_python_env
prepare_env_file
install_service

if [[ $START_SERVICE -eq 1 ]]; then
  start_service
else
  echo "Webhook bridge service installed but not started because --no-start was used."
fi
