#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Sequence, Tuple

from salesforce_peru_arm_emulator import (
    UTC,
    Config,
    SalesforcePayload,
    SalesforcePayloadExtractor,
    ServiceNowClient,
    load_env_file,
    to_sn_datetime,
)

LOGGER = logging.getLogger("sf_peru_bridge")


@dataclass
class BridgeConfig:
    bind_host: str = "0.0.0.0"
    port: int = 8090
    path: str = "/salesforce/peru"
    auth_token: str = ""
    event_source: str = "salesforce"
    event_type: str = "Amazon"
    event_class: str = "salesforce"
    metric_name: str = "salesforce_case"

    @staticmethod
    def from_env() -> "BridgeConfig":
        return BridgeConfig(
            bind_host=os.getenv("WEBHOOK_BIND_HOST", "0.0.0.0").strip() or "0.0.0.0",
            port=int((os.getenv("WEBHOOK_PORT", "8090").strip() or "8090")),
            path=os.getenv("WEBHOOK_PATH", "/salesforce/peru").strip() or "/salesforce/peru",
            auth_token=os.getenv("WEBHOOK_AUTH_TOKEN", "").strip(),
            event_source=os.getenv("WEBHOOK_EVENT_SOURCE", "salesforce").strip() or "salesforce",
            event_type=os.getenv("WEBHOOK_EVENT_TYPE", "Amazon").strip() or "Amazon",
            event_class=os.getenv("WEBHOOK_EVENT_CLASS", "salesforce").strip() or "salesforce",
            metric_name=os.getenv("WEBHOOK_METRIC_NAME", "salesforce_case").strip() or "salesforce_case",
        )


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Off-platform Salesforce PERU payload bridge")
    parser.add_argument("--env", dest="env_file", default=".env", help="path to .env file")
    parser.add_argument("--serve", action="store_true", help="start HTTP webhook bridge")
    parser.add_argument("--send-file", dest="send_file", help="normalize a payload file and forward it to ServiceNow")
    parser.add_argument("--print-normalized", dest="print_normalized", help="print the normalized event payload for a raw Salesforce payload file")
    return parser.parse_args(argv)


def normalize_case_status(value: str) -> str:
    text = (value or "").strip().lower()
    if text in {"closed", "close", "resolved", "clear", "cleared"}:
        return "closed"
    return "open"


def infer_severity(sf: SalesforcePayload) -> str:
    status_bucket = normalize_case_status(sf.case_status)
    subject = (sf.case_subject or "").strip().lower()
    try:
        impacted = int((sf.number_of_customers_impacted or "").strip())
    except Exception:
        impacted = 0

    if status_bucket == "closed":
        return "0"
    if any(token in subject for token in ("down", "outage", "critical", "sev1", "sev2", "major", "unavailable")):
        return "2"
    if impacted >= 5:
        return "2"
    if impacted > 0:
        return "3"
    return "3"


class SalesforcePeruBridge:
    def __init__(self, sn_cfg: Config, bridge_cfg: BridgeConfig):
        self.sn_cfg = sn_cfg
        self.bridge_cfg = bridge_cfg
        self.client = ServiceNowClient(sn_cfg)
        self.extractor = SalesforcePayloadExtractor()

    def build_normalized_event(self, raw_payload: Any) -> Tuple[SalesforcePayload, Dict[str, Any]]:
        if not isinstance(raw_payload, dict):
            raise ValueError("Salesforce bridge expects a JSON object payload")

        sf = self.extractor.extract(raw_payload, [])
        stable_text = json.dumps(raw_payload, sort_keys=True, default=str)
        generated_case = "sf-" + hashlib.sha1(stable_text.encode("utf-8")).hexdigest()[:12]
        case_number = (sf.case_number or "").strip() or generated_case
        description = (sf.case_subject or "").strip() or f"Salesforce case {case_number}"
        event: Dict[str, Any] = {
            "source": self.bridge_cfg.event_source,
            "event_class": self.bridge_cfg.event_class,
            "type": self.bridge_cfg.event_type,
            "resource": case_number,
            "metric_name": self.bridge_cfg.metric_name,
            "message_key": case_number,
            "severity": infer_severity(sf),
            "description": description,
            "resolution_state": "Closing" if normalize_case_status(sf.case_status) == "closed" else "New",
            "time_of_event": (sf.case_created_date or "").strip() or to_sn_datetime(datetime.now(tz=UTC)),
            "additional_info": raw_payload,
        }

        if (sf.primary_offering or "").strip():
            event["usbem_offering"] = sf.primary_offering.strip()
        elif (sf.secondary_offering or "").strip():
            event["usbem_offering"] = sf.secondary_offering.strip()

        return sf, event

    def forward_payload(self, raw_payload: Any) -> Dict[str, Any]:
        sf, event = self.build_normalized_event(raw_payload)
        response = self.client.push_connector_call(event)
        return {
            "status": "forwarded",
            "case_number": sf.case_number or event["message_key"],
            "normalized_event": event,
            "servicenow_response": response,
        }


class BridgeRequestHandler(BaseHTTPRequestHandler):
    bridge: SalesforcePeruBridge = None  # type: ignore[assignment]

    def do_POST(self) -> None:
        bridge = self.bridge
        if self.path != bridge.bridge_cfg.path:
            self._write_json(404, {"status": "not_found"})
            return

        if bridge.bridge_cfg.auth_token:
            incoming = self.headers.get("Authorization", "").strip()
            expected = f"Bearer {bridge.bridge_cfg.auth_token}"
            if incoming != expected:
                self._write_json(401, {"status": "unauthorized"})
                return

        try:
            content_length = int(self.headers.get("Content-Length", "0") or "0")
        except Exception:
            content_length = 0
        body = self.rfile.read(content_length) if content_length > 0 else b""

        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            self._write_json(400, {"status": "invalid_json", "message": str(exc)})
            return

        try:
            result = bridge.forward_payload(payload)
        except Exception as exc:
            LOGGER.exception("bridge forwarding failed")
            self._write_json(500, {"status": "error", "message": str(exc)})
            return

        self._write_json(200, result)

    def log_message(self, fmt: str, *args: Any) -> None:
        LOGGER.info("%s - %s", self.address_string(), fmt % args)

    def _write_json(self, status_code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_server(bridge: SalesforcePeruBridge) -> int:
    BridgeRequestHandler.bridge = bridge
    server = ThreadingHTTPServer((bridge.bridge_cfg.bind_host, bridge.bridge_cfg.port), BridgeRequestHandler)

    def handle_signal(signum: int, frame: Any) -> None:
        LOGGER.info("received signal %s, shutting down bridge", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    LOGGER.info(
        "starting Salesforce PERU bridge on http://%s:%s%s",
        bridge.bridge_cfg.bind_host,
        bridge.bridge_cfg.port,
        bridge.bridge_cfg.path,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.env_file:
        load_env_file(args.env_file, override=True)

    sn_cfg = Config.from_env(args)
    configure_logging(sn_cfg.log_level)
    bridge_cfg = BridgeConfig.from_env()
    bridge = SalesforcePeruBridge(sn_cfg, bridge_cfg)

    if args.print_normalized:
        with open(args.print_normalized, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        result = bridge.build_normalized_event(payload)[1]
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.send_file:
        with open(args.send_file, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        result = bridge.forward_payload(payload)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.serve:
        return run_server(bridge)

    raise SystemExit("choose one of --serve, --send-file, or --print-normalized")


if __name__ == "__main__":
    raise SystemExit(main())
