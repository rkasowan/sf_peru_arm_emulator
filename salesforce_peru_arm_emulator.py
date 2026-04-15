#!/usr/bin/env python3
"""
Peru Incident Management Process worker for:
  source = salesforce
  type contains Amazon

What it does:
- Polls em_alert for matching alerts updated since the last watermark.
- Emulates the missed Alert Management Rule by sending a helper DTI event with a
  fresh message key, so the connector creates a new alert and opens the incident.
- Clears stale alert incident/task links before DTI on reopen scenarios so a new
  incident is created when the alert reopens.
- Patches the resulting incident with CI, assignment, external Salesforce case id,
  pretty description, and PINC tag.
- Updates em_alert.task to the created incident.
- Detects reopen transitions and creates a new incident for the reopened alert.

Designed as a fast temporary workaround until the real Alert Management Rule is pushed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import signal
import sys
import tempfile
import time
from urllib.parse import parse_qs, urlparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

UTC = timezone.utc
LOGGER = logging.getLogger("sf_peru_arm_emulator")
SYS_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)
INC_NUMBER_RE = re.compile(r"\bINC\d{4,}\b", re.IGNORECASE)
DTI_HELPER_MESSAGE_KEY_PREFIX = "PIMP-DTI"
DTI_HELPER_METRIC_NAME = "peru_dti_helper"


class PushConnectorTimeoutError(RuntimeError):
    def __init__(self, url: str, timeout_seconds: int):
        super().__init__(f"push connector {url} timed out after {timeout_seconds}s")
        self.url = url
        self.timeout_seconds = timeout_seconds


def load_env_file(path: str, override: bool = False) -> None:
    file_path = Path(path)
    if not file_path.exists():
        return
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if not override and key in os.environ:
            continue
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ[key] = value

KV_RE = re.compile(
    r"[\"']?([A-Za-z0-9_ \-/\.]+)[\"']?\s*[:=]\s*(?:[\"']([^\"']*)[\"']|([^,\n\r}]+))"
)

KNOWN_SF_FIELDS = [
    "case_created_date",
    "case_number",
    "case_owner",
    "case_status",
    "case_subject",
    "expected_result",
    "number_of_customers_impacted",
    "primary_offering",
    "secondary_offering",
    "environment",
    "http_endpoint",
]

FIELD_ALIASES = {
    "case_created_date": {"case_created_date", "casecreateddate", "created_date", "case_opened_date"},
    "case_number": {"case_number", "casenumber", "external_case_id", "salesforce_case_id", "sf_case_number"},
    "case_owner": {"case_owner", "caseowner", "owner", "salesforce_case_owner", "sf_case_owner"},
    "case_status": {"case_status", "casestatus", "status", "salesforce_case_status", "sf_case_status"},
    "case_subject": {"case_subject", "casesubject", "subject", "salesforce_case_subject", "sf_case_subject"},
    "expected_result": {"expected_result", "expectedresult"},
    "number_of_customers_impacted": {
        "number_of_customers_impacted",
        "customers_impacted",
        "customer_count",
        "numberofcustomersimpacted",
    },
    "primary_offering": {"primary_offering", "primaryoffering", "primary_service_offering", "service_offering_primary"},
    "secondary_offering": {
        "secondary_offering",
        "secondaryoffering",
        "secondary_service_offering",
        "service_offering_secondary",
    },
    "environment": {"environment", "env", "salesforce_environment", "sf_environment"},
    "http_endpoint": {"http_endpoint", "httpendpoint", "endpoint", "salesforce_http_endpoint", "sf_http_endpoint", "url"},
}

STATE_OPEN_DEFAULT = {"open", "reopen", "reopened", "new", "active"}
STATE_CLOSED_DEFAULT = {"closed", "resolved", "clear", "cleared"}


class GracefulShutdown:
    def __init__(self) -> None:
        self.stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, frame: Any) -> None:
        LOGGER.info("received signal %s, stopping...", signum)
        self.stop = True


@dataclass
class Config:
    sn_instance_url: str
    sn_username: str
    sn_password: str
    verify_ssl: bool = True
    request_timeout_seconds: int = 30

    alert_table: str = "em_alert"
    event_table: str = "em_event"
    incident_table: str = "incident"
    cmdb_table: str = "cmdb_ci"
    label_entry_table: str = "label_entry"

    alert_source: str = "salesforce"
    alert_type_contains: str = "Amazon"
    alert_query_extra: str = ""
    poll_interval_seconds: int = 3
    query_overlap_seconds: int = 45
    initial_lookback_seconds: int = 3600
    alert_query_limit: int = 100
    alert_bootstrap_limit: int = 25
    alert_bootstrap_ignore_watermark_on_empty: bool = True
    alert_discovery_hydrate_limit: int = 25

    state_file: str = "/var/lib/sf-peru-incident-management-process/state.json"
    log_level: str = "INFO"
    persist_state_in_dry_run: bool = False

    push_connector_url: str = ""
    push_connector_method: str = "POST"
    push_connector_header_name: str = ""
    push_connector_header_value: str = ""
    push_connector_username: str = ""
    push_connector_password: str = ""
    push_connector_bearer_token: str = ""
    push_connector_timeout_seconds: int = 90
    push_connector_payload_mode: str = "single_event"  # single_event | events_array
    push_connector_wait_seconds: int = 30
    push_connector_wait_poll_seconds: int = 2

    pinc_tag_sys_id: str = ""
    default_cmdb_ci_sys_id: str = ""
    default_cmdb_ci_name: str = ""
    salesforce_user_sys_id: str = ""

    set_assigned_to: bool = True
    set_caller_id: bool = True
    enable_tagging: bool = True
    enable_ci_lookup: bool = True

    incident_external_case_field: str = "u_external_salesforce_case_id"
    incident_generating_alert_field: str = "u_generating_alert"
    incident_extra_static_fields_json: str = ""
    salesforce_offering_service_name: str = "Salesforce PERU Services"
    auto_create_service_offerings: bool = False
    default_assignment_group_sys_id: str = ""

    dry_run: bool = False

    open_state_tokens: Sequence[str] = field(default_factory=lambda: sorted(STATE_OPEN_DEFAULT))
    closed_state_tokens: Sequence[str] = field(default_factory=lambda: sorted(STATE_CLOSED_DEFAULT))

    @staticmethod
    def from_env(args: argparse.Namespace) -> "Config":
        def env(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        def env_bool(name: str, default: bool) -> bool:
            raw = os.getenv(name)
            if raw is None:
                return default
            return raw.strip().lower() in {"1", "true", "yes", "y", "on"}

        def env_int(name: str, default: int) -> int:
            raw = os.getenv(name)
            if raw is None or not raw.strip():
                return default
            return int(raw.strip())

        def env_csv(name: str, default: Sequence[str]) -> Sequence[str]:
            raw = os.getenv(name)
            if not raw:
                return list(default)
            return [item.strip().lower() for item in raw.split(",") if item.strip()]

        url = env("SN_INSTANCE_URL")
        username = env("SN_USERNAME")
        password = env("SN_PASSWORD")
        if not url or not username or not password:
            raise ValueError("SN_INSTANCE_URL, SN_USERNAME, and SN_PASSWORD are required")

        cfg = Config(
            sn_instance_url=url.rstrip("/"),
            sn_username=username,
            sn_password=password,
            verify_ssl=env_bool("SN_VERIFY_SSL", True),
            request_timeout_seconds=env_int("SN_REQUEST_TIMEOUT_SECONDS", 30),
            alert_table=env("ALERT_TABLE", "em_alert"),
            event_table=env("EVENT_TABLE", "em_event"),
            incident_table=env("INCIDENT_TABLE", "incident"),
            cmdb_table=env("CMDB_TABLE", "cmdb_ci"),
            label_entry_table=env("LABEL_ENTRY_TABLE", "label_entry"),
            alert_source=env("ALERT_SOURCE", "salesforce"),
            alert_type_contains=env("ALERT_TYPE_CONTAINS", "Amazon"),
            alert_query_extra=env("ALERT_QUERY_EXTRA", ""),
            poll_interval_seconds=env_int("POLL_INTERVAL_SECONDS", 3),
            query_overlap_seconds=env_int("QUERY_OVERLAP_SECONDS", 45),
            initial_lookback_seconds=env_int("INITIAL_LOOKBACK_SECONDS", 3600),
            alert_query_limit=env_int("ALERT_QUERY_LIMIT", 100),
            alert_bootstrap_limit=env_int("ALERT_BOOTSTRAP_LIMIT", 25),
            alert_bootstrap_ignore_watermark_on_empty=env_bool("ALERT_BOOTSTRAP_IGNORE_WATERMARK_ON_EMPTY", True),
            alert_discovery_hydrate_limit=env_int("ALERT_DISCOVERY_HYDRATE_LIMIT", 25),
            state_file=env("STATE_FILE", "/var/lib/sf-peru-incident-management-process/state.json"),
            log_level=env("LOG_LEVEL", "INFO"),
            persist_state_in_dry_run=env_bool("PERSIST_STATE_IN_DRY_RUN", False),
            push_connector_url=env("PUSH_CONNECTOR_URL", ""),
            push_connector_method=env("PUSH_CONNECTOR_METHOD", "POST").upper(),
            push_connector_header_name=env("PUSH_CONNECTOR_HEADER_NAME", ""),
            push_connector_header_value=env("PUSH_CONNECTOR_HEADER_VALUE", ""),
            push_connector_username=env("PUSH_CONNECTOR_USERNAME", ""),
            push_connector_password=env("PUSH_CONNECTOR_PASSWORD", ""),
            push_connector_bearer_token=env("PUSH_CONNECTOR_BEARER_TOKEN", ""),
            push_connector_timeout_seconds=env_int("PUSH_CONNECTOR_TIMEOUT_SECONDS", 90),
            push_connector_payload_mode=env("PUSH_CONNECTOR_PAYLOAD_MODE", "single_event").lower(),
            push_connector_wait_seconds=env_int("PUSH_CONNECTOR_WAIT_SECONDS", 30),
            push_connector_wait_poll_seconds=env_int("PUSH_CONNECTOR_WAIT_POLL_SECONDS", 2),
            pinc_tag_sys_id=env("PINC_TAG_SYS_ID", ""),
            default_cmdb_ci_sys_id=env("DEFAULT_CMDB_CI_SYS_ID", ""),
            default_cmdb_ci_name=env("DEFAULT_CMDB_CI_NAME", ""),
            salesforce_user_sys_id=env("SALESFORCE_USER_SYS_ID", ""),
            set_assigned_to=env_bool("SET_ASSIGNED_TO", True),
            set_caller_id=env_bool("SET_CALLER_ID", True),
            enable_tagging=env_bool("ENABLE_TAGGING", True),
            enable_ci_lookup=env_bool("ENABLE_CI_LOOKUP", True),
            incident_external_case_field=env("INCIDENT_EXTERNAL_CASE_FIELD", "u_external_salesforce_case_id"),
            incident_generating_alert_field=env("INCIDENT_GENERATING_ALERT_FIELD", "u_generating_alert"),
            incident_extra_static_fields_json=env("INCIDENT_EXTRA_STATIC_FIELDS_JSON", ""),
            salesforce_offering_service_name=env("SALESFORCE_OFFERING_SERVICE_NAME", "Salesforce PERU Services"),
            auto_create_service_offerings=env_bool("AUTO_CREATE_SERVICE_OFFERINGS", False),
            default_assignment_group_sys_id=env("DEFAULT_ASSIGNMENT_GROUP_SYS_ID", ""),
            dry_run=env_bool("DRY_RUN", False) or bool(getattr(args, "dry_run", False)),
            open_state_tokens=env_csv("ALERT_OPEN_STATE_TOKENS", sorted(STATE_OPEN_DEFAULT)),
            closed_state_tokens=env_csv("ALERT_CLOSED_STATE_TOKENS", sorted(STATE_CLOSED_DEFAULT)),
        )

        if not cfg.push_connector_url:
            raise ValueError("PUSH_CONNECTOR_URL is required for this workaround")

        return cfg


@dataclass
class CIChoice:
    sys_id: str
    name: str
    source: str


@dataclass
class SalesforcePayload:
    case_created_date: str = ""
    case_number: str = ""
    case_owner: str = ""
    case_status: str = ""
    case_subject: str = ""
    expected_result: str = ""
    number_of_customers_impacted: str = ""
    primary_offering: str = ""
    secondary_offering: str = ""
    environment: str = ""
    http_endpoint: str = ""
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    sources: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "case_created_date": self.case_created_date,
            "case_number": self.case_number,
            "case_owner": self.case_owner,
            "case_status": self.case_status,
            "case_subject": self.case_subject,
            "expected_result": self.expected_result,
            "number_of_customers_impacted": self.number_of_customers_impacted,
            "primary_offering": self.primary_offering,
            "secondary_offering": self.secondary_offering,
            "environment": self.environment,
            "http_endpoint": self.http_endpoint,
        }

    def preferred_offering(self) -> str:
        return first_non_empty(self.primary_offering, self.secondary_offering)


@dataclass
class IncidentRef:
    sys_id: str
    number: str = ""


@dataclass
class ServiceOfferingRef:
    offering_sys_id: str = ""
    offering_name: str = ""
    service_sys_id: str = ""
    service_name: str = ""
    source: str = ""


@dataclass
class AssignmentGroupRef:
    sys_id: str = ""
    name: str = ""
    source: str = ""


class StateStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: Dict[str, Any] = {
            "last_watermark": "",
            "alerts": {},
        }
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            LOGGER.exception("failed to load state file %s; starting fresh", self.path)
            self.data = {"last_watermark": "", "alerts": {}}

    def save(self) -> None:
        fd, tmp_name = tempfile.mkstemp(prefix=self.path.name, dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.data, handle, indent=2, sort_keys=True)
            os.replace(tmp_name, self.path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def get_alert(self, alert_sys_id: str) -> Dict[str, Any]:
        return self.data.setdefault("alerts", {}).get(alert_sys_id, {})

    def set_alert(self, alert_sys_id: str, value: Dict[str, Any]) -> None:
        self.data.setdefault("alerts", {})[alert_sys_id] = value

    def watermark(self) -> Optional[datetime]:
        raw = str(self.data.get("last_watermark") or "").strip()
        if not raw:
            return None
        return parse_sn_datetime(raw)

    def set_watermark(self, dt: datetime) -> None:
        self.data["last_watermark"] = to_sn_datetime(dt)


class ServiceNowClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.auth = (cfg.sn_username, cfg.sn_password)
        self.session.verify = cfg.verify_ssl
        self.session.headers.update({"Accept": "application/json"})

    def _allowed_update_tables(self) -> Tuple[str, ...]:
        return (self.cfg.alert_table, self.cfg.incident_table)

    def _allowed_create_tables(self) -> Tuple[str, ...]:
        return (self.cfg.event_table, self.cfg.label_entry_table)

    def _assert_table_update_allowed(self, table: str) -> None:
        if table not in self._allowed_update_tables():
            raise RuntimeError(
                f"write policy violation: updates are only allowed on {', '.join(self._allowed_update_tables())}; refused update to {table}"
            )

    def _assert_table_create_allowed(self, table: str) -> None:
        if table not in self._allowed_create_tables():
            raise RuntimeError(
                f"write policy violation: creates are only allowed on {', '.join(self._allowed_create_tables())}; refused create on {table}"
            )

    def _url(self, path: str) -> str:
        return f"{self.cfg.sn_instance_url}{path}"

    def table_get(
        self,
        table: str,
        query: str = "",
        fields: Optional[Sequence[str]] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "sysparm_display_value": "all",
            "sysparm_exclude_reference_link": "true",
            "sysparm_limit": str(limit),
            "sysparm_offset": str(offset),
        }
        if query:
            params["sysparm_query"] = query
        if fields:
            params["sysparm_fields"] = ",".join(fields)
        if order_by:
            params["sysparm_query"] = f"{query}^{order_by}" if query else order_by

        response = self.session.get(
            self._url(f"/api/now/table/{table}"),
            params=params,
            timeout=self.cfg.request_timeout_seconds,
        )
        self._raise_for_status(response, f"GET table {table}")
        body = response.json()
        result = body.get("result", [])
        if isinstance(result, dict):
            return [result]
        return list(result)

    def table_get_record(self, table: str, sys_id: str) -> Dict[str, Any]:
        response = self.session.get(
            self._url(f"/api/now/table/{table}/{sys_id}"),
            params={
                "sysparm_display_value": "all",
                "sysparm_exclude_reference_link": "true",
            },
            timeout=self.cfg.request_timeout_seconds,
        )
        self._raise_for_status(response, f"GET record {table}/{sys_id}")
        result = response.json().get("result", {})
        if not isinstance(result, dict):
            raise RuntimeError(f"unexpected record response for {table}/{sys_id}: {result!r}")
        return result

    def table_update(self, table: str, sys_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        self._assert_table_update_allowed(table)
        if self.cfg.dry_run:
            LOGGER.info("[dry-run] PATCH %s/%s %s", table, sys_id, json.dumps(data, sort_keys=True))
            return {"result": {"sys_id": sys_id, **data}}
        response = self.session.patch(
            self._url(f"/api/now/table/{table}/{sys_id}"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            data=json.dumps(data),
            timeout=self.cfg.request_timeout_seconds,
        )
        self._raise_for_status(response, f"PATCH {table}/{sys_id}")
        return response.json().get("result", {})

    def table_create(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        self._assert_table_create_allowed(table)
        if self.cfg.dry_run:
            LOGGER.info("[dry-run] POST %s %s", table, json.dumps(data, sort_keys=True))
            return {"sys_id": "0" * 32, **data}
        response = self.session.post(
            self._url(f"/api/now/table/{table}"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            data=json.dumps(data),
            timeout=self.cfg.request_timeout_seconds,
        )
        self._raise_for_status(response, f"POST {table}")
        result = response.json().get("result", {})
        if not isinstance(result, dict):
            raise RuntimeError(f"unexpected create response for {table}: {result!r}")
        return result

    def push_connector_call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.cfg.push_connector_header_name and self.cfg.push_connector_header_value:
            headers[self.cfg.push_connector_header_name] = self.cfg.push_connector_header_value
        auth = None
        if self.cfg.push_connector_bearer_token:
            headers["Authorization"] = f"Bearer {self.cfg.push_connector_bearer_token}"
        elif self.cfg.push_connector_username and self.cfg.push_connector_password:
            auth = (self.cfg.push_connector_username, self.cfg.push_connector_password)
        else:
            auth = (self.cfg.sn_username, self.cfg.sn_password)

        if self.cfg.dry_run:
            LOGGER.info("[dry-run] %s %s headers=%s payload=%s", self.cfg.push_connector_method, self.cfg.push_connector_url, json.dumps(headers, sort_keys=True), json.dumps(payload, sort_keys=True))
            return {"result": {"dry_run": True, "payload": payload, "headers": headers}}

        try:
            response = self.session.request(
                method=self.cfg.push_connector_method,
                url=self.cfg.push_connector_url,
                headers=headers,
                auth=auth,
                data=json.dumps(payload),
                timeout=self.cfg.push_connector_timeout_seconds,
            )
        except requests.ReadTimeout as exc:
            raise PushConnectorTimeoutError(self.cfg.push_connector_url, self.cfg.push_connector_timeout_seconds) from exc
        self._raise_for_status(response, f"push connector {self.cfg.push_connector_url}")
        if not response.text.strip():
            return {}
        try:
            return response.json()
        except ValueError:
            return {"raw_text": response.text}

    @staticmethod
    def _raise_for_status(response: requests.Response, action: str) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            details = response.text[:2000]
            raise RuntimeError(f"{action} failed: {exc}; body={details}") from exc


class CMDBResolver:
    def __init__(self, client: ServiceNowClient, cfg: Config):
        self.client = client
        self.cfg = cfg
        self._service_environment_cache: Dict[str, str] = {}

    def resolve(self, alert: Dict[str, Any], sf: SalesforcePayload) -> CIChoice:
        alert_ci_value = raw_value(alert.get("cmdb_ci"))
        alert_ci_display = display_value(alert.get("cmdb_ci"))
        if alert_ci_value and is_sys_id(alert_ci_value):
            return CIChoice(sys_id=str(alert_ci_value), name=str(alert_ci_display or ""), source="alert.cmdb_ci")

        if self.cfg.enable_ci_lookup:
            for offering_name, label in [
                (sf.primary_offering, "primary_offering"),
                (sf.secondary_offering, "secondary_offering"),
            ]:
                offering_name = (offering_name or "").strip()
                if not offering_name:
                    continue
                exact = self._lookup_exact(offering_name, sf.environment)
                if exact:
                    env_label = normalize_environment(sf.environment) or "best_match"
                    exact.source = f"cmdb lookup exact:{label}:{env_label}"
                    return exact
                fuzzy = self._lookup_fuzzy(offering_name, sf.environment)
                if fuzzy:
                    env_label = normalize_environment(sf.environment) or "best_match"
                    fuzzy.source = f"cmdb lookup fuzzy:{label}:{env_label}"
                    return fuzzy

        return CIChoice(
            sys_id=self.cfg.default_cmdb_ci_sys_id,
            name=self.cfg.default_cmdb_ci_name,
            source="default",
        )

    def resolve_service_offering(self, sf: SalesforcePayload) -> ServiceOfferingRef:
        offering_name = sf.preferred_offering().strip()
        if not offering_name:
            return ServiceOfferingRef()

        existing = self._lookup_service_offering(offering_name, sf.environment)
        if existing:
            existing.source = "service_offering.existing"
            return existing

        LOGGER.warning(
            "service offering %s was not found; runtime write policy forbids creating service_offering/business_service records",
            offering_name,
        )
        return ServiceOfferingRef(offering_name=offering_name, source="service_offering.missing")

    def _lookup_exact(self, name: str, desired_environment: str = "") -> Optional[CIChoice]:
        safe = escape_query_value(name)
        rows = self.client.table_get(
            self.cfg.cmdb_table,
            query=f"name={safe}",
            fields=["sys_id", "name", "sys_class_name", "operational_status", "install_status", "environment"],
            limit=20,
        )
        return self._pick_best(rows, desired_environment)

    def _lookup_fuzzy(self, name: str, desired_environment: str = "") -> Optional[CIChoice]:
        safe = escape_query_value(name)
        rows = self.client.table_get(
            self.cfg.cmdb_table,
            query=f"nameLIKE{safe}",
            fields=["sys_id", "name", "sys_class_name", "operational_status", "install_status", "environment"],
            limit=20,
        )
        return self._pick_best(rows, desired_environment)

    @staticmethod
    def _pick_best(rows: Sequence[Dict[str, Any]], desired_environment: str = "") -> Optional[CIChoice]:
        if not rows:
            return None

        def score(row: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
            cls = normalize(str(display_value(row.get("sys_class_name")) or raw_value(row.get("sys_class_name")) or ""))
            op = normalize(str(display_value(row.get("operational_status")) or raw_value(row.get("operational_status")) or ""))
            install = normalize(str(display_value(row.get("install_status")) or raw_value(row.get("install_status")) or ""))
            env = first_non_empty(display_value(row.get("environment")), raw_value(row.get("environment")))
            env_score = environment_match_score(env, desired_environment)
            serviceish = 1 if any(token in cls for token in ("service", "business_app", "application")) else 0
            operational = 1 if any(token in op for token in ("operational", "up", "online")) else 0
            installed = 1 if "production" in install or "installed" in install else 0
            named = 1 if bool(display_value(row.get("name")) or raw_value(row.get("name"))) else 0
            return (env_score, serviceish, operational, installed, named)

        best = sorted(rows, key=score, reverse=True)[0]
        return CIChoice(
            sys_id=str(raw_value(best.get("sys_id")) or ""),
            name=str(display_value(best.get("name")) or raw_value(best.get("name")) or ""),
            source="cmdb lookup",
        )

    def _lookup_service_offering(self, name: str, desired_environment: str = "") -> Optional[ServiceOfferingRef]:
        safe = escape_query_value(name)
        rows = self.client.table_get(
            "service_offering",
            query=f"name={safe}",
            fields=["sys_id", "name", "parent", "environment"],
            limit=5,
        )
        if not rows:
            return None
        first = self._pick_best_service_offering(rows, desired_environment)
        if first is None:
            return None
        return ServiceOfferingRef(
            offering_sys_id=str(raw_value(first.get("sys_id")) or ""),
            offering_name=str(display_value(first.get("name")) or raw_value(first.get("name")) or name),
            service_sys_id=str(raw_value(first.get("parent")) or ""),
            service_name=str(display_value(first.get("parent")) or ""),
            source="service_offering.lookup",
        )

    def _pick_best_service_offering(
        self,
        rows: Sequence[Dict[str, Any]],
        desired_environment: str = "",
    ) -> Optional[Dict[str, Any]]:
        if not rows:
            return None

        def score(row: Dict[str, Any]) -> Tuple[int, int, int, int]:
            offering_environment = first_non_empty(display_value(row.get("environment")), raw_value(row.get("environment")))
            parent_sys_id = str(raw_value(row.get("parent")) or "")
            parent_environment = self._lookup_service_environment(parent_sys_id) if parent_sys_id else ""
            offering_environment_score = environment_match_score(offering_environment, desired_environment)
            parent_environment_score = environment_match_score(parent_environment, desired_environment)
            has_parent = 1 if parent_sys_id else 0
            named = 1 if bool(display_value(row.get("name")) or raw_value(row.get("name"))) else 0
            return (offering_environment_score, parent_environment_score, has_parent, named)

        return sorted(rows, key=score, reverse=True)[0]

    def _lookup_service_environment(self, service_sys_id: str) -> str:
        service_sys_id = str(service_sys_id or "").strip()
        if not service_sys_id:
            return ""
        if service_sys_id in self._service_environment_cache:
            return self._service_environment_cache[service_sys_id]
        try:
            row = self.client.table_get_record("cmdb_ci_service", service_sys_id)
        except Exception:
            self._service_environment_cache[service_sys_id] = ""
            return ""
        value = first_non_empty(display_value(row.get("environment")), raw_value(row.get("environment")))
        self._service_environment_cache[service_sys_id] = value
        return value

class AssignmentGroupResolver:
    def __init__(self, client: ServiceNowClient, cfg: Config):
        self.client = client
        self.cfg = cfg

    def resolve(self, alert: Dict[str, Any], offering: ServiceOfferingRef, ci: CIChoice) -> AssignmentGroupRef:
        offering_group = self._from_service_offering(offering)
        if offering_group.sys_id:
            return offering_group

        if self.cfg.default_assignment_group_sys_id:
            return AssignmentGroupRef(
                sys_id=self.cfg.default_assignment_group_sys_id,
                source="config.default_assignment_group",
            )

        dummy_group = self._from_dummy_ci()
        if dummy_group.sys_id:
            return dummy_group

        return AssignmentGroupRef()

    def _from_service_offering(self, offering: ServiceOfferingRef) -> AssignmentGroupRef:
        if not is_sys_id(offering.offering_sys_id):
            return AssignmentGroupRef()
        try:
            record = self.client.table_get_record("service_offering", offering.offering_sys_id)
        except Exception:
            LOGGER.warning("could not read service_offering %s for assignment group lookup", offering.offering_sys_id)
            return AssignmentGroupRef()
        return self._group_from_record(
            record,
            source_prefix="service_offering",
            field_order=[
                "support_group",
                "u_level_2_support_assignee_group",
                "u_level2_support_assignee_group",
                "u_level_3_support_assignee_group",
                "u_level3_support_assignee_group",
            ],
        )

    def _from_dummy_ci(self) -> AssignmentGroupRef:
        if not is_sys_id(self.cfg.default_cmdb_ci_sys_id):
            return AssignmentGroupRef()
        try:
            record = self.client.table_get_record(self.cfg.cmdb_table, self.cfg.default_cmdb_ci_sys_id)
        except Exception:
            LOGGER.warning("could not read default cmdb_ci %s for assignment group fallback", self.cfg.default_cmdb_ci_sys_id)
            return AssignmentGroupRef()
        return self._group_from_record(
            record,
            source_prefix="default_cmdb_ci",
            field_order=[
                "support_group",
                "u_level_2_support_assignee_group",
                "u_level2_support_assignee_group",
                "u_level_3_support_assignee_group",
                "u_level3_support_assignee_group",
            ],
        )

    @staticmethod
    def _group_from_record(
        record: Dict[str, Any],
        source_prefix: str,
        field_order: Sequence[str],
    ) -> AssignmentGroupRef:
        for field_name in field_order:
            group_value = raw_value(record.get(field_name))
            group_name = first_non_empty(display_value(record.get(field_name)), group_value)
            if is_sys_id(group_value):
                return AssignmentGroupRef(
                    sys_id=str(group_value),
                    name=group_name,
                    source=f"{source_prefix}.{field_name}",
                )
        return AssignmentGroupRef()


class SalesforcePayloadExtractor:
    def __init__(self) -> None:
        pass

    def extract(self, alert: Dict[str, Any], related_events: Sequence[Dict[str, Any]]) -> SalesforcePayload:
        payload = SalesforcePayload()
        merged_raw: Dict[str, Any] = {}

        # 1) direct key matches from alert and related event records
        for source_name, record in [("alert", alert), *[(f"event[{idx}]", ev) for idx, ev in enumerate(related_events)]]:
            self._walk_record(record, payload, merged_raw, source_name)

        # 2) fallbacks from alert fields if subject / endpoint / etc. are still missing
        if not payload.case_subject:
            payload.case_subject = first_non_empty(
                display_value(alert.get("short_description")),
                raw_value(alert.get("short_description")),
                display_value(alert.get("description")),
                raw_value(alert.get("description")),
            )
        if not payload.http_endpoint:
            payload.http_endpoint = first_non_empty(
                raw_value(alert.get("http_endpoint")),
                raw_value(alert.get("u_http_endpoint")),
                raw_value(alert.get("url")),
            )

        payload.raw_payload = merged_raw
        return payload

    def _walk_record(
        self,
        value: Any,
        payload: SalesforcePayload,
        merged_raw: Dict[str, Any],
        source_name: str,
        parent_key: str = "",
    ) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                canonical = canonicalize_key(key)
                source_key = f"{parent_key}.{key}" if parent_key else key
                maybe_set_canonical_field(payload, merged_raw, canonical, nested, source_name, source_key)
                self._walk_record(nested, payload, merged_raw, source_name, source_key)
            return

        if isinstance(value, list):
            for idx, item in enumerate(value):
                source_key = f"{parent_key}[{idx}]" if parent_key else f"[{idx}]"
                self._walk_record(item, payload, merged_raw, source_name, source_key)
            return

        if isinstance(value, str):
            self._walk_text(value, payload, merged_raw, source_name, parent_key)

    def _walk_text(
        self,
        text: str,
        payload: SalesforcePayload,
        merged_raw: Dict[str, Any],
        source_name: str,
        source_key: str,
    ) -> None:
        text = text.strip()
        if not text:
            return

        # whole-text JSON parse
        parsed = try_parse_jsonish(text)
        if isinstance(parsed, dict):
            self._walk_record(parsed, payload, merged_raw, source_name, source_key or "parsed_json")
        elif isinstance(parsed, list):
            self._walk_record(parsed, payload, merged_raw, source_name, source_key or "parsed_json")

        # key:value extraction fallback
        for match in KV_RE.finditer(text):
            key = canonicalize_key(match.group(1) or "")
            value = (match.group(2) if match.group(2) is not None else match.group(3) or "").strip().strip(",")
            maybe_set_canonical_field(payload, merged_raw, key, value, source_name, source_key or "regex")


def maybe_set_canonical_field(
    payload: SalesforcePayload,
    merged_raw: Dict[str, Any],
    canonical_key: str,
    value: Any,
    source_name: str,
    source_key: str,
) -> None:
    if value is None:
        return

    if isinstance(value, dict):
        candidate = first_non_empty(display_value(value), raw_value(value))
    else:
        candidate = str(value).strip()

    if candidate is None:
        return
    candidate = str(candidate).strip().strip(",")

    normalized_key = normalize(canonical_key)
    target_field = None
    for canonical_field, aliases in FIELD_ALIASES.items():
        if normalized_key in aliases:
            target_field = canonical_field
            break

    if target_field:
        if not getattr(payload, target_field):
            setattr(payload, target_field, candidate)
        merged_raw.setdefault(target_field, candidate)
        payload.sources.append(f"{source_name}:{source_key}")


def try_parse_jsonish(text: str) -> Any:
    text = text.strip()
    if not text:
        return None
    candidates = [text]

    # Also try the largest {...} or [...] substring if the whole text is noisy.
    for open_char, close_char in (("{", "}"), ("[", "]")):
        start = text.find(open_char)
        end = text.rfind(close_char)
        if start != -1 and end != -1 and end > start:
            candidates.append(text[start : end + 1])

    for candidate in candidates:
        for adjusted in (candidate, candidate.replace("\r", ""), candidate.replace("\t", " ")):
            try:
                return json.loads(adjusted)
            except Exception:
                continue
    return None


def canonicalize_key(key: str) -> str:
    key = key.strip().lower()
    key = re.sub(r"[^a-z0-9]+", "_", key)
    return key.strip("_")


def normalize(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip().lower()


def is_disabled_config_value(value: Any) -> bool:
    return normalize(str(value or "")) in {"0", "false", "no", "none", "null", "off", "disabled"}


ENVIRONMENT_ALIASES = {
    "prod": "prod",
    "production": "prod",
    "prd": "prod",
    "uat": "test",
    "qa": "test",
    "test": "test",
    "tst": "test",
    "sit": "test",
    "dev": "dev",
    "development": "dev",
    "stage": "stage",
    "staging": "stage",
    "stg": "stage",
    "sandbox": "sandbox",
    "sbx": "sandbox",
    "nonprod": "nonprod",
    "nonproduction": "nonprod",
}


def normalize_environment(value: Any) -> str:
    text = canonicalize_key(str(value or ""))
    if not text:
        return ""
    return ENVIRONMENT_ALIASES.get(text, text)


def environment_match_score(candidate_environment: Any, desired_environment: Any) -> int:
    candidate = normalize_environment(candidate_environment)
    desired = normalize_environment(desired_environment)
    if desired:
        if candidate == desired:
            return 4
        if not candidate:
            return 2
        return 0
    if not candidate:
        return 3
    return 1


def case_variants(value: str) -> List[str]:
    text = str(value or "").strip()
    if not text:
        return []
    variants: List[str] = []
    for candidate in (text, text.lower(), text.upper(), text.title()):
        candidate = candidate.strip()
        if candidate and candidate not in variants:
            variants.append(candidate)
    normalized = normalize(text)
    if normalized == "salesforce" and "SalesForce" not in variants:
        variants.append("SalesForce")
    return variants


def collect_text_candidates(value: Any) -> List[str]:
    collected: List[str] = []

    def _walk(item: Any) -> None:
        if item is None:
            return
        if isinstance(item, dict):
            for key in ("display_value", "value", "name", "label", "sys_id"):
                if key in item:
                    _walk(item.get(key))
            return
        if isinstance(item, (list, tuple, set)):
            for child in item:
                _walk(child)
            return
        text = str(item).strip()
        if text:
            collected.append(text)

    _walk(value)
    result: List[str] = []
    seen = set()
    for item in collected:
        key = normalize(item)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def parse_embedded_json(value: Any) -> Any:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text[0] not in "{[":
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


def find_nested_text(value: Any, keys: Sequence[str]) -> str:
    wanted = {str(key).strip() for key in keys if str(key).strip()}
    found = ""

    def walk(item: Any) -> None:
        nonlocal found
        if found or item is None:
            return
        if isinstance(item, dict):
            for key, nested in item.items():
                if str(key).strip() in wanted:
                    candidate = first_non_empty(nested)
                    if candidate:
                        found = candidate
                        return
            for nested in item.values():
                walk(nested)
            return
        if isinstance(item, (list, tuple, set)):
            for child in item:
                walk(child)
            return
        embedded = parse_embedded_json(item)
        if embedded is not None:
            walk(embedded)

    walk(value)
    return found


def first_non_empty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        if isinstance(value, dict):
            maybe = first_non_empty(display_value(value), raw_value(value))
            if maybe:
                return maybe
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def is_sys_id(value: Any) -> bool:
    return bool(value) and bool(SYS_ID_RE.match(str(value).strip()))


def raw_value(value: Any) -> Any:
    if isinstance(value, dict):
        if "value" in value:
            return value.get("value")
        if "sys_id" in value:
            return value.get("sys_id")
        return None
    return value


def display_value(value: Any) -> Any:
    if isinstance(value, dict):
        if "display_value" in value:
            return value.get("display_value")
        if "name" in value:
            return value.get("name")
        return None
    return value


def parse_sn_datetime(value: str) -> datetime:
    value = value.strip()
    if not value:
        raise ValueError("empty ServiceNow datetime")
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def to_sn_datetime(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def escape_query_value(value: str) -> str:
    return str(value).replace("^", " ").strip()


def severity_to_numeric(value: Any) -> Any:
    if value is None:
        return value
    raw = str(raw_value(value) or "").strip()
    if raw.isdigit():
        return int(raw)
    text = normalize(str(display_value(value) or value))
    mapping = {
        "critical": 1,
        "major": 2,
        "high": 2,
        "minor": 3,
        "warning": 4,
        "warn": 4,
        "info": 5,
        "informational": 5,
        "clear": 0,
        "ok": 0,
    }
    return mapping.get(text, raw or text)


def severity_to_dti_impact_urgency(value: Any) -> Tuple[str, str]:
    severity = severity_to_numeric(value)
    mapping = {
        0: ("4", "4"),
        1: ("2", "2"),
        2: ("2", "2"),
        3: ("3", "3"),
        4: ("4", "4"),
        5: ("4", "4"),
        "0": ("4", "4"),
        "1": ("2", "2"),
        "2": ("2", "2"),
        "3": ("3", "3"),
        "4": ("4", "4"),
        "5": ("4", "4"),
    }
    return mapping.get(severity, ("4", "4"))


def bucketize_alert_state(state_value: Any, cfg: Config) -> str:
    text = normalize(str(display_value(state_value) or raw_value(state_value) or state_value or ""))
    if not text:
        return "unknown"
    if any(token in text for token in cfg.closed_state_tokens):
        return "closed"
    if any(token in text for token in cfg.open_state_tokens):
        return "open"
    return text


def alert_task_field_name(alert: Dict[str, Any]) -> str:
    if "task" in alert:
        return "task"
    if "incident" in alert:
        return "incident"
    return "task"


def alert_task_sys_id(alert: Dict[str, Any]) -> str:
    field_name = alert_task_field_name(alert)
    return str(raw_value(alert.get(field_name)) or "")


def alert_incident_sys_id(alert: Dict[str, Any]) -> str:
    return str(raw_value(alert.get("incident")) or alert_task_sys_id(alert) or "")


def dti_helper_message_key(workload_token: str, alert: Dict[str, Any], create_reason: str) -> str:
    alert_id = str(raw_value(alert.get("sys_id")) or "")
    stamp = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S%f")
    digest = hashlib.sha1(f"{alert_id}|{workload_token}|{create_reason}|{stamp}".encode("utf-8")).hexdigest()[:12]
    return f"{DTI_HELPER_MESSAGE_KEY_PREFIX}-{digest}-{stamp}"


def is_dti_helper_alert(alert: Dict[str, Any]) -> bool:
    metric_name = normalize(first_non_empty(raw_value(alert.get("metric_name")), display_value(alert.get("metric_name"))))
    if metric_name == normalize(DTI_HELPER_METRIC_NAME):
        return True
    message_key = first_non_empty(raw_value(alert.get("message_key")), display_value(alert.get("message_key")))
    return normalize(message_key).startswith(normalize(DTI_HELPER_MESSAGE_KEY_PREFIX))


def build_alert_fingerprint(alert: Dict[str, Any], cfg: Config) -> str:
    stable = {
        "sys_id": raw_value(alert.get("sys_id")),
        "sys_updated_on": first_non_empty(raw_value(alert.get("sys_updated_on")), display_value(alert.get("sys_updated_on"))),
        "state": bucketize_alert_state(alert.get("state"), cfg),
        "task": alert_task_sys_id(alert),
        "incident": alert_incident_sys_id(alert),
        "severity": raw_value(alert.get("severity")),
        "source": raw_value(alert.get("source")),
        "type": raw_value(alert.get("type")),
    }
    return hashlib.sha1(json.dumps(stable, sort_keys=True).encode("utf-8")).hexdigest()


def render_incident_description(
    alert: Dict[str, Any],
    sf: SalesforcePayload,
    offering: ServiceOfferingRef,
) -> str:
    alert_number = first_non_empty(raw_value(alert.get("number")), display_value(alert.get("number")))

    lines = [
        "SalesForce Case Details",
        "=======================",
        f"SalesForce Case Number: {sf.case_number}",
        f"SalesForce Case Subject: {sf.case_subject}",
        f"SalesForce Case Status: {sf.case_status}",
        f"SalesForce Case Owner: {sf.case_owner}",
        f"SalesForce Case Created Date: {sf.case_created_date}",
        f"Expected Result: {sf.expected_result}",
        f"Number of Customers Impacted: {sf.number_of_customers_impacted}",
        f"SalesForce Primary Offering: {sf.primary_offering}",
        f"SalesForce Secondary Offering: {sf.secondary_offering}",
        f"SalesForce Environment: {sf.environment}",
    ]
    if sf.http_endpoint:
        lines.append(f"SalesForce HTTP Endpoint: {sf.http_endpoint}")
    if alert_number:
        lines.extend(["", "ServiceNow Context", "=================", f"Related Alert: {alert_number}"])
    if offering.offering_name:
        lines.append(f"Linked Service Offering: {offering.offering_name}")
    if offering.service_name:
        lines.append(f"Linked Business Service: {offering.service_name}")

    extras = {
        k: v
        for k, v in sf.raw_payload.items()
        if k not in KNOWN_SF_FIELDS and str(v).strip()
    }
    if extras:
        lines.extend(["", "Additional Salesforce Fields", "=========================="])
        for key in sorted(extras):
            label = key.replace("_", " ").strip().title()
            lines.append(f"{label}: {extras[key]}")

    return "\n".join(lines).rstrip()


def compose_short_description(alert: Dict[str, Any], sf: SalesforcePayload) -> str:
    subject = first_non_empty(sf.case_subject, raw_value(alert.get("short_description")), raw_value(alert.get("description")))
    if subject:
        subject = re.sub(r"\s+", " ", subject).strip()
    else:
        subject = "SalesForce alert"
    if not subject.lower().startswith("peru"):
        return f"PERU - {subject}"
    return subject


def describe_push_connector_failure(body: Any) -> str:
    incident_sys_id = find_nested_text(body, ("incident_sys_id",))
    if is_sys_id(incident_sys_id):
        return ""

    dti_status = normalize(find_nested_text(body, ("dti_incident_status",)))
    if dti_status not in {"create_failed", "failed", "error", "timeout", "timed_out"}:
        return ""

    alert_number = find_nested_text(body, ("alert_number",))
    processing_ms = find_nested_text(body, ("usbem_processing_ms",))
    connector_status = find_nested_text(body, ("status",))
    detail = find_nested_text(body, ("detail", "message", "reason", "error_message"))

    details: List[str] = [f"dti_incident_status={dti_status}"]
    if alert_number:
        details.append(f"alert={alert_number}")
    if processing_ms:
        details.append(f"processing_ms={processing_ms}")
    if connector_status and normalize(connector_status) != dti_status:
        details.append(f"connector_status={connector_status}")

    message = "genericJsonV2 accepted the event but did not create an incident"
    if details:
        message += f" ({', '.join(details)})"
    if detail and normalize(detail) not in {dti_status, normalize(connector_status)}:
        message += f": {detail}"
    message += (
        "; common causes are incident insert ACL/role restrictions, mandatory incident fields, "
        "or incident business rules on the target instance"
    )
    return message


class IncidentLocator:
    def __init__(self, client: ServiceNowClient, cfg: Config):
        self.client = client
        self.cfg = cfg

    def locate(
        self,
        response_body: Dict[str, Any],
        created_after: datetime,
        case_number: str,
        short_description: str,
        workaround_token: str,
        helper_message_key: str = "",
        alert_sys_id: str = "",
        previous_task: str = "",
        previous_incident: str = "",
    ) -> Optional[IncidentRef]:
        direct = self._extract_from_response(response_body)
        if direct:
            return direct

        deadline = time.time() + self.cfg.push_connector_wait_seconds
        while time.time() < deadline:
            found = (
                self._search_on_helper_alert(helper_message_key, created_after, previous_task, previous_incident)
                or self._search_on_description(workaround_token, created_after)
                or self._search_on_alert(alert_sys_id, previous_task, previous_incident)
                or self._search_by_external_case(case_number, created_after)
                or self._search_by_short_description(short_description, created_after)
                or self._search_on_description(case_number, created_after)
            )
            if found:
                return found
            time.sleep(max(1, self.cfg.push_connector_wait_poll_seconds))
        return None

    def _extract_from_response(self, body: Any) -> Optional[IncidentRef]:
        found_sys_id = ""
        found_number = ""

        def walk(value: Any) -> None:
            nonlocal found_sys_id, found_number
            if found_sys_id and found_number:
                return
            if isinstance(value, dict):
                incident_sys_id = value.get("incident_sys_id")
                incident_number = value.get("incident_number")
                if isinstance(incident_sys_id, str) and is_sys_id(incident_sys_id):
                    found_sys_id = incident_sys_id
                    if isinstance(incident_number, str) and incident_number.strip():
                        found_number = incident_number.strip().upper()
                    return
                incident_obj = value.get("incident")
                if isinstance(incident_obj, dict):
                    nested_sys_id = first_non_empty(
                        raw_value(incident_obj.get("sys_id")),
                        raw_value(incident_obj.get("value")),
                    )
                    nested_number = first_non_empty(
                        display_value(incident_obj.get("number")),
                        raw_value(incident_obj.get("number")),
                    )
                    if is_sys_id(nested_sys_id):
                        found_sys_id = nested_sys_id
                        found_number = nested_number.upper() if nested_number else ""
                        return
                for nested in value.values():
                    walk(nested)
                return
            if isinstance(value, list):
                for item in value:
                    walk(item)
                return
            embedded = parse_embedded_json(value)
            if embedded is not None:
                walk(embedded)

        walk(body)
        if found_sys_id:
            return IncidentRef(sys_id=found_sys_id, number=found_number)
        return None

    def _search_on_alert(self, alert_sys_id: str, previous_task: str, previous_incident: str) -> Optional[IncidentRef]:
        if not alert_sys_id:
            return None
        rows = self.client.table_get(
            self.cfg.alert_table,
            query=f"sys_id={escape_query_value(alert_sys_id)}",
            fields=["sys_id", "task", "incident"],
            limit=1,
        )
        if not rows:
            return None
        return self._incident_from_alert_row(rows[0], previous_task, previous_incident)

    def _search_on_helper_alert(
        self,
        message_key: str,
        created_after: datetime,
        previous_task: str,
        previous_incident: str,
    ) -> Optional[IncidentRef]:
        if not message_key:
            return None
        rows = self.client.table_get(
            self.cfg.alert_table,
            query=(
                f"message_key={escape_query_value(message_key)}"
                f"^sys_created_on>={to_sn_datetime(created_after - timedelta(seconds=30))}"
                "^ORDERBYDESCsys_created_on"
            ),
            fields=["sys_id", "number", "message_key", "task", "incident", "sys_created_on"],
            limit=3,
        )
        for row in rows:
            found = self._incident_from_alert_row(row, previous_task, previous_incident)
            if found:
                return found
        return None

    def _incident_from_alert_row(
        self,
        row: Dict[str, Any],
        previous_task: str,
        previous_incident: str,
    ) -> Optional[IncidentRef]:
        for key in ("task", "incident"):
            candidate = first_non_empty(raw_value(row.get(key)), display_value(row.get(key)))
            if not is_sys_id(candidate):
                continue
            if candidate in {previous_task, previous_incident}:
                continue
            number = ""
            try:
                incident = self.client.table_get_record(self.cfg.incident_table, candidate)
                number = first_non_empty(display_value(incident.get("number")), raw_value(incident.get("number")))
            except Exception:
                LOGGER.warning("could not read incident %s after DTI", candidate)
            return IncidentRef(sys_id=candidate, number=number)
        return None

    def _search_by_external_case(self, case_number: str, created_after: datetime) -> Optional[IncidentRef]:
        if not case_number:
            return None
        query = f"{self.cfg.incident_external_case_field}={escape_query_value(case_number)}^sys_created_on>={to_sn_datetime(created_after)}^ORDERBYDESCsys_created_on"
        rows = self.client.table_get(
            self.cfg.incident_table,
            query=query,
            fields=["sys_id", "number", self.cfg.incident_external_case_field, "short_description", "sys_created_on"],
            limit=5,
        )
        return self._first(rows)

    def _search_by_short_description(self, short_description: str, created_after: datetime) -> Optional[IncidentRef]:
        if not short_description:
            return None
        query = f"short_descriptionLIKE{escape_query_value(short_description[:80])}^sys_created_on>={to_sn_datetime(created_after)}^ORDERBYDESCsys_created_on"
        rows = self.client.table_get(
            self.cfg.incident_table,
            query=query,
            fields=["sys_id", "number", "short_description", "sys_created_on"],
            limit=5,
        )
        return self._first(rows)

    def _search_on_description(self, token: str, created_after: datetime) -> Optional[IncidentRef]:
        if not token:
            return None
        query = f"descriptionLIKE{escape_query_value(token[:80])}^sys_created_on>={to_sn_datetime(created_after)}^ORDERBYDESCsys_created_on"
        rows = self.client.table_get(
            self.cfg.incident_table,
            query=query,
            fields=["sys_id", "number", "description", "sys_created_on"],
            limit=5,
        )
        return self._first(rows)

    @staticmethod
    def _first(rows: Sequence[Dict[str, Any]]) -> Optional[IncidentRef]:
        if not rows:
            return None
        first = rows[0]
        sys_id = str(raw_value(first.get("sys_id")) or "")
        if not sys_id:
            return None
        number = str(first_non_empty(display_value(first.get("number")), raw_value(first.get("number"))))
        return IncidentRef(sys_id=sys_id, number=number)


class ARMEmulator:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        if self.cfg.auto_create_service_offerings:
            LOGGER.warning(
                "AUTO_CREATE_SERVICE_OFFERINGS=true is ignored; runtime write policy forbids CMDB/service_offering creates"
            )
            self.cfg.auto_create_service_offerings = False
        self.client = ServiceNowClient(cfg)
        self.cfg.incident_external_case_field = self._resolve_incident_external_case_field()
        self.cfg.incident_generating_alert_field = self._resolve_incident_generating_alert_field()
        self.state = StateStore(cfg.state_file)
        self.shutdown = GracefulShutdown()
        self.extractor = SalesforcePayloadExtractor()
        self.locator = IncidentLocator(self.client, cfg)
        self.cmdb = CMDBResolver(self.client, cfg)
        self.assignment_groups = AssignmentGroupResolver(self.client, cfg)

    def _resolve_incident_external_case_field(self) -> str:
        configured = str(self.cfg.incident_external_case_field or "").strip()
        if is_disabled_config_value(configured):
            return "u_external_salesforce_case_id"
        return configured or "u_external_salesforce_case_id"

    def _resolve_incident_generating_alert_field(self) -> str:
        configured = str(self.cfg.incident_generating_alert_field or "").strip()
        if is_disabled_config_value(configured):
            return ""
        return configured or "u_generating_alert"

    def validate_config(self) -> None:
        sn_host = urlparse(self.cfg.sn_instance_url).netloc.lower()
        push_url = self.cfg.push_connector_url or ''
        push_host = urlparse(push_url).netloc.lower() if push_url else ''
        push_source = ''
        if push_url:
            push_source = (parse_qs(urlparse(push_url).query).get('source') or [''])[0]

        LOGGER.info('ServiceNow instance URL: %s', self.cfg.sn_instance_url)
        LOGGER.info('Push connector URL: %s', self.cfg.push_connector_url)
        LOGGER.info('Incident external case field: %s', self.cfg.incident_external_case_field)
        if self.cfg.incident_generating_alert_field:
            LOGGER.info('Incident generating alert field: %s', self.cfg.incident_generating_alert_field)
        else:
            LOGGER.info('Incident generating alert field: not found, skipping')
        if sn_host and push_host and sn_host != push_host:
            LOGGER.warning('SN_INSTANCE_URL host (%s) and PUSH_CONNECTOR_URL host (%s) do not match', sn_host, push_host)
        if push_source:
            LOGGER.info('Push connector source parameter: %s', push_source)
        else:
            LOGGER.warning('PUSH_CONNECTOR_URL has no source= query parameter; verify the connector endpoint URL')

    def _candidate_key_tokens(self, kind: str) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        if kind == 'source':
            exact = ('source', 'event_source', 'u_source', 'source_instance', 'sourceinstance')
            fuzzy = ('source',)
        elif kind == 'type':
            exact = ('type', 'type_name', 'typename', 'alert_type', 'alerttype', 'event_type', 'eventtype', 'u_type', 'em_type', 'type_name_display')
            fuzzy = ('type',)
        else:
            exact = (kind,)
            fuzzy = (kind,)
        return exact, fuzzy

    def _collect_candidate_values_from_structure(self, value: Any, kind: str) -> List[str]:
        exact_keys, fuzzy_tokens = self._candidate_key_tokens(kind)
        collected: List[str] = []

        def add_values(raw: Any) -> None:
            for item in collect_text_candidates(raw):
                if item not in collected:
                    collected.append(item)

        def walk(item: Any) -> None:
            if item is None:
                return
            if isinstance(item, dict):
                for key, nested in item.items():
                    normalized_key = canonicalize_key(str(key))
                    if normalized_key in exact_keys or any(token in normalized_key for token in fuzzy_tokens):
                        add_values(nested)
                    walk(nested)
                return
            if isinstance(item, list):
                for child in item:
                    walk(child)
                return
            if isinstance(item, str):
                parsed = try_parse_jsonish(item)
                if isinstance(parsed, (dict, list)):
                    walk(parsed)

        walk(value)
        return collected

    def _field_candidates(self, alert: Dict[str, Any], kind: str, related_records: Optional[Sequence[Dict[str, Any]]] = None) -> List[str]:
        collected: List[str] = []

        def add_values(raw: Any) -> None:
            for item in self._collect_candidate_values_from_structure(raw, kind):
                normalized_item = normalize(item)
                if not normalized_item:
                    continue
                if item not in collected:
                    collected.append(item)

        add_values(alert)
        if related_records:
            for record in related_records:
                add_values(record)
        return collected

    def _salesforce_payload_signature(
        self,
        alert: Dict[str, Any],
        related_records: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> Tuple[bool, SalesforcePayload, List[str]]:
        sf = self.extractor.extract(alert, related_records or [])
        populated = [name for name, value in sf.as_dict().items() if str(value or "").strip()]
        supporting = [name for name in populated if name != "case_number"]
        has_offering = bool((sf.primary_offering or "").strip() or (sf.secondary_offering or "").strip())
        matches = bool((sf.case_number or "").strip()) and (has_offering or len(supporting) >= 2)
        return matches, sf, populated

    def _source_matches(self, alert: Dict[str, Any], related_records: Optional[Sequence[Dict[str, Any]]] = None) -> bool:
        expected = {normalize(item) for item in case_variants(self.cfg.alert_source)}
        if not expected:
            return False
        actual = {normalize(item) for item in self._field_candidates(alert, 'source', related_records)}
        return bool(expected & actual)

    def _raw_text_contains_expected_type(
        self,
        alert: Dict[str, Any],
        related_records: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> bool:
        expected = normalize(self.cfg.alert_type_contains)
        if not expected:
            return True

        blobs: List[str] = []
        for record in [alert, *(related_records or [])]:
            try:
                blobs.append(json.dumps(record, sort_keys=True, default=str))
            except Exception:
                blobs.append(str(record))

        haystack = normalize(" ".join(blobs))
        if not haystack:
            return False

        pattern = rf"(?<![a-z0-9]){re.escape(expected)}(?![a-z0-9])"
        return re.search(pattern, haystack, flags=re.IGNORECASE) is not None

    def _type_matches(self, alert: Dict[str, Any], related_records: Optional[Sequence[Dict[str, Any]]] = None) -> bool:
        expected = normalize(self.cfg.alert_type_contains)
        if not expected:
            return True

        raw_candidates = self._field_candidates(alert, 'type', related_records)
        normalized_candidates = [normalize(item) for item in raw_candidates if normalize(item)]
        meaningful_candidates = [item for item in normalized_candidates if not is_sys_id(item)]

        if any(expected in item for item in meaningful_candidates):
            return True

        if meaningful_candidates:
            return False

        # ServiceNow has been inconsistent across environments when filtering alert "Type" with
        # server-side LIKE queries. When the structured type fields are blank or only contain
        # sys_ids, fall back to a case-insensitive raw text scan over the hydrated alert and
        # related event JSON.
        return self._raw_text_contains_expected_type(alert, related_records)

    def _alert_match_diagnostics(
        self,
        alert: Dict[str, Any],
        related_records: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> Tuple[List[str], List[str], bool, str, List[str]]:
        payload_match, sf, populated = self._salesforce_payload_signature(alert, related_records)
        return (
            self._field_candidates(alert, 'source', related_records),
            self._field_candidates(alert, 'type', related_records),
            payload_match,
            sf.case_number,
            populated,
        )

    def _alert_matches_client_filter(
        self,
        alert: Dict[str, Any],
        related_records: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> bool:
        if is_dti_helper_alert(alert):
            return False
        if self._source_matches(alert, related_records) and self._type_matches(alert, related_records):
            return True
        payload_match, _, _ = self._salesforce_payload_signature(alert, related_records)
        return payload_match

    def probe_alerts(self, limit: int = 10) -> int:
        fields = self._alert_fields()
        LOGGER.info("probing alerts on %s", self.cfg.sn_instance_url)
        LOGGER.info("server-side discovery query: source candidates plus alert-table payload matching")
        LOGGER.info("local alert rule: %s", self._build_alert_base_query())
        alerts = self._discover_alerts(limit=max(1, limit), since=None, bootstrap=True, order_by="ORDERBYDESCsys_updated_on")
        LOGGER.info("probe returned %d matching alert(s)", len(alerts))
        for alert in alerts:
            source_candidates, type_candidates, payload_match, case_number, populated = self._alert_match_diagnostics(alert)
            LOGGER.info(
                "probe alert number=%s sys_id=%s source_candidates=%s type_candidates=%s payload_match=%s case_number=%s payload_fields=%s raw_type_fallback=%s state=%s task=%s updated=%s",
                first_non_empty(raw_value(alert.get("number")), display_value(alert.get("number"))),
                first_non_empty(raw_value(alert.get("sys_id")), display_value(alert.get("sys_id"))),
                source_candidates,
                type_candidates,
                payload_match,
                case_number,
                populated,
                self._raw_text_contains_expected_type(alert),
                first_non_empty(raw_value(alert.get("state")), display_value(alert.get("state"))),
                alert_task_sys_id(alert),
                first_non_empty(raw_value(alert.get("sys_updated_on")), display_value(alert.get("sys_updated_on"))),
            )
        if alerts:
            return 0

        LOGGER.warning("probe found no matching alerts; dumping latest source=%s alerts for diagnosis", self.cfg.alert_source)
        source_query = f"source={escape_query_value(self.cfg.alert_source)}"
        raw_rows = self.client.table_get(
            self.cfg.alert_table,
            query=source_query,
            fields=fields,
            limit=max(1, limit),
            offset=0,
            order_by="ORDERBYDESCsys_updated_on",
        )
        if not raw_rows:
            raw_rows = self.client.table_get(
                self.cfg.alert_table,
                query="",
                fields=fields,
                limit=max(1, limit),
                offset=0,
                order_by="ORDERBYDESCsys_updated_on",
        )
        LOGGER.info("diagnostic raw alert sample count=%d", len(raw_rows))
        for alert in raw_rows:
            source_candidates, type_candidates, payload_match, case_number, populated = self._alert_match_diagnostics(alert)
            LOGGER.info(
                "raw alert number=%s sys_id=%s source_candidates=%s type_candidates=%s payload_match=%s case_number=%s payload_fields=%s raw_type_fallback=%s state=%s task=%s updated=%s",
                first_non_empty(raw_value(alert.get("number")), display_value(alert.get("number"))),
                first_non_empty(raw_value(alert.get("sys_id")), display_value(alert.get("sys_id"))),
                source_candidates,
                type_candidates,
                payload_match,
                case_number,
                populated,
                self._raw_text_contains_expected_type(alert),
                first_non_empty(raw_value(alert.get("state")), display_value(alert.get("state"))),
                alert_task_sys_id(alert),
                first_non_empty(raw_value(alert.get("sys_updated_on")), display_value(alert.get("sys_updated_on"))),
            )
        return 0

    def run(self, once: bool = False) -> int:
        self.validate_config()
        LOGGER.info("starting Peru Incident Management Process")
        while not self.shutdown.stop:
            try:
                self.run_once()
            except Exception:
                LOGGER.exception("poll cycle failed")
            if once:
                break
            self.shutdown_wait(self.cfg.poll_interval_seconds)
        LOGGER.info("stopped")
        return 0

    def run_once(self) -> None:
        now = datetime.now(tz=UTC)
        watermark = self.state.watermark() or (now - timedelta(seconds=self.cfg.initial_lookback_seconds))
        query_since = watermark - timedelta(seconds=self.cfg.query_overlap_seconds)
        LOGGER.debug("querying alerts updated since %s", to_sn_datetime(query_since))

        alerts = self.fetch_recent_alerts(query_since)
        LOGGER.info("fetched %d matching alert(s) from %s", len(alerts), self.cfg.sn_instance_url)
        if not alerts:
            LOGGER.info("effective alert base query: %s", self._build_alert_base_query())
        max_seen = watermark

        for alert in alerts:
            updated_on_text = first_non_empty(raw_value(alert.get("sys_updated_on")), display_value(alert.get("sys_updated_on")))
            try:
                updated_on = parse_sn_datetime(updated_on_text)
            except Exception:
                updated_on = now
            if updated_on > max_seen:
                max_seen = updated_on
            self.process_alert(alert)

        self.state.set_watermark(max_seen)
        if self.cfg.dry_run and not self.cfg.persist_state_in_dry_run:
            LOGGER.info("dry-run mode: not persisting watermark/state to %s", self.cfg.state_file)
            return
        self.state.save()

    def _alert_fields(self) -> List[str]:
        return [
            "sys_id",
            "number",
            "source",
            "source_instance",
            "type",
            "type.name",
            "type_name",
            "event_type",
            "state",
            "task",
            "incident",
            "cmdb_ci",
            "severity",
            "short_description",
            "description",
            "node",
            "resource",
            "metric_name",
            "message_key",
            "additional_info",
            "sys_updated_on",
            "sys_created_on",
            "initial_event",
            "last_event",
        ]

    def _build_alert_query_candidates(self, since: Optional[datetime], bootstrap: bool) -> List[str]:
        source_values: List[str] = []
        for seed in [self.cfg.alert_source, "GenericJSON", "salesforce"]:
            for variant in case_variants(seed):
                if variant and variant not in source_values:
                    source_values.append(variant)
        timestamp_clause = f"sys_updated_on>={to_sn_datetime(since)}" if since else ""
        extra = self.cfg.alert_query_extra.strip()
        candidates: List[str] = []
        seen = set()

        def add(parts: Sequence[str]) -> None:
            query = "^".join([part for part in parts if part])
            if query in seen:
                return
            seen.add(query)
            candidates.append(query)

        # Final workaround requirement: Salesforce still posts directly to genericJsonV2,
        # so we have to discover candidate alerts as they already exist on the platform.
        # Query a few likely sources first, then fall back to recent alerts and match the
        # Salesforce payload signature locally from alert/event content.
        for source_value in source_values:
            safe_source = escape_query_value(source_value)
            add([f"source={safe_source}", extra, timestamp_clause])
            add([f"sourceLIKE{safe_source}", extra, timestamp_clause])
            add([f"source.nameLIKE{safe_source}", extra, timestamp_clause])

        # Last-resort fallback: recent alerts without source filter, still matched locally.
        add([timestamp_clause])

        if bootstrap:
            for source_value in source_values:
                safe_source = escape_query_value(source_value)
                add([f"source={safe_source}", extra])
                add([f"sourceLIKE{safe_source}", extra])
                add([f"source.nameLIKE{safe_source}", extra])
            add([""])

        return candidates

    def _sort_alerts_by_updated(self, alerts: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def sort_key(alert: Dict[str, Any]) -> Tuple[int, str]:
            raw_text = first_non_empty(raw_value(alert.get("sys_updated_on")), display_value(alert.get("sys_updated_on")))
            try:
                dt = parse_sn_datetime(raw_text)
                return (int(dt.timestamp()), raw_text)
            except Exception:
                return (0, raw_text)

        return sorted(list(alerts), key=sort_key)

    def _hydrate_alert_row_for_match(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        alert_sys_id = str(raw_value(row.get('sys_id')) or '')
        if not alert_sys_id:
            return row if self._alert_matches_client_filter(row) else None
        try:
            full_alert = self.client.table_get_record(self.cfg.alert_table, alert_sys_id)
        except Exception:
            LOGGER.warning('failed hydrating alert %s during discovery', alert_sys_id)
            return row if self._alert_matches_client_filter(row) else None

        related_events: List[Dict[str, Any]] = []
        try:
            related_events = self._load_related_events(full_alert)
        except Exception:
            LOGGER.debug('related event lookup failed during discovery for alert %s', alert_sys_id)

        if self._alert_matches_client_filter(full_alert, related_events):
            return full_alert
        return None

    def _discover_alerts(
        self,
        limit: int,
        since: Optional[datetime],
        bootstrap: bool,
        order_by: str,
    ) -> List[Dict[str, Any]]:
        fields = self._alert_fields()
        candidates = self._build_alert_query_candidates(since=since, bootstrap=bootstrap)
        dedup: Dict[str, Dict[str, Any]] = {}

        for query in candidates:
            rows = self.client.table_get(
                self.cfg.alert_table,
                query=query,
                fields=fields,
                limit=max(1, limit),
                offset=0,
                order_by=order_by,
            )
            direct_matches: List[Dict[str, Any]] = []
            hydrate_candidates: List[Dict[str, Any]] = []
            query_mentions_source = 'source' in query.lower()

            for row in rows:
                if self._alert_matches_client_filter(row):
                    direct_matches.append(row)
                    continue
                if query_mentions_source or self._source_matches(row):
                    hydrate_candidates.append(row)

            hydrated_matches: List[Dict[str, Any]] = []
            if hydrate_candidates and len(direct_matches) < limit:
                hydrate_limit = min(self.cfg.alert_discovery_hydrate_limit, max(1, limit))
                for row in hydrate_candidates[:hydrate_limit]:
                    hydrated = self._hydrate_alert_row_for_match(row)
                    if hydrated is not None:
                        hydrated_matches.append(hydrated)

            LOGGER.info(
                "alert discovery candidate query=%s returned %d row(s), %d direct client-side match(es), %d hydrated match(es) [payload matched locally]",
                query or "<all alerts>",
                len(rows),
                len(direct_matches),
                len(hydrated_matches),
            )

            for row in [*direct_matches, *hydrated_matches]:
                sys_id = str(raw_value(row.get("sys_id")) or "")
                if sys_id and sys_id not in dedup:
                    dedup[sys_id] = row
            if dedup:
                return self._sort_alerts_by_updated(dedup.values())
        return []

    def fetch_recent_alerts(self, since: datetime) -> List[Dict[str, Any]]:
        results = self._discover_alerts(
            limit=self.cfg.alert_query_limit,
            since=since,
            bootstrap=False,
            order_by="ORDERBYDESCsys_updated_on",
        )
        if results:
            return results

        should_bootstrap = (
            self.cfg.alert_bootstrap_ignore_watermark_on_empty
            and not self.state.data.get("alerts")
        )
        if should_bootstrap:
            LOGGER.info(
                "no alerts matched timestamped query set on %s; retrying latest matching alerts without watermark",
                self.cfg.sn_instance_url,
            )
            return self._discover_alerts(
                limit=self.cfg.alert_bootstrap_limit,
                since=None,
                bootstrap=True,
                order_by="ORDERBYDESCsys_updated_on",
            )

        return []

    def _build_alert_base_query(self) -> str:
        query_parts = []
        if self.cfg.alert_source:
            query_parts.append(f"source={escape_query_value(self.cfg.alert_source)}")
        if self.cfg.alert_type_contains:
            query_parts.append(f"typeLIKE{escape_query_value(self.cfg.alert_type_contains)}")
        if self.cfg.alert_query_extra:
            query_parts.append(self.cfg.alert_query_extra)
        query_parts.append("payload looks like Salesforce case")
        return "^".join(part for part in query_parts if part)

    def process_alert(self, alert: Dict[str, Any]) -> None:
        alert_sys_id = str(raw_value(alert.get("sys_id")) or "")
        alert_number = first_non_empty(raw_value(alert.get("number")), display_value(alert.get("number")))
        if not alert_sys_id:
            LOGGER.warning("skipping alert with no sys_id: %s", alert)
            return

        bucket = bucketize_alert_state(alert.get("state"), self.cfg)
        task_sys_id = alert_task_sys_id(alert)
        incident_sys_id = alert_incident_sys_id(alert)
        fingerprint = build_alert_fingerprint(alert, self.cfg)
        existing = self.state.get_alert(alert_sys_id)
        prev_bucket = str(existing.get("last_bucket") or "")
        prev_task = str(existing.get("last_task") or "")
        prev_incident = str(existing.get("last_incident_link") or "")
        prev_fingerprint = str(existing.get("last_fingerprint") or "")

        if prev_fingerprint == fingerprint:
            LOGGER.debug("alert %s unchanged, skipping", alert_number or alert_sys_id)
            return

        create_reason = self._decide_create_reason(bucket, task_sys_id, prev_bucket, prev_task, existing)
        LOGGER.info(
            "alert=%s state=%s task=%s prev_state=%s prev_task=%s create_reason=%s",
            alert_number or alert_sys_id,
            bucket,
            task_sys_id or "<empty>",
            prev_bucket or "<none>",
            prev_task or "<empty>",
            create_reason or "<skip>",
        )

        state_record = {
            **existing,
            "last_bucket": bucket,
            "last_task": task_sys_id,
            "last_incident_link": incident_sys_id,
            "last_fingerprint": fingerprint,
            "last_seen_updated_on": first_non_empty(raw_value(alert.get("sys_updated_on")), display_value(alert.get("sys_updated_on"))),
        }

        if create_reason:
            try:
                full_alert = self.client.table_get_record(self.cfg.alert_table, alert_sys_id)
                previous_task, previous_incident = self._prepare_alert_for_dti(full_alert, create_reason)
                related_events = self._load_related_events(full_alert)
                sf = self.extractor.extract(full_alert, related_events)
                offering = self.cmdb.resolve_service_offering(sf)
                ci = self.cmdb.resolve(full_alert, sf)
                assignment_group = self.assignment_groups.resolve(full_alert, offering, ci)
                workload_token = self._build_workload_token(full_alert)
                short_description = compose_short_description(full_alert, sf)
                description = render_incident_description(full_alert, sf, offering)
                helper_message_key = dti_helper_message_key(workload_token, full_alert, create_reason)

                created_after = datetime.now(tz=UTC) - timedelta(seconds=5)
                push_payload = self._build_push_connector_payload(
                    alert=full_alert,
                    sf=sf,
                    offering=offering,
                    ci=ci,
                    assignment_group=assignment_group,
                    short_description=short_description,
                    description=description,
                    workload_token=workload_token,
                    helper_message_key=helper_message_key,
                    create_reason=create_reason,
                )
                LOGGER.debug("push connector payload for alert %s: %s", alert_number or alert_sys_id, json.dumps(push_payload, sort_keys=True))
                LOGGER.info(
                    "sending helper DTI event for alert %s with helper message_key=%s",
                    alert_number or alert_sys_id,
                    helper_message_key,
                )
                push_response: Dict[str, Any] = {}
                push_timed_out = False
                try:
                    push_response = self.client.push_connector_call(push_payload)
                    LOGGER.debug("push connector response for alert %s: %s", alert_number or alert_sys_id, json.dumps(push_response, sort_keys=True))
                    LOGGER.info("push connector accepted alert %s", alert_number or alert_sys_id)
                except PushConnectorTimeoutError:
                    push_timed_out = True
                    LOGGER.warning(
                        "push connector timed out for alert %s after %ss; waiting up to %ss for any linked incident to appear",
                        alert_number or alert_sys_id,
                        self.cfg.push_connector_timeout_seconds,
                        self.cfg.push_connector_wait_seconds,
                    )

                incident_ref = self.locator.locate(
                    push_response,
                    created_after=created_after,
                    case_number=sf.case_number,
                    short_description=short_description,
                    workaround_token=workload_token,
                    helper_message_key=helper_message_key,
                    alert_sys_id=alert_sys_id,
                    previous_task=previous_task,
                    previous_incident=previous_incident,
                )
                connector_failure = describe_push_connector_failure(push_response)
                if connector_failure and incident_ref is None:
                    raise RuntimeError(connector_failure)
                if connector_failure and incident_ref is not None:
                    LOGGER.warning(
                        "connector reported a DTI issue for alert %s, but incident %s became visible anyway: %s",
                        alert_number or alert_sys_id,
                        incident_ref.number or incident_ref.sys_id,
                        connector_failure,
                    )
                if incident_ref is None:
                    if push_timed_out:
                        raise RuntimeError(
                            f"push connector timed out after {self.cfg.push_connector_timeout_seconds}s and no incident became "
                            f"visible within {self.cfg.push_connector_wait_seconds}s. Increase "
                            "PUSH_CONNECTOR_TIMEOUT_SECONDS if this instance responds slowly."
                        )
                    raise RuntimeError(
                        "push connector call succeeded but no incident could be located on the alert or in the response; "
                        "verify PUSH_CONNECTOR_URL, alert dedupe fields, and your genericMappedJson connector install"
                    )

                self._patch_incident(incident_ref, full_alert, sf, offering, ci, assignment_group, short_description, description)
                self._tag_incident(incident_ref)
                self._update_alert_task(full_alert, incident_ref)

                state_record.update(
                    {
                        "last_task": incident_ref.sys_id,
                        "last_incident_link": incident_ref.sys_id,
                        "last_incident_sys_id": incident_ref.sys_id,
                        "last_incident_number": incident_ref.number,
                        "last_helper_message_key": helper_message_key,
                        "last_action": "created_incident",
                        "last_action_reason": create_reason,
                        "last_action_at": to_sn_datetime(datetime.now(tz=UTC)),
                    }
                )
            except Exception as exc:
                LOGGER.exception("failed processing alert %s", alert_number or alert_sys_id)
                state_record.update(
                    {
                        "last_action": "error",
                        "last_error": str(exc),
                        "last_helper_message_key": helper_message_key if 'helper_message_key' in locals() else "",
                        "last_action_reason": create_reason,
                        "last_action_at": to_sn_datetime(datetime.now(tz=UTC)),
                    }
                )
        else:
            state_record.update(
                {
                    "last_action": "skipped",
                    "last_action_reason": "already_linked_or_not_open",
                    "last_action_at": to_sn_datetime(datetime.now(tz=UTC)),
                }
            )

        self.state.set_alert(alert_sys_id, state_record)

    def _decide_create_reason(
        self,
        bucket: str,
        current_task: str,
        prev_bucket: str,
        prev_task: str,
        existing: Dict[str, Any],
    ) -> str:
        if bucket != "open":
            return ""

        if not existing:
            return "new_open_alert_missing_task" if not current_task else ""

        # If something else already linked a new task, stay out of the way.
        if current_task and current_task != prev_task:
            return ""

        if prev_bucket and prev_bucket != "open":
            return "reopened_alert_new_incident"

        if prev_task and not current_task:
            return "open_alert_task_cleared"

        if not prev_task and not current_task and existing.get("last_action") == "error":
            return "retry_after_previous_error"

        return ""

    def _load_related_events(self, alert: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates: List[str] = []
        for key, value in alert.items():
            if "event" not in normalize(key):
                continue
            raw = raw_value(value)
            if is_sys_id(raw):
                candidates.append(str(raw))
        unique = []
        for item in candidates:
            if item not in unique:
                unique.append(item)
        events: List[Dict[str, Any]] = []
        for event_sys_id in unique[:3]:
            try:
                events.append(self.client.table_get_record(self.cfg.event_table, event_sys_id))
            except Exception:
                LOGGER.warning("could not load related event %s for alert %s", event_sys_id, raw_value(alert.get("sys_id")))
        return events

    def _prepare_alert_for_dti(self, alert: Dict[str, Any], create_reason: str) -> Tuple[str, str]:
        task_field = alert_task_field_name(alert)
        previous_task = alert_task_sys_id(alert)
        previous_incident = alert_incident_sys_id(alert)

        patch: Dict[str, Any] = {}
        if create_reason == "reopened_alert_new_incident":
            if previous_task:
                patch[task_field] = ""
            if "incident" in alert and previous_incident and task_field != "incident":
                patch["incident"] = ""
        elif create_reason in {"new_open_alert_missing_task", "open_alert_task_cleared"}:
            if "incident" in alert and previous_incident:
                patch["incident"] = ""

        if patch:
            try:
                self.client.table_update(self.cfg.alert_table, str(raw_value(alert.get("sys_id"))), patch)
            except Exception:
                if "incident" in patch and "task" not in patch:
                    LOGGER.warning("failed clearing alert incident field; retrying without incident clear for alert %s", raw_value(alert.get("number")) or raw_value(alert.get("sys_id")))
                    patch = {k: v for k, v in patch.items() if k != "incident"}
                    if patch:
                        self.client.table_update(self.cfg.alert_table, str(raw_value(alert.get("sys_id"))), patch)
                else:
                    raise

            for field_name in patch:
                alert[field_name] = {"value": "", "display_value": ""}

        return previous_task, previous_incident

    def _build_workload_token(self, alert: Dict[str, Any]) -> str:
        alert_id = str(raw_value(alert.get("sys_id")) or "")
        updated_on = first_non_empty(raw_value(alert.get("sys_updated_on")), display_value(alert.get("sys_updated_on")))
        digest = hashlib.sha1(f"{alert_id}|{updated_on}".encode("utf-8")).hexdigest()[:12]
        return f"TEMP-ARM-{digest}"

    def _build_push_connector_payload(
        self,
        alert: Dict[str, Any],
        sf: SalesforcePayload,
        offering: ServiceOfferingRef,
        ci: CIChoice,
        assignment_group: AssignmentGroupRef,
        short_description: str,
        description: str,
        workload_token: str,
        helper_message_key: str,
        create_reason: str,
    ) -> Dict[str, Any]:
        source = first_non_empty(raw_value(alert.get("source")), display_value(alert.get("source")), self.cfg.alert_source)
        alert_type = first_non_empty(display_value(alert.get("type")), raw_value(alert.get("type")), self.cfg.alert_type_contains)
        node = first_non_empty(raw_value(alert.get("node")), display_value(alert.get("node")), ci.name, "Salesforce")
        resource = first_non_empty(
            raw_value(alert.get("resource")),
            display_value(alert.get("resource")),
            sf.case_number,
            raw_value(alert.get("number")),
            raw_value(alert.get("sys_id")),
        )
        metric_name = DTI_HELPER_METRIC_NAME
        severity = severity_to_numeric(alert.get("severity"))
        base_description = first_non_empty(
            f"Salesforce case {sf.case_number}" if sf.case_number else "",
            sf.case_subject,
            raw_value(alert.get("short_description")),
            raw_value(alert.get("description")),
            short_description,
        )
        helper_description = f"{base_description} [{workload_token}]".strip() if workload_token else base_description

        base_payload: Dict[str, Any] = {
            "source": source,
            "type": alert_type,
            "node": node,
            "resource": resource,
            "metric_name": metric_name,
            "severity": severity,
            "description": helper_description,
            "time_of_event": to_sn_datetime(datetime.now(tz=UTC)),
            "message_key": helper_message_key,
            "direct_to_incident": "true",
            "dti_wait_for_incident": "true",
            "dti_short_description": short_description,
            "dti_work_note": "Direct To Incident Via Event Management Generic JSON Endpoint",
        }

        if ci.sys_id:
            base_payload["cmdb_ci"] = ci.sys_id
        if assignment_group.sys_id:
            base_payload["assignment_group"] = assignment_group.sys_id

        if self.cfg.push_connector_payload_mode == "events_array":
            return {"events": [base_payload]}
        return base_payload

    def _patch_incident(

        self,
        incident: IncidentRef,
        alert: Dict[str, Any],
        sf: SalesforcePayload,
        offering: ServiceOfferingRef,
        ci: CIChoice,
        assignment_group: AssignmentGroupRef,
        short_description: str,
        description: str,
    ) -> None:
        alert_sys_id = str(raw_value(alert.get("sys_id")) or "")
        patch: Dict[str, Any] = {
            "short_description": short_description,
            "description": description,
            self.cfg.incident_external_case_field: sf.case_number,
        }
        if self.cfg.incident_generating_alert_field and alert_sys_id:
            patch[self.cfg.incident_generating_alert_field] = alert_sys_id
        if ci.sys_id:
            patch["cmdb_ci"] = ci.sys_id
        if offering.service_sys_id:
            patch["business_service"] = offering.service_sys_id
        if offering.offering_sys_id:
            patch["service_offering"] = offering.offering_sys_id
        if assignment_group.sys_id:
            patch["assignment_group"] = assignment_group.sys_id
        if self.cfg.set_assigned_to and self.cfg.salesforce_user_sys_id:
            patch["assigned_to"] = self.cfg.salesforce_user_sys_id
        if self.cfg.set_caller_id and self.cfg.salesforce_user_sys_id:
            patch["caller_id"] = self.cfg.salesforce_user_sys_id
        if self.cfg.incident_extra_static_fields_json:
            try:
                extra = json.loads(self.cfg.incident_extra_static_fields_json)
                if isinstance(extra, dict):
                    patch.update(extra)
            except Exception:
                LOGGER.warning("INCIDENT_EXTRA_STATIC_FIELDS_JSON is not valid JSON, ignoring")

        try:
            self.client.table_update(self.cfg.incident_table, incident.sys_id, patch)
        except RuntimeError as exc:
            optional_field = self.cfg.incident_generating_alert_field
            if optional_field and optional_field in patch:
                retry_patch = dict(patch)
                retry_patch.pop(optional_field, None)
                LOGGER.warning(
                    "incident patch failed with optional field %s present; retrying without it: %s",
                    optional_field,
                    exc,
                )
                self.client.table_update(self.cfg.incident_table, incident.sys_id, retry_patch)
                self.cfg.incident_generating_alert_field = ""
                LOGGER.warning(
                    "disabled optional incident field %s for the rest of this run after patch rejection",
                    optional_field,
                )
            else:
                raise
        LOGGER.info("patched incident %s (%s)", incident.number or incident.sys_id, incident.sys_id)

    def _tag_incident(self, incident: IncidentRef) -> None:
        if not self.cfg.enable_tagging or not self.cfg.pinc_tag_sys_id:
            return
        try:
            existing = self.client.table_get(
                self.cfg.label_entry_table,
                query=f"table={escape_query_value(self.cfg.incident_table)}^table_key={incident.sys_id}^label={self.cfg.pinc_tag_sys_id}",
                fields=["sys_id", "label", "table", "table_key"],
                limit=1,
            )
            if existing:
                LOGGER.info("incident %s already has PINC tag", incident.number or incident.sys_id)
                return
            self.client.table_create(
                self.cfg.label_entry_table,
                {
                    "table": self.cfg.incident_table,
                    "table_key": incident.sys_id,
                    "label": self.cfg.pinc_tag_sys_id,
                },
            )
            LOGGER.info("tagged incident %s with PINC", incident.number or incident.sys_id)
        except Exception:
            LOGGER.exception("failed to tag incident %s with PINC", incident.number or incident.sys_id)

    def _update_alert_task(self, alert: Dict[str, Any], incident: IncidentRef) -> None:
        alert_sys_id = str(raw_value(alert.get("sys_id")) or "")
        if not alert_sys_id:
            return
        task_field = alert_task_field_name(alert)
        patch = {task_field: incident.sys_id}
        if "incident" in alert and task_field != "incident":
            patch["incident"] = incident.sys_id
        self.client.table_update(self.cfg.alert_table, alert_sys_id, patch)
        LOGGER.info(
            "updated alert %s %s -> incident %s",
            raw_value(alert.get("number")) or alert_sys_id,
            task_field,
            incident.number or incident.sys_id,
        )

    def shutdown_wait(self, seconds: int) -> None:
        deadline = time.time() + max(1, seconds)
        while time.time() < deadline:
            if self.shutdown.stop:
                return
            time.sleep(0.25)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Peru Incident Management Process")
    parser.add_argument("--env", dest="env_file", default=".env", help="path to .env file")
    parser.add_argument("--once", action="store_true", help="run one poll cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="log planned actions without changing ServiceNow")
    parser.add_argument("--probe-alerts", action="store_true", help="list matching alerts and exit")
    parser.add_argument("--probe-limit", type=int, default=10, help="number of alerts to list with --probe-alerts")
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.env_file:
        load_env_file(args.env_file, override=True)
    cfg = Config.from_env(args)
    configure_logging(cfg.log_level)
    LOGGER.info("using state file %s", cfg.state_file)
    emulator = ARMEmulator(cfg)
    if args.probe_alerts:
        return emulator.probe_alerts(limit=args.probe_limit)
    return emulator.run(once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
