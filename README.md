# Peru Incident Management Process

This is the freeze-safe path for the Salesforce PERU workaround.

Repo version: `2026.04.16.4`
Release history: [CHANGELOG.md](CHANGELOG.md)

It does not require changing the existing `genericJsonV2` DTI mechanism on the
ServiceNow side. Salesforce keeps posting directly to the existing
`genericJsonV2` endpoint, and the workaround operates only on alerts that
already exist on the platform.

```text
Salesforce raw payload -> genericJsonV2 endpoint -> em_event / em_alert in
ServiceNow -> off-platform process -> existing DTI
```

## Components

The main Python runtime watches existing alerts and identifies Salesforce PERU
cases from the payload that is already stored on the alert and related event
records, then uses the existing DTI path to create or relink incidents.
By default it will accept alert type matches for either `Amazon` or `peru`
case-insensitively.

## What the Process Does

- matches PERU alerts client-side from the Salesforce payload shape already on
  the alert, so it does not depend on custom event routing or a modified source
  / type mapping
- prefers direct incident creation through the `incident` table API when the
  current environment allows it
- falls back to the existing DTI connector flow only when direct incident
  creation fails, then patches the resulting incident afterward
- ships with DTI fallback disabled by default to avoid duplicate incidents when
  direct incident creation is already working
- handles reopen scenarios by clearing stale alert links before DTI
- patches the created incident with clean Salesforce case details
- prefers matching `environment` values on duplicate offerings and CIs when
  Salesforce sends one, then falls back to the best available name match
- links `primary_offering` to `service_offering` and `business_service`
- feeds `assignment_group` into DTI from the service offering support group,
  then the offering L2/L3 custom group fields, then an optional configured
  dummy fallback
- sets the external Salesforce case field, or falls back to `correlation_id`
  when that custom field does not exist
- stamps the linked alert into `u_generating_alert` when that incident field exists
- leaves PINC tagging disabled by default; if tagging is enabled and the API
  rejects it, tagging is turned off for the rest of that run
- enforces a runtime write policy: create only `em_event`, `incident`, and
  `label_entry`, and update only `em_alert` and `incident`

## Local PDI Quick Start

These helpers read the root `.env` in this workspace and map it to the local
runtime:

- `./run_local.sh`
- `./send_sample_payload.sh`

Typical local workflow:

```bash
cd /path/to/project
./run_local.sh --continuous
```

To test with the sample payload:

```bash
cd /path/to/project
./send_sample_payload.sh
./run_local.sh --once
```

Helpful local commands:

```bash
./run_local.sh --probe-alerts --probe-limit 10
./run_local.sh --once
./run_local.sh --continuous
```

Current local defaults are tuned for the connected PDI:

- `genericJsonV2` endpoint
- header `user-agent: genericendpoint`
- repo-local `state.local.json`
- auto-resolution of the external case field on `incident`
- auto-resolution of the generating alert reference field on `incident`
- CMDB reads only by default; missing offerings are not auto-created
- auto-resolve assignment group from the chosen service offering before DTI
- caller / assignee patching disabled unless `SALESFORCE_USER_SYS_ID` is set
- PINC tagging disabled unless `PINC_TAG_SYS_ID` is set
- a safer default `PUSH_CONNECTOR_TIMEOUT_SECONDS=90` for slower instances

## Linux Deployment

The final workaround only needs the Peru Incident Management Process service:

```bash
cd /path/to/project
./install.sh
```

The installer now asks for an instance label first, such as `dev`, `it`,
`uat`, or `prod`. It uses that label to derive:

- service: `sf-peru-incident-management-process-dev`, `sf-peru-incident-management-process-it`, etc.
- deploy paths under `/opt`, `/etc`, and `/var/lib`
- state file: `/var/lib/<service>/state.json`

It then prompts for the important values and writes them into a temporary
staging env file, copies the final config to `/etc/<service>/.env`, and removes
the staging file on exit. Press Enter to keep the current value when rerunning
it. Before the prompts, the installer can also walk you through an optional
interactive legacy cleanup so you can approve deletion of old config, state,
and old-prefix service files one item at a time. After install, it runs the
alert probe and leaves the service disabled until you explicitly approve
starting it. On reruns, prompted values now default from the deployed
`/etc/<service>/.env` when that file already has real values. For automation, use
`INSTANCE_PROFILE=uat ./install.sh --non-interactive`.

If you already know you want it enabled immediately, use:

```bash
./install.sh --enable-now
```

`apply_usb_core_patch.sh` is now explicitly guarded and will refuse to patch
platform records unless you opt in with
`ALLOW_PLATFORM_PATCHES=PDI_ONLY_I_UNDERSTAND`.

Stable runtime paths:

- app: `/opt/sf-peru-incident-management-process-uat`
- config: `/etc/sf-peru-incident-management-process-uat/.env`
- state: `/var/lib/sf-peru-incident-management-process-uat/state.json`

## Proxy Support

If the Linux server must go through a proxy to reach ServiceNow, add these to
the env file you deploy:

```bash
HTTPS_PROXY=http://proxy.example.com:8080
HTTP_PROXY=http://proxy.example.com:8080
NO_PROXY=localhost,127.0.0.1
```

The bundle now uses those values in all the important places:

- `install.sh` exports them before `pip install`, probe runs, and service setup
- the process loads them from the deployed `.env` before making `requests` calls
- `send_sample_payload.sh` passes them through to `curl`

## UAT And PROD

Best practice is one Peru Incident Management Process service instance per ServiceNow environment.

- do not point one daemon at both instances
- keep separate service names, config files, and state directories
- use the same code version in UAT and PROD, but different credentials and URLs

If you can, run them on separate hosts. If you want multiple instances on one
Linux host, just run the installer once per label:

```bash
cd /path/to/project

./install.sh
./install.sh
```

On the first run, answer `dev`. On the next runs, answer `it`, `uat`, and
`prod`. Each run creates its own systemd service and keeps its config and state
separate under `/etc/<service>` and `/var/lib/<service>`.

For fully non-interactive installs, you can still pin the label yourself:

```bash
INSTANCE_PROFILE=uat ./install.sh --non-interactive
INSTANCE_PROFILE=prod ./install.sh --non-interactive
```

Useful commands after that:

```bash
journalctl -u sf-peru-incident-management-process-uat.service -f
journalctl -u sf-peru-incident-management-process-prod.service -f
sudo systemctl enable --now sf-peru-incident-management-process-uat.service
sudo systemctl enable --now sf-peru-incident-management-process-prod.service
sudo systemctl restart sf-peru-incident-management-process-uat.service
sudo systemctl restart sf-peru-incident-management-process-prod.service
```

Useful service commands:

```bash
journalctl -u sf-peru-incident-management-process.service -f
sudo systemctl enable --now sf-peru-incident-management-process.service
sudo systemctl restart sf-peru-incident-management-process.service
sudo systemctl stop sf-peru-incident-management-process.service
```

## Connector Notes

Keep using the exact endpoint and header values already configured on the
target instance.

Your current example uses:

- endpoint source parameter: `genericJsonV2`
- required header: `user-agent: genericendpoint`

Those stay configurable in `.env`.

If `genericJsonV2` responds with `dti_incident_status=create_failed`, the event
was accepted but the target instance could not create the incident. The most
common causes are incident insert ACL or role restrictions, mandatory incident
fields, or instance-specific business rules.

If the target instance expects an assignment group at incident create time, the
process now tries the chosen service offering fields in this order:
`support_group`, `u_level_2_support_assignee_group`,
`u_level_3_support_assignee_group`. If those are empty, it falls back to
`DEFAULT_ASSIGNMENT_GROUP_SYS_ID` when set, then to the dummy/default CI group
fields using the same order.

## Payload Shape

Salesforce posts the raw payload shape directly to `genericJsonV2`, for example:

```json
{
  "case created_date": "2026-03-25 09:13:01",
  "case_number": "00367546",
  "case_owner": "SF User",
  "case_status": "New",
  "case_subject": "The System Is Down",
  "expected result": "Fix The Problem",
  "number_of_customers_impacted": "5",
  "primary_offering": "Myoffering1",
  "secondary_offering": "Myoffering2",
  "environment": "UAT"
}
```

The process does not modify that inbound event path. It reads the resulting
alert and related event content and treats it as a Salesforce PERU alert when
the stored payload looks like this schema.

## Reference Patch

`apply_usb_core_patch.sh` and
`../servicenow_generic_mapped_json_bundle_v8/src/USBEM_Core.genericJsonV2.salesforce_peru.js`
are kept only as historical reference from earlier PDI testing. They are not
part of the final freeze-safe solution.

## Bridge Note

`salesforce_peru_bridge.py`, `run_bridge_local.sh`, and `install_bridge.sh`
are leftover lab tooling from an earlier experiment. They are not part of the
final production workaround.
