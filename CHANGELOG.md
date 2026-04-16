# Changelog

All notable project updates should be recorded here when work is completed and pushed.

## 2026.04.16.8 - 2026-04-16

- added a shared `secret_codec.py` helper and support for `OBFMD5:...` env values so sensitive credentials can be stored obfuscated instead of plain text
- updated the runtime to decode obfuscated env credentials before authenticating to ServiceNow
- updated the installer to preserve prompt defaults while writing known secret keys back in obfuscated form, even on non-interactive installs
- updated `run_local.sh` and `send_sample_payload.sh` so local helper flows still work with obfuscated credentials in the root `.env`

## 2026.04.16.7 - 2026-04-16

- changed direct incident creation to try the minimal payload first again, then standard and rich fallbacks, so insert ACL quirks are less likely to block incident creation
- made `INCIDENT_EXTERNAL_CASE_FIELD` truly disable-able with values like `false`, and skip external-case searches when that field is disabled
- stopped treating the external Salesforce case field as a mandatory post-create incident patch field; if that field is rejected, it is disabled for the rest of the run and the rest of the incident enrichment continues
- documented the external-case-field disable switch in `.env.example` and refreshed the README version and behavior notes

## 2026.04.16.6 - 2026-04-16

- made direct incident creation more robust by trying rich, standard, and minimal create payloads before giving up
- fixed the direct-create call path so the richer payload builder receives the alert, service offering, and Salesforce context it needs
- added INFO-level logging for unchanged alerts so silent fingerprint skips are visible in normal logs
- retry unchanged open alerts after a prior error instead of skipping them forever when the fingerprint has not changed

## 2026.04.16.5 - 2026-04-16

- added the source alert short description and alert description to the rendered incident description so the original alert text is preserved in the incident record

## 2026.04.16.4 - 2026-04-16

- disabled PINC tagging by default and stop retrying tag API calls after the first failure in a run
- disabled DTI fallback by default so the process will not create a second DTI incident when direct incident creation already works
- made `PUSH_CONNECTOR_URL` optional unless DTI fallback is explicitly enabled
- updated the installer and local runner defaults so generated configs now default to `ENABLE_TAGGING=false` and `ENABLE_DTI_FALLBACK=false`

## 2026.04.16.3 - 2026-04-16

- fixed installer reruns so `SN_INSTANCE_URL`, `SN_USERNAME`, `SN_PASSWORD`, and other prompted values default from the deployed config instead of the temporary `.env.example` staging placeholders
- kept persistent source env files working by preferring any non-placeholder values already present there before falling back to `/etc/<service>/.env`

## 2026.04.16.2 - 2026-04-16

- switched incident creation to prefer direct `incident` table API inserts and only fall back to DTI when direct create fails
- stopped preparing or triggering the DTI helper flow unless the direct create path actually fails
- kept the existing post-create enrichment path so DTI fallback incidents are still patched as completely as possible afterward
- added creation-path tracking in state so runs show whether an incident came from `table_api` or `dti_fallback`

## 2026.04.16.1 - 2026-04-16

- made incident enrichment best-effort across environments so a rejected field does not block the rest of the incident update
- made alert linking happen independently of PINC tagging so `label_entry` failures do not leave orphan incidents
- changed helper event message keys from `PIMP-DTI-*` to `INCHELPER-DTI-*`
- expanded alert-type matching to accept either `Amazon` or `peru` case-insensitively
- documented repo-level release tracking with `VERSION` and `CHANGELOG.md`

## 2026.04.15.1 - 2026-04-15

- initialized the GitHub project and imported the Peru Incident Management Process source
- added interactive legacy cleanup to the installer so old config, state, and legacy service files can be reviewed before deletion
- switched the default alert type trigger from only `peru` to `Amazon`
- changed the DTI helper flow to create a fresh helper alert with a unique message key before incident creation

## 2026.04.13.1 - 2026-04-13

- established the off-platform workaround flow for polling existing alerts, creating incidents through DTI, and patching incidents afterward
- added service offering, CI, assignment group, and generating-alert linking logic
- added Linux installer, service unit, proxy support, and local helper scripts
