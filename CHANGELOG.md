# Changelog

All notable project updates should be recorded here when work is completed and pushed.

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
