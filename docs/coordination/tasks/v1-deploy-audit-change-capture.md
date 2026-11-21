# Task Claim: v1 Deploy Audit Change Capture

- **Task ID:** `v1-deploy-audit-change-capture`
- **Model:** Antigravity
- **Branch:** `antigravity/v1-deploy-audit-change-capture`
- **Reviewer:** Muse (exact-head review)
- **Coordinator / Merger:** GPT
- **Reference:** `docs/v1/ANTIGRAVITY_REPAIR_HANDOFF_2026-11-21.md` (First repairs #2, part 2 — change capture)

## Scope

1. In `v1/services/deploy/service.mjs`, record a bounded operation summary and relevant `before`/`after` changed fields atomically inside `product_audit.details_json` when ordinary Deploy saves commit via `save()` -> `finish()` -> `commitOperation()`.
2. Include readable `actorLabel` (from verified principal email/name when available) and `operationLabel` (derived from the committed edit operation(s)), while preserving `event_id`, `operationId`, `originalActor` (`actor_id`), `capabilityAction`, `versionId`, `scope`, `changedRows`, and `previewId`.
3. Bound captured `before`/`after` payload size (row count and byte cap with explicit `truncated`/`omittedChanges` indicators) so large saves or workbook imports stay safe.
4. Preserve historical audit rows unchanged, emit exactly one audit event on idempotent retry with the same `operationId`, and emit zero audit events on rejected writes.
5. Add regression test `tests/v1-product-deploy-audit-change-capture.mjs` proving single Subject Load change, multi-edit save, one event on retry, zero events on rejected write, readable actor/operation labels with original actor/eventId/operationId preserved, and historical rows preserved.

## Files Planned

- `docs/coordination/tasks/v1-deploy-audit-change-capture.md`
- `v1/services/deploy/service.mjs`
- `tests/v1-product-deploy-audit-change-capture.mjs`

## Out of Scope

- v0.9 source and generated files (`roster-update/`, `public/`, `shared/`) — zero drift required.
- `/auditPage` list/detail route contract (owned by PR #264 `antigravity/v1-deploy-audit-list-detail`).
- Deployment, live school data, or enabling live switches.
