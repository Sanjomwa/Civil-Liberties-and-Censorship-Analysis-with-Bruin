# 02-architecture

**Why this folder exists:** the canonical technical description of CLIO and
the durable record of why structural decisions were made.

**Who reads it:** developers and technical partners.

**When it's used:** onboarding, and any structural or architectural change.

## Files

- `architecture-assessment.md` — Phase 0 read-only system architecture
  overview: the layered ELT pipeline shape (ingest → load → staging →
  intermediate → features → intelligence → marts → reporting), grounded in a
  direct read of the full repository (all Streamlit pages, services,
  scripts, infra modules) rather than restated from the prior Project Zero
  Review.
- `data-modelling.md` — the marts/dims/facts layer specifically: entity and
  schema documentation grounded directly in the live BigQuery
  `INFORMATION_SCHEMA.COLUMNS` and a clean `bruin validate` run, re-verified
  periodically rather than hand-edited from memory of what changed.
- `data_sources.md` — catalog of data sources: which are actually live and
  ingested today (OONI, ACLED, Google Transparency Report, Lumen) versus
  which are recommended by ADR-0003 but not yet ingested, grounded in the
  live Bruin DAG's `depends:` fields.
- `decision-log.md` — the project's single authoritative, continuously
  updated record: an ADR index table plus a "Narrative decision log" section
  preserving the reasoning behind decisions and research conclusions, some
  of which haven't yet become implementations. Read this, not just this
  README's highlight reel, for the current state of any specific decision.
- `erd-lineage.md` — the pipeline dependency graph: per-source lineage
  diagrams (raw → load → staging → intermediate → features → intelligence →
  marts → reporting), extracted directly from each asset's `name:` and
  `depends:` fields, not hand-drawn.
- `implementation-roadmap.md` — the sequenced engineering roadmap; every
  item references a specific finding in `architecture-assessment.md`,
  `technical-debt-inventory.md`, or `methodology-consistency-review.md`.
- `methodology-consistency-review.md` — an audit of how the codebase actually
  implements evidence, confidence, provenance, and interpretation across the
  OONI and ACLED integration paths, naming what's conceptually consistent
  and what isn't, ranked by severity.
- `methodology.md` — CLIO's general evidential method (admissibility,
  confidence, provenance) stated independent of any data source, followed by
  a worked example using the real OONI and ACLED pipelines.
- `streamlit-ux-redesign-audit-2026-08-17.md` — a whole-layer Streamlit
  UX/IA audit (all 9 pages, `core/`, `components/`, `services/marts.py`),
  checked directly against the live repository and live BigQuery warehouse
  rather than inferred secondhand.
- `technical-debt-inventory.md` — itemized technical debt with location,
  severity, and recommended action; kept current in the same commit as any
  fix that resolves an item (see `CLAUDE.md`'s "Engineering principles").
- `adr/` — Architecture Decision Records, numbered sequentially (`0001`
  through `0013` as of this writing). One file per decision: title, status,
  context, decision, consequences. A decision is superseded by a later ADR,
  never edited in place. See `docs/03-development/documentation-standards.md`
  for the template.

## Status

Populated. The list above reflects this folder's actual contents as of
2026-09-03 (confirmed via direct directory listing); update it directly when
a file is added, renamed, or removed rather than letting it drift back into
a stale "planned" state.
