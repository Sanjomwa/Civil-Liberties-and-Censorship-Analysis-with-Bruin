# ADR-0013: OONI agreement check — a standing guardrail against CLIO's own classification drift, using OONI's live data as a reference oracle

**Status:** Accepted; steps 1-4 of the Build sequencing below (harness, config, dispatch-only workflow, WIF/verdict-table/API-shape verification) implemented and live-verified 2026-08-20. Design finalized 2026-08-17 (Fable advisor pass, invoking this project's genuine-novelty carve-out). **Update, 2026-08-22: both named blockers are now resolved.** TD-101 is closed for all three affected test types (dnscheck fixed via a narrow bogon carve-out; telegram investigated and closed with zero code changes, no real FAILED-worthy population exists for Kenya, 47/47 matched-pair agreement; whatsapp resolved separately as TD-105, two independent bugs found and fixed, ANOMALOUS rate corrected from 9,350 to 835). TD-102 is closed (signal's two disagreement shapes — `test_version=0.2.3`, a one-time OONI data-quality patch, and `test_version=0.2.2`, a permanent dead-endpoint defect — both root-caused and fixed, each independently verified against OONI's live API as an exact match). Combined with psiphon (already fixed) and `dnscheck`'s own `DEGENERATE-EXEMPT` routing below, every OONI test type this project currently exposes on its dashboard now has a verified-correct classification — `tor` confirmed to carry zero rows corpus-wide and was never exposed. **Update, 2026-08-30: step 5 (panel/allowlist population) is done; step 6 (the weekly cron) is deliberately overridden, not merely delayed.** The panel was seeded — 144 entries, signal/whatsapp/telegram/psiphon, dnscheck correctly excluded per its `DEGENERATE-EXEMPT` status below. All four eligible types verified end to end via live `--mode monitor` runs. **The project owner explicitly decided not to enable the schedule at all**, overriding this ADR's own step 6 rather than just deferring it: CLIO is still pilot-stage on a historical backfill, not continuously ingesting live data, so a fixed weekly timer would mostly spend OONI's rate-limited API budget re-confirming an unchanging result. **The workflow stays `workflow_dispatch`-only indefinitely.** The operating procedure going forward: run the harness by hand, once per test type, right after either (a) CLIO begins ingesting live/new data on an ongoing basis, or (b) someone changes the classification/agreement SQL this harness checks — event-triggered, not calendar-triggered.

**Two real gaps in this design were found during panel-seeding and are now resolved; a third remains a permanent, structural limit, not a bug.** `check_panel()` was found to be tautological — a panel entry's "current" CLIO verdict was read straight from the panel file's own recorded value, so `PANEL_REGRESSION` could never fire for either of this ADR's own permanent regression locks (the canary-hostname measurement, TD-68 in this repo's own numbering — a separate planning workspace has called it "TD-71," a documented cross-environment numbering collision; and the whispersystems incident, TD-93). Root-caused, designed cheaply (a snapshot-and-refresh model — a new `--mode refresh-panel` CLI mode populates a `clio_verdict_now` snapshot only when manually triggered, gated by a free BigQuery metadata staleness check, rather than a live query on every check), built, and live-verified via deliberate fault injection against the real panel file (refresh cost confirmed ~$0.0005 for all 144 entries). **TD-93's own regression lock could not be seeded at all** — the TD-102 discard fix already removes those measurements from classification entirely, leaving no live CLIO verdict to pin; this ADR's two-permanent-lock design is therefore down to one working lock (TD-68) in practice, a structural fact worth carrying into any future revision of this design, not a to-do item. Separately, psiphon's systematic FAILED-vs-OONI-ANOMALOUS divergence (93 instances) was documented and allowlisted rather than investigated as a bug, by explicit project-owner decision. `dnscheck` remains marked `DEGENERATE-EXEMPT`, routed to its own internal validation path outside this guardrail, per the reasoning below — that routing is unaffected by this update.

## Context

This project has caught three real OONI-classification problems to date, all by accident:

- **TD-71** — a DNS bogon canary hostname wasn't excluded from `int.ooni_experiment_results.sql`'s classification logic, inflating the single most-cited flagship figure (the Finance Bill 2024 incident) from 0 to 177. Caught because OONI's own team flagged it in a Slack exchange, not by any check CLIO runs itself.
- **TD-93** — Signal's `ANOMALOUS` verdict was majority-driven (62.5% of its entire historical population) by a legacy-domain artifact: a deprecated hostname (`textsecure-service.whispersystems.org`) that OONI's own backend classifies `FAILED`, not `ANOMALOUS`. Caught by a one-off matched-pair audit that happened to run during an unrelated milestone review.
- **TD-97** (2026-08-17) — while spot-checking a newly-built dashboard chart, found that `dnscheck`'s two nominally-independent OONI signal series (`anomalous_rate`/`blocked_rate`) are evidentially coupled, both driven by the same underlying bootstrap-DNS event (the `lookups` extraction gap). Caught because Sam looked closely at one rendered chart and asked whether the numbers could be confirmed.

**Only the first two are actually within scope of the guardrail this ADR proposes.** TD-97 is a structural coupling between two derived series, invisible by construction to any check that compares CLIO's per-measurement verdict against OONI's per-measurement verdict. Stated here explicitly so this guardrail's coverage claim is never overstated in any external-facing description of this discipline.

**The validation method that caught the two real, in-scope incidents already exists and is proven**: sample CLIO measurements by `report_id`, fetch OONI's own real, published classification for the same measurements via their public, unauthenticated `/api/v1/measurements` API, and compare CLIO's `ooni_verdict`/`result_state` against OONI's `anomaly`/`confirmed`/`failure` booleans. This has never once been run except by accident, a milestone review, or a direct human question — there is no standing process that runs it.

**A measured constraint on any design**: OONI's public API is rate-limited to roughly 1 request/second in practice.

**A known-unaudited surface**: three test types whose verdict layer had never been externally matched-pair validated at all — `whatsapp`, `telegram`, `dnscheck` — unlike `signal` and `psiphon` (both validated, both found and fixed real bugs).

## Decision

**Build a standing, two-part external-agreement check comparing CLIO's own warehouse verdicts against OONI's live, published classification — never auto-remediating, never gated on a percentage threshold, and sequenced so its own one-time baseline audits (not its recurring schedule) deliver most of the value.**

### Why not a percentage threshold

Verdict agreement between CLIO and OONI is deterministic per row, not a stochastic quantity. A percentage-drop-from-baseline design requires an arbitrary calibration nobody can defend, and it structurally hides small, real, category-shaped failures. Every real incident in this project's record so far is category-shaped — a specific hostname, a specific hostname/failure-mode pair — not a diffuse drift. The alarm mechanism is categorical, not proportional:

1. **A fixed known-answer panel** (roughly 30-50 `report_id`s per test type), seeded from baseline audits, deliberately including TD-71's canary-hostname measurements and TD-93's whispersystems measurements as permanent regression locks. Alarm condition: any change at all, on either side.
2. **A rotating, category-stratified sample**, compared row by row and bucketed by disagreement signature (`test_name`, CLIO verdict, OONI's `anomaly`/`confirmed`/`failure`, failure mode, and hostname/target where applicable). Alarm condition: any disagreement signature not already present in a TD-referenced allowlist file.

Agreement percentage is still recorded per run for trend visibility — it never gates.

### Why "reference oracle," not "ground truth"

OONI's own published classification shares CLIO's vantage point. A disagreement is therefore a prompt for investigation with three possible resolutions — CLIO bug, OONI-side artifact, or a legitimate, already-documented divergence — not an automatic verdict that CLIO is wrong.

**Confirmed as a real, concrete failure mode — 2026-08-20.** A wide 1,166-row live baseline against `dnscheck` found OONI's own `failure`/`anomaly`/`confirmed` fields show zero variance for that test type specifically, confirmed globally (200 measurements, 15 countries). Root cause: `dnscheck` has no entry in OONI's own fastpath scorer's dispatch chain at all — it falls through to an "unsupported test" branch that hardcodes `accuracy=0.0`. **This is a fourth possible resolution — not "CLIO bug, OONI-side artifact, or legitimate divergence," but "the oracle doesn't cover this test type at all, and abstains rather than answers."** Practical consequence, adopted project-wide: an oracle-status taxonomy per test type — VALIDATED / DEGENERATE-EXEMPT / UNCHECKED. `dnscheck` is `DEGENERATE-EXEMPT`. `signal`/`psiphon` are `VALIDATED`. `whatsapp`/`telegram`/`tor` were `UNCHECKED` at design time (since resolved — see TD-105/TD-101 below).

### Cadence: weekly cron + `workflow_dispatch`, not daily — since amended, see Status above

CLIO's warehouse is not continuously updated even though OONI's own probes measure continuously. The only things that can change CLIO's verdicts are a classification-SQL change plus rematerialization, or a manual OONI ingest — both discrete, human-initiated events. The trigger was designed as a weekly scheduled run plus `workflow_dispatch`, plus a social trigger (a printed reminder in the materialization wrapper's output, and a `CLAUDE.md` line). **As of 2026-08-30, the scheduled half is permanently disabled — see Status above; `workflow_dispatch` plus the social trigger are the only mechanisms actually in effect.**

### Sampling strategy

- **Baseline audits:** full population where feasible. For `dnscheck`, full population is arithmetically infeasible (~13 days at the API's rate limit), so signature-cell stratification is used instead: enumerate distinct `(verdict, failure_type, hostname/target)` cells, sample up to ~25 rows per cell plus ~1,000 uniform-random as a catch-all.
- **Weekly/on-demand monitor:** the fixed panel (~150-250 requests total across test types) plus ~100 rotating requests per live test type.
- Sampling is by `report_id`, and a single API request can return multiple measurements per report — budget the rate limit by request, not by row.

### Where results land

- **`audit.ooni_agreement_runs`**, a BigQuery table — one row per `(run timestamp, test_type)`, with sample size, agreement count, disagreement signatures as a JSON column, and panel status. Lives in a dedicated `audit` dataset outside Bruin's DAG entirely.
- **A GitHub Actions pass/fail**, `.github/workflows/ooni-agreement-check.yml` — the panel is a versioned JSON fixture in the repo (reviewable, diffable), the allowlist is a file next to the script.

### Panel transparency and adversarial-disclosure tradeoff (added 2026-08-31)

`Bruin/scripts/agreement_check/config/panel.json` — the guardrail's fixed 144-entry known-answer panel — is committed to this public repository, matching the transparency convention this project already uses for `tests/fixtures/` and other golden-fixture data. This was a deliberate choice, not an oversight, and the tradeoff is worth stating explicitly for anyone auditing this guardrail's design.

The panel's whole purpose is to re-check a small set of measurements whose correct classification is already known, so that a change in this project's output on any of them is treated as a signal worth investigating. Publishing the exact membership of that set carries one real, narrow risk: anyone who reads it learns precisely which 144 measurements are being watched, and could in principle ensure those specific cases stay clean while evading detection elsewhere — the panel's diagnostic value depends in part on an adversary not knowing which cases it covers.

Three considerations support keeping it public anyway. First, consistency: every other golden-fixture and regression-test asset in this repository is public and diffable by design (see `docs/03-development/testing-strategy.md`); a hidden panel would be an unexplained exception, weakening the auditability this project has otherwise leaned on. Second, scale: this project is not currently a live, continuously-operating detector that a motivated adversary has reason to probe — it is a historical-analytics pilot over a fixed Kenya window, and the guardrail itself runs on a `workflow_dispatch`-only cadence, not continuously. The realistic value to an adversary of gaming 144 specific historical measurements is low today. Third, the guardrail does not rely on the panel alone — a rotating, category-stratified sample checks measurements outside the fixed panel on every run (see the section above), so an adversary who successfully games the known 144 still faces an unpredictable, unpublished second check.

This is a considered position, not a permanent one. If this project's guardrail becomes a real-time operational tool, or the panel's membership starts changing evasion behavior detectably, that would be sufficient reason to revisit this decision and move the panel to non-public configuration.

### No auto-remediation

No mechanical fix exists even in principle for a CLIO/OONI disagreement — distinguishing CLIO bug from OONI artifact from legitimate divergence has required real investigation every time this project has hit one. The only sanctioned write this check ever performs is appending to its own audit table; the allowlist is the only sanctioned "resolution" a session may write, and only with a TD reference attached.

### Resilience against OONI-side instability

Three-way exit semantics: pass / real disagreement / inconclusive. A fetch error is never treated as a disagreement. If the fetch-error rate in a run exceeds ~20%, the run exits "inconclusive"; only after ~3 consecutive inconclusive runs does the workflow actually fail. The fixed panel doubles as an external-drift canary: if a large fraction of the panel's OONI-side answers change in one run, that's flagged as external change to investigate, categorically separate from a CLIO-side regression.

## Consequences

- Most of this guardrail's value is delivered by the one-time baseline audits, not the recurring monitor — both catchable incidents (TD-71, TD-93) were latent from the day the classifier shipped, not regressions over time.
- The credential gap (CI having no GCP credentials for live checks) was not a hard sequential blocker — a copyable WIF auth pattern already existed in this repo's own staleness-check workflow and was reused directly.
- This guardrail and any golden-file test suite are complementary, not redundant — different oracle (external vs. internal), different failure semantics.
- The guardrail structurally cannot see TD-97-class problems (evidential coupling between two derived series that individually agree with OONI perfectly). Any description of this discipline's coverage must state that limitation rather than imply it catches "OONI-classification problems" generally.
- Cost is negligible: the check reads verdict-level derived tables only, never the expensive raw column. At worst, ~2 GiB scanned per run, roughly $0.05/month at full cadence — realistically cents per year given the schedule is now event-triggered rather than weekly. The binding constraint on this design is OONI's API rate limit and this project's relationship with OONI's own team, not GCP spend.
- API etiquette is a real design constraint: OONI's own team caught the first real incident this guardrail is built to prevent a repeat of, directly, in conversation — a design that hammers their public API for no detection-latency benefit at this project's current scale would be a poor use of that relationship.

## Alternatives considered

- **Percentage-drop-from-historical-baseline thresholds** (rejected): not defensible to calibrate, structurally blind to small, real, category-shaped failures.
- **Daily cadence** (rejected): CLIO's warehouse doesn't change daily; a daily run on a static corpus costs real API etiquette for no benefit.
- **Push-triggered CI on classification-SQL changes** (rejected): validates warehouse state, not commit state — would pass the exact commit introducing a bug, since the warehouse hasn't been rematerialized yet at push time.
- **Auto-remediation** (rejected): no mechanical fix exists even in principle.
- **OONI's aggregate-endpoint as a cheap pre-screening tier** (rejected): would conflate corpus-membership differences with real classification disagreement.

## Follow-ups, deliberately deferred, not forgotten

1. A dashboard view over the audit table's historical trend — useful eventually, not needed for v1.
2. Embedding a lightweight version of this check inside the materialization wrapper's own execution — deliberately not done, wrong shape for that script.
3. Multi-country parameterization — out of scope until a second country's ingestion is actually underway.
4. An allowlist-entry stub auto-generated by the harness for each new disagreement signature.
5. Unmatched-row rate as its own first-class, separately-reported alarm.
6. Mechanically enforce the cron precondition at the harness level.
7. Bound and checkpoint the monitor run (hard row cap, wall-clock budget, resumability).
8. Investigate whether OONI publishes bulk measurement data as an alternative to the per-measurement API.
9. Persist OONI's own per-measurement `scores.accuracy` in the audit layer generally, as an allowlist-entry justification field — audit-layer-only, never a classification-path input.

Explicitly out of scope for this ADR: the evidential-coupling problem between derived series (different failure category, different fix); any auto-remediation of any kind; a daily schedule.

## Build sequencing

1. Step 0: confirm WIF auth is copyable; confirm the live verdict table's exact name/columns; confirm the OONI API response shape. **Done, 2026-08-20** — one correction found: OONI's API returns one result per `(report_id, input)` pair, not per `report_id` alone.
2. Build the parameterized sample/fetch/compare/allowlist-check script. **Done, 2026-08-20.**
3. Add the `workflow_dispatch`-only GitHub Actions workflow, cron left disabled. **Done, 2026-08-20.**
4. Add the materialization-wrapper reminder and the `CLAUDE.md` prompting-standard line. **Done, 2026-08-20.**
5. Run baseline audits for whatsapp, telegram, dnscheck. **Done — dnscheck marked `DEGENERATE-EXEMPT`; whatsapp and telegram both validated and closed (see TD-101/TD-105 below).**
6. Enable the weekly cron. **Permanently overridden, not done — see Status above.**
