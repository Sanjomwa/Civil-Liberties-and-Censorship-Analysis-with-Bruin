"""
Regression lock for TD-126's fix: whatsapp's test_anomaly_flag arm now
recomputes registration-server accessibility from raw $.requests[]
instead of trusting the probe-submitted registration_server_status field
-- the same pattern TD-105 bug (a) already shipped for the web check
(whatsapp_web_accessible), applied here to the one OR-chain condition
that fix flagged (2026-08-21, "bug (c)") but never gave the same
treatment.

A 2026-08-30 characterization session (triggered by the TD-100/ADR-0013
agreement-check harness's rotating sample surfacing a new whatsapp
disagreement) found registration_server_status shares bug (a)/(b)'s
exact shape: OONI's own backend fastpath scorer (ooni/pipeline
af/fastpath/fastpath/core.py, score_measurement_whatsapp()) explicitly
disables reading this field, citing the same probe bug already cited for
bug (b) (ooni/probe-engine#341), and instead recomputes accessibility
from the matching $.requests[] entry's own `failure` field -- the actual
HTTP status code returned is irrelevant to OONI's own scoring. Confirmed
by hand-applying OONI's own published scorer logic to the two real
disagreeing measurements and exactly reproducing OONI's live OK answer,
not by inference.

Population is small and fully characterized, not estimated: parsing raw
$.requests[] across all 278 registration_server_status='blocked'
sole-driver rows (not a sample) found exactly 2 with a genuinely
non-null v2/register request failure -- both a real HTTP 503 response
(transport succeeded, only the probe's own higher-level status field
disagreed). The other 276 have a real, matching transport failure and
are unaffected, consistent with TD-105's own "89% genuinely correct"
figure for this field measured on 2026-08-21.

THIS TEST EXISTS SPECIFICALLY TO STOP A FUTURE REFACTOR FROM SILENTLY
WIDENING THIS FIX INTO THE OTHER TWO OR-CHAIN CONDITIONS' TERRITORY, OR
FROM SILENTLY REVERTING IT BACK TO THE RAW registration_server_status
FIELD. If a future edit changes whatsapp_endpoints_status or
whatsapp_web_accessible, or the NULL-check branch, this file's static
assertions must fail.

NOTE: this fix supersedes the "bug (c), explicitly untouched" framing in
tests/test_ooni_whatsapp_web_accessible_classification.py's and
tests/test_ooni_whatsapp_endpoints_blocked_count_removed.py's own module
docstrings and fixtures -- both files' "surviving conditions unchanged"
assertions and additive-proof live queries were updated in the same
TD-126 build session to reflect the new OR-chain shape. Neither of those
files' own fixtures needed new entries: none of their pinned
measurement_ids happen to be one of the 2 rows this fix reclassifies.

Two layers of protection, same pattern as
tests/test_ooni_whatsapp_web_accessible_classification.py:

1. Static SQL-text assertions (always run, zero credentials). Checks
   that the whatsapp OR-chain's registration-check condition now reads
   whatsapp_registration_accessible IS FALSE (not
   registration_server_status = 'blocked'), that the other two
   conditions and the NULL-check branch are byte-for-byte unchanged, and
   that whatsapp_registration_accessible's extraction exists in staging.

2. Live-BigQuery behavioral assertions, gated behind RUN_BIGQUERY_TESTS=1.
   whatsapp has a valid, non-degenerate oracle (OONI's own
   /api/v1/measurements) -- used as ground truth throughout, same basis
   as TD-105's own whatsapp fixtures.

PINNED FIXTURE (tests/fixtures/ooni_whatsapp_registration_accessible_classification/
pinned_measurements.json): 5 real measurement_ids -- 2 reclassified
(the entire live population of this shape, ANOMALOUS->OK), 1 regression
guard proving a genuine transport failure keeps a row ANOMALOUS despite
the fix, 1 regression guard proving a different, untouched OR-chain
condition (whatsapp_web_accessible) keeps a row ANOMALOUS regardless of
this fix, and 1 guard for an already-OK row never touched by
registration_server_status at all.
"""
import json
import os
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
VERDICTS_CANDIDATE_SQL = (
    REPO_ROOT / "Bruin" / "assets" / "intermediate" / "int.ooni_measurement_verdicts_candidate.sql"
)
STG_SUMMARY_SQL = (
    REPO_ROOT / "Bruin" / "assets" / "staging" / "stg.ooni_measurement_summary.sql"
)
FIXTURE_PATH = (
    REPO_ROOT / "tests" / "fixtures" / "ooni_whatsapp_registration_accessible_classification"
    / "pinned_measurements.json"
)

requires_bigquery = pytest.mark.skipif(
    os.environ.get("RUN_BIGQUERY_TESTS") != "1",
    reason="Set RUN_BIGQUERY_TESTS=1 to run tests against live BigQuery data",
)

PROJECT_ID = "encoded-joy-485413-k5"


def _normalized(path):
    return re.sub(r"\s+", " ", path.read_text())


def _load_fixture():
    with open(FIXTURE_PATH) as f:
        return json.load(f)["entries"]


# ---------------------------------------------------------------------
# Static layer
# ---------------------------------------------------------------------

def test_whatsapp_registration_check_uses_accessible_not_status():
    sql = _normalized(VERDICTS_CANDIDATE_SQL)
    assert "OR s.whatsapp_registration_accessible IS FALSE" in sql, (
        "TD-126 regression: whatsapp's test_anomaly_flag arm no longer routes "
        "the registration-check condition through "
        "whatsapp_registration_accessible -- it may have reverted to the "
        "raw registration_server_status = 'blocked' field, which is "
        "confirmed wrong for the http_request_failed shape (a real "
        "transport success the probe's own status field still marks "
        "blocked)."
    )
    assert "OR s.registration_server_status = 'blocked'" not in sql, (
        "TD-126 regression: the old, raw registration_server_status = "
        "'blocked' condition is back in the OR-chain."
    )


def test_whatsapp_other_orchain_conditions_unchanged():
    """The other two conditions (whatsapp_endpoints_status,
    whatsapp_web_accessible) must stay byte-for-byte unchanged -- this fix
    touches ONLY the registration-check condition."""
    sql = _normalized(VERDICTS_CANDIDATE_SQL)
    assert (
        "WHEN s.whatsapp_endpoints_status = 'blocked' "
        "OR s.whatsapp_web_accessible IS FALSE "
        "OR s.whatsapp_registration_accessible IS FALSE THEN TRUE"
        in sql
    ), (
        "TD-126 regression: the whatsapp OR-chain's full text has drifted "
        "from the exact shape this fix shipped -- whatsapp_endpoints_status "
        "and whatsapp_web_accessible must stay byte-for-byte unchanged. If "
        "this assertion fails, check whether an unrelated edit reordered "
        "or changed one of the other two conditions."
    )


def test_whatsapp_null_check_branch_still_untouched():
    """TD-126 deliberately does NOT touch the NULL-check branch, mirroring
    TD-105 bug (a)'s own precedent: it still checks the raw
    registration_server_status IS NULL, not whatsapp_registration_
    accessible IS NULL."""
    sql = _normalized(VERDICTS_CANDIDATE_SQL)
    assert (
        "WHEN s.whatsapp_endpoints_status IS NULL "
        "AND s.whatsapp_web_status IS NULL "
        "AND s.registration_server_status IS NULL THEN NULL"
        in sql
    ), (
        "TD-126 regression: the whatsapp NULL-check branch (deciding when "
        "the whole row is unscored) has changed -- this fix deliberately "
        "left it on the raw fields, matching TD-105 bug (a)'s own "
        "precedent, since this branch was explicitly out of scope for "
        "both fixes."
    )


def test_whatsapp_registration_accessible_extraction_exists_in_staging():
    sql = _normalized(STG_SUMMARY_SQL)
    assert "AS whatsapp_registration_accessible" in sql, (
        "TD-126 regression: whatsapp_registration_accessible is no longer "
        "extracted in stg.ooni_measurement_summary.sql -- the verdicts "
        "candidate asset's whatsapp arm has nothing to read."
    )
    assert "https://v.whatsapp.net/v2/register" in sql, (
        "TD-126 regression: the whatsapp_registration_accessible "
        "extraction no longer matches on the expected fixed URL -- "
        "confirm it wasn't changed to a pattern match or a different "
        "target."
    )


# ---------------------------------------------------------------------
# Live layer
# ---------------------------------------------------------------------

@requires_bigquery
def test_live_reclassified_population_is_exactly_two():
    """The load-bearing population claim this fix rests on: parsing raw
    $.requests[] across every registration_server_status='blocked'
    sole-driver row (not a sample) found exactly 2 with a genuinely
    non-null v2/register request failure, as of the 2026-08-30
    characterization and build sessions. This is a real population, not
    a fixed constant -- allow drift as new data lands, but a collapse to
    0 or an unexplained jump would mean either the mechanism changed or
    this test's own query has drifted from the characterization's."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        WITH sole_driver AS (
          SELECT s.measurement_id, m.raw_test_keys
          FROM `{PROJECT_ID}.stg.ooni_measurement_summary` s
          JOIN `{PROJECT_ID}.int.ooni_measurement_verdicts` v USING (measurement_id)
          JOIN `{PROJECT_ID}.stg.ooni_measurements` m USING (measurement_id)
          WHERE s.test_name = 'whatsapp'
            AND s.registration_server_status = 'blocked'
            AND s.whatsapp_endpoints_status != 'blocked'
            AND s.whatsapp_web_accessible IS NOT FALSE
        )
        SELECT COUNT(*) AS n
        FROM sole_driver sd
        WHERE (
          SELECT JSON_VALUE(elem, '$.failure')
          FROM UNNEST(JSON_EXTRACT_ARRAY(sd.raw_test_keys, '$.requests')) AS elem
          WHERE JSON_VALUE(elem, '$.request.url') = 'https://v.whatsapp.net/v2/register'
          LIMIT 1
        ) IS NULL
    """
    n = next(client.query(query).result()).n
    assert n == 2, (
        f"TD-126 population drift: expected exactly 2 registration_server_"
        f"status='blocked' sole-driver rows with a genuinely non-null "
        f"v2/register request failure (the http_request_failed shape), "
        f"got {n}. If this grew, new data may have introduced the same "
        f"shape again -- investigate before assuming this test is simply "
        f"stale."
    )


@requires_bigquery
def test_live_reclassified_population_is_now_ok():
    """Structural, time-invariant guard for the fix's actual effect: any
    row where the http_request_failed shape was the ONLY signal (every
    other condition clean) must now be OK, not ANOMALOUS."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        WITH sole_driver AS (
          SELECT s.measurement_id, m.raw_test_keys
          FROM `{PROJECT_ID}.stg.ooni_measurement_summary` s
          JOIN `{PROJECT_ID}.int.ooni_measurement_verdicts` v USING (measurement_id)
          JOIN `{PROJECT_ID}.stg.ooni_measurements` m USING (measurement_id)
          WHERE s.test_name = 'whatsapp'
            AND s.registration_server_status = 'blocked'
            AND s.whatsapp_endpoints_status != 'blocked'
            AND s.whatsapp_web_accessible IS NOT FALSE
        )
        SELECT COUNT(*) AS n
        FROM sole_driver sd
        JOIN `{PROJECT_ID}.int.ooni_measurement_verdicts` v ON v.measurement_id = sd.measurement_id
        WHERE (
          SELECT JSON_VALUE(elem, '$.failure')
          FROM UNNEST(JSON_EXTRACT_ARRAY(sd.raw_test_keys, '$.requests')) AS elem
          WHERE JSON_VALUE(elem, '$.request.url') = 'https://v.whatsapp.net/v2/register'
          LIMIT 1
        ) IS NULL
        AND v.ooni_verdict != 'OK'
    """
    n = next(client.query(query).result()).n
    assert n == 0, (
        f"TD-126 regression (live): {n} whatsapp rows where the "
        "http_request_failed shape was the sole signal are NOT classified "
        "OK -- the fix did not take effect for these rows."
    )


@requires_bigquery
def test_live_no_row_with_other_driver_true_is_missing_anomalous():
    """Additive-proof regression guard: any whatsapp row where
    endpoints_status/whatsapp_web_accessible/whatsapp_registration_
    accessible independently indicate blocking must be ANOMALOUS."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT COUNT(*) AS n
        FROM `{PROJECT_ID}.int.ooni_measurement_verdicts` v
        JOIN `{PROJECT_ID}.stg.ooni_measurement_summary` s USING (measurement_id)
        WHERE s.test_name = 'whatsapp'
          AND (
            s.whatsapp_endpoints_status = 'blocked'
            OR s.whatsapp_web_accessible IS FALSE
            OR s.whatsapp_registration_accessible IS FALSE
          )
          AND v.ooni_verdict != 'ANOMALOUS'
    """
    n = next(client.query(query).result()).n
    assert n == 0, (
        f"TD-126 additive-claim regression (live): {n} whatsapp rows have "
        "a real, current driver true but are NOT classified ANOMALOUS."
    )


@requires_bigquery
def test_pinned_measurements_classify_as_expected():
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    entries = _load_fixture()
    ids = [e["measurement_id"] for e in entries]
    query = f"""
        SELECT measurement_id, ooni_verdict
        FROM `{PROJECT_ID}.int.ooni_measurement_verdicts`
        WHERE measurement_id IN UNNEST(@ids)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("ids", "STRING", ids),
    ])
    actual = {row.measurement_id: row.ooni_verdict for row in client.query(query, job_config=job_config).result()}

    for entry in entries:
        mid = entry["measurement_id"]
        assert mid in actual, f"pinned measurement_id {mid} not found live -- data may have moved/been re-ingested."
        assert actual[mid] == entry["expected_ooni_verdict"], (
            f"{mid} (test_version={entry['test_version']}, {entry['note']}): "
            f"expected {entry['expected_ooni_verdict']!r}, got {actual[mid]!r}"
        )
