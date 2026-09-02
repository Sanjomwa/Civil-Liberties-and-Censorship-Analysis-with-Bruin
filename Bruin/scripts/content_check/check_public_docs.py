#!/usr/bin/env python3
"""TD-132: pre-commit content tripwire for CLAUDE.md and decision-log.md.

CLAUDE.md and docs/02-architecture/decision-log.md are deliberately public,
tracked files (see CLAUDE.md's "Public repository docs policy"). The
folder-level .gitignore boundary that keeps the other six docs/ folders
private cannot catch a sensitive sentence written directly into one of these
two files - that already happened once (see decision-log.md's 2026-07-12
entry) and sat unfixed in git history for a period before a human caught it
by hand. This script is a loud, cheap tripwire against that recurring, not a
precision content classifier - see the two rules below for exactly what it
checks and what it deliberately does not guarantee.

Exit 0 (silent) if nothing is flagged. Exit 1 with a human-readable report if
something is. Both rules are advisory: a flag requires a deliberate human
override (`git commit --no-verify`), never a hard block - see CLAUDE.md.
"""

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKED_PATHS = [
    "CLAUDE.md",
    "docs/02-architecture/decision-log.md",
]
DENYLIST_PATH = Path(__file__).with_name("sensitive_terms.yml")
DENYLIST_LOCAL_PATH = Path(__file__).with_name("sensitive_terms.local.yml")

# Rule 2 (name heuristic): two consecutive capitalized words, e.g. "Sadia
# Nourin" or "Finance Bill". Deliberately simple - see module docstring and
# the false-positive note below.
CAP_PAIR_RE = re.compile(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b")

# Terms that would otherwise trip the name heuristic on ordinary, already-fine
# prose the first time they appear in a new sentence position. Not
# exhaustive - a genuinely new legitimate capitalized term (a new ADR title,
# a new product/dataset name) will still flag once, and that's an expected,
# intentional false positive requiring a one-time human override, not a bug.
NAME_HEURISTIC_ALLOWLIST = {
    "claude code",
    "public repository",
    "not done",
    "not remediated",
    "civil liberties",
}


def load_yaml_terms(path):
    if not path.exists():
        return []
    import yaml

    with path.open() as f:
        data = yaml.safe_load(f) or {}
    return [str(t) for t in data.get("terms", [])]


def load_denylist():
    terms = load_yaml_terms(DENYLIST_PATH)
    local_terms = load_yaml_terms(DENYLIST_LOCAL_PATH)
    if not DENYLIST_LOCAL_PATH.exists():
        print(
            f"[check_public_docs] note: {DENYLIST_LOCAL_PATH.name} not present "
            "(expected on a fresh clone or in CI) - checking against the "
            "public denylist only.",
            file=sys.stderr,
        )
    return terms + local_terms


def git_show(ref_and_path):
    result = subprocess.run(
        ["git", "show", ref_and_path],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return ""
    return result.stdout


def baseline_cap_pairs(path):
    """Capitalized word-pairs already present in the file's last committed
    version - these are exempt from the name heuristic on the theory that
    they're already public and this check only cares about *new* exposure."""
    content = git_show(f"HEAD:{path}")
    return {m.group(0).lower() for m in CAP_PAIR_RE.finditer(content)}


def added_lines(paths):
    """Yields (path, new_line_number, line_text) for every added line in the
    staged diff of the given paths, using --unified=0 so hunk headers give
    exact new-file line numbers without needing to walk context lines."""
    result = subprocess.run(
        ["git", "diff", "--cached", "--unified=0", "--"] + paths,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    current_path = None
    next_line = None
    hunk_re = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")
    for line in result.stdout.splitlines():
        if line.startswith("+++ b/"):
            current_path = line[len("+++ b/") :]
            continue
        m = hunk_re.match(line)
        if m:
            next_line = int(m.group(1))
            continue
        if line.startswith("+++") or line.startswith("---"):
            continue
        if line.startswith("+"):
            if current_path is not None and next_line is not None:
                yield current_path, next_line, line[1:]
                next_line += 1


def check_denylist(path, line_no, text, denylist):
    lowered = text.lower()
    hits = []
    for term in denylist:
        if term.lower() in lowered:
            hits.append(term)
    return hits


def check_name_heuristic(path, line_no, text, baseline_pairs):
    hits = []
    for m in CAP_PAIR_RE.finditer(text):
        pair = m.group(0)
        pair_l = pair.lower()
        if pair_l in NAME_HEURISTIC_ALLOWLIST:
            continue
        if pair_l in baseline_pairs:
            continue
        hits.append(pair)
    return hits


def main():
    checked_paths = [p for p in CHECKED_PATHS if (REPO_ROOT / p).exists()]
    denylist = load_denylist()
    baselines = {p: baseline_cap_pairs(p) for p in checked_paths}

    findings = []
    for path, line_no, text in added_lines(checked_paths):
        if path not in checked_paths:
            continue
        denylist_hits = check_denylist(path, line_no, text, denylist)
        for term in denylist_hits:
            findings.append(
                (path, line_no, "denylist", f'matched term "{term}"', text)
            )
        name_hits = check_name_heuristic(path, line_no, text, baselines[path])
        for pair in name_hits:
            findings.append(
                (
                    path,
                    line_no,
                    "name-heuristic",
                    f'new capitalized word-pair "{pair}"',
                    text,
                )
            )

    if not findings:
        return 0

    print("=" * 72, file=sys.stderr)
    print(
        "check_public_docs.py (TD-132): possible sensitive content staged for "
        "CLAUDE.md / decision-log.md",
        file=sys.stderr,
    )
    print("=" * 72, file=sys.stderr)
    for path, line_no, rule, detail, text in findings:
        print(f"  {path}:{line_no} [{rule}] {detail}", file=sys.stderr)
        print(f"      + {text.strip()}", file=sys.stderr)
    print(file=sys.stderr)
    print(
        "This is an advisory tripwire, not a hard block - it requires human "
        "judgment, not a guarantee of precision (see the name-heuristic's "
        "documented false-positive rate in this script's module docstring "
        "and in sensitive_terms.yml's header).",
        file=sys.stderr,
    )
    print(
        "If this is a false positive, or the content is intentional and "
        "fine, override with: git commit --no-verify",
        file=sys.stderr,
    )
    print(
        "If it's real, remove it from the staged change before committing - "
        "see CLAUDE.md's 'Public repository docs policy' for where private "
        "content actually belongs.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
