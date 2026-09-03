# 03-development

**Why this folder exists:** how to set up, contribute to, test, and document
work on CLIO. This is process, not product or architecture content.

**Who reads it:** any contributor, including a future second engineer.

**When it's used:** onboarding, and any new asset, PR, or documentation
change.

## Files

- `coding-standards.md` — standards drawn from patterns already working well
  in this codebase (e.g. the OONI evidence/interpretation separation, the
  ACLED path A confidence split), not imported from a generic style guide.
- `testing-strategy.md` — what is actually tested today and, explicitly,
  what is not. Covers the real current contents of `tests/` (now well beyond
  the original `test_contracts.py`-only baseline — see that file's own
  "Current state" section for the live count) and the golden-file regression
  approach tied to known historical incidents (e.g. the Finance Bill 2024
  window).
- `documentation-standards.md` — the documentation policy: every feature or
  asset documents rationale, implementation, tests, assumptions,
  limitations, and future work; the ADR template; and where the Bruin
  `@bruin` YAML description block maps onto this policy.
- `claude-code-environment.md` — environment setup and migration notes for
  running Claude Code inside this project's GitHub Codespaces devcontainer,
  companion to `coding-standards.md` and `implementation-roadmap.md`.

## Status

Populated. The list above reflects this folder's actual contents as of
2026-09-03 (confirmed via direct directory listing); update it directly when
a file is added, renamed, or removed rather than letting it drift back into
a stale "planned" state.
