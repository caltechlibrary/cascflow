# 1. Record architecture decisions

- Status: accepted
- Date: 2026-09-30

## Context and Problem Statement

cascflow is the shared library: Distillery and Alchemist both depend on it, and
a change here changes both pipelines' behavior without either repository being
touched. That makes its semantics the most consequential in the stack and,
until now, the least documented — the README is three lines.

Worse, the decisions cascflow owns were being recorded in its *consumers*.
Alchemist's ADR 0002 documents this library's `file_version` merge and
ordering rules from the caller's side; Distillery's ADR 0006 documents why the
OCFL writer here was hand-rolled rather than taken from a library. Both are
legitimate — they record what those choices cost each caller — but the
authority for a shared library's behavior should not be reachable only by
reading one of the things that calls it.

## Decision

Record architecturally significant decisions as ADRs in `docs/decisions/`,
following [MADR](https://adr.github.io/madr/), matching the convention
Distillery and Alchemist adopted.

Conventions:

- One file per decision, `NNNN-title-in-kebab-case.md`, numbered sequentially
  in the order recorded (not the order decided).
- Each has a status: `proposed`, `accepted`, `rejected`, `deprecated`, or
  `superseded by ADR-NNNN`.
- **ADRs are immutable once accepted.** A decision that changes is not edited;
  a new ADR supersedes it, and the old one is marked as superseded.
- Corrections of *fact* about what shipped are fine to amend in place, dated
  and noted. Changes of *decision* get a new ADR.
- Record the rejected options and why.

Consumer ADRs stay where they are. They answer a different question — "why
does my output come back like this?" — and are correctly owned by the
repository whose behavior is surprising. These ADRs are the authority for what
the library does and why; consumer ADRs describe living with it.

## Consequences

A breaking change here now has somewhere to explain itself beyond a changelog
line, which matters because consumers pin by git tag and adopt changes on
their own schedule. A reader can find out why `save_digital_object_file_versions()`
reorders their input without reading Alchemist.

The cost is the usual one: an ADR nobody supersedes when the decision changes
is worse than none. Ordinary changes belong in `CHANGELOG.md` — this is for
decisions whose consequences outlive the release that introduced them.
