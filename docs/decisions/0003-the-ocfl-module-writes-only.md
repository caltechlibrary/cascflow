# 3. The `ocfl` module writes; it does not read or validate

- Status: accepted
- Date: 2026-07-24 (shipped in 4.3.0)

## Context and Problem Statement

Distillery needed to write [OCFL](https://ocfl.io/latest/spec/) objects —
create an object, add a version, maintain its inventory. The obvious move was
`ocfl-py` or another existing implementation. The obvious move was not taken,
and the reason should be recorded before someone reasonably proposes it again.

## Decision Outcome

A small, purpose-built `ocfl` module inside cascflow, covering exactly two
operations: **build an inventory for a new object, and add a version to one.**
No reading of arbitrary existing objects, no validation, no repository-level
operations, no migration between OCFL versions.

It is **pure functions over dicts and paths, with no S3 or network I/O**. The
caller decides where bytes go and when. That is what lets Distillery stage a
version and `os.rename()` it into place — its atomicity requirement, which a
library that owned the writing could not accommodate.

It lives in cascflow rather than Distillery because it is exactly the kind of
thing a future audit tool or Alchemist might need, and because it is testable
without a pipeline around it.

## Considered Options

- **`ocfl-py` or a comparable library.** Rejected: a dependency whose surface
  is mostly operations this stack never performs, for a spec subset small
  enough to implement against the spec text. It would also own the file
  writing, which conflicts with the caller-controlled staging that
  Distillery's atomic-rename design requires.
- **Implementing it inside Distillery.** Rejected: pure logic with no
  pipeline dependencies, and plausibly needed by more than one consumer.
- **A fuller implementation here, including validation.** Rejected as
  speculative: nothing needs to validate an object today, and a
  half-implemented validator is worse than none because it invites trust.

This matches the stack's existing style — `arkminter` is the same kind of
small, purpose-built library for a spec nobody needs generally.

## Consequences

**Conformance is our responsibility, and it is checked at the edges.** The
storage root's declaration, `ocfl_layout.json` and the Extension 0007 config
are verified by the caller (Distillery's `validate_ocfl_storage_root()`), not
by this module. Nothing here will catch a malformed object.

**The spec's own behavior has to be honored deliberately.** OCFL's
forward-delta versioning — unchanged files referenced by digest in a new
version's manifest rather than re-copied — falls out of implementing the spec
correctly. It is not a feature that was built, and it is easy to break by
"simplifying" manifest construction.

**Structural parameters are the caller's to fix and never change.**
`object_root_path()` takes the Extension 0007 parameters as arguments rather
than hard-coding Caltech's, so the module stays general — but a version-padding
or tuple-size change after an object exists corrupts its layout, and this
module cannot detect that. See Distillery's ADR 0002.

Reading an existing object is out of scope, so a consumer that needs it —
the sync-audit tool, most likely — either parses `inventory.json` itself or
this scope gets revisited with a new ADR.

## More Information

- `src/cascflow/ocfl.py`: `object_root_path()`, `build_inventory()`,
  `add_version()`, `build_extension_0007_config()`.
- `distillery/docs/decisions/0006-forward-only-migration-with-a-hand-rolled-ocfl-writer.md`,
  which records the same decision from the consumer's side, and ADR 0002
  there for the layout parameters.
