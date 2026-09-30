# Caltech Archives & Special Collections Workflow Library

This is a library used by [Distillery](https://github.com/caltechlibrary/distillery) and [Alchemist](https://github.com/caltechlibrary/alchemist).

## Architecture decisions

Significant decisions are recorded as [ADRs](https://adr.github.io/madr/) in
[`docs/decisions/`](docs/decisions/). Start with
[ADR 0001](docs/decisions/0001-record-architecture-decisions.md) for the
conventions.

Behavior worth knowing before calling this library:

- [ADR 0002](docs/decisions/0002-file-version-merge-is-keyed-by-uri-and-canonically-ordered.md)
  — `save_digital_object_file_versions()` reorders and relabels what you pass
  it, and publishes the `digital_object`.
- [ADR 0003](docs/decisions/0003-the-ocfl-module-writes-only.md) — the `ocfl`
  module writes and does not read or validate.
- [ADR 0004](docs/decisions/0004-digital-object-id-derives-from-the-ark.md) —
  an ARK must be minted before a `digital_object` can be created or updated.
