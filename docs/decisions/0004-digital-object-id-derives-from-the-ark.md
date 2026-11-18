# 4. Derive `digital_object_id` from the ARK, and require the ARK first

- Status: accepted
- Date: 2026-07-27 (shipped in 4.4.0)

## Context and Problem Statement

`digital_object_id` was derived from the archival object's `component_id`.
That identifier is assigned by archivists, is not guaranteed stable, and is
not the identifier anything outside ArchivesSpace uses to refer to the object.
Meanwhile every object in these pipelines gets an ARK minted for it, and the
ARK *is* the external identifier — it is what the resolver resolves, what the
OCFL object is keyed by, and what appears in a `file_version` URL.

Two identifiers for one object, with the less durable one in the
ArchivesSpace field, meant a record could not be located from the identifier
the outside world holds.

## Decision Outcome

`digital_object_id_for_archival_object()` returns the **ARK Base Compact Name**
(e.g. `ark:77914/b3xq7z`), derived from `archival_object["external_ark_url"]`
via `ark_base_compact_name()`. Both `create_digital_object()` and
`save_digital_object_file_versions()` use it.

**It raises `ValueError` when `external_ark_url` is absent** rather than
falling back to `component_id`. An ARK must be minted before a
`digital_object` can be created or updated for its archival object.

Raising is the decision, not an implementation detail. A fallback would
produce a `digital_object` whose id silently disagrees with every other
object's, discoverable only by noticing later that an identifier lookup
misses. The failure mode of raising is an immediate, legible error in a
pipeline run; the failure mode of a fallback is a record nobody can find.

The compact form is used rather than the full resolvable URL because that is
the ARK's own canonical identifier per the ARK spec — the URL is a resolvable
rendering of it, appropriate for a `file_version` link, not for an identifier
field.

## Considered Options

- **Keep `component_id`.** Rejected: not stable, not external, and not what
  anything else in the stack keys on.
- **Derive from the ARK, falling back to `component_id`.** Rejected as above:
  a silent inconsistency beats a loud failure only until you need to find the
  record.
- **Store the ARK in a second field alongside a `component_id`-derived id.**
  Rejected: two sources of truth for one identifier.
- **The full resolvable ARK URL as the id.** Rejected on spec grounds.

## Consequences

**Callers must mint the ARK before creating or updating a `digital_object`.**
This reordered Distillery's `execute` loop, which previously created the
digital object first. Any new caller inherits the constraint, and the error
message names the archival object so the ordering bug is obvious rather than
mysterious.

Records created before 4.4.0 keep their `component_id`-derived
`digital_object_id` until something touches them — a
`save_digital_object_file_versions()` call rewrites the id as a side effect
(ADR 0002). So the two shapes coexist, and "which scheme is this record
using?" depends on when it was last written rather than on any flag.

No migration was performed. That is consistent with Distillery's forward-only
stance (its ADR 0006), but it does mean an identifier lookup across the whole
repository has to try both forms.

## More Information

- `digital_object_id_for_archival_object()`, `ark_base_compact_name()`,
  `create_digital_object()`, `save_digital_object_file_versions()`.
