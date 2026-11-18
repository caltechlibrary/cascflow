# 2. Merge `file_versions` by `file_uri`, in a canonical `use_statement` order

- Status: accepted
- Date: 2026-07-24 (ordering added in 4.4.0; relabelling in 4.5.0; ownership in 5.1.0)

## Context and Problem Statement

Two pipelines write `file_version`s onto the same ArchivesSpace
`digital_object`. Alchemist writes web-access entries — thumbnails, the
persistent URL, the web-access derivative — as objects are published and
republished. Distillery writes a preservation entry pointing into OCFL
storage. Neither knows when the other last ran.

A naive "replace the list" would mean whichever pipeline ran last erased the
other's work. A naive "append" would accumulate duplicates and stale entries
with no way to tell which is current. And ArchivesSpace itself cares about
order in one specific, undocumented way.

## Decision Outcome

`save_digital_object_file_versions()` **merges by `file_uri`** and then sorts
into a **canonical `use_statement` order**.

### Merge

Incoming entries win for their `file_uri`. An existing entry whose `file_uri`
is not in the incoming list is **kept but demoted** — `publish: false`,
`is_representative: false` — rather than deleted, **if the caller owns it**.
Nothing this library does removes a `file_version`: a URL that was once
published stays visible to staff as a record of what was there.

Each canonical `use_statement` belongs to the pipeline that writes it, so a
caller owns an existing entry when:

- its `use_statement` is one the caller is writing in this save, or
- it has no canonical `use_statement` at all (a hand-entered YouTube link,
  a share-drive path), which no pipeline owns.

Any other existing entry is another pipeline’s, and comes through exactly as
it was. Without this, Distillery saving only its `OCFL-Object-Root` demotes
Alchemist’s published thumbnail, so the public UI shows no image.

### Order

`FILE_VERSION_USE_STATEMENT_ORDER`:

1. `image-thumbnail`
2. `Persistent-URL`
3. `Web-Access`
4. `OCFL-Object-Root`
5. `OCFL-Object-Inventory`
6. `URL-Redirected`

An unrecognized `use_statement` sorts after all recognized ones, stably.

**`image-thumbnail` must lead: ArchivesSpace's representative-image linking in
the public UI depends on its position.** This is the load-bearing part of the
whole decision and the reason the ordering cannot be left to whatever order
callers happen to pass. Everything after it is Caltech convention —
Alchemist's entries, then the preservation entries, then retired ones.

### Relabelling

A demoted `image-thumbnail` or `Web-Access` entry whose `file_uri` does **not**
contain the object's current ARK is relabelled `URL-Redirected`, sending it to
the end of the order.

That ARK test is the point of the rule: it distinguishes an entry retired by
the ARK migration — a genuinely legacy redirect target — from one merely
superseded by a newer same-scheme entry, such as re-sequenced ptifs, which
keeps its original `use_statement`.

## Considered Options

- **Replace the whole list.** Rejected: each pipeline erases the other.
- **Append without merging.** Rejected: duplicates per run, and no way to
  identify the current entry.
- **Merge without sorting.** Rejected: breaks public-UI thumbnails whenever
  Distillery's write happens to land an entry ahead of `image-thumbnail`.
- **Sort in each caller.** Rejected: two implementations of one invariant,
  and the next caller inherits the trap.
- **Delete superseded entries instead of demoting them.** Rejected: loses the
  record of a URL that was once public, which is what makes a legacy redirect
  traceable.

## Consequences

**Callers cannot assume the order they pass is the order stored.** Code written
as though it can is wrong, even when it happens to work.

**Changing this order changes both pipelines' ArchivesSpace output with no
change in either repository** — it takes effect when a consumer bumps its pin.
That happened with 5.0.0, which inserted `OCFL-Object-Root` and so moved
Distillery's preservation entry from below the retired entries to position 4.

**Adding a `use_statement` is not a local change.** ArchivesSpace rejects any
value absent from its own `use_statement` enumeration, so a new value needs a
one-time addition to each instance — including instances restored from a
backup predating it.

Saving `file_version`s has side effects on the `digital_object` itself:
`digital_object_id` is set from the ARK (ADR 0004), `title` from the archival
object, and `publish` to `true`. Publishing is not opt-in.

## More Information

- `sort_file_versions()`, `FILE_VERSION_USE_STATEMENT_ORDER`,
  `save_digital_object_file_versions()`.
- `alchemist/docs/decisions/0002-file-version-conventions-live-in-cascflow.md`
  and `distillery/docs/decisions/0006-describe-the-object-with-one-file-version-and-a-note.md`
  for how each caller lives with this.
