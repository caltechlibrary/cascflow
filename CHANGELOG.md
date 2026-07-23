# Cascflow Changelog

## 4.3.1

- `ocfl.add_version()` now omits `message`/`user`/`user.address` from the version block entirely when not provided, instead of writing them as empty strings (all optional per the OCFL spec)

## 4.3.0

- add `cascflow.ocfl` module (`object_root_path()`, `build_inventory()`, `add_version()`) for writing OCFL Extension 0007 storage layouts and inventories
- add `enrich_ancestors()` for resolving each ancestor's own `linked_agents`/`subjects` inline, since ArchivesSpace's `resolve[]` has no nested syntax
- fix `save_digital_object_file_versions()` to re-fetch the digital_object fresh instead of trusting `archival_object`'s embedded `_resolved` snapshot, which could go stale (e.g. an intervening archival_object save bumping its `lock_version`) and 409 on update
- `ineligible_archival_objects` entries are now `{"component_id", "detail"}` instead of bare component_id strings, so callers can tell "not found" from "multiple found" (and which records conflict) without re-querying ArchivesSpace by hand

## 4.2.2

- type `config()` with the same overloads as `decouple`'s `Config.__call__`/`AutoConfig.__call__` so callers get accurate return types (e.g. `cast=int` resolves to `int`) instead of `str | Unknown`
- fix false-positive type errors in `execute()`/`delete_files_to_remove()` where `Csv()`'s `None` handling wasn't reflected in the inferred type

## 4.2.1

- fix `save_digital_object_file_versions()` to also sync the Digital Object's `title` with the Archival Object's title (caltechlibrary/alchemist#71)

## 4.2.0

- add `validate_setting()`/`validate_settings()` for checking that settings.ini variables are set and usable (executable paths, directories, URLs, integers, CSV lists)

## 4.1.1

- fix `establish_s3_connection()` to return the S3 client it creates
- begin changelog
