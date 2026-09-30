# Cascflow Changelog

## Unreleased

- add `OCFL-Object-Root` to `FILE_VERSION_USE_STATEMENT_ORDER`, between `Web-Access` and `OCFL-Object-Inventory` -- Distillery's preservation `file_version` now points at the OCFL object root in the ArchivesVault gateway, and an unrecognized `use_statement` would sort it below even the legacy `URL-Redirected` entries

## 4.5.0

- `save_digital_object_file_versions()` now relabels a demoted `image-thumbnail`/`Web-Access` file_version as `URL-Redirected` when its `file_uri` predates the object's current ARK, instead of leaving it under its original `use_statement` -- distinguishes a genuine pre-ARK-migration entry (which should read as a retired redirect target) from one merely superseded by a newer same-scheme refresh (e.g. re-sequenced ptifs), which is left alone
- add `s3_copy_object()` and `s3_list_object_keys()`
- add an optional `website_redirect_location` parameter to `s3_put_object()`

## 4.4.0

- `create_digital_object()`/`save_digital_object_file_versions()` now derive `digital_object_id` from the archival object's ARK (`digital_object_id_for_archival_object()`) instead of `component_id`; raises if `external_ark_url` isn't set yet
- move `ark_base_compact_name()` here from Distillery so both it and Alchemist can share it
- `save_digital_object_file_versions()` now sorts `file_versions` into a canonical `use_statement` order (`image-thumbnail`, `Persistent-URL`, `Web-Access`, `OCFL-Object-Inventory`, `URL-Redirected`) via the new `sort_file_versions()`, so Distillery's preservation-only file_versions no longer scramble Alchemist's web-access ordering when both touch the same digital_object

## 4.3.3

- add `ocfl.build_extension_0007_config()` for recording Extension 0007's storage-layout parameters in a storage root's `config.json` (OCFL 1.1 §3.1)

## 4.3.2

- add `invalidate_cloudfront_paths()`, a shared CloudFront invalidation helper (moved out of Alchemist's own copy, which had an identical implementation)
- add an optional `content_type` parameter to `s3_put_object()`

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
