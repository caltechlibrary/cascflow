# Cascflow Changelog

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
