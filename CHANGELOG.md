# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.2.0] - 2026-04-21

### Breaking Changes

- `compact`, `migrate_values`, `migrate_keys`, and `migrate_index` now require
  a `&SingleUserToken` as their final argument.  A `SingleUserToken` can only
  be obtained from the closure projected by `as_single_user`, enforcing at
  compile time that these administrative methods are never called outside of
  single-user mode.

  **Before:**
  ```rust
  db.compact()?;
  db.migrate_index("city", 1, |v| Ok(v))?;
  ```

  **After:**
  ```rust
  db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| {
      db.compact(token)?;
      db.migrate_index("city", 1, |v| Ok(v), token)?;
      Ok(())
  })?;
  ```

- `as_single_user` closure signature changed from `FnOnce() -> IsamResult<T>`
  to `FnOnce(&SingleUserToken, Isam<K, V>) -> IsamResult<T>`.  The token and a
  clone of the database handle are projected into the closure automatically.

### Added

- `SingleUserToken` — opaque capability type exported from both
  `highlandcows-isam` and `highlandcows`.
- `DEFAULT_SINGLE_USER_TIMEOUT` is now also exported from the `highlandcows`
  facade crate.

---

## [0.1.3] - 2026-04-20

### Added

- Single-user mode (`Isam::as_single_user`) — lets one thread take exclusive
  access for administration while other threads fail fast with
  `IsamError::SingleUserMode`.
- `DEFAULT_SINGLE_USER_TIMEOUT` constant (30 seconds).
- `IsamError::Timeout` — returned when an in-flight transaction does not
  release the storage lock within the supplied timeout.

---

## [0.1.2] - 2026-04-06

### Added

- Secondary indices — define additional lookup indices via the `DeriveKey`
  trait; maintained automatically and rolled back with transactions.
- `Isam::migrate_index` — rebuild a secondary index with an optional value
  transformation and schema version bump.
- `IsamBuilder::rebuild_index` — force a full index rebuild on open without
  a version bump.
- `IndexInfo` and `Isam::secondary_indices` — inspect registered indices and
  their current schema versions.

---

## [0.1.1] - 2026-04-04

### Added

- `Isam::migrate_values` and `Isam::migrate_keys` — rewrite all records
  through a transformation function, bumping the respective schema version.
- `Isam::key_schema_version` / `Isam::val_schema_version` — read the current
  schema versions from disk.
- `Isam::range` — efficient key-range iteration.
- `Isam::min_key` / `Isam::max_key` helpers.

---

## [0.1.0] - 2026-03-05

### Added

- Initial release: persistent ISAM key/value store backed by an on-disk
  B-tree with ACID transactions, compaction, and key-ordered iteration.
