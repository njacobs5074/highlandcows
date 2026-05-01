# CLAUDE.md

This file provides guidance to Claude Code when working in this repository.

## Workflow

For any non-trivial change — new features, bug fixes, refactoring, or significant
documentation updates — create a new git branch before making edits.

Branch naming follows conventional-commit style:

| Type | Example branch |
|------|----------------|
| Feature | `feat/range-search` |
| Bug fix | `fix/leaf-merge-underflow` |
| Refactor | `refactor/btree-ordering` |
| Docs | `docs/readme-api-section` |
| Chore | `chore/update-dependencies` |

Typo fixes and trivial comment edits may be made directly on the current branch.

## Commands

```bash
cargo build                # Build all crates in the workspace
cargo test                 # Run all tests
cargo test <name>          # Run a single test by name
cargo clippy               # Lint
cargo fmt                  # Format
```

## Architecture

This is a Cargo workspace with two crates:

| Crate | Path | Published as |
|-------|------|--------------|
| `highlandcows-isam` | `crates/isam/` | `highlandcows-isam` on crates.io |
| `highlandcows` | `crates/highlandcows/` | `highlandcows` on crates.io (umbrella re-export) |

### highlandcows-isam

A persistent ISAM (Indexed Sequential Access Method) key-value store backed by an
on-disk B-tree. Key features:

- **ACID transactions**: every read or write wraps an explicit `begin_transaction` /
  `commit` pair via the `Transaction` type.
- **Generic keys and values**: `K: Serialize + Deserialize`, `V: Serialize + Deserialize`.
- **Secondary indices**: optional inverted indices over value fields (`secondary_index.rs`).
- **Convenience helpers**: `Isam::read` and `Isam::write` wrap single-operation
  transactions.

Source layout under `crates/isam/src/`:

| File | Purpose |
|------|---------|
| `isam.rs` | Public `Isam<K, V>` struct — create/open, insert/get/delete/scan |
| `store.rs` | On-disk B-tree page store |
| `storage.rs` | Raw page I/O |
| `transaction.rs` | Transaction handle and commit/rollback logic |
| `manager.rs` | Concurrency control |
| `secondary_index.rs` | Optional secondary index support |
| `error.rs` | Error types |

### highlandcows (umbrella)

Re-exports the public API of `highlandcows-isam` for consumers who prefer a single
dependency name.

## Testing ground

`highlandcows_graphdb` (a sibling project at `../highlandcows_graphdb/`) is used as a
real-world testing ground for this library. New ISAM features are often exercised
there before being considered stable. When making significant changes here, consider
whether they should be validated against `highlandcows_graphdb` as well.
