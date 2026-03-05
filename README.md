# highlandcows

![Build & Tests](https://github.com/njacobs5074/highlandcows/actions/workflows/rust.yml/badge.svg?branch=master)

A Cargo workspace of Rust libraries published under the `highlandcows` umbrella crate.

> **Created with [Claude Code](https://claude.ai/code) by Anthropic.**

---

## Crates

| Crate | Description |
|-------|-------------|
| [`highlandcows-isam`](crates/isam/) | Persistent ISAM key/value store backed by an on-disk B-tree |

---

## Usage

Add the umbrella crate to your `Cargo.toml`:

```toml
[dependencies]
highlandcows = { path = "path/to/highlandcows/crates/highlandcows" }
# or, once published to crates.io:
highlandcows = "0.1"
```

Then import what you need:

```rust
use highlandcows::Isam;
```

---

## highlandcows-isam

A persistent ISAM (Indexed Sequential Access Method) library. Records are stored on disk indexed by a user-supplied key type, with support for full CRUD operations, key-ordered iteration, and compaction.

### Features

- **Generic key and value types** — any type that implements `serde::Serialize + DeserializeOwned + Ord + Clone` can be used as a key; any serializable type can be a value
- **On-disk B-tree index** — page-based (4096 bytes/page), no in-memory tree required
- **Append-only data file** — mutations never overwrite existing records; stale data is reclaimed by `compact()`
- **Key-ordered iteration** — sequential scan via a linked leaf-page chain
- **Compaction** — atomically rewrites the data and index files, removing tombstones and stale records
- **Persistence** — data survives process restart; just `open()` the same path

### File layout on disk

Each logical database is stored as two files:

| File    | Contents                                      |
|---------|-----------------------------------------------|
| `*.idb` | Append-only data records (bincode-encoded)    |
| `*.idx` | On-disk B-tree index (fixed 4096-byte pages)  |

### Quick start

```rust
use highlandcows::Isam;

// Create a new database (pass any path prefix — extensions are added automatically)
let mut db: Isam<String, u64> = Isam::create("/tmp/mydb")?;

// Insert
db.insert("alice".to_string(), &42)?;
db.insert("bob".to_string(), &99)?;

// Get
let val = db.get(&"alice".to_string())?; // Some(42)

// Update
db.update("alice".to_string(), &100)?;

// Delete
db.delete(&"bob".to_string())?;

// Iterate in key order
for result in db.iter()? {
    let (key, value) = result?;
    println!("{key} => {value}");
}

// Remove stale records and reclaim disk space
db.compact()?;

// Open an existing database
let mut db: Isam<String, u64> = Isam::open("/tmp/mydb")?;
```

### API

```rust
Isam::create(path) -> IsamResult<Self>
Isam::open(path)   -> IsamResult<Self>

db.insert(key, &value)  -> IsamResult<()>   // errors on duplicate key
db.get(&key)            -> IsamResult<Option<V>>
db.update(key, &value)  -> IsamResult<()>   // errors if key not found
db.delete(&key)         -> IsamResult<()>   // errors if key not found
db.iter()               -> IsamResult<IsamIter<K, V>>
db.compact()            -> IsamResult<()>
```

### Error types

| Variant | When |
|---------|------|
| `IsamError::DuplicateKey` | `insert()` called with an existing key |
| `IsamError::KeyNotFound` | `update()` or `delete()` called with a missing key |
| `IsamError::Io(_)` | underlying file I/O error |
| `IsamError::Bincode(_)` | serialization/deserialization failure |
| `IsamError::CorruptIndex(_)` | index file has an invalid magic number or page type |

---

## Building

Requires Rust 1.70 or later. Install via [rustup](https://rustup.rs) if needed.

```sh
# Build all crates
cargo build

# Run all tests
cargo test

# Build optimized
cargo build --release
```

---

## Workspace structure

```
highlandcows/
├── Cargo.toml                  # workspace root
├── crates/
│   ├── highlandcows/           # umbrella facade crate
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   └── isam/                   # highlandcows-isam implementation
│       ├── Cargo.toml
│       ├── src/
│       └── tests/
├── README.md
└── LICENSE
```

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| [`serde`](https://crates.io/crates/serde) | Serialization framework |
| [`bincode`](https://crates.io/crates/bincode) 1.x | Compact binary encoding |
| [`thiserror`](https://crates.io/crates/thiserror) | Ergonomic error type derivation |

---

## License

MIT — see [LICENSE](LICENSE).
