# highlandcows

![Build & Tests](https://github.com/njacobs5074/highlandcows/actions/workflows/rust.yml/badge.svg?branch=main)

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
use highlandcows::{Isam, Transaction};
```

---

## highlandcows-isam

A persistent ISAM (Indexed Sequential Access Method) library. Records are stored on disk indexed by a user-supplied key type, with support for full CRUD operations, key-ordered iteration, range queries, compaction, and ACID transactions.

### Features

- **ACID transactions** — serializable isolation via a mutex-held transaction; undo-log rollback; `fsync` at commit for durability; auto-rollback on drop
- **Generic key and value types** — any type that implements `serde::Serialize + DeserializeOwned + Ord + Clone` can be used as a key; any serializable type can be a value
- **On-disk B-tree index** — page-based (4096 bytes/page), no in-memory tree required
- **Append-only data file** — mutations never overwrite existing records; stale data is reclaimed by `compact()`
- **Key-ordered iteration** — sequential scan via a linked leaf-page chain
- **Range queries** — efficient key-range iteration using `range(a..=b)`, `range(a..)`, etc.
- **Secondary indices** — define additional indices on any field of the value type via the `DeriveKey` trait; non-unique (many records per secondary key); maintained automatically and rolled back with transactions
- **Compaction** — atomically rewrites the data and index files, removing tombstones and stale records
- **Cloneable handle** — `Isam` is `Clone`; each clone is another handle to the same underlying storage, safe to share across threads

### File layout on disk

Each logical database is stored as two files:

| File | Contents |
|------|----------|
| `*.idb` | Append-only data records (bincode-encoded) |
| `*.idx` | On-disk B-tree index (fixed 4096-byte pages) |
| `*_<name>.sidb` | Secondary index data store (one per named index) |
| `*_<name>.sidx` | Secondary index B-tree (one per named index) |

### Quick start

```rust
use highlandcows::Isam;

// Create a new database (pass any path prefix — extensions are added automatically)
let db: Isam<String, u64> = Isam::create("/tmp/mydb")?;

// All mutations and reads happen inside a transaction.
let mut txn = db.begin_transaction()?;

db.insert(&mut txn, "alice".to_string(), &42)?;
db.insert(&mut txn, "bob".to_string(), &99)?;

let val = db.get(&mut txn, &"alice".to_string())?; // Some(42)

db.update(&mut txn, "alice".to_string(), &100)?;
db.delete(&mut txn, &"bob".to_string())?;

// Flush to disk and release the lock.
txn.commit()?;

// Iterate in key order
let mut txn = db.begin_transaction()?;
for result in db.iter(&mut txn)? {
    let (key, value) = result?;
    println!("{key} => {value}");
}
txn.commit()?;

// Remove stale records and reclaim disk space (outside any transaction)
db.compact()?;

// Open an existing database
let db: Isam<String, u64> = Isam::open("/tmp/mydb")?;
```

### Transaction semantics

A `Transaction` holds an exclusive lock on the database for its entire lifetime, giving **serializable isolation** — only one transaction can be active at a time.

```rust
let mut txn = db.begin_transaction()?;

// Changes are visible within the same transaction immediately.
db.insert(&mut txn, 1u32, &"hello".to_string())?;
assert_eq!(db.get(&mut txn, &1)?, Some("hello".to_string()));

// Commit writes to disk (fsync) and releases the lock.
txn.commit()?;

// Or roll back all changes explicitly.
// txn.rollback()?;

// Dropping a transaction without committing auto-rolls back.
{
    let mut txn = db.begin_transaction()?;
    db.insert(&mut txn, 2u32, &"gone".to_string())?;
    // txn dropped here → rolled back automatically
}
assert_eq!(db.get(&mut db.begin_transaction()?, &2)?, None);
```

Because `Isam` is `Clone`, multiple handles to the same database can be shared across threads. Each thread calls `begin_transaction()` on its own handle; the lock ensures they are serialized:

```rust
let db2 = db.clone();
std::thread::spawn(move || {
    let mut txn = db2.begin_transaction()?;
    db2.insert(&mut txn, 3u32, &"from thread".to_string())?;
    txn.commit()
});
```

> **Note:** `compact()`, `migrate_values()`, `migrate_keys()`, `key_schema_version()`, and
> `val_schema_version()` all acquire the database lock internally. They must not be called
> while a `Transaction` is live on the same thread, as this will deadlock. These are
> intended as offline administration operations — commit or roll back all open transactions
> before calling them.

### Secondary indices

A secondary index lets you look up records by a field other than the primary key.
Implement the `DeriveKey<V>` trait on a marker struct to describe what to index,
then register it on the database before any writes.

```rust
use serde::{Serialize, Deserialize};
use highlandcows::{Isam, DeriveKey};

#[derive(Serialize, Deserialize, Clone)]
struct User {
    name: String,
    city: String,
}

// One marker struct per index.
struct CityIndex;

impl DeriveKey<User> for CityIndex {
    type Key = String;
    fn derive(u: &User) -> String { u.city.clone() }
}

let db: Isam<u64, User> = Isam::create("/tmp/users")?;

// Register before any writes. Re-register on every open.
let city_idx = db.register_secondary_index("city", CityIndex)?;

let mut txn = db.begin_transaction()?;
db.insert(&mut txn, 1, &User { name: "Alice".into(), city: "London".into() })?;
db.insert(&mut txn, 2, &User { name: "Bob".into(),   city: "London".into() })?;
db.insert(&mut txn, 3, &User { name: "Carol".into(), city: "Paris".into()  })?;
txn.commit()?;

// Look up all users in London.
let mut txn = db.begin_transaction()?;
let londoners = city_idx.lookup(&mut txn, &"London".to_string())?;
// → [(1, User{Alice, London}), (2, User{Bob, London})]
txn.commit()?;
```

A few things to keep in mind:

- **Non-unique** — multiple records can share the same secondary key value.
- **Automatic maintenance** — `insert`, `update`, and `delete` keep all registered indices in sync.
- **Transactional** — secondary index changes are rolled back when a transaction rolls back.
- **Persistent** — index files survive process restarts; re-register the same indices on every `open`.
- **Composite indices** — not yet built in, but achievable by deriving a tuple key: `type Key = (String, u32)`.
- **No schema evolution support** — `migrate_values()` and `migrate_keys()` rewrite only the primary store; secondary index files are left untouched. If a value migration changes the fields that a secondary index derives its key from, the index will silently become stale. Drop and rebuild secondary index files manually after any such migration.

### API

```rust
// Lifecycle
Isam::create(path)          -> IsamResult<Self>
Isam::open(path)            -> IsamResult<Self>
db.begin_transaction()      -> IsamResult<Transaction<'_, K, V>>

// CRUD (all take &mut Transaction)
db.insert(&mut txn, key, &value)  -> IsamResult<()>   // errors on duplicate key
db.get(&mut txn, &key)            -> IsamResult<Option<V>>
db.update(&mut txn, key, &value)  -> IsamResult<()>   // errors if key not found
db.delete(&mut txn, &key)         -> IsamResult<()>   // errors if key not found

// Scanning (take &mut Transaction)
db.iter(&mut txn)                 -> IsamResult<IsamIter<K, V>>
db.range(&mut txn, a..=b)         -> IsamResult<RangeIter<K, V>>
db.min_key(&mut txn)              -> IsamResult<Option<K>>
db.max_key(&mut txn)              -> IsamResult<Option<K>>

// Transaction control
txn.commit()                      -> IsamResult<()>
txn.rollback()                    -> IsamResult<()>
// drop(txn) also rolls back if not yet committed

// Secondary indices (register before any writes; re-register on every open)
db.register_secondary_index(name, extractor)  -> IsamResult<SecondaryIndexHandle<K, V, SK>>
handle.lookup(&mut txn, &sk)                  -> IsamResult<Vec<(K, V)>>

// Offline administration (must not be called while a Transaction is live)
db.compact()                      -> IsamResult<()>
db.key_schema_version()           -> IsamResult<u32>
db.val_schema_version()           -> IsamResult<u32>
db.migrate_values(version, f)     -> IsamResult<Isam<K, V2>>
db.migrate_keys(version, f)       -> IsamResult<Isam<K2, V>>
```

### Error types

| Variant | When |
|---------|------|
| `IsamError::DuplicateKey` | `insert()` called with an existing key |
| `IsamError::KeyNotFound` | `update()` or `delete()` called with a missing key |
| `IsamError::LockPoisoned` | a thread panicked while holding the database lock |
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
