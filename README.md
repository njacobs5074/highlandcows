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

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
highlandcows = "0.2.0"
```

Or, if you prefer to depend on the ISAM crate directly:

```toml
[dependencies]
highlandcows-isam = "0.2.0"
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
- **Single-user mode** — `as_single_user(timeout, |token, db| { ... })` lets one thread take exclusive access for administration; admin methods (`compact`, `migrate_*`) require the projected `&SingleUserToken`, enforcing correct usage at compile time

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

// Single-operation helpers — begin/commit/rollback handled automatically.
db.write(|txn| db.insert(txn, "alice".to_string(), &42))?;
db.write(|txn| db.insert(txn, "bob".to_string(), &99))?;

let val = db.read(|txn| db.get(txn, &"alice".to_string()))?; // Some(42)

// Multi-step writes use the same closure — commit on Ok, rollback on Err.
db.write(|txn| {
    db.update(txn, "alice".to_string(), &100)?;
    db.delete(txn, &"bob".to_string())?;
    Ok(())
})?;

// Iterate in key order
db.read(|txn| {
    for result in db.iter(txn)? {
        let (key, value) = result?;
        println!("{key} => {value}");
    }
    Ok(())
})?;

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

> **Note:** `compact()`, `migrate_values()`, `migrate_keys()`, `migrate_index()`,
> `key_schema_version()`, `val_schema_version()`, and `secondary_indices()` all acquire
> the database lock internally. They must not be called while a `Transaction` is live on
> the same thread, as this will deadlock. These are intended as offline administration
> operations — commit or roll back all open transactions before calling them.

### Single-user mode

`as_single_user` lets one thread take exclusive access to the database for administration operations such as compaction and index migration. While the closure is running, any other thread that calls any `Isam` operation on a clone of the same handle receives `IsamError::SingleUserMode` immediately — those threads are never blocked, they fail fast.

```rust
use highlandcows_isam::{Isam, DEFAULT_SINGLE_USER_TIMEOUT};

db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| {
    db.compact(token)?;
    Ok(())
})?;
```

`DEFAULT_SINGLE_USER_TIMEOUT` is exported at the crate root and equals 30 seconds. Pass a custom `Duration` if you need a shorter or longer window.

#### How it works

1. The exclusive flag is set atomically. From this point on, other threads fail immediately with `IsamError::SingleUserMode`.
2. The call then waits (spinning with 1 ms sleeps) for any in-flight transaction on another thread to finish and release the storage lock.
3. Once the lock is confirmed free, the closure runs with exclusive access.
4. When the closure returns — normally or via panic — the exclusive flag is cleared and other threads can operate again.

If step 2 does not complete within `timeout`, the flag is cleared and `IsamError::Timeout` is returned. The database is left fully operational.

#### What to run inside the closure

Single-user mode is intended for operations that must not run concurrently with reads or writes:

```rust
db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| {
    // Reclaim disk space from deleted/updated records.
    db.compact(token)?;

    // Rebuild a secondary index after updating the DeriveKey logic,
    // and record the migration with a version bump.
    db.migrate_index("city", 1, |mut u: User| {
        u.city = u.city.to_lowercase();
        Ok(u)
    }, token)?;

    Ok(())
})?;
```

Inside the closure you can call `write`, `read`, `begin_transaction`, and any of the offline administration methods (`compact`, `migrate_values`, `migrate_keys`, `migrate_index`). Normal CRUD operations work as usual; the exclusivity guarantee is enforced against other threads, not against the closure itself.

#### Caveats

- **Deadlock if you hold a transaction**: `as_single_user` waits for the storage lock to be free. If the calling thread already holds an open `Transaction`, the storage lock is already held by that same thread, so the spin will never succeed and the call will time out with `IsamError::Timeout`. Commit or roll back all open transactions on the calling thread before calling `as_single_user`.
- **Not re-entrant**: calling `as_single_user` again from inside the closure returns `IsamError::SingleUserMode`.
- **In-process only**: the exclusive flag is an in-memory atomic; it does not prevent access from a separate process opening the same database files.

### Secondary indices

A secondary index lets you look up records by a field other than the primary key.
Implement the `DeriveKey<V>` trait on a marker struct to describe what to index,
then register it via the builder when creating or opening the database.

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

// Register indices via the builder — must also be done on every open.
let db = Isam::<u64, User>::builder()
    .with_index("city", CityIndex)
    .create("/tmp/users")?;

let city_idx = db.index::<CityIndex>("city");

db.write(|txn| {
    db.insert(txn, 1, &User { name: "Alice".into(), city: "London".into() })?;
    db.insert(txn, 2, &User { name: "Bob".into(),   city: "London".into() })?;
    db.insert(txn, 3, &User { name: "Carol".into(), city: "Paris".into()  })?;
    Ok(())
})?;

// Look up all users in London.
let londoners = db.read(|txn| city_idx.lookup(txn, &"London".to_string()))?;
// → [(1, User{Alice, London}), (2, User{Bob, London})]
```

A few things to keep in mind:

- **Non-unique** — multiple records can share the same secondary key value.
- **Automatic maintenance** — `insert`, `update`, and `delete` keep all registered indices in sync.
- **Transactional** — secondary index changes are rolled back when a transaction rolls back.
- **Persistent** — index files survive process restarts; re-register the same indices on every `open`.
- **Composite indices** — not yet built in, but achievable by deriving a tuple key: `type Key = (String, u32)`.
- **Schema evolution** — use `migrate_index(name, version, f)` to rebuild a secondary index while bumping its `schema_version`.  The closure `f` transforms each primary value before `DeriveKey::derive` runs, letting you adapt the index to updated derivation logic.  Primary records are not modified.  For a plain rebuild without versioning, reopen with `builder.rebuild_index(name)` instead.

### API

```rust
// Lifecycle (no secondary indices)
Isam::create(path)          -> IsamResult<Self>
Isam::open(path)            -> IsamResult<Self>

// Lifecycle (with secondary indices)
Isam::builder()                              -> IsamBuilder<K, V>
builder.with_index(name, extractor)          -> IsamBuilder<K, V>
builder.rebuild_index(name)                  -> IsamBuilder<K, V>
builder.create(path)                         -> IsamResult<Isam<K, V>>
builder.open(path)                           -> IsamResult<Isam<K, V>>
db.index::<E>(name)                          -> SecondaryIndexHandle<K, V, E::Key>

// Transaction helpers (recommended for single operations)
db.write(|txn| { ... })     -> IsamResult<T>   // commits on Ok, rolls back on Err
db.read(|txn| { ... })      -> IsamResult<T>   // always rolls back

// Manual transaction control (for multi-step or fine-grained use)
db.begin_transaction()      -> IsamResult<Transaction<'_, K, V>>
txn.commit()                -> IsamResult<()>
txn.rollback()              -> IsamResult<()>
// drop(txn) also rolls back if not yet committed

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

// Secondary index lookup and inspection
handle.lookup(&mut txn, &sk)      -> IsamResult<Vec<(K, V)>>
db.secondary_indices()            -> IsamResult<Vec<IndexInfo>>

// Single-user mode
db.as_single_user(timeout, |token, db| { ... })  -> IsamResult<T>  // projects token + db clone into closure

// Offline administration (require a &SingleUserToken — must be called inside as_single_user)
db.compact(token)                      -> IsamResult<()>
db.key_schema_version()                -> IsamResult<u32>
db.val_schema_version()                -> IsamResult<u32>
db.migrate_values(version, f, token)   -> IsamResult<Isam<K, V2>>
db.migrate_keys(version, f, token)     -> IsamResult<Isam<K2, V>>
db.migrate_index(name, version, f, token) -> IsamResult<()>
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
| `IsamError::IndexNotFound(_)` | `migrate_index()` called with an unregistered index name |
| `IsamError::SingleUserMode` | a non-owner thread attempted an operation while single-user mode is active |
| `IsamError::Timeout` | an in-flight transaction did not finish within the timeout passed to `as_single_user` |

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
