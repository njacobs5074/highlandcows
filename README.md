# highlandcows

![Build & Tests (x86-64)](https://github.com/njacobs5074/highlandcows/actions/workflows/rust-x86.yml/badge.svg?branch=main)
![Build & Tests (ARM64)](https://github.com/njacobs5074/highlandcows/actions/workflows/rust-arm.yml/badge.svg?branch=main)
![Build & Tests (macOS)](https://github.com/njacobs5074/highlandcows/actions/workflows/rust-macos.yml/badge.svg?branch=main)
[![dependency status](https://deps.rs/repo/github/njacobs5074/highlandcows/status.svg)](https://deps.rs/repo/github/njacobs5074/highlandcows)

A Cargo workspace of Rust libraries published under the `highlandcows` umbrella crate.

> **Created with [Claude Code](https://claude.ai/code) by Anthropic.**

---

## Crates

| Crate | Description |
|-------|-------------|
| [`highlandcows-isam`](crates/isam/) | Persistent ISAM key/value store backed by an on-disk B-tree |
| [`highlandcows-eventkit`](crates/eventkit/) | macOS-only Rust wrapper for Apple's EventKit — full CRUD for Reminders and Calendar events |

---

## Usage

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
highlandcows = "0.5.8"
```

Or, if you prefer to depend on the ISAM crate directly:

```toml
[dependencies]
highlandcows-isam = "0.5.8"
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

let db = db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| {
    db.compact(token)?;
    Ok(db)
})?;
```

When a migration changes the value type, return the *new* handle from the closure rather than the original `db`:

```rust
// db is Isam<u32, String>; migrate to Isam<u32, Vec<u8>>.
let db: Isam<u32, Vec<u8>> =
    db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| {
        db.migrate_values(1, |s: String| Ok(s.into_bytes()), token)
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
let db = db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| {
    // Reclaim disk space from deleted/updated records.
    db.compact(token)?;

    // Rebuild a secondary index after updating the DeriveKey logic,
    // and record the migration with a version bump.
    db.migrate_index("city", 1, |mut u: User| {
        u.city = u.city.to_lowercase();
        Ok(u)
    }, token)?;

    Ok(db)
})?;
```

Inside the closure you can call `write`, `read`, `begin_transaction`, and any of the offline administration methods (`compact`, `migrate_values`, `migrate_keys`, `migrate_index`). Normal CRUD operations work as usual; the exclusivity guarantee is enforced against other threads, not against the closure itself.

#### Caveats

- **Consumes `self`**: `as_single_user` takes ownership of the handle. Return `db` from the closure (`Ok(db)`) to keep using it afterward. On error, the handle is dropped — clone before calling if you need to retry on failure.
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
db.as_single_user(timeout, |token, db| { ... })  -> IsamResult<T>  // consumes db; return it from closure to keep using it

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

## highlandcows-eventkit

> **macOS only.** The crate is compiled with `#![cfg(target_os = "macos")]` and produces no output on other platforms.

A Rust wrapper around Apple's EventKit framework providing full CRUD access to both the system Reminders and Calendar databases.

### Features

- **Two domains, one framework** — `ReminderStore` for tasks, `CalendarStore` for calendar events; both share one underlying `EKEventStore`
- **Compile-time authorization enforcement** — every CRUD method requires a capability token obtained from `authorize()`. Code that skips authorization does not compile
- **Blocking authorization** — bridges EventKit's async callback over `std::sync::mpsc`; the calling thread blocks until the system permission dialog is dismissed (or permission was already decided)
- **Full Reminder CRUD** — `fetch`, `fetch_all`, `fetch_incomplete`, `save`, `remove`
- **Full Calendar CRUD** — `fetch`, `fetch_in_range`, `save`, `remove`; `fetch_in_range` is synchronous (no callback bridge needed)
- **List/calendar enumeration** — `lists`, `default_list` (Reminders); `lists`, `default_calendar` (Calendar)
- **Reminder list management** — `create_list` (in a named source), `remove_list`
- **Source enumeration** — `sources`, `default_source` expose the account sources (iCloud, On My Mac, …) that contain Reminder lists
- **Cloneable handles** — `ReminderStore` and `CalendarStore` are `Clone + Send + Sync`; all clones share one underlying `EKEventStore`
- **`with_access` closure helper** — bundles authorization and access in one call, mirroring `Isam::read` / `Isam::write`

### Installation

```toml
[dependencies]
highlandcows-eventkit = "0.5.8"
```

### Quick start — Reminders

```rust
use highlandcows_eventkit::{EventKitResult, ReminderStore};

fn main() -> EventKitResult<()> {
    let store = ReminderStore::builder().connect()?;

    // Blocks until the user answers the system permission dialog (returns
    // immediately if access was already granted or denied).
    let token = store.authorize()?;

    for reminder in store.fetch_incomplete(None, &token)? {
        println!("{} (due {:?})", reminder.title, reminder.due_date);
    }
    Ok(())
}
```

### Quick start — Calendar

```rust
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Duration, Utc};
use highlandcows_eventkit::{CalendarStore, EventKitResult};

fn main() -> EventKitResult<()> {
    let store = CalendarStore::builder().connect()?;
    let token = store.authorize()?;

    // chrono is built without the `clock` feature; use SystemTime for the current time.
    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let end: DateTime<Utc> = DateTime::from_timestamp(now_secs, 0).unwrap();
    let start = end - Duration::days(7);

    for event in store.fetch_in_range(start, end, None, &token)? {
        println!("{} ({:?} – {:?})", event.title, event.start_date, event.end_date);
    }
    Ok(())
}
```

Or use `with_access` to bundle authorization and access in one call (works for both stores):

```rust
let store = CalendarStore::builder().connect()?;
let calendars = store.with_access(|token, store| store.lists(token))?;
```

### Authorization and capability tokens

EventKit requires user consent before data can be read or written. This crate enforces that at compile time: every CRUD method takes a token that can only be obtained from an `authorize` method.

| Token | Grants | Obtained from |
|-------|--------|---------------|
| `FullAccessToken` | Reminders read + write | `ReminderStore::authorize()` |
| `WriteOnlyToken` | Reminders write only | `ReminderStore::authorize_write_only()` |
| `CalendarFullAccessToken` | Calendar read + write | `CalendarStore::authorize()` |
| `CalendarWriteOnlyToken` | Calendar write only | `CalendarStore::authorize_write_only()` |

The sealed trait hierarchy (`RemindersAccess` / `FullAccess` for Reminders; `CalendarAccess` / `CalendarFullAccess` for Calendar) lets write methods accept either token while fetch methods require the full-access variant.

Unlike Reminders, Calendar events have a genuine write-only authorization mode in EventKit: `CalendarWriteOnlyToken` permits creating and modifying events without granting read access. For Reminders, `authorize_write_only()` delegates to full access internally (EventKit does not distinguish the two for Reminders).

### App requirements

The host application's `Info.plist` must declare the appropriate usage description key, or macOS will deny access without showing a permission dialog:

| Domain | Info.plist key |
|--------|----------------|
| Reminders (full) | `NSRemindersFullAccessUsageDescription` |
| Calendar (full) | `NSCalendarsFullAccessUsageDescription` |
| Calendar (write-only) | `NSCalendarsWriteOnlyAccessUsageDescription` |

Plain command-line binaries inherit the TCC identity of the terminal that launches them, so during development the permission prompt names your terminal app (Terminal, iTerm2, etc.).

### API — Reminders

| Method | Description |
|--------|-------------|
| `ReminderStore::builder()` | Create a `ReminderStoreBuilder` |
| `ReminderStoreBuilder::connect()` | Connect to the system Reminders database |
| `ReminderStore::authorization_status()` | Query current authorization without prompting |
| `ReminderStore::authorize()` | Request full access (blocking) — returns `FullAccessToken` |
| `ReminderStore::authorize_write_only()` | Request write-only access — returns `WriteOnlyToken` |
| `ReminderStore::with_access(f)` | Authorize then run a closure with the token |
| `ReminderStore::fetch(id, &token)` | Fetch one reminder by stable ID |
| `ReminderStore::fetch_all(lists, &token)` | Fetch all reminders, optionally filtered to specific lists |
| `ReminderStore::fetch_incomplete(lists, &token)` | Fetch only incomplete (not-yet-done) reminders |
| `ReminderStore::save(&reminder, &token)` | Create or update a reminder; returns the stable ID |
| `ReminderStore::remove(id, &token)` | Delete a reminder by stable ID |
| `ReminderStore::lists(&token)` | Return all Reminder lists visible to this store |
| `ReminderStore::default_list(&token)` | Return the default list for new reminders |
| `ReminderStore::create_list(title, source_id, &token)` | Create a new Reminder list in the given source; returns the created list (tested with iCloud sources only) |
| `ReminderStore::remove_list(id, &token)` | Delete a Reminder list by its identifier |
| `ReminderStore::sources(&token)` | Return all account sources (iCloud, On My Mac, …) visible to this store |
| `ReminderStore::default_source(&token)` | Return the source that owns the system default Reminders list |

### API — Calendar

| Method | Description |
|--------|-------------|
| `CalendarStore::builder()` | Create a `CalendarStoreBuilder` |
| `CalendarStoreBuilder::connect()` | Connect to the system Calendar database |
| `CalendarStore::authorization_status()` | Query current authorization without prompting |
| `CalendarStore::authorize()` | Request full access (blocking) — returns `CalendarFullAccessToken` |
| `CalendarStore::authorize_write_only()` | Request write-only access — returns `CalendarWriteOnlyToken` |
| `CalendarStore::with_access(f)` | Authorize then run a closure with the token |
| `CalendarStore::fetch(id, &token)` | Fetch one event by stable ID |
| `CalendarStore::fetch_in_range(start, end, calendars, &token)` | Fetch all events in a date range (synchronous) |
| `CalendarStore::save(&event, &token)` | Create or update an event; returns the stable ID |
| `CalendarStore::remove(id, &token)` | Delete an event by stable ID |
| `CalendarStore::lists(&token)` | Return all Calendars visible to this store |
| `CalendarStore::default_calendar(&token)` | Return the default calendar for new events |

### Testing

Most tests run automatically with `cargo test -p highlandcows-eventkit`. Tests that interact with the live Reminders or Calendar database require TCC authorization and are marked `#[ignore]` so they are skipped by default. To run them locally:

1. Grant **Reminders** and **Calendar** access to your terminal in **System Settings → Privacy & Security**.
2. Run:

```sh
cargo test -p highlandcows-eventkit -- --ignored
```

These tests create and delete real reminders and events in your system databases. They are not run in CI.

### Error types

| Variant | When |
|---------|------|
| `EventKitError::AuthorizationDenied` | The user denied access, or the required `Info.plist` key is missing |
| `EventKitError::AuthorizationRestricted` | System policy prevents access (parental controls, MDM) |
| `EventKitError::AuthorizationNotDetermined` | `authorize()` was not called before a CRUD method |
| `EventKitError::ReminderNotFound(id)` | A reminder with the given ID was not found |
| `EventKitError::EventNotFound(id)` | A calendar event with the given ID was not found |
| `EventKitError::ListNotFound(id)` | A reminder list identifier resolved to nothing |
| `EventKitError::SourceNotFound(id)` | A source identifier passed to `create_list` resolved to nothing |
| `EventKitError::CalendarNotFound(id)` | A calendar identifier resolved to nothing |
| `EventKitError::SaveFailed(msg)` | EventKit rejected the save; message from `NSError.localizedDescription` |
| `EventKitError::RemoveFailed(msg)` | EventKit rejected the remove |
| `EventKitError::Framework(msg)` | Internal framework error (e.g., callback channel dropped) |
| `EventKitError::LockPoisoned` | A thread panicked while holding the store lock |

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
│   ├── isam/                   # highlandcows-isam implementation
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   └── tests/
│   └── eventkit/               # highlandcows-eventkit (macOS only)
│       ├── Cargo.toml
│       ├── src/
│       └── tests/
├── README.md
└── LICENSE
```

---

## Dependencies

**highlandcows-isam:**

| Crate | Purpose |
|-------|---------|
| [`serde`](https://crates.io/crates/serde) | Serialization framework |
| [`bincode`](https://crates.io/crates/bincode) 1.x | Compact binary encoding |
| [`thiserror`](https://crates.io/crates/thiserror) | Ergonomic error type derivation |

**highlandcows-eventkit (macOS only):**

| Crate | Purpose |
|-------|---------|
| [`objc2`](https://crates.io/crates/objc2) | Rust bindings to the Objective-C runtime |
| [`objc2-event-kit`](https://crates.io/crates/objc2-event-kit) | Generated bindings for Apple's EventKit framework |
| [`objc2-foundation`](https://crates.io/crates/objc2-foundation) | Generated bindings for the Foundation framework |
| [`block2`](https://crates.io/crates/block2) | Rust bindings to Objective-C blocks |
| [`chrono`](https://crates.io/crates/chrono) | Date and time handling (NSDate ↔ DateTime<Utc> conversion) |
| [`thiserror`](https://crates.io/crates/thiserror) | Ergonomic error type derivation |

---

## Developer Notes

### Prerequisites

Rust 1.70 or later. Install via [rustup](https://rustup.rs) if needed.

### Daily development

Run these from the workspace root:

```sh
cargo build        # build all crates
cargo test         # run all tests
cargo clippy       # lint
cargo fmt          # format
```

### Branching

For any non-trivial change, create a branch before editing. Follow conventional-commit naming:

| Type | Example branch |
|------|----------------|
| Feature | `feat/range-search` |
| Bug fix | `fix/leaf-merge-underflow` |
| Refactor | `refactor/btree-ordering` |
| Docs | `docs/readme-api-section` |
| Chore | `chore/update-dependencies` |

Typo fixes and trivial comment edits may go directly on the current branch.

### Security

CI runs two supply-chain checks on every push and pull request:

- **`cargo audit`** — scans `Cargo.lock` against the [RustSec advisory database](https://rustsec.org) and fails on any known vulnerability
- **`cargo deny`** — enforces allowed dependency licenses and blocks crates listed in security advisories; configuration is in [`deny.toml`](deny.toml)

**GitHub Actions** are pinned to full commit SHAs rather than mutable version tags (e.g. `actions/checkout@<sha>  # v5`). This prevents a compromised tag from injecting malicious code into CI. When upgrading an action, resolve the new commit SHA and update both the SHA and the comment tag.

**Known advisory exceptions** — `bincode` 1.x is flagged as unmaintained (`RUSTSEC-2025-0141`) and is explicitly ignored in `deny.toml`. Bincode 1.3.3 is the final stable release; the maintainers stated it is complete. Migration to an alternative serialization library is a separate workstream.

### Releasing a new version

Use the release script to bump versions, commit, and tag atomically:

```sh
./scripts/release.sh 0.4.0
```

This patches the version in both `Cargo.toml` files and the `README.md` usage examples,
runs `cargo check` to validate the workspace compiles, commits all changes, and creates
an annotated `v0.4.0` tag. Then push:

```sh
git push && git push origin v0.4.0
```

The `publish.yml` CI workflow will verify all version strings match the tag before
publishing to crates.io, and will fail with a diagnostic if anything is out of sync.

---

## License

MIT — see [LICENSE](LICENSE).
