//! # highlandcows-isam
//!
//! An ISAM (Indexed Sequential Access Method) library with ACID transactions
//! and optional secondary indices.
//!
//! ## Quick start
//!
//! Use the [`write`](Isam::write) and [`read`](Isam::read) helpers for simple
//! single-operation transactions:
//!
//! ```
//! # use tempfile::TempDir;
//! use highlandcows_isam::Isam;
//!
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("db");
//! let db: Isam<String, String> = Isam::create(&path).unwrap();
//! db.write(|txn| db.insert(txn, "hello".to_string(), &"world".to_string())).unwrap();
//! let v = db.read(|txn| db.get(txn, &"hello".to_string())).unwrap();
//! assert_eq!(v, Some("world".to_string()));
//! ```
//!
//! For multi-operation transactions, use [`begin_transaction`](Isam::begin_transaction) directly:
//!
//! ```
//! # use tempfile::TempDir;
//! use highlandcows_isam::Isam;
//!
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("db");
//! # let db: Isam<String, String> = Isam::create(&path).unwrap();
//! let mut txn = db.begin_transaction().unwrap();
//! db.insert(&mut txn, "a".to_string(), &"1".to_string()).unwrap();
//! db.insert(&mut txn, "b".to_string(), &"2".to_string()).unwrap();
//! txn.commit().unwrap();
//! ```
//!
//! ## Secondary indices
//!
//! Secondary indices let you look up records by a field other than the primary
//! key.  Implement [`DeriveKey`] on a marker struct, then register it via
//! [`Isam::builder`] when creating or opening the database.
//!
//! ```
//! # use tempfile::TempDir;
//! use serde::{Serialize, Deserialize};
//! use highlandcows_isam::{Isam, DeriveKey};
//!
//! #[derive(Serialize, Deserialize, Clone)]
//! struct User { name: String, city: String }
//!
//! struct CityIndex;
//! impl DeriveKey<User> for CityIndex {
//!     type Key = String;
//!     fn derive(u: &User) -> String { u.city.clone() }
//! }
//!
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("users");
//! let db = Isam::<u64, User>::builder()
//!     .with_index("city", CityIndex)
//!     .create(&path)
//!     .unwrap();
//! let city_idx = db.index::<CityIndex>("city");
//!
//! db.write(|txn| {
//!     db.insert(txn, 1, &User { name: "Alice".into(), city: "London".into() })?;
//!     db.insert(txn, 2, &User { name: "Bob".into(),   city: "London".into() })?;
//!     db.insert(txn, 3, &User { name: "Carol".into(), city: "Paris".into()  })
//! }).unwrap();
//!
//! let londoners = db.read(|txn| city_idx.lookup(txn, &"London".to_string())).unwrap();
//! assert_eq!(londoners.len(), 2);
//! ```
//!
//! ### Inspecting registered indices, rebuilding, and migrating
//!
//! Use [`Isam::secondary_indices`] to list the indices registered on an open
//! database.  Each [`IndexInfo`] entry includes the index name, the
//! fully-qualified extractor type name, and a `schema_version` that reflects
//! the last [`migrate_index`](Isam::migrate_index) call.
//!
//! To rebuild a stale index from primary data without versioning — for example
//! after the [`DeriveKey`] extractor logic has changed — drop the current handle
//! and reopen with [`IsamBuilder::rebuild_index`]:
//!
//! ```
//! # use tempfile::TempDir;
//! # use serde::{Serialize, Deserialize};
//! # use highlandcows_isam::{Isam, DeriveKey};
//! # #[derive(Serialize, Deserialize, Clone)]
//! # struct User { name: String, city: String }
//! # struct CityIndex;
//! # impl DeriveKey<User> for CityIndex {
//! #     type Key = String;
//! #     fn derive(u: &User) -> String { u.city.clone() }
//! # }
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("users");
//! # Isam::<u64, User>::builder().with_index("city", CityIndex).create(&path).unwrap();
//! // Inspect which indices are registered.
//! let indices = {
//!     let db = Isam::<u64, User>::builder()
//!         .with_index("city", CityIndex)
//!         .open(&path)
//!         .unwrap();
//!     db.secondary_indices().unwrap()
//!     // db is fully dropped here — all file handles released.
//! };
//! assert_eq!(indices[0].name, "city");
//!
//! // Reopen, forcing a full rebuild of the "city" index.
//! let db = Isam::<u64, User>::builder()
//!     .with_index("city", CityIndex)
//!     .rebuild_index("city")
//!     .open(&path)
//!     .unwrap();
//! ```
//!
//! To migrate a secondary index with a version bump — for instance when the
//! derivation logic changes and you want to record that the migration was
//! applied — use [`Isam::migrate_index`] on a live database handle.  Pass a closure that
//! transforms each primary value before [`DeriveKey::derive`] runs; pass the
//! identity closure (`|v| Ok(v)`) for a plain rebuild.  Primary records are
//! not modified.
//!
//! ```
//! # use tempfile::TempDir;
//! # use serde::{Serialize, Deserialize};
//! # use highlandcows_isam::{Isam, DeriveKey, DEFAULT_SINGLE_USER_TIMEOUT};
//! # #[derive(Serialize, Deserialize, Clone)]
//! # struct User { name: String, city: String }
//! # struct CityIndex;
//! # impl DeriveKey<User> for CityIndex {
//! #     type Key = String;
//! #     // derive now normalizes to lowercase
//! #     fn derive(u: &User) -> String { u.city.to_lowercase() }
//! # }
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("users");
//! # let db = Isam::<u64, User>::builder().with_index("city", CityIndex).create(&path).unwrap();
//! # db.write(|txn| db.insert(txn, 1u64, &User { name: "Alice".into(), city: "London".into() })).unwrap();
//! // Rebuild the city index, normalizing city names to lowercase so the
//! // on-disk data matches the updated DeriveKey logic.  Bumps schema_version to 1.
//! db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| {
//!     db.migrate_index("city", 1, |mut u: User| {
//!         u.city = u.city.to_lowercase();
//!         Ok(u)
//!     }, token)
//! }).unwrap();
//!
//! let info = db.secondary_indices().unwrap();
//! assert_eq!(info[0].schema_version, 1);
//! ```
//!
//! ## Single-user mode
//!
//! [`Isam::as_single_user`] lets one thread take exclusive access to the
//! database for administration operations such as compaction and index
//! migration.  While the closure is running, any other thread that calls any
//! [`Isam`] operation on a clone of the same handle receives
//! [`IsamError::SingleUserMode`] immediately — those threads are never
//! blocked, they fail fast.
//!
//! ```
//! # use tempfile::TempDir;
//! use highlandcows_isam::{Isam, DEFAULT_SINGLE_USER_TIMEOUT};
//!
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("db");
//! # let db: Isam<u32, String> = Isam::create(&path).unwrap();
//! db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| db.compact(token)).unwrap();
//! ```
//!
//! [`DEFAULT_SINGLE_USER_TIMEOUT`] is 30 seconds.  Pass a custom
//! [`std::time::Duration`] if you need a shorter or longer window.
//!
//! ### How it works
//!
//! 1. The exclusive flag is set atomically.  From this point on, other threads
//!    fail immediately with [`IsamError::SingleUserMode`].
//! 2. The call waits (spinning with 1 ms sleeps) for any in-flight transaction
//!    on another thread to finish and release the storage lock.
//! 3. Once the lock is confirmed free, the closure runs with exclusive access.
//! 4. When the closure returns — normally or via panic — the exclusive flag is
//!    cleared and other threads can operate again.
//!
//! If step 2 does not complete within `timeout`, the flag is cleared and
//! [`IsamError::Timeout`] is returned.  The database is left fully operational.
//!
//! ### What to run inside the closure
//!
//! Single-user mode is intended for operations that must not run concurrently
//! with reads or writes:
//!
//! ```
//! # use tempfile::TempDir;
//! # use serde::{Serialize, Deserialize};
//! # use highlandcows_isam::{Isam, DeriveKey, DEFAULT_SINGLE_USER_TIMEOUT};
//! # #[derive(Serialize, Deserialize, Clone)]
//! # struct User { name: String, city: String }
//! # struct CityIndex;
//! # impl DeriveKey<User> for CityIndex {
//! #     type Key = String;
//! #     fn derive(u: &User) -> String { u.city.to_lowercase() }
//! # }
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("db");
//! # let db = Isam::<u64, User>::builder().with_index("city", CityIndex).create(&path).unwrap();
//! # db.write(|txn| db.insert(txn, 1u64, &User { name: "Alice".into(), city: "London".into() })).unwrap();
//! db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, |token, db| {
//!     // Reclaim disk space from deleted/updated records.
//!     db.compact(token)?;
//!     // Rebuild a secondary index after updating the DeriveKey logic.
//!     db.migrate_index("city", 1, |mut u: User| {
//!         u.city = u.city.to_lowercase();
//!         Ok(u)
//!     }, token)?;
//!     Ok(())
//! }).unwrap();
//! ```
//!
//! Inside the closure you can call [`Isam::write`], [`Isam::read`],
//! [`Isam::begin_transaction`], and any of the offline administration methods
//! ([`Isam::compact`], [`Isam::migrate_values`], [`Isam::migrate_keys`],
//! [`Isam::migrate_index`]).
//!
//! ### Caveats
//!
//! - **Deadlock if you hold a transaction**: `as_single_user` waits for the
//!   storage lock to be free.  If the calling thread already holds an open
//!   [`Transaction`], the storage lock is already taken, so the spin will
//!   never succeed and the call will time out.  Commit or roll back all open
//!   transactions on the calling thread before calling `as_single_user`.
//! - **Not re-entrant**: calling `as_single_user` again from inside the
//!   closure returns [`IsamError::SingleUserMode`].
//! - **In-process only**: the exclusive flag is an in-memory atomic; it does
//!   not prevent access from a separate process opening the same database
//!   files.
//!
//! ## Files on disk
//!
//! | File                  | Contents                                       |
//! |-----------------------|------------------------------------------------|
//! | `*.idb`               | Append-only data records (bincode)             |
//! | `*.idx`               | On-disk B-tree index (page-based)              |
//! | `*_<name>.sidb`       | Secondary index data store (one per index)     |
//! | `*_<name>.sidx`       | Secondary index B-tree (one per index)         |

pub mod error;
pub mod index;
pub mod isam;
pub mod manager;
pub mod secondary_index;
pub mod storage;
pub mod store;
pub mod transaction;

// Re-export the main types at the crate root for convenience.
pub use error::{IsamError, IsamResult};
pub use isam::{IndexInfo, Isam, IsamBuilder, IsamIter, RangeIter, SecondaryIndexHandle, SingleUserToken, DEFAULT_SINGLE_USER_TIMEOUT};
pub use secondary_index::DeriveKey;
pub use transaction::Transaction;
