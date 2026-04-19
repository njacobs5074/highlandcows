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
//! To migrate an index with a version bump — for instance when the derivation
//! logic changes and you want to record that the migration was applied — use
//! [`Isam::migrate_index`] on a live database handle.  Pass a closure that
//! transforms each primary value before [`DeriveKey::derive`] runs; pass the
//! identity closure (`|v| Ok(v)`) for a plain rebuild.  Primary records are
//! not modified.
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
//! #     // derive now normalizes to lowercase
//! #     fn derive(u: &User) -> String { u.city.to_lowercase() }
//! # }
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("users");
//! # let db = Isam::<u64, User>::builder().with_index("city", CityIndex).create(&path).unwrap();
//! # db.write(|txn| db.insert(txn, 1u64, &User { name: "Alice".into(), city: "London".into() })).unwrap();
//! // Rebuild the city index, normalizing city names to lowercase so the
//! // on-disk data matches the updated DeriveKey logic.  Bumps schema_version to 1.
//! db.migrate_index("city", 1, |mut u: User| {
//!     u.city = u.city.to_lowercase();
//!     Ok(u)
//! }).unwrap();
//!
//! let info = db.secondary_indices().unwrap();
//! assert_eq!(info[0].schema_version, 1);
//! ```
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
pub use isam::{IndexInfo, Isam, IsamBuilder, IsamIter, RangeIter, SecondaryIndexHandle};
pub use secondary_index::DeriveKey;
pub use transaction::Transaction;
