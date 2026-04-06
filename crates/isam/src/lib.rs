//! # highlandcows-isam
//!
//! An ISAM (Indexed Sequential Access Method) library with ACID transactions
//! and optional secondary indices.
//!
//! ## Quick start
//!
//! ```
//! # use tempfile::TempDir;
//! use highlandcows_isam::Isam;
//!
//! # let dir = TempDir::new().unwrap();
//! # let path = dir.path().join("db");
//! let db: Isam<String, String> = Isam::create(&path).unwrap();
//! let mut txn = db.begin_transaction().unwrap();
//! db.insert(&mut txn, "hello".to_string(), &"world".to_string()).unwrap();
//! let v = db.get(&mut txn, &"hello".to_string()).unwrap();
//! assert_eq!(v, Some("world".to_string()));
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
//! let mut txn = db.begin_transaction().unwrap();
//! db.insert(&mut txn, 1, &User { name: "Alice".into(), city: "London".into() }).unwrap();
//! db.insert(&mut txn, 2, &User { name: "Bob".into(),   city: "London".into() }).unwrap();
//! db.insert(&mut txn, 3, &User { name: "Carol".into(), city: "Paris".into()  }).unwrap();
//! txn.commit().unwrap();
//!
//! let mut txn = db.begin_transaction().unwrap();
//! let londoners = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
//! assert_eq!(londoners.len(), 2);
//! txn.commit().unwrap();
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
pub use isam::{Isam, IsamBuilder, IsamIter, RangeIter, SecondaryIndexHandle};
pub use secondary_index::DeriveKey;
pub use transaction::Transaction;
