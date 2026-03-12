//! # highlandcows-isam
//!
//! An ISAM (Indexed Sequential Access Method) library with ACID transactions.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use highlandcows_isam::{Isam, Transaction};
//!
//! let db: Isam<String, String> = Isam::create("/tmp/mydb").unwrap();
//! let mut txn = db.begin_transaction().unwrap();
//! db.insert(&mut txn, "hello".to_string(), &"world".to_string()).unwrap();
//! let v = db.get(&mut txn, &"hello".to_string()).unwrap();
//! assert_eq!(v, Some("world".to_string()));
//! txn.commit().unwrap();
//! ```
//!
//! ## Files on disk
//!
//! | File       | Contents                              |
//! |------------|---------------------------------------|
//! | `*.idb`    | Append-only data records (bincode)    |
//! | `*.idx`    | On-disk B-tree index (page-based)     |

pub mod error;
pub mod index;
pub mod isam;
pub mod manager;
pub mod storage;
pub mod store;
pub mod transaction;

// Re-export the main types at the crate root for convenience.
pub use error::{IsamError, IsamResult};
pub use isam::{Isam, IsamIter, RangeIter};
pub use transaction::Transaction;
