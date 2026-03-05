//! # rust-isam
//!
//! An ISAM (Indexed Sequential Access Method) library that stores records
//! indexed by a user-supplied key type and persists them to local files.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use rust_isam::Isam;
//!
//! let mut db: Isam<String, String> = Isam::create("/tmp/mydb").unwrap();
//! db.insert("hello".to_string(), &"world".to_string()).unwrap();
//! let v = db.get(&"hello".to_string()).unwrap();
//! assert_eq!(v, Some("world".to_string()));
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
pub mod store;

// Re-export the main types at the crate root for convenience.
pub use error::{IsamError, IsamResult};
pub use isam::{Isam, IsamIter};
