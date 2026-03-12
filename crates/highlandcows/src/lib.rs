//! # highlandcows
//!
//! Umbrella crate that re-exports all Highlandcows libraries.
//!
//! ## Available types
//!
//! | Import | Description |
//! |--------|-------------|
//! | `highlandcows::Isam` | Persistent ISAM key/value store |
//! | `highlandcows::Transaction` | ACID transaction handle |

pub use highlandcows_isam::{Isam, IsamError, IsamIter, IsamResult, RangeIter, Transaction};
