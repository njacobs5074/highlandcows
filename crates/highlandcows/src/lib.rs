//! # highlandcows
//!
//! Umbrella crate that re-exports all Highlandcows libraries.
//!
//! ## Available types
//!
//! | Import | Description |
//! |--------|-------------|
//! | `highlandcows::Isam` | Persistent ISAM key/value store |

pub use highlandcows_isam::{Isam, IsamError, IsamIter, IsamResult};
