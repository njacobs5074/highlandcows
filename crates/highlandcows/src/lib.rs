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
//! | `highlandcows::SingleUserToken` | Capability token required by admin methods (`compact`, `migrate_*`) |
//! | `highlandcows::DEFAULT_SINGLE_USER_TIMEOUT` | Default 30-second timeout for `as_single_user` |
//! | `highlandcows::eventkit::ReminderStore` | macOS Reminders CRUD via EventKit (macOS only) |

pub use highlandcows_isam::{
    Isam, IsamError, IsamIter, IsamResult, RangeIter, SingleUserToken, Transaction,
    DEFAULT_SINGLE_USER_TIMEOUT,
};

/// Re-exports of [`highlandcows_eventkit`] for macOS Reminders access.
#[cfg(target_os = "macos")]
pub mod eventkit {
    pub use highlandcows_eventkit::{
        EkAuthStatus, EkEntityType, EventKitError, EventKitResult, FullAccess, FullAccessToken,
        Reminder, ReminderList, ReminderStore, ReminderStoreBuilder, RemindersAccess,
        WriteOnlyToken,
    };
}
