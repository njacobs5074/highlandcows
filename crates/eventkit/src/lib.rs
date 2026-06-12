//! # highlandcows-eventkit
//!
//! A macOS-only Rust wrapper around Apple's EventKit framework, providing
//! CRUD access to the system Reminders database.
//!
//! ## Quick start
//!
//! ```ignore
//! use highlandcows_eventkit::{EventKitResult, ReminderStore};
//!
//! # fn main() -> EventKitResult<()> {
//! let store = ReminderStore::builder().connect()?;
//!
//! // Blocks until the user answers the system permission dialog (returns
//! // immediately if access was already granted or denied).
//! let token = store.authorize()?;
//!
//! for reminder in store.fetch_incomplete(None, &token)? {
//!     println!("{} (due {:?})", reminder.title, reminder.due_date);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Or use [`ReminderStore::with_access`] to bundle authorization and access
//! in one call (mirrors `Isam::read` / `Isam::write` from `highlandcows-isam`):
//!
//! ```ignore
//! # use highlandcows_eventkit::{EventKitResult, ReminderStore};
//! # fn main() -> EventKitResult<()> {
//! let store = ReminderStore::builder().connect()?;
//! let lists = store.with_access(|token, store| store.lists(token))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Authorization and capability tokens
//!
//! EventKit requires user consent before Reminders data can be read or
//! written. This crate enforces that at compile time: every CRUD method takes
//! a token ([`FullAccessToken`] or [`WriteOnlyToken`]) that can only be
//! obtained from [`ReminderStore::authorize`] or
//! [`ReminderStore::authorize_write_only`]. Code that skips authorization
//! does not compile.
//!
//! ## App requirements
//!
//! The host application's `Info.plist` must declare
//! `NSRemindersFullAccessUsageDescription`, or macOS will deny access without
//! showing a permission dialog. Plain command-line binaries inherit the TCC
//! identity of the terminal that launches them, so during development the
//! permission prompt names your terminal app.
//!
//! ## Threading
//!
//! [`ReminderStore`] is `Send + Sync` and cheap to clone — all clones share
//! one underlying `EKEventStore`. Authorization and fetch methods block the
//! calling thread while bridging EventKit's callback APIs, so call them off
//! the main thread in UI applications.

#![cfg(target_os = "macos")]

mod auth;
mod builder;
mod error;
mod inner;
mod list;
mod reminder;
mod store;
mod types;

pub use auth::{FullAccess, FullAccessToken, RemindersAccess, WriteOnlyToken};
pub use builder::ReminderStoreBuilder;
pub use error::{EventKitError, EventKitResult};
pub use list::ReminderList;
pub use reminder::Reminder;
pub use store::ReminderStore;
pub use types::{EkAuthStatus, EkEntityType};
