//! # highlandcows-eventkit
//!
//! A macOS-only Rust wrapper around Apple's EventKit framework, providing
//! access to the system Reminders and Calendar databases.
//!
//! | Type | Domain |
//! |------|--------|
//! | [`ReminderStore`] | Reminders (tasks) |
//! | [`CalendarStore`] | Calendar (events) |
//!
//! ## Quick start — Reminders
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
//! ## Quick start — Calendar
//!
//! ```ignore
//! use chrono::{DateTime, Duration, Utc};
//! use highlandcows_eventkit::{CalendarStore, EventKitResult};
//!
//! # fn main() -> EventKitResult<()> {
//! let store = CalendarStore::builder().connect()?;
//! let token = store.authorize()?;
//!
//! let end = Utc::now();
//! let start = end - Duration::days(7);
//! for event in store.fetch_in_range(start, end, None, &token)? {
//!     println!("{} ({:?} – {:?})", event.title, event.start_date, event.end_date);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Both stores support a `with_access` convenience method that bundles
//! authorization and access in one call:
//!
//! ```ignore
//! # use highlandcows_eventkit::{CalendarStore, EventKitResult};
//! # fn main() -> EventKitResult<()> {
//! let store = CalendarStore::builder().connect()?;
//! let calendars = store.with_access(|token, store| store.lists(token))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Authorization and capability tokens
//!
//! EventKit requires user consent before data can be read or written. This
//! crate enforces that at compile time: every CRUD method takes a token that
//! can only be obtained from an `authorize` method. Code that skips
//! authorization does not compile.
//!
//! | Token | Grants | Obtained from |
//! |-------|--------|---------------|
//! | [`FullAccessToken`] | Reminders read + write | [`ReminderStore::authorize`] |
//! | [`WriteOnlyToken`] | Reminders write only | [`ReminderStore::authorize_write_only`] |
//! | [`CalendarFullAccessToken`] | Calendar read + write | [`CalendarStore::authorize`] |
//! | [`CalendarWriteOnlyToken`] | Calendar write only | [`CalendarStore::authorize_write_only`] |
//!
//! Unlike Reminders, Calendar events have a genuine write-only authorization
//! mode in EventKit: [`CalendarWriteOnlyToken`] permits creating and modifying
//! events without granting read access.
//!
//! ## App requirements
//!
//! The host application's `Info.plist` must declare the appropriate usage
//! description key, or macOS will deny access without showing a permission
//! dialog:
//!
//! - Reminders: `NSRemindersFullAccessUsageDescription`
//! - Calendar: `NSCalendarsFullAccessUsageDescription` (full access) and/or
//!   `NSCalendarsWriteOnlyAccessUsageDescription` (write-only)
//!
//! Plain command-line binaries inherit the TCC identity of the terminal that
//! launches them, so during development the permission prompt names your
//! terminal app.
//!
//! ## Threading
//!
//! Both [`ReminderStore`] and [`CalendarStore`] are `Send + Sync` and cheap
//! to clone — all clones share one underlying `EKEventStore`. Authorization
//! and reminder-fetch methods block the calling thread while bridging
//! EventKit's callback APIs; calendar event fetches (`fetch_in_range`) are
//! synchronous and do not use callbacks. Call these off the main thread in UI
//! applications.

#![cfg(target_os = "macos")]

mod auth;
mod builder;
mod calendar;
mod calendar_builder;
mod calendar_event;
mod calendar_store;
mod date_util;
mod error;
mod inner;
mod list;
mod reminder;
mod store;
mod types;

pub use auth::{
    CalendarAccess, CalendarFullAccess, CalendarFullAccessToken, CalendarWriteOnlyToken,
    FullAccess, FullAccessToken, RemindersAccess, WriteOnlyToken,
};
pub use builder::ReminderStoreBuilder;
pub use calendar::Calendar;
pub use calendar_builder::CalendarStoreBuilder;
pub use calendar_event::CalendarEvent;
pub use calendar_store::CalendarStore;
pub use error::{EventKitError, EventKitResult};
pub use list::ReminderList;
pub use reminder::Reminder;
pub use store::ReminderStore;
pub use types::{EkAuthStatus, EkEntityType};
