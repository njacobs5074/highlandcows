# highlandcows-eventkit

![Build & Tests (macOS)](https://github.com/njacobs5074/highlandcows/actions/workflows/rust-macos.yml/badge.svg?branch=main)

> **macOS only.** The crate is compiled with `#![cfg(target_os = "macos")]` and produces no output on other platforms.

A Rust wrapper around Apple's EventKit framework providing full CRUD access to both the system Reminders and Calendar databases.

> **Created with [Claude Code](https://claude.ai/code) by Anthropic.**

## Installation

```toml
[dependencies]
highlandcows-eventkit = "0.5.7"
```

---

## Features

- **Two domains, one framework** — `ReminderStore` for tasks, `CalendarStore` for calendar events; both share one underlying `EKEventStore`
- **Compile-time authorization enforcement** — every CRUD method requires a capability token obtained from `authorize()`. Code that skips authorization does not compile
- **Blocking authorization** — bridges EventKit's async callback over `std::sync::mpsc`; the calling thread blocks until the system permission dialog is dismissed (or permission was already decided)
- **Full Reminder CRUD** — `fetch`, `fetch_all`, `fetch_incomplete`, `save`, `remove`
- **Full Calendar CRUD** — `fetch`, `fetch_in_range`, `save`, `remove`; `fetch_in_range` is synchronous (no callback bridge needed)
- **List/calendar enumeration** — `lists`, `default_list` (Reminders); `lists`, `default_calendar` (Calendar)
- **Reminder list management** — `create_list` (in a named source), `remove_list`
- **Source enumeration** — `sources`, `default_source` expose the account sources (iCloud, On My Mac, …) that contain Reminder lists
- **Cloneable handles** — `ReminderStore` and `CalendarStore` are `Clone + Send + Sync`; all clones share one underlying `EKEventStore`
- **`with_access` closure helper** — bundles authorization and access in one call, mirroring `Isam::read` / `Isam::write`

---

## Quick start — Reminders

```rust
use highlandcows_eventkit::{EventKitResult, ReminderStore};

fn main() -> EventKitResult<()> {
    let store = ReminderStore::builder().connect()?;

    // Blocks until the user answers the system permission dialog (returns
    // immediately if access was already granted or denied).
    let token = store.authorize()?;

    for reminder in store.fetch_incomplete(None, &token)? {
        println!("{} (due {:?})", reminder.title, reminder.due_date);
    }
    Ok(())
}
```

---

## Quick start — Calendar

```rust
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Duration, Utc};
use highlandcows_eventkit::{CalendarStore, EventKitResult};

fn main() -> EventKitResult<()> {
    let store = CalendarStore::builder().connect()?;
    let token = store.authorize()?;

    // chrono is built without the `clock` feature; use SystemTime for the current time.
    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let end: DateTime<Utc> = DateTime::from_timestamp(now_secs, 0).unwrap();
    let start = end - Duration::days(7);

    for event in store.fetch_in_range(start, end, None, &token)? {
        println!("{} ({:?} – {:?})", event.title, event.start_date, event.end_date);
    }
    Ok(())
}
```

Or use `with_access` to bundle authorization and access in one call (works for both stores):

```rust
let store = CalendarStore::builder().connect()?;
let calendars = store.with_access(|token, store| store.lists(token))?;
```

---

## Authorization and capability tokens

EventKit requires user consent before data can be read or written. This crate enforces that at compile time: every CRUD method takes a token that can only be obtained from an `authorize` method.

| Token | Grants | Obtained from |
|-------|--------|---------------|
| `FullAccessToken` | Reminders read + write | `ReminderStore::authorize()` |
| `WriteOnlyToken` | Reminders write only | `ReminderStore::authorize_write_only()` |
| `CalendarFullAccessToken` | Calendar read + write | `CalendarStore::authorize()` |
| `CalendarWriteOnlyToken` | Calendar write only | `CalendarStore::authorize_write_only()` |

The sealed trait hierarchy (`RemindersAccess` / `FullAccess` for Reminders; `CalendarAccess` / `CalendarFullAccess` for Calendar) lets write methods accept either token while fetch methods require the full-access variant.

Unlike Reminders, Calendar events have a genuine write-only authorization mode in EventKit: `CalendarWriteOnlyToken` permits creating and modifying events without granting read access. For Reminders, `authorize_write_only()` delegates to full access internally (EventKit does not distinguish the two for Reminders).

---

## App requirements

The host application's `Info.plist` must declare the appropriate usage description key, or macOS will deny access without showing a permission dialog:

| Domain | Info.plist key |
|--------|----------------|
| Reminders (full) | `NSRemindersFullAccessUsageDescription` |
| Calendar (full) | `NSCalendarsFullAccessUsageDescription` |
| Calendar (write-only) | `NSCalendarsWriteOnlyAccessUsageDescription` |

Plain command-line binaries inherit the TCC identity of the terminal that launches them, so during development the permission prompt names your terminal app (Terminal, iTerm2, etc.).

---

## API — Reminders

| Method | Description |
|--------|-------------|
| `ReminderStore::builder()` | Create a `ReminderStoreBuilder` |
| `ReminderStoreBuilder::connect()` | Connect to the system Reminders database |
| `ReminderStore::authorization_status()` | Query current authorization without prompting |
| `ReminderStore::authorize()` | Request full access (blocking) — returns `FullAccessToken` |
| `ReminderStore::authorize_write_only()` | Request write-only access — returns `WriteOnlyToken` |
| `ReminderStore::with_access(f)` | Authorize then run a closure with the token |
| `ReminderStore::fetch(id, &token)` | Fetch one reminder by stable ID |
| `ReminderStore::fetch_all(lists, &token)` | Fetch all reminders, optionally filtered to specific lists |
| `ReminderStore::fetch_incomplete(lists, &token)` | Fetch only incomplete (not-yet-done) reminders |
| `ReminderStore::save(&reminder, &token)` | Create or update a reminder; returns the stable ID |
| `ReminderStore::remove(id, &token)` | Delete a reminder by stable ID |
| `ReminderStore::lists(&token)` | Return all Reminder lists visible to this store |
| `ReminderStore::default_list(&token)` | Return the default list for new reminders |
| `ReminderStore::create_list(title, source_id, &token)` | Create a new Reminder list in the given source; returns the created list |
| `ReminderStore::remove_list(id, &token)` | Delete a Reminder list by its identifier |
| `ReminderStore::sources(&token)` | Return all account sources (iCloud, On My Mac, …) visible to this store |
| `ReminderStore::default_source(&token)` | Return the source that owns the system default Reminders list |

---

## API — Calendar

| Method | Description |
|--------|-------------|
| `CalendarStore::builder()` | Create a `CalendarStoreBuilder` |
| `CalendarStoreBuilder::connect()` | Connect to the system Calendar database |
| `CalendarStore::authorization_status()` | Query current authorization without prompting |
| `CalendarStore::authorize()` | Request full access (blocking) — returns `CalendarFullAccessToken` |
| `CalendarStore::authorize_write_only()` | Request write-only access — returns `CalendarWriteOnlyToken` |
| `CalendarStore::with_access(f)` | Authorize then run a closure with the token |
| `CalendarStore::fetch(id, &token)` | Fetch one event by stable ID |
| `CalendarStore::fetch_in_range(start, end, calendars, &token)` | Fetch all events in a date range (synchronous) |
| `CalendarStore::save(&event, &token)` | Create or update an event; returns the stable ID |
| `CalendarStore::remove(id, &token)` | Delete an event by stable ID |
| `CalendarStore::lists(&token)` | Return all Calendars visible to this store |
| `CalendarStore::default_calendar(&token)` | Return the default calendar for new events |

---

## Testing

Most tests run automatically with `cargo test -p highlandcows-eventkit`. Tests that interact with the live Reminders or Calendar database require TCC authorization and are marked `#[ignore]` so they are skipped by default. To run them locally:

1. Grant **Reminders** and **Calendar** access to your terminal in **System Settings → Privacy & Security**.
2. Run:

```sh
cargo test -p highlandcows-eventkit -- --ignored
```

These tests create and delete real reminders and events in your system databases. They are not run in CI.

---

## Error types

| Variant | When |
|---------|------|
| `EventKitError::AuthorizationDenied` | The user denied access, or the required `Info.plist` key is missing |
| `EventKitError::AuthorizationRestricted` | System policy prevents access (parental controls, MDM) |
| `EventKitError::AuthorizationNotDetermined` | `authorize()` was not called before a CRUD method |
| `EventKitError::ReminderNotFound(id)` | A reminder with the given ID was not found |
| `EventKitError::EventNotFound(id)` | A calendar event with the given ID was not found |
| `EventKitError::ListNotFound(id)` | A reminder list identifier resolved to nothing |
| `EventKitError::SourceNotFound(id)` | A source identifier passed to `create_list` resolved to nothing |
| `EventKitError::CalendarNotFound(id)` | A calendar identifier resolved to nothing |
| `EventKitError::SaveFailed(msg)` | EventKit rejected the save; message from `NSError.localizedDescription` |
| `EventKitError::RemoveFailed(msg)` | EventKit rejected the remove |
| `EventKitError::Framework(msg)` | Internal framework error (e.g., callback channel dropped) |
| `EventKitError::LockPoisoned` | A thread panicked while holding the store lock |

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| [`objc2`](https://crates.io/crates/objc2) | Rust bindings to the Objective-C runtime |
| [`objc2-event-kit`](https://crates.io/crates/objc2-event-kit) | Generated bindings for Apple's EventKit framework |
| [`objc2-foundation`](https://crates.io/crates/objc2-foundation) | Generated bindings for the Foundation framework |
| [`block2`](https://crates.io/crates/block2) | Rust bindings to Objective-C blocks |
| [`chrono`](https://crates.io/crates/chrono) | Date and time handling (NSDate ↔ DateTime<Utc> conversion) |
| [`thiserror`](https://crates.io/crates/thiserror) | Ergonomic error type derivation |

---

## License

MIT — see [LICENSE](../../LICENSE).
