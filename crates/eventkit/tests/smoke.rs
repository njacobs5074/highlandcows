#![cfg(target_os = "macos")]

//! Smoke tests that require full Reminders access (TCC authorization).
//!
//! These tests are skipped by default (`#[ignore]`). To run them:
//!
//! ```sh
//! cargo test -p highlandcows-eventkit -- --ignored
//! ```
//!
//! Before running, grant Reminders access to your terminal in:
//! **System Settings → Privacy & Security → Reminders**

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Duration, NaiveDate, Utc};
use highlandcows_eventkit::{CalendarStore, Reminder, ReminderStore};

#[test]
#[ignore = "requires TCC Reminders authorization"]
fn test_authorize_and_fetch_incomplete() {
    let store = ReminderStore::builder().connect().unwrap();
    let token = store.authorize().expect("authorization failed");
    let reminders = store
        .fetch_incomplete(None, &token)
        .expect("fetch_incomplete failed");
    let _ = reminders;
}

#[test]
#[ignore = "requires TCC Reminders authorization"]
fn test_lists() {
    let store = ReminderStore::builder().connect().unwrap();
    let token = store.authorize().expect("authorization failed");
    let lists = store.lists(&token).expect("lists() failed");
    assert!(
        !lists.is_empty(),
        "expected at least one Reminder list in the system"
    );
}

#[test]
#[ignore = "requires TCC Reminders authorization; creates and deletes a real reminder"]
fn test_save_and_remove() {
    let store = ReminderStore::builder().connect().unwrap();
    let token = store.authorize().expect("authorization failed");

    let title = format!(
        "eventkit-test-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );

    let new = Reminder {
        title: title.clone(),
        ..Default::default()
    };
    let id = store.save(&new, &token).expect("save failed");
    assert!(!id.is_empty(), "expected a non-empty identifier from save");

    let fetched = store
        .fetch(&id, &token)
        .expect("fetch failed")
        .expect("reminder not found after save");
    assert_eq!(fetched.title, title);

    store.remove(&id, &token).expect("remove failed");

    let gone = store.fetch(&id, &token).expect("fetch after remove failed");
    assert!(gone.is_none(), "reminder should be absent after remove");
}

#[test]
#[ignore = "requires TCC Reminders authorization; creates and deletes a real reminder"]
fn test_update_round_trip() {
    let store = ReminderStore::builder().connect().unwrap();
    let token = store.authorize().expect("authorization failed");

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Create.
    let id = store
        .save(
            &Reminder {
                title: format!("eventkit-update-test-{ts}"),
                ..Default::default()
            },
            &token,
        )
        .expect("initial save failed");

    // Fetch, modify title, save again.
    let mut fetched = store
        .fetch(&id, &token)
        .expect("fetch failed")
        .expect("reminder not found after create");
    fetched.title = format!("eventkit-update-test-{ts}-updated");
    let id2 = store.save(&fetched, &token).expect("update save failed");

    // Same identifier — no duplicate created.
    assert_eq!(id, id2, "save should return the same identifier on update");

    // Title persisted.
    let updated = store
        .fetch(&id, &token)
        .expect("fetch after update failed")
        .expect("reminder not found after update");
    assert_eq!(updated.title, fetched.title);

    store.remove(&id, &token).expect("cleanup remove failed");
}

#[test]
#[ignore = "requires TCC Reminders authorization; creates and deletes a real reminder"]
fn test_due_date_round_trip() {
    let store = ReminderStore::builder().connect().unwrap();
    let token = store.authorize().expect("authorization failed");

    // Fixed future date (truncated to seconds so NSDateComponents round-trips cleanly).
    let due: DateTime<Utc> = NaiveDate::from_ymd_opt(2027, 3, 15)
        .unwrap()
        .and_hms_opt(14, 30, 0)
        .unwrap()
        .and_utc();

    let id = store
        .save(
            &Reminder {
                title: "eventkit-due-date-test".into(),
                due_date: Some(due),
                ..Default::default()
            },
            &token,
        )
        .expect("save failed");

    let fetched = store
        .fetch(&id, &token)
        .expect("fetch failed")
        .expect("reminder not found after save");

    let fetched_due = fetched.due_date.expect("due_date missing after round-trip");
    let delta = (fetched_due.timestamp() - due.timestamp()).abs();
    assert!(
        delta < 2,
        "due_date diverged by {delta}s: saved {due}, got {fetched_due}"
    );

    store.remove(&id, &token).expect("cleanup remove failed");
}

// ── Calendar smoke tests ──────────────────────────────────────────────────────

#[test]
#[ignore = "requires TCC Calendar authorization"]
fn test_calendar_authorize_and_list() {
    let store = CalendarStore::builder().connect().unwrap();
    let token = store.authorize().expect("calendar authorization failed");
    let calendars = store.lists(&token).expect("lists() failed");
    assert!(
        !calendars.is_empty(),
        "expected at least one Calendar in the system"
    );
}

#[test]
#[ignore = "requires TCC Calendar authorization"]
fn test_calendar_fetch_in_range() {
    let store = CalendarStore::builder().connect().unwrap();
    let token = store.authorize().expect("calendar authorization failed");

    // Fetch everything in the past 7 days — just verifying no panic or error.
    let end: DateTime<Utc> = {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        DateTime::from_timestamp(secs, 0).unwrap()
    };
    let start = end - Duration::days(7);
    let events = store
        .fetch_in_range(start, end, None, &token)
        .expect("fetch_in_range failed");
    let _ = events;
}

#[test]
#[ignore = "requires TCC Calendar authorization"]
fn test_calendar_with_access() {
    let store = CalendarStore::builder().connect().unwrap();
    let calendars = store
        .with_access(|token, store| store.lists(token))
        .expect("with_access failed");
    assert!(!calendars.is_empty(), "expected at least one Calendar");
}

#[test]
#[ignore = "requires TCC Calendar authorization"]
fn test_calendar_default_calendar() {
    let store = CalendarStore::builder().connect().unwrap();
    let token = store.authorize().expect("calendar authorization failed");
    // default_calendar returning None is valid if the user hasn't configured one,
    // so we just verify the call doesn't error or panic.
    let _ = store
        .default_calendar(&token)
        .expect("default_calendar failed");
}

#[test]
#[ignore = "requires TCC Reminders authorization"]
fn test_with_access() {
    let store = ReminderStore::builder().connect().unwrap();
    let lists = store
        .with_access(|token, store| store.lists(token))
        .expect("with_access failed");
    assert!(!lists.is_empty(), "expected at least one Reminder list");
}

#[test]
#[ignore = "requires TCC Reminders authorization; creates and deletes a real reminder"]
fn test_fetch_all_vs_fetch_incomplete() {
    let store = ReminderStore::builder().connect().unwrap();
    let token = store.authorize().expect("authorization failed");

    // Anchor the assertion on a reminder we control so the test doesn't
    // depend on the state of pre-existing user data or concurrent test runs.
    let id = store
        .save(
            &Reminder {
                title: "eventkit-fetch-all-test".into(),
                ..Default::default()
            },
            &token,
        )
        .expect("save failed");

    let all = store.fetch_all(None, &token).expect("fetch_all failed");
    let incomplete = store
        .fetch_incomplete(None, &token)
        .expect("fetch_incomplete failed");

    assert!(
        all.iter().any(|r| r.identifier.as_deref() == Some(id.as_str())),
        "test reminder {id} missing from fetch_all"
    );
    assert!(
        incomplete.iter().any(|r| r.identifier.as_deref() == Some(id.as_str())),
        "test reminder {id} missing from fetch_incomplete"
    );
    assert!(
        all.len() >= incomplete.len(),
        "fetch_all ({}) should return at least as many as fetch_incomplete ({})",
        all.len(),
        incomplete.len()
    );

    store.remove(&id, &token).expect("cleanup remove failed");
}
