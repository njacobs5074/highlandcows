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

use highlandcows_eventkit::{Reminder, ReminderStore};

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
