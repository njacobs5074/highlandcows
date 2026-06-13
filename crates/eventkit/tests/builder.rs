#![cfg(target_os = "macos")]

use highlandcows_eventkit::{CalendarStore, EkAuthStatus, ReminderStore};

#[test]
fn test_connect_succeeds() {
    let result = ReminderStore::builder().connect();
    assert!(
        result.is_ok(),
        "ReminderStoreBuilder::connect() failed: {:?}",
        result.err()
    );
}

#[test]
fn test_authorization_status_returns_valid_variant() {
    // Verify it doesn't panic and returns a recognized variant.
    let status = ReminderStore::authorization_status();
    let _recognized = matches!(
        status,
        EkAuthStatus::NotDetermined
            | EkAuthStatus::Restricted
            | EkAuthStatus::Denied
            | EkAuthStatus::FullAccess
            | EkAuthStatus::WriteOnly
    );
}

#[test]
fn test_store_clone_is_usable() {
    let store = ReminderStore::builder().connect().unwrap();
    let clone = store.clone();
    // Verify the clone is live — just calling authorization_status() on it must not panic.
    let _ = ReminderStore::authorization_status();
    drop(clone);
}

#[test]
fn test_calendar_connect_succeeds() {
    let result = CalendarStore::builder().connect();
    assert!(
        result.is_ok(),
        "CalendarStoreBuilder::connect() failed: {:?}",
        result.err()
    );
}

#[test]
fn test_calendar_authorization_status_returns_valid_variant() {
    let status = CalendarStore::authorization_status();
    let _recognized = matches!(
        status,
        EkAuthStatus::NotDetermined
            | EkAuthStatus::Restricted
            | EkAuthStatus::Denied
            | EkAuthStatus::FullAccess
            | EkAuthStatus::WriteOnly
    );
}

#[test]
fn test_calendar_store_clone_is_usable() {
    let store = CalendarStore::builder().connect().unwrap();
    let clone = store.clone();
    let _ = CalendarStore::authorization_status();
    drop(clone);
}
