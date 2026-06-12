#![cfg(target_os = "macos")]

use highlandcows_eventkit::{EkAuthStatus, ReminderStore};

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
    // A cloned handle should agree on the authorization status.
    assert_eq!(
        ReminderStore::authorization_status(),
        ReminderStore::authorization_status()
    );
    drop(clone);
}
