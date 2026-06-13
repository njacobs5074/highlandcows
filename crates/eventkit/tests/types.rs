#![cfg(target_os = "macos")]

use highlandcows_eventkit::{CalendarEvent, EkAuthStatus, EkEntityType, EventKitError, Reminder};

#[test]
fn test_reminder_default() {
    let r = Reminder::default();
    assert!(r.identifier.is_none());
    assert_eq!(r.title, "");
    assert!(r.notes.is_none());
    assert!(r.list_identifier.is_none());
    assert!(r.due_date.is_none());
    assert!(r.completion_date.is_none());
    assert!(!r.is_completed);
    assert_eq!(r.priority, 0);
}

#[test]
fn test_reminder_clone_eq() {
    let r = Reminder {
        title: "Buy milk".into(),
        priority: 1,
        ..Default::default()
    };
    assert_eq!(r.clone(), r);
}

#[test]
fn test_ek_auth_status_variants_are_distinct() {
    let all = [
        EkAuthStatus::NotDetermined,
        EkAuthStatus::Restricted,
        EkAuthStatus::Denied,
        EkAuthStatus::FullAccess,
        EkAuthStatus::WriteOnly,
    ];
    // Verify each variant compares equal to itself and not to a different one.
    for (i, a) in all.iter().enumerate() {
        for (j, b) in all.iter().enumerate() {
            assert_eq!(a == b, i == j, "variant {i} vs {j}");
        }
    }
}

#[test]
fn test_ek_entity_type_variants() {
    let _ = EkEntityType::Event;
    let _ = EkEntityType::Reminder;
}

#[test]
fn test_eventkit_error_display() {
    assert_eq!(
        EventKitError::AuthorizationDenied.to_string(),
        "authorization denied"
    );
    assert_eq!(
        EventKitError::ReminderNotFound("abc-123".into()).to_string(),
        "reminder not found: abc-123"
    );
    assert_eq!(
        EventKitError::EventNotFound("xyz-456".into()).to_string(),
        "event not found: xyz-456"
    );
    assert_eq!(
        EventKitError::CalendarNotFound("cal-789".into()).to_string(),
        "calendar not found: cal-789"
    );
    assert_eq!(
        EventKitError::SaveFailed("disk full".into()).to_string(),
        "save failed: disk full"
    );
    assert_eq!(
        EventKitError::RemoveFailed("not found".into()).to_string(),
        "remove failed: not found"
    );
}

#[test]
fn test_calendar_event_default() {
    let e = CalendarEvent::default();
    assert!(e.identifier.is_none());
    assert_eq!(e.title, "");
    assert!(e.notes.is_none());
    assert!(e.calendar_identifier.is_none());
    assert!(e.start_date.is_none());
    assert!(e.end_date.is_none());
    assert!(!e.is_all_day);
    assert!(e.location.is_none());
}

#[test]
fn test_calendar_event_clone_eq() {
    let e = CalendarEvent {
        title: "Team sync".into(),
        is_all_day: false,
        location: Some("Conf room A".into()),
        ..Default::default()
    };
    assert_eq!(e.clone(), e);
}
