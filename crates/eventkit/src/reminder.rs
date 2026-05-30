use chrono::{DateTime, Utc};
use objc2::rc::Retained;
use objc2_event_kit::{EKEventStore, EKReminder};
use objc2_foundation::NSString;

use crate::error::{EventKitError, EventKitResult};

/// A Reminders task, free of Objective-C pointers and safely `Send + Sync`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Reminder {
    /// Stable system identifier. `None` for reminders not yet saved.
    pub identifier: Option<String>,
    pub title: String,
    pub notes: Option<String>,
    /// Identifier of the containing [`crate::ReminderList`].
    pub list_identifier: Option<String>,
    pub due_date: Option<DateTime<Utc>>,
    pub completion_date: Option<DateTime<Utc>>,
    pub is_completed: bool,
    /// 0 = none, 1 = high, 5 = medium, 9 = low (EKReminder convention).
    pub priority: u8,
}

impl Reminder {
    /// Convert an `EKReminder` into a `Reminder`.
    pub(crate) fn from_ek(ek: &EKReminder) -> Self {
        let identifier = unsafe {
            ek.calendarItemIdentifier().map(|s| s.to_string())
        };
        let title = unsafe {
            ek.title()
                .map(|s| s.to_string())
                .unwrap_or_default()
        };
        let notes = unsafe { ek.notes().map(|s| s.to_string()) };
        let list_identifier = unsafe {
            ek.calendar()
                .and_then(|c| Some(c.calendarIdentifier().to_string()))
        };
        let is_completed = unsafe { ek.isCompleted() };
        let priority = unsafe { ek.priority() as u8 };
        let due_date = unsafe { ek.dueDateComponents() }.and_then(nsdate_components_to_utc);
        let completion_date = unsafe { ek.completionDate() }.and_then(nsdate_to_utc);

        Reminder {
            identifier,
            title,
            notes,
            list_identifier,
            due_date,
            completion_date,
            is_completed,
            priority,
        }
    }

    /// Build an `EKReminder` from this value for saving.
    ///
    /// If `identifier` is `Some`, it is ignored — EventKit identifies saved
    /// reminders by the object returned from `EKEventStore::save`.
    pub(crate) fn to_ek(&self, store: &EKEventStore) -> EventKitResult<Retained<EKReminder>> {
        let ek = unsafe { EKReminder::reminderWithEventStore(store) };

        unsafe {
            ek.setTitle(Some(&NSString::from_str(&self.title)));
            ek.setNotes(self.notes.as_deref().map(NSString::from_str).as_deref());
            ek.setCompleted(self.is_completed);
            ek.setPriority(self.priority as _);
        }

        if let Some(ref due) = self.due_date {
            let components = utc_to_nsdate_components(due);
            unsafe { ek.setDueDateComponents(Some(&components)) };
        }

        Ok(ek)
    }
}

// ── Date conversion helpers ───────────────────────────────────────────────────

/// NSDate reference date is 2001-01-01 00:00:00 UTC; Unix epoch is 1970-01-01.
const APPLE_EPOCH_OFFSET: f64 = 978_307_200.0;

fn nsdate_to_utc(date: Retained<objc2_foundation::NSDate>) -> Option<DateTime<Utc>> {
    let secs_since_apple = unsafe { date.timeIntervalSinceReferenceDate() };
    let unix_secs = secs_since_apple + APPLE_EPOCH_OFFSET;
    let secs = unix_secs.trunc() as i64;
    let nanos = (unix_secs.fract().abs() * 1e9) as u32;
    DateTime::from_timestamp(secs, nanos)
}

fn nsdate_components_to_utc(
    components: Retained<objc2_foundation::NSDateComponents>,
) -> Option<DateTime<Utc>> {
    // Use the Gregorian calendar to resolve partial components to an NSDate.
    use objc2_foundation::{NSCalendar, NSCalendarIdentifier};
    let cal = unsafe {
        NSCalendar::calendarWithIdentifier(NSCalendarIdentifier::Gregorian)?
    };
    let date = unsafe { cal.dateFromComponents(&components)? };
    nsdate_to_utc(date)
}

fn utc_to_nsdate_components(dt: &DateTime<Utc>) -> Retained<objc2_foundation::NSDateComponents> {
    use objc2_foundation::NSDateComponents;
    use chrono::Datelike;
    use chrono::Timelike;

    let components = unsafe { NSDateComponents::new() };
    unsafe {
        components.setYear(dt.year() as _);
        components.setMonth(dt.month() as _);
        components.setDay(dt.day() as _);
        components.setHour(dt.hour() as _);
        components.setMinute(dt.minute() as _);
        components.setSecond(dt.second() as _);
    }
    components
}
