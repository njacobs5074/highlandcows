use chrono::{DateTime, Utc};
use objc2_event_kit::EKEvent;

use crate::date_util::nsdate_to_utc;

/// A Calendar event, free of Objective-C pointers and safely `Send + Sync`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CalendarEvent {
    /// Stable system identifier. `None` for events not yet saved.
    pub identifier: Option<String>,
    pub title: String,
    pub notes: Option<String>,
    /// Identifier of the containing [`crate::Calendar`].
    pub calendar_identifier: Option<String>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub is_all_day: bool,
    pub location: Option<String>,
}

impl CalendarEvent {
    pub(crate) fn from_ek(ek: &EKEvent) -> Self {
        let identifier = Some(unsafe { ek.calendarItemIdentifier().to_string() });
        let title = unsafe { ek.title().to_string() };
        let notes = unsafe { ek.notes().map(|s| s.to_string()) };
        let calendar_identifier =
            unsafe { ek.calendar().map(|c| c.calendarIdentifier().to_string()) };
        let is_all_day = unsafe { ek.isAllDay() };
        let location = unsafe { ek.location().map(|s| s.to_string()) };
        let start_date = nsdate_to_utc(unsafe { ek.startDate() });
        let end_date = nsdate_to_utc(unsafe { ek.endDate() });

        CalendarEvent {
            identifier,
            title,
            notes,
            calendar_identifier,
            start_date,
            end_date,
            is_all_day,
            location,
        }
    }
}
