use chrono::{DateTime, Utc};
use objc2::rc::Retained;
use objc2_event_kit::{EKEvent, EKEventStore};
use objc2_foundation::NSString;

use crate::date_util::{nsdate_to_utc, utc_to_nsdate};
use crate::error::{EventKitError, EventKitResult};

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
    /// Build an `EKEvent` from this value for saving.
    ///
    /// If `identifier` is `Some`, the existing event with that identifier is
    /// fetched and updated; if it is `None`, a new event is created.
    /// `start_date` and `end_date` must both be `Some`.
    pub(crate) fn to_ek(&self, store: &EKEventStore) -> EventKitResult<Retained<EKEvent>> {
        let ek = match self.identifier {
            Some(ref id) => {
                let ns_id = NSString::from_str(id);
                unsafe { store.calendarItemWithIdentifier(&ns_id) }
                    .and_then(|item| item.downcast::<EKEvent>().ok())
                    .ok_or_else(|| EventKitError::EventNotFound(id.clone()))?
            }
            None => unsafe { EKEvent::eventWithEventStore(store) },
        };

        let start = self.start_date.ok_or_else(|| {
            EventKitError::Framework("CalendarEvent.start_date is required".into())
        })?;
        let end = self.end_date.ok_or_else(|| {
            EventKitError::Framework("CalendarEvent.end_date is required".into())
        })?;

        unsafe {
            ek.setTitle(Some(&NSString::from_str(&self.title)));
            ek.setNotes(self.notes.as_deref().map(NSString::from_str).as_deref());
            ek.setStartDate(Some(&utc_to_nsdate(&start)));
            ek.setEndDate(Some(&utc_to_nsdate(&end)));
            ek.setAllDay(self.is_all_day);
            ek.setLocation(self.location.as_deref().map(NSString::from_str).as_deref());
        }

        Ok(ek)
    }

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
