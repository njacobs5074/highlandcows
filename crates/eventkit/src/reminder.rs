use chrono::{DateTime, Utc};
use objc2::rc::Retained;
use objc2_event_kit::{EKEventStore, EKReminder};
use objc2_foundation::NSString;

use crate::date_util::{nsdate_components_to_utc, nsdate_to_utc, utc_to_nsdate_components};
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
        let identifier = Some(unsafe { ek.calendarItemIdentifier().to_string() });
        let title = unsafe { ek.title().to_string() };
        let notes = unsafe { ek.notes().map(|s| s.to_string()) };
        let list_identifier = unsafe { ek.calendar().map(|c| c.calendarIdentifier().to_string()) };
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
    /// If `identifier` is `Some`, the existing reminder with that identifier is
    /// fetched and updated; if it is `None`, a new reminder is created.
    pub(crate) fn to_ek(&self, store: &EKEventStore) -> EventKitResult<Retained<EKReminder>> {
        let ek = match self.identifier {
            Some(ref id) => {
                let ns_id = NSString::from_str(id);
                unsafe { store.calendarItemWithIdentifier(&ns_id) }
                    .and_then(|item| item.downcast::<EKReminder>().ok())
                    .ok_or_else(|| EventKitError::ReminderNotFound(id.clone()))?
            }
            None => unsafe { EKReminder::reminderWithEventStore(store) },
        };

        unsafe {
            ek.setTitle(Some(&NSString::from_str(&self.title)));
            ek.setNotes(self.notes.as_deref().map(NSString::from_str).as_deref());
            ek.setCompleted(self.is_completed);
            ek.setPriority(self.priority as _);
        }

        match self.due_date {
            Some(ref due) => {
                let components = utc_to_nsdate_components(due);
                unsafe { ek.setDueDateComponents(Some(&components)) };
            }
            None => unsafe { ek.setDueDateComponents(None) },
        }

        Ok(ek)
    }
}

