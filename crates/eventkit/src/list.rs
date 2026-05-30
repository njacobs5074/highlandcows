use objc2_event_kit::EKCalendar;

/// A Reminders list (backed by `EKCalendar` with entity type = Reminder).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReminderList {
    pub list_identifier: String,
    pub title: String,
    /// Whether reminders can be added, edited, or removed in this list.
    pub allows_content_modifications: bool,
    pub source_title: Option<String>,
}

impl ReminderList {
    pub(crate) fn from_ek(cal: &EKCalendar) -> Self {
        let list_identifier = unsafe { cal.calendarIdentifier().to_string() };
        let title = unsafe {
            cal.title()
                .map(|s| s.to_string())
                .unwrap_or_default()
        };
        let allows_content_modifications = unsafe { cal.allowsContentModifications() };
        let source_title = unsafe {
            cal.source()
                .and_then(|s| s.title().map(|t| t.to_string()))
        };

        ReminderList {
            list_identifier,
            title,
            allows_content_modifications,
            source_title,
        }
    }
}
