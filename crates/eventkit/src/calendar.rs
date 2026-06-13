use objc2_event_kit::EKCalendar;

/// A Calendar entry (backed by `EKCalendar` with entity type = Event).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Calendar {
    pub calendar_identifier: String,
    pub title: String,
    /// Whether events can be added, edited, or removed in this calendar.
    pub allows_content_modifications: bool,
    pub source_title: Option<String>,
}

impl Calendar {
    pub(crate) fn from_ek(cal: &EKCalendar) -> Self {
        let calendar_identifier = unsafe { cal.calendarIdentifier().to_string() };
        let title = unsafe { cal.title().to_string() };
        let allows_content_modifications = unsafe { cal.allowsContentModifications() };
        let source_title = unsafe { cal.source().map(|s| s.title().to_string()) };

        Calendar {
            calendar_identifier,
            title,
            allows_content_modifications,
            source_title,
        }
    }
}
