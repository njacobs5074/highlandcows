use objc2_event_kit::EKSource;

/// An account source that can contain reminder lists or calendars (e.g. iCloud, On My Mac).
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub source_identifier: String,
    pub title: String,
}

impl Source {
    pub(crate) fn from_ek(src: &EKSource) -> Self {
        Self {
            source_identifier: unsafe { src.sourceIdentifier().to_string() },
            title: unsafe { src.title().to_string() },
        }
    }
}
