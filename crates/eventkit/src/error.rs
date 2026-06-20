#[derive(Debug, thiserror::Error)]
pub enum EventKitError {
    #[error("authorization denied")]
    AuthorizationDenied,
    #[error("authorization restricted by system policy")]
    AuthorizationRestricted,
    #[error("authorization not yet determined — call authorize() first")]
    AuthorizationNotDetermined,
    #[error("EventKit framework error: {0}")]
    Framework(String),
    #[error("mutex poisoned: a thread panicked while holding the store lock")]
    LockPoisoned,
    #[error("reminder not found: {0}")]
    ReminderNotFound(String),
    #[error("reminder list not found: {0}")]
    ListNotFound(String),
    #[error("source not found: {0}")]
    SourceNotFound(String),
    #[error("event not found: {0}")]
    EventNotFound(String),
    #[error("calendar not found: {0}")]
    CalendarNotFound(String),
    #[error("save failed: {0}")]
    SaveFailed(String),
    #[error("remove failed: {0}")]
    RemoveFailed(String),
}

pub type EventKitResult<T> = Result<T, EventKitError>;
