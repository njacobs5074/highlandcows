/// Proves the caller holds full read+write access to Reminders.
/// Constructed only inside [`crate::ReminderStore::authorize`].
pub struct FullAccessToken(pub(crate) ());

/// Proves the caller holds write-only access to Reminders.
/// Constructed only inside [`crate::ReminderStore::authorize_write_only`].
///
/// Note: EventKit does not expose a write-only mode for Reminders (only for
/// Calendar events). This token is provided for API symmetry and future
/// compatibility; [`crate::ReminderStore::authorize_write_only`] delegates to full
/// access for Reminders.
pub struct WriteOnlyToken(pub(crate) ());

mod private {
    pub trait Sealed {}
}

/// Marker for tokens that grant write access to Reminders.
pub trait RemindersAccess: private::Sealed {}

/// Marker for tokens that grant full read+write access to Reminders.
pub trait FullAccess: RemindersAccess {}

impl private::Sealed for FullAccessToken {}
impl RemindersAccess for FullAccessToken {}
impl FullAccess for FullAccessToken {}

impl private::Sealed for WriteOnlyToken {}
impl RemindersAccess for WriteOnlyToken {}
// WriteOnlyToken intentionally does NOT impl FullAccess.
