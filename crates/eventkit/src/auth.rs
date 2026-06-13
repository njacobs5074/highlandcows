// ── Reminders tokens ─────────────────────────────────────────────────────────

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

// ── Calendar tokens ───────────────────────────────────────────────────────────

/// Proves the caller holds full read+write access to Calendar events.
/// Constructed only inside [`crate::CalendarStore::authorize`].
pub struct CalendarFullAccessToken(pub(crate) ());

/// Proves the caller holds write-only access to Calendar events.
/// Constructed only inside [`crate::CalendarStore::authorize_write_only`].
///
/// Write-only access allows creating and modifying events but not reading them.
/// Use [`crate::CalendarStore::authorize`] for full read+write access.
pub struct CalendarWriteOnlyToken(pub(crate) ());

// ── Sealed trait infrastructure ───────────────────────────────────────────────

mod private {
    pub trait Sealed {}
}

// ── Reminders access traits ───────────────────────────────────────────────────

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

// ── Calendar access traits ────────────────────────────────────────────────────

/// Marker for tokens that grant write access to Calendar events.
pub trait CalendarAccess: private::Sealed {}

/// Marker for tokens that grant full read+write access to Calendar events.
pub trait CalendarFullAccess: CalendarAccess {}

impl private::Sealed for CalendarFullAccessToken {}
impl CalendarAccess for CalendarFullAccessToken {}
impl CalendarFullAccess for CalendarFullAccessToken {}

impl private::Sealed for CalendarWriteOnlyToken {}
impl CalendarAccess for CalendarWriteOnlyToken {}
// CalendarWriteOnlyToken intentionally does NOT impl CalendarFullAccess.
