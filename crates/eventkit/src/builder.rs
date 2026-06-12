use std::sync::Arc;

use objc2_event_kit::EKEventStore;

use crate::error::EventKitResult;
use crate::inner::Inner;
use crate::store::ReminderStore;

/// Builder for [`ReminderStore`]. Obtain one via [`ReminderStore::builder`].
#[derive(Default)]
pub struct ReminderStoreBuilder {
    // Reserved for future options (e.g. custom store URL on macOS).
    _private: (),
}

impl ReminderStoreBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Open the system-managed Reminders database and return a [`ReminderStore`].
    ///
    /// This is synchronous. Authorization is a separate step:
    /// call [`ReminderStore::authorize`] before performing any CRUD operations.
    pub fn connect(self) -> EventKitResult<ReminderStore> {
        let ek_store = unsafe { EKEventStore::new() };
        Ok(ReminderStore {
            inner: Arc::new(Inner(ek_store)),
        })
    }
}
