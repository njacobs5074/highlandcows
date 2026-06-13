use std::sync::Arc;

use objc2_event_kit::EKEventStore;

use crate::calendar_store::CalendarStore;
use crate::error::EventKitResult;
use crate::inner::Inner;

/// Builder for [`CalendarStore`]. Obtain one via [`CalendarStore::builder`].
#[derive(Default)]
pub struct CalendarStoreBuilder {
    _private: (),
}

impl CalendarStoreBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Open the system-managed Calendar database and return a [`CalendarStore`].
    ///
    /// This is synchronous. Authorization is a separate step:
    /// call [`CalendarStore::authorize`] before performing any CRUD operations.
    pub fn connect(self) -> EventKitResult<CalendarStore> {
        let ek_store = unsafe { EKEventStore::new() };
        Ok(CalendarStore {
            inner: Arc::new(Inner(ek_store)),
        })
    }
}
