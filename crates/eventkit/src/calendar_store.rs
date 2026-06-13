use std::sync::mpsc;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use objc2::runtime::Bool;
use objc2_event_kit::{EKEntityType, EKEvent, EKEventStore};
use objc2_foundation::NSError;

use crate::auth::{CalendarAccess, CalendarFullAccessToken, CalendarWriteOnlyToken};
use crate::calendar::Calendar;
use crate::calendar_builder::CalendarStoreBuilder;
use crate::calendar_event::CalendarEvent;
use crate::date_util::utc_to_nsdate;
use crate::error::{EventKitError, EventKitResult};
use crate::inner::Inner;
use crate::types::EkAuthStatus;

/// Facade for accessing the system Calendar database via Apple's EventKit.
///
/// Clone is cheap — all clones share the same underlying `EKEventStore`.
/// Construct via [`CalendarStore::builder`].
pub struct CalendarStore {
    pub(crate) inner: Arc<Inner>,
}

impl Clone for CalendarStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl CalendarStore {
    // ── Lifecycle ─────────────────────────────────────────────────────────────

    pub fn builder() -> CalendarStoreBuilder {
        CalendarStoreBuilder::new()
    }

    // ── Authorization ─────────────────────────────────────────────────────────

    /// Return the current Calendar authorization status without prompting.
    pub fn authorization_status() -> EkAuthStatus {
        let status =
            unsafe { EKEventStore::authorizationStatusForEntityType(EKEntityType::Event) };
        EkAuthStatus::from(status)
    }

    /// Request full read+write access to Calendar events (blocking).
    ///
    /// Bridges EventKit's callback API via `std::sync::mpsc`. The calling thread
    /// blocks until the system permission dialog is dismissed.
    ///
    /// Returns a [`CalendarFullAccessToken`] on success. The token must be passed
    /// to all read/write methods as proof of authorization.
    pub fn authorize(&self) -> EventKitResult<CalendarFullAccessToken> {
        match Self::authorization_status() {
            EkAuthStatus::FullAccess => return Ok(CalendarFullAccessToken(())),
            EkAuthStatus::Denied => return Err(EventKitError::AuthorizationDenied),
            EkAuthStatus::Restricted => return Err(EventKitError::AuthorizationRestricted),
            _ => {}
        }

        let (tx, rx) = mpsc::channel::<EventKitResult<CalendarFullAccessToken>>();
        let inner = Arc::clone(&self.inner);

        let block = block2::RcBlock::new(move |granted: Bool, _err: *mut NSError| {
            let result = if granted.as_bool() {
                Ok(CalendarFullAccessToken(()))
            } else {
                Err(EventKitError::AuthorizationDenied)
            };
            let _ = tx.send(result);
            let _ = &inner;
        });

        unsafe {
            self.inner
                .0
                .requestFullAccessToEventsWithCompletion(block2::RcBlock::as_ptr(&block));
        }

        rx.recv()
            .map_err(|_| EventKitError::Framework("authorization callback never fired".into()))?
    }

    /// Request write-only access to Calendar events (blocking).
    ///
    /// Write-only access permits creating and modifying events but not reading them.
    /// Use [`authorize`](Self::authorize) for full read+write access.
    pub fn authorize_write_only(&self) -> EventKitResult<CalendarWriteOnlyToken> {
        match Self::authorization_status() {
            EkAuthStatus::FullAccess | EkAuthStatus::WriteOnly => {
                return Ok(CalendarWriteOnlyToken(()))
            }
            EkAuthStatus::Denied => return Err(EventKitError::AuthorizationDenied),
            EkAuthStatus::Restricted => return Err(EventKitError::AuthorizationRestricted),
            _ => {}
        }

        let (tx, rx) = mpsc::channel::<EventKitResult<CalendarWriteOnlyToken>>();
        let inner = Arc::clone(&self.inner);

        let block = block2::RcBlock::new(move |granted: Bool, _err: *mut NSError| {
            let result = if granted.as_bool() {
                Ok(CalendarWriteOnlyToken(()))
            } else {
                Err(EventKitError::AuthorizationDenied)
            };
            let _ = tx.send(result);
            let _ = &inner;
        });

        unsafe {
            self.inner
                .0
                .requestWriteOnlyAccessToEventsWithCompletion(block2::RcBlock::as_ptr(&block));
        }

        rx.recv()
            .map_err(|_| EventKitError::Framework("authorization callback never fired".into()))?
    }

    /// Authorize (if needed) then execute `f` with a [`CalendarFullAccessToken`].
    pub fn with_access<F, T>(&self, f: F) -> EventKitResult<T>
    where
        F: FnOnce(&CalendarFullAccessToken, &CalendarStore) -> EventKitResult<T>,
    {
        let token = self.authorize()?;
        f(&token, self)
    }

    // ── Fetch ─────────────────────────────────────────────────────────────────

    /// Fetch a single event by its stable identifier.
    pub fn fetch(
        &self,
        id: &str,
        _token: &CalendarFullAccessToken,
    ) -> EventKitResult<Option<CalendarEvent>> {
        use objc2_foundation::NSString;

        let ns_id = NSString::from_str(id);
        let item = unsafe { self.inner.0.calendarItemWithIdentifier(&ns_id) };

        match item {
            None => Ok(None),
            Some(item) => match item.downcast::<EKEvent>() {
                Ok(ek_event) => Ok(Some(CalendarEvent::from_ek(&ek_event))),
                Err(_) => Err(EventKitError::EventNotFound(id.to_owned())),
            },
        }
    }

    /// Fetch all events whose start date falls within `[start, end]`, optionally
    /// filtered to specific calendars.
    ///
    /// `calendars`: calendar identifiers to filter by; `None` searches all calendars.
    pub fn fetch_in_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        calendars: Option<&[&str]>,
        _token: &CalendarFullAccessToken,
    ) -> EventKitResult<Vec<CalendarEvent>> {
        use objc2_foundation::{NSArray, NSString};

        let ek_cals: Option<Vec<_>> = calendars.map(|ids| {
            ids.iter()
                .filter_map(|id| {
                    let ns_id = NSString::from_str(id);
                    unsafe { self.inner.0.calendarWithIdentifier(&ns_id) }
                })
                .collect()
        });

        let ns_cals = ek_cals.as_ref().map(|v| NSArray::from_retained_slice(v));
        let ns_start = utc_to_nsdate(&start);
        let ns_end = utc_to_nsdate(&end);

        let predicate = unsafe {
            self.inner
                .0
                .predicateForEventsWithStartDate_endDate_calendars(
                    &ns_start,
                    &ns_end,
                    ns_cals.as_deref(),
                )
        };

        let events = unsafe { self.inner.0.eventsMatchingPredicate(&predicate) };
        Ok(events.iter().map(|e| CalendarEvent::from_ek(&e)).collect())
    }

    // ── Calendars ─────────────────────────────────────────────────────────────

    /// Return all Calendar entries visible to this store.
    pub fn lists(&self, _token: &CalendarFullAccessToken) -> EventKitResult<Vec<Calendar>> {
        let cals = unsafe { self.inner.0.calendarsForEntityType(EKEntityType::Event) };
        Ok(cals.iter().map(|c| Calendar::from_ek(&c)).collect())
    }

    /// Return the default calendar for new events, if one is configured.
    pub fn default_calendar(
        &self,
        _token: &impl CalendarAccess,
    ) -> EventKitResult<Option<Calendar>> {
        let cal = unsafe { self.inner.0.defaultCalendarForNewEvents() };
        Ok(cal.map(|c| Calendar::from_ek(&c)))
    }
}

