use std::sync::mpsc;
use std::sync::Arc;

use objc2::runtime::Bool;
use objc2_event_kit::{EKEntityType, EKEventStore, EKReminder};
use objc2_foundation::NSError;

use crate::auth::{FullAccessToken, RemindersAccess, WriteOnlyToken};
use crate::builder::ReminderStoreBuilder;
use crate::error::{EventKitError, EventKitResult};
use crate::inner::Inner;
use crate::list::ReminderList;
use crate::reminder::Reminder;
use crate::types::EkAuthStatus;

/// Facade for accessing the system Reminders database via Apple's EventKit.
///
/// Clone is cheap — all clones share the same underlying `EKEventStore`.
/// Construct via [`ReminderStore::builder`].
pub struct ReminderStore {
    pub(crate) inner: Arc<Inner>,
}

impl Clone for ReminderStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl ReminderStore {
    // ── Lifecycle ─────────────────────────────────────────────────────────────

    pub fn builder() -> ReminderStoreBuilder {
        ReminderStoreBuilder::new()
    }

    // ── Authorization ─────────────────────────────────────────────────────────

    /// Return the current Reminders authorization status without prompting.
    pub fn authorization_status() -> EkAuthStatus {
        let status =
            unsafe { EKEventStore::authorizationStatusForEntityType(EKEntityType::Reminder) };
        EkAuthStatus::from(status)
    }

    /// Request full access to Reminders (blocking).
    ///
    /// Bridges EventKit's callback API via `std::sync::mpsc`. The calling thread
    /// blocks until the system permission dialog is dismissed.
    ///
    /// Returns a [`FullAccessToken`] on success. The token must be passed to all
    /// CRUD methods as proof of authorization.
    pub fn authorize(&self) -> EventKitResult<FullAccessToken> {
        // Short-circuit if permission is already decided.
        match Self::authorization_status() {
            EkAuthStatus::FullAccess => return Ok(FullAccessToken(())),
            EkAuthStatus::Denied => return Err(EventKitError::AuthorizationDenied),
            EkAuthStatus::Restricted => return Err(EventKitError::AuthorizationRestricted),
            _ => {}
        }

        let (tx, rx) = mpsc::channel::<EventKitResult<FullAccessToken>>();

        // Clone the Arc so the block can own a reference independent of `self`.
        let inner = Arc::clone(&self.inner);

        let block = block2::RcBlock::new(move |granted: Bool, _err: *mut NSError| {
            let result = if granted.as_bool() {
                Ok(FullAccessToken(()))
            } else {
                Err(EventKitError::AuthorizationDenied)
            };
            let _ = tx.send(result);
            // inner is kept alive until the block fires, ensuring EKEventStore
            // is not dropped before the callback completes.
            let _ = &inner;
        });

        unsafe {
            self.inner
                .0
                .requestFullAccessToRemindersWithCompletion(block2::RcBlock::as_ptr(&block));
        }

        rx.recv()
            .map_err(|_| EventKitError::Framework("authorization callback never fired".into()))?
    }

    /// Convenience: request write-only access.
    ///
    /// EventKit does not distinguish write-only from full access for Reminders
    /// (only Calendar events have write-only mode). This delegates to
    /// [`authorize`](Self::authorize) and wraps the result in a [`WriteOnlyToken`].
    pub fn authorize_write_only(&self) -> EventKitResult<WriteOnlyToken> {
        self.authorize().map(|_| WriteOnlyToken(()))
    }

    // ── Closure helpers ───────────────────────────────────────────────────────

    /// Authorize (if needed) then execute `f` with a [`FullAccessToken`].
    ///
    /// Mirrors `Isam::read` / `Isam::write` from `highlandcows-isam`.
    pub fn with_access<F, T>(&self, f: F) -> EventKitResult<T>
    where
        F: FnOnce(&FullAccessToken, &ReminderStore) -> EventKitResult<T>,
    {
        let token = self.authorize()?;
        f(&token, self)
    }

    // ── Reminder CRUD ─────────────────────────────────────────────────────────

    /// Fetch a single reminder by its stable identifier.
    pub fn fetch(&self, id: &str, _token: &FullAccessToken) -> EventKitResult<Option<Reminder>> {
        use objc2_foundation::NSString;

        let ns_id = NSString::from_str(id);
        let item = unsafe { self.inner.0.calendarItemWithIdentifier(&ns_id) };

        match item {
            None => Ok(None),
            Some(item) => match item.downcast::<EKReminder>() {
                Ok(ek_reminder) => Ok(Some(Reminder::from_ek(&ek_reminder))),
                Err(_) => Err(EventKitError::ReminderNotFound(id.to_owned())),
            },
        }
    }

    /// Fetch all reminders, optionally filtered to specific lists.
    ///
    /// `lists`: list identifiers to filter by; `None` fetches from all lists.
    pub fn fetch_all(
        &self,
        lists: Option<&[&str]>,
        _token: &FullAccessToken,
    ) -> EventKitResult<Vec<Reminder>> {
        self.fetch_with_predicate(lists, false)
    }

    /// Fetch only incomplete (not-yet-done) reminders.
    pub fn fetch_incomplete(
        &self,
        lists: Option<&[&str]>,
        _token: &FullAccessToken,
    ) -> EventKitResult<Vec<Reminder>> {
        self.fetch_with_predicate(lists, true)
    }

    /// Save a reminder. Returns the stable identifier assigned by EventKit.
    ///
    /// If `reminder.identifier` is `None`, a new reminder is created.
    /// If it is `Some`, EventKit updates the existing reminder with that identifier.
    pub fn save(
        &self,
        reminder: &Reminder,
        _token: &impl RemindersAccess,
    ) -> EventKitResult<String> {
        let ek = reminder.to_ek(&self.inner.0)?;

        // If the reminder has a list_identifier, set its calendar.
        if let Some(ref list_id) = reminder.list_identifier {
            use objc2_foundation::NSString;
            let ns_id = NSString::from_str(list_id);
            let cal = unsafe { self.inner.0.calendarWithIdentifier(&ns_id) };
            if let Some(cal) = cal {
                unsafe { ek.setCalendar(Some(&cal)) };
            }
        }

        match unsafe { self.inner.0.saveReminder_commit_error(&ek, true) } {
            Ok(()) => Ok(unsafe { ek.calendarItemIdentifier().to_string() }),
            Err(err) => Err(EventKitError::SaveFailed(nserror_message(&err))),
        }
    }

    /// Remove a reminder by its identifier.
    pub fn remove(&self, id: &str, _token: &FullAccessToken) -> EventKitResult<()> {
        use objc2_foundation::NSString;

        let ns_id = NSString::from_str(id);
        let ek = unsafe { self.inner.0.calendarItemWithIdentifier(&ns_id) }
            .and_then(|item| item.downcast::<EKReminder>().ok())
            .ok_or_else(|| EventKitError::ReminderNotFound(id.to_owned()))?;

        unsafe { self.inner.0.removeReminder_commit_error(&ek, true) }
            .map_err(|err| EventKitError::RemoveFailed(nserror_message(&err)))
    }

    // ── Reminder lists ────────────────────────────────────────────────────────

    /// Return all Reminder lists visible to this store.
    pub fn lists(&self, _token: &FullAccessToken) -> EventKitResult<Vec<ReminderList>> {
        let cals = unsafe { self.inner.0.calendarsForEntityType(EKEntityType::Reminder) };
        Ok(cals.iter().map(|c| ReminderList::from_ek(&c)).collect())
    }

    /// Return the default list for new reminders, if one is configured.
    pub fn default_list(
        &self,
        _token: &impl RemindersAccess,
    ) -> EventKitResult<Option<ReminderList>> {
        let cal = unsafe { self.inner.0.defaultCalendarForNewReminders() };
        Ok(cal.map(|c| ReminderList::from_ek(&c)))
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn fetch_with_predicate(
        &self,
        lists: Option<&[&str]>,
        incomplete_only: bool,
    ) -> EventKitResult<Vec<Reminder>> {
        use objc2_foundation::{NSArray, NSString};

        // Resolve list identifiers to EKCalendar objects.
        let ek_cals: Option<Vec<_>> = lists.map(|ids| {
            ids.iter()
                .filter_map(|id| {
                    let ns_id = NSString::from_str(id);
                    unsafe { self.inner.0.calendarWithIdentifier(&ns_id) }
                })
                .collect()
        });

        let ns_cals = ek_cals.as_ref().map(|v| NSArray::from_retained_slice(v));
        let predicate = unsafe {
            if incomplete_only {
                self.inner
                    .0
                    .predicateForIncompleteRemindersWithDueDateStarting_ending_calendars(
                        None,
                        None,
                        ns_cals.as_deref(),
                    )
            } else {
                self.inner
                    .0
                    .predicateForRemindersInCalendars(ns_cals.as_deref())
            }
        };

        // Bridge the async callback to a synchronous Result.
        let (tx, rx) = mpsc::channel::<EventKitResult<Vec<Reminder>>>();

        let block = block2::RcBlock::new(
            move |reminders: *mut objc2_foundation::NSArray<objc2_event_kit::EKReminder>| {
                let result = if reminders.is_null() {
                    Ok(vec![])
                } else {
                    let arr = unsafe { &*reminders };
                    Ok(arr.iter().map(|r| Reminder::from_ek(&r)).collect())
                };
                let _ = tx.send(result);
            },
        );

        unsafe {
            self.inner
                .0
                .fetchRemindersMatchingPredicate_completion(&predicate, &block);
        }

        rx.recv()
            .map_err(|_| EventKitError::Framework("fetch callback never fired".into()))?
    }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

fn nserror_message(err: &NSError) -> String {
    err.localizedDescription().to_string()
}
