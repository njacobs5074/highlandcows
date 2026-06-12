use objc2::rc::Retained;
use objc2_event_kit::EKEventStore;

/// Wraps `Retained<EKEventStore>` to make it `Send + Sync`.
///
/// Safety: EKEventStore is documented to be safe to use from any thread.
/// We additionally protect all mutation through EventKit's own serialization.
pub(crate) struct Inner(pub(crate) Retained<EKEventStore>);

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}
