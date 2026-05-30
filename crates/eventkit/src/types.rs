use objc2_event_kit::{EKAuthorizationStatus, EKEntityType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EkEntityType {
    Event,
    Reminder,
}

impl EkEntityType {
    pub(crate) fn to_ek(self) -> EKEntityType {
        match self {
            EkEntityType::Event => EKEntityType::Event,
            EkEntityType::Reminder => EKEntityType::Reminder,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EkAuthStatus {
    NotDetermined,
    Restricted,
    Denied,
    FullAccess,
    WriteOnly,
}

impl From<EKAuthorizationStatus> for EkAuthStatus {
    fn from(s: EKAuthorizationStatus) -> Self {
        // EKAuthorizationStatusFullAccess (3) == EKAuthorizationStatusAuthorized (3, deprecated)
        match s.0 {
            1 => EkAuthStatus::Restricted,
            2 => EkAuthStatus::Denied,
            3 => EkAuthStatus::FullAccess,
            4 => EkAuthStatus::WriteOnly,
            _ => EkAuthStatus::NotDetermined,
        }
    }
}
