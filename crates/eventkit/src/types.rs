use objc2_event_kit::EKAuthorizationStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EkEntityType {
    Event,
    Reminder,
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
