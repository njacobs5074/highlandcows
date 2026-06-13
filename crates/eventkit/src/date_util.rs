use chrono::{DateTime, Utc};
use objc2::rc::Retained;
use objc2_foundation::{NSDate, NSDateComponents};

/// NSDate reference date is 2001-01-01 00:00:00 UTC; Unix epoch is 1970-01-01.
pub(crate) const APPLE_EPOCH_OFFSET: f64 = 978_307_200.0;

pub(crate) fn nsdate_to_utc(date: Retained<NSDate>) -> Option<DateTime<Utc>> {
    let secs_since_apple = date.timeIntervalSinceReferenceDate();
    let unix_secs = secs_since_apple + APPLE_EPOCH_OFFSET;
    let secs = unix_secs.trunc() as i64;
    let nanos = (unix_secs.fract().abs() * 1e9) as u32;
    DateTime::from_timestamp(secs, nanos)
}

pub(crate) fn nsdate_components_to_utc(
    components: Retained<NSDateComponents>,
) -> Option<DateTime<Utc>> {
    use objc2_foundation::{NSCalendar, NSCalendarIdentifierGregorian, NSString, NSTimeZone};
    let cal = NSCalendar::calendarWithIdentifier(unsafe { NSCalendarIdentifierGregorian })?;
    let utc = NSTimeZone::timeZoneWithName(&NSString::from_str("UTC"))?;
    cal.setTimeZone(&utc);
    let date = cal.dateFromComponents(&components)?;
    nsdate_to_utc(date)
}

pub(crate) fn utc_to_nsdate_components(dt: &DateTime<Utc>) -> Retained<NSDateComponents> {
    use chrono::{Datelike, Timelike};

    let components = NSDateComponents::new();
    components.setYear(dt.year() as _);
    components.setMonth(dt.month() as _);
    components.setDay(dt.day() as _);
    components.setHour(dt.hour() as _);
    components.setMinute(dt.minute() as _);
    components.setSecond(dt.second() as _);
    components
}

pub(crate) fn utc_to_nsdate(dt: &DateTime<Utc>) -> Retained<NSDate> {
    let unix_secs = dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1e9;
    let apple_secs = unix_secs - APPLE_EPOCH_OFFSET;
    NSDate::dateWithTimeIntervalSinceReferenceDate(apple_secs)
}
