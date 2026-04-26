use chrono::DateTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::Utc;
use sunrise::Coordinates;
use sunrise::SolarDay;
use sunrise::SolarEvent;

/// Sunset as UTC for `(lat, lon)` on `date` (the local *calendar* date).
/// Returns `None` for polar-day/night edge cases or invalid coordinates
/// (sunrise::Coordinates::new rejects out-of-range lat/lon). Passing the
/// caller's local date keeps semantics intuitive: "today's sunset" even
/// if UTC has already rolled over.
pub fn sunset_utc(lat: f64, lon: f64, date: NaiveDate) -> Option<DateTime<Utc>> {
    let coord = Coordinates::new(lat, lon)?;
    SolarDay::new(coord, date).event_time(SolarEvent::Sunset)
}

/// Convenience: sunset as a UTC `NaiveDateTime`, matching the rest of
/// kayaknav's tide pipeline (which stores absolute timestamps as naive
/// UTC in `all_sweep_start_indices` / `materialize_sweep_df`).
pub fn sunset_naive_utc(lat: f64, lon: f64, date: NaiveDate) -> Option<NaiveDateTime> {
    sunset_utc(lat, lon, date).map(|dt| dt.naive_utc())
}
