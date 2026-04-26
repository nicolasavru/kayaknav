//! Small helpers shared across `noaa_tides` binaries, tests, and benches —
//! unit conversions, timestamp parsing, geographic filters, and a running
//! error-stats accumulator. Nothing here is library-internal; all items are
//! `pub` so the `bin/`, `tests/`, and `benches/` trees can reuse a single
//! canonical definition instead of redefining constants and helpers.

use chrono::NaiveDate;
use chrono::NaiveDateTime;

use crate::noaa::StationInfo;
use crate::prelude::*;

/// cm/s per knot. NOAA current amplitudes arrive in cm/s; knots are the
/// kayaknav/UI unit.
pub const CMS_PER_KNOT: f64 = 51.4444;

/// Parse a NOAA `"YYYY-MM-DD HH:MM"` timestamp into a naive datetime. The
/// on-disk JSONs are fetched with `time_zone=gmt`, so the result is UTC.
pub fn parse_dt(s: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M").with_context(|| format!("parse '{s}'"))
}

/// Parse a `YYYY-MM-DD` date string. The `String` signature lets this drop
/// into `#[bpaf(parse(parse_date))]` CLI positional hooks.
pub fn parse_date(s: String) -> std::result::Result<NaiveDate, String> {
    NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|e| format!("bad date '{s}': {e}"))
}

/// Latitude bounds of the ~1°×1° region around Battery, NY used by
/// validation and benchmark harnesses.
pub const NYC_LAT: (f64, f64) = (39.7, 41.7);
/// Longitude bounds of the NYC bounding box (see [`NYC_LAT`]).
pub const NYC_LON: (f64, f64) = (-75.0, -73.0);

/// True if the station's lat/lon lies inside the NYC bounding box. Does not
/// filter on `station_type` — callers compose that themselves.
pub fn in_nyc_box(s: &StationInfo) -> bool {
    matches!(s.lat, Some(lat) if (NYC_LAT.0..=NYC_LAT.1).contains(&lat))
        && matches!(s.lon, Some(lon) if (NYC_LON.0..=NYC_LON.1).contains(&lon))
}

/// Running mean-abs, RMS, and peak-abs accumulator for signed errors.
/// Signed values are folded into absolute-value and squared aggregates on
/// `push`, so the struct can later report both mean-|err| and RMS without
/// holding the raw sample stream.
#[derive(Default, Clone)]
pub struct Stats {
    pub n: usize,
    pub sum_abs: f64,
    pub max_abs: f64,
    pub sq: f64,
}

impl Stats {
    pub fn push(&mut self, e: f64) {
        let a = e.abs();
        self.n += 1;
        self.sum_abs += a;
        self.sq += e * e;
        if a > self.max_abs {
            self.max_abs = a;
        }
    }

    pub fn mean(&self) -> f64 {
        if self.n > 0 {
            self.sum_abs / self.n as f64
        } else {
            0.0
        }
    }

    pub fn rms(&self) -> f64 {
        if self.n > 0 {
            (self.sq / self.n as f64).sqrt()
        } else {
            0.0
        }
    }

    pub fn merge(&mut self, o: &Self) {
        self.n += o.n;
        self.sum_abs += o.sum_abs;
        self.sq += o.sq;
        if o.max_abs > self.max_abs {
            self.max_abs = o.max_abs;
        }
    }
}

/// NOAA datagetter product variant. Every `Product` carries the parameters
/// that belong to it (bin for currents, datum for tides), so callers pass one
/// value rather than juggling several free-floating query args.
pub enum Product {
    /// `product=predictions&datum=MLLW&interval={interval}`. Typical intervals
    /// are `"h"` (hourly heights) and `"hilo"` (high/low events).
    TidePredictions { interval: &'static str },
    /// `product=currents_predictions&interval=h&vel_type=speed_dir`, optionally
    /// pinned to a specific bin number.
    CurrentsHourly { bin: Option<i32> },
    /// `product=currents_predictions&interval=max_slack` — slack-before-ebb /
    /// slack-before-flood / max-flood / max-ebb events.
    CurrentsMaxSlack { bin: Option<i32> },
}

/// Build a NOAA `datagetter` URL for the given station, date range, and
/// [`Product`]. Dates are formatted as `YYYYMMDD` per NOAA's API, everything
/// is GMT (`time_zone=gmt`) in English units (`units=english`), and the
/// identifying `application=noaa_tides` is always set.
pub fn datagetter_url(
    station: &str,
    begin: NaiveDate,
    end: NaiveDate,
    product: &Product,
) -> String {
    let bin_suffix = |bin: &Option<i32>| bin.map(|b| format!("&bin={b}")).unwrap_or_default();
    let (name, extra) = match product {
        Product::TidePredictions { interval } => (
            "predictions".to_string(),
            format!("&datum=MLLW&interval={interval}"),
        ),
        Product::CurrentsHourly { bin } => (
            "currents_predictions".to_string(),
            format!("&interval=h&vel_type=speed_dir{}", bin_suffix(bin)),
        ),
        Product::CurrentsMaxSlack { bin } => (
            "currents_predictions".to_string(),
            format!("&interval=max_slack&vel_type=default{}", bin_suffix(bin)),
        ),
    };
    format!(
        "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter\
         ?time_zone=gmt&units=english&application=noaa_tides&format=json\
         &product={name}&station={station}&begin_date={}&end_date={}{extra}",
        begin.format("%Y%m%d"),
        end.format("%Y%m%d"),
    )
}
