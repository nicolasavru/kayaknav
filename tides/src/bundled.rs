//! Process-wide access to the NOAA harmonic-constants store embedded at
//! compile-time, plus small time-zone and slot-rounding helpers used by
//! prediction consumers.
//!
//! The store is decompressed on first access and then held for the life of
//! the process — all tide and current predictors share this single copy, so
//! there is no per-call allocation or network I/O. Consumers that never
//! touch `STORE` don't pay the decompression cost.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;

use chrono::Local;
use chrono::NaiveDateTime;
use chrono::TimeZone;

use crate::events::Event;
use crate::events::detect_events;
use crate::prelude::*;
use crate::store::HarconStore;

/// NOAA harmonic constants for every predictable station, embedded at
/// compile-time from `data/harcons.bin.zst` (zstd-level-22 bitcode, ~1 MB).
static HARCON_BYTES_ZSTD: &[u8] = include_bytes!("../data/harcons.bin.zst");

/// Lazily-decoded bundled store. First access pays the zstd decompression
/// cost (~40 ms on a modern CPU); subsequent accesses are a plain pointer
/// load.
pub static STORE: LazyLock<HarconStore> = LazyLock::new(|| {
    HarconStore::from_binary_zstd(HARCON_BYTES_ZSTD)
        .expect("embedded harcons.bin.zst decodes cleanly")
});

/// Interpret a naive datetime as local wall-clock time and convert to UTC.
/// Falls back gracefully during DST ambiguity by taking the earliest (or
/// latest) valid interpretation, so the function never panics or loses a
/// timestamp across the spring-forward / fall-back transitions.
pub fn local_to_utc(naive_local: NaiveDateTime) -> NaiveDateTime {
    let lr = Local.from_local_datetime(&naive_local);
    lr.earliest()
        .or_else(|| lr.latest())
        .map(|dt| dt.naive_utc())
        .unwrap_or(naive_local)
}

/// Snap a naive datetime to the nearest 30-minute boundary. Used to pin
/// event times onto UI prediction grids whose slots are 30 minutes apart.
pub fn round_to_30m(t: NaiveDateTime) -> NaiveDateTime {
    let ms = t.and_utc().timestamp_millis();
    let step = 30 * 60 * 1000;
    let rounded_ms = ((ms + step / 2).div_euclid(step)) * step;
    chrono::DateTime::from_timestamp_millis(rounded_ms)
        .unwrap()
        .naive_utc()
}

/// Process-wide cache of reference-station event timelines. Keyed on
/// `(ref_id, ref_bin, start, end)` — many subordinate stations share the
/// same reference predictor (in the bundled store, ~1,941 subordinates
/// resolve to ~64 unique references), and recomputing events per
/// subordinate is the dominant cost of startup prediction.
static REFERENCE_EVENTS: LazyLock<
    Mutex<HashMap<(String, i32, NaiveDateTime, NaiveDateTime), Arc<Vec<Event>>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Return the cached event timeline for `(ref_id, ref_bin)` over
/// `[start, end]`, computing it against [`STORE`] on the first call. The
/// returned `Arc` is shared by every caller with the same key, so downstream
/// consumers can clone freely without re-running event detection.
pub fn cached_reference_events(
    ref_id: &str,
    ref_bin: i32,
    t_ref_utc: NaiveDateTime,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Result<Arc<Vec<Event>>> {
    let key = (ref_id.to_string(), ref_bin, start, end);
    if let Some(ev) = REFERENCE_EVENTS.lock().unwrap().get(&key) {
        return Ok(ev.clone());
    }
    let predictor = STORE
        .current_predictor(ref_id, Some(ref_bin), t_ref_utc)
        .ok_or_else(|| anyhow!("no harcon for reference station {ref_id} bin {ref_bin}"))?;
    let ev = Arc::new(detect_events(&predictor, start, end));
    REFERENCE_EVENTS.lock().unwrap().insert(key, ev.clone());
    Ok(ev)
}
