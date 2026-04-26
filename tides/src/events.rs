//! Event-based sampling of the signed major-axis speed of a harmonic-current
//! predictor. The event timeline — max-flood peaks, max-ebb troughs, and
//! slack zero-crossings — is what NOAA publishes for subordinate stations,
//! and it's what kayaknav's subordinate model consumes.
//!
//! Two-pass event detection: a coarse sweep locates candidate extrema (by
//! slope sign change) and zero-crossing brackets, then a fine 1-minute pass
//! around each candidate pins the time and amplitude via parabolic-vertex
//! refinement (for extrema) or linear interpolation (for zero crossings).
//! With a 5-min coarse step the result is bit-identical to a pure 1-min
//! dense sweep on a 60-day window, at ~4× the speed.
//!
//! Shape mirrors NOAA's `max_slack` product: events are emitted in time
//! order, with each extremum labeled flood/ebb by the sign of the signed
//! major-axis speed, and each zero crossing labeled slack-before-flood or
//! slack-before-ebb by the direction the signal is moving through zero.
//!
//! ```no_run
//! use noaa_tides::events::{detect_events, apply_offsets, interp_events};
//! # fn demo(pred: &noaa_tides::CurrentPredictor,
//! #         offsets: &noaa_tides::SubordinateOffsets,
//! #         start: chrono::NaiveDateTime,
//! #         end: chrono::NaiveDateTime,
//! #         t: chrono::NaiveDateTime) {
//! let ref_events = detect_events(pred, start, end);
//! let sub_events = apply_offsets(&ref_events, offsets);
//! let speed_kt = interp_events(&sub_events, t);
//! # }
//! ```
use chrono::Duration;
use chrono::NaiveDateTime;

use crate::SubordinateOffsets;
use crate::predictor::CurrentPredictor;
use crate::util::CMS_PER_KNOT;

/// Default coarse-pass step. At 5 minutes, the full US subordinate benchmark
/// matches a 1-minute dense sweep with zero missed events and bit-identical
/// time/amplitude on every match.
pub const DEFAULT_COARSE_SEC: i64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    MaxFlood,
    MaxEbb,
    SlackBeforeEbb,
    SlackBeforeFlood,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub kind: EventKind,
    pub t: NaiveDateTime,
    pub speed_kt: f64,
}

/// Detect events across `[start, end]` using the default coarse/fine pass
/// (5-min coarse, 1-min fine).
pub fn detect_events(
    predictor: &CurrentPredictor,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Vec<Event> {
    detect_events_with_step(predictor, start, end, DEFAULT_COARSE_SEC)
}

/// Like [`detect_events`] but with a configurable coarse step. Useful for
/// benchmarking; production callers should use [`detect_events`].
pub fn detect_events_with_step(
    predictor: &CurrentPredictor,
    start: NaiveDateTime,
    end: NaiveDateTime,
    coarse_sec: i64,
) -> Vec<Event> {
    let fine_sec: i64 = 60;
    let half_win_sec: i64 = coarse_sec;

    let n = ((end - start).num_seconds() / coarse_sec) as usize + 1;
    let mut v: Vec<(NaiveDateTime, f64)> = Vec::with_capacity(n);
    for i in 0..n {
        let t = start + Duration::seconds(i as i64 * coarse_sec);
        v.push((t, predictor.at(t).major / CMS_PER_KNOT));
    }

    let mut events = Vec::new();

    for i in 1..v.len() - 1 {
        let (_, a) = v[i - 1];
        let (tb, b) = v[i];
        let (_, c) = v[i + 1];
        if (b - a).signum() == (c - b).signum() {
            continue;
        }
        let kind_is_max = b > a;
        let r_start = tb - Duration::seconds(half_win_sec);
        let r_end = tb + Duration::seconds(half_win_sec);
        let m = ((r_end - r_start).num_seconds() / fine_sec) as usize + 1;
        let mut fv: Vec<f64> = Vec::with_capacity(m);
        let mut best_j = 0usize;
        let mut best = if kind_is_max {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
        for j in 0..m {
            let t = r_start + Duration::seconds(j as i64 * fine_sec);
            let s = predictor.at(t).major / CMS_PER_KNOT;
            fv.push(s);
            if (kind_is_max && s > best) || (!kind_is_max && s < best) {
                best = s;
                best_j = j;
            }
        }
        if best_j == 0 || best_j == m - 1 {
            continue;
        }
        let pa = fv[best_j - 1];
        let pb = fv[best_j];
        let pc = fv[best_j + 1];
        let denom = pa - 2.0 * pb + pc;
        if denom.abs() <= 1e-12 {
            continue;
        }
        let x = 0.5 * (pa - pc) / denom;
        if x.abs() > 1.0 {
            continue;
        }
        let peak = pb - (pa - pc) * (pa - pc) / (8.0 * denom);
        let tbj = r_start + Duration::seconds(best_j as i64 * fine_sec);
        let t_peak = tbj + Duration::milliseconds((x * fine_sec as f64 * 1000.0) as i64);
        let kind = if kind_is_max {
            EventKind::MaxFlood
        } else {
            EventKind::MaxEbb
        };
        events.push(Event {
            kind,
            t: t_peak,
            speed_kt: peak,
        });
    }

    for i in 0..v.len() - 1 {
        let (ta, a) = v[i];
        let (tb, b) = v[i + 1];
        if a == 0.0 && b == 0.0 {
            continue;
        }
        if !((a <= 0.0 && b > 0.0) || (a >= 0.0 && b < 0.0)) {
            continue;
        }
        let rising = b > a;
        let m = ((tb - ta).num_seconds() / fine_sec) as usize;
        let mut prev_t = ta;
        let mut prev_v = a;
        for j in 1..=m {
            let t = ta + Duration::seconds(j as i64 * fine_sec);
            let s = predictor.at(t).major / CMS_PER_KNOT;
            if (prev_v <= 0.0 && s > 0.0) || (prev_v >= 0.0 && s < 0.0) {
                let frac = -prev_v / (s - prev_v);
                let span_ms = (t - prev_t).num_milliseconds() as f64;
                let t0 = prev_t + Duration::milliseconds((frac * span_ms) as i64);
                let kind = if rising {
                    EventKind::SlackBeforeFlood
                } else {
                    EventKind::SlackBeforeEbb
                };
                events.push(Event {
                    kind,
                    t: t0,
                    speed_kt: 0.0,
                });
                break;
            }
            prev_t = t;
            prev_v = s;
        }
    }

    events.sort_by_key(|e| e.t);
    events
}

/// Apply a subordinate station's time and amplitude offsets to a reference
/// station's event timeline, producing the subordinate's event timeline.
pub fn apply_offsets(events: &[Event], o: &SubordinateOffsets) -> Vec<Event> {
    events
        .iter()
        .map(|e| {
            let (dt_min, amp) = match e.kind {
                EventKind::MaxFlood => (o.mfc_time_min, o.mfc_amp),
                EventKind::MaxEbb => (o.mec_time_min, o.mec_amp),
                EventKind::SlackBeforeEbb => (o.sbe_time_min, 1.0),
                EventKind::SlackBeforeFlood => (o.sbf_time_min, 1.0),
            };
            Event {
                kind: e.kind,
                t: e.t + Duration::seconds((dt_min * 60.0) as i64),
                speed_kt: e.speed_kt * amp,
            }
        })
        .collect()
}

/// Linearly interpolate the signed major-axis speed between the two events
/// that bracket `t`. Outside the event range, clamp to the nearest endpoint.
pub fn interp_events(events: &[Event], t: NaiveDateTime) -> f64 {
    if events.is_empty() {
        return 0.0;
    }
    if t <= events[0].t {
        return events[0].speed_kt;
    }
    if t >= events[events.len() - 1].t {
        return events[events.len() - 1].speed_kt;
    }
    let idx = events.partition_point(|e| e.t <= t);
    let a = &events[idx - 1];
    let b = &events[idx];
    let span = (b.t - a.t).num_milliseconds() as f64;
    if span <= 0.0 {
        return a.speed_kt;
    }
    let frac = (t - a.t).num_milliseconds() as f64 / span;
    a.speed_kt + frac * (b.speed_kt - a.speed_kt)
}
