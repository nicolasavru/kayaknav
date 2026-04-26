//! Offline validator against cached NOAA reference data under `noaa_out/`.
//! Expensive + requires fixture data, so it's gated behind `#[ignore]`.
//!
//! Run with:
//!   cargo test --release -p noaa_tides --test offline -- --ignored --nocapture
//!
//! The test expects `noaa_out/` at the workspace root (one level above the
//! tides crate). Override with `NOAA_OUT=/path/to/noaa_out`.
//!
//! Units: currents in knots, tide heights in feet. On-disk JSONs are fetched
//! with `time_zone=gmt`, so timestamps parse as UTC with no DST conversion.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use chrono::Duration;
use chrono::NaiveDateTime;
use noaa_tides::CurrentPredictor;
use noaa_tides::STORE;
use noaa_tides::SubordinateOffsets;
use noaa_tides::events::Event;
use noaa_tides::events::EventKind;
use noaa_tides::events::apply_offsets;
use noaa_tides::events::detect_events;
use noaa_tides::events::interp_events;
use noaa_tides::prelude::*;
use noaa_tides::util::CMS_PER_KNOT;
use noaa_tides::util::Stats;
use noaa_tides::util::parse_dt;
use serde_json::Value;

const YEARS: [i32; 8] = [2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032];

fn noaa_out_dir() -> PathBuf {
    if let Ok(s) = std::env::var("NOAA_OUT") {
        return PathBuf::from(s);
    }
    // tides crate's CARGO_MANIFEST_DIR -> workspace root/noaa_out
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .map(|p| p.join("noaa_out"))
        .unwrap_or_else(|| PathBuf::from("noaa_out"))
}

#[derive(Default, Clone)]
struct SubMatch {
    time_min: Stats,
    amp_kt: Stats,
    unmatched: usize,
}

#[derive(Default, Clone)]
struct StationReport {
    sample: Stats,
    matched: Option<SubMatch>,
}

fn year_t_ref_utc(year: i32) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(&format!("{year}-07-01 00:00"), "%Y-%m-%d %H:%M").unwrap()
}

fn year_bounds_utc(year: i32) -> (NaiveDateTime, NaiveDateTime) {
    let s =
        NaiveDateTime::parse_from_str(&format!("{year}-01-01 00:00"), "%Y-%m-%d %H:%M").unwrap();
    let e =
        NaiveDateTime::parse_from_str(&format!("{year}-12-31 23:59"), "%Y-%m-%d %H:%M").unwrap();
    (s, e)
}

fn nearest_same_kind(
    events: &[Event],
    t: NaiveDateTime,
    want: Option<EventKind>,
) -> Option<&Event> {
    let mut best: Option<&Event> = None;
    let mut best_dt = Duration::days(9999);
    for e in events {
        let kind_ok = match (want, e.kind) {
            (Some(k), ek) => k == ek,
            (None, EventKind::SlackBeforeEbb | EventKind::SlackBeforeFlood) => true,
            (None, _) => false,
        };
        if !kind_ok {
            continue;
        }
        let d = (e.t - t).abs();
        if d < best_dt {
            best_dt = d;
            best = Some(e);
        }
    }
    if best_dt > Duration::minutes(180) {
        None
    } else {
        best
    }
}

fn validate_current_station(root: &Path, id: &str) -> Result<Option<StationReport>> {
    let is_sub = STORE.subordinate(id).is_some();
    let mut stats = Stats::default();
    let mut matched = if is_sub {
        Some(SubMatch::default())
    } else {
        None
    };

    for y in YEARS {
        let path: PathBuf = root.join(format!("{y}_currents_nyc/{id}.json"));
        if !path.exists() {
            continue;
        }
        let raw: Value = serde_json::from_str(&fs::read_to_string(&path)?)
            .with_context(|| format!("parse {}", path.display()))?;
        let events = raw["current_predictions"]["cp"]
            .as_array()
            .context("missing cp array")?;
        if events.is_empty() {
            continue;
        }
        let t_ref_utc = year_t_ref_utc(y);

        if is_sub {
            let entry = STORE.subordinate(id).unwrap();
            let offsets: SubordinateOffsets = entry.offsets.clone();
            let Some(ref_pred) =
                STORE.current_predictor(&offsets.ref_id, Some(offsets.ref_bin), t_ref_utc)
            else {
                continue;
            };
            let (t_start, t_end) = year_bounds_utc(y);
            let raw_events = detect_events(
                &ref_pred,
                t_start - Duration::hours(6),
                t_end + Duration::hours(6),
            );
            let our_events = apply_offsets(&raw_events, &offsets);

            for ev in events {
                let t_utc = parse_dt(ev["Time"].as_str().unwrap())?;
                let noaa_major = ev["Velocity_Major"].as_f64().unwrap_or(0.0);
                let ours_kt = interp_events(&our_events, t_utc);
                stats.push(ours_kt - noaa_major);

                let noaa_type = ev["Type"].as_str().unwrap_or("");
                let want = match noaa_type {
                    "flood" => Some(EventKind::MaxFlood),
                    "ebb" => Some(EventKind::MaxEbb),
                    "slack" => None,
                    _ => continue,
                };
                let m = matched.as_mut().unwrap();
                match nearest_same_kind(&our_events, t_utc, want) {
                    Some(best) => {
                        let dt_min = (best.t - t_utc).num_seconds() as f64 / 60.0;
                        m.time_min.push(dt_min);
                        m.amp_kt.push(best.speed_kt - noaa_major);
                    },
                    None => m.unmatched += 1,
                }
            }
        } else {
            let mut current_bin: Option<(i32, CurrentPredictor)> = None;
            for ev in events {
                let t_utc = parse_dt(ev["Time"].as_str().unwrap())?;
                let bin: i32 = ev["Bin"].as_str().and_then(|s| s.parse().ok()).unwrap_or(1);
                let pred = match &current_bin {
                    Some((b, p)) if *b == bin => p,
                    _ => {
                        let Some(p) = STORE.current_predictor(id, Some(bin), t_ref_utc) else {
                            current_bin = None;
                            continue;
                        };
                        current_bin = Some((bin, p));
                        &current_bin.as_ref().unwrap().1
                    },
                };
                let sample = pred.at(t_utc);
                if let Some(speed_s) = ev.get("Speed").and_then(|v| v.as_str()) {
                    let noaa_kt: f64 = speed_s.parse()?;
                    stats.push(sample.speed / CMS_PER_KNOT - noaa_kt);
                } else if let Some(vmaj) =
                    ev.get("Velocity_Major").and_then(serde_json::Value::as_f64)
                {
                    stats.push(sample.major / CMS_PER_KNOT - vmaj);
                }
            }
        }
    }

    if stats.n == 0 {
        Ok(None)
    } else {
        Ok(Some(StationReport {
            sample: stats,
            matched,
        }))
    }
}

/// Battery tide validation. Returns (raw height stats, bias-corrected stats,
/// time-error stats, bias). Our harcon store carries no datum offset (Z0) so
/// raw predictions are MSL-relative while the NOAA file is MLLW-relative —
/// the raw stats capture that datum gap; the detrended stats capture the
/// true prediction-shape error.
fn validate_battery_tides(root: &Path) -> Result<(Stats, Stats, Stats, f64)> {
    let id = "8518750";
    let mut raw_value = Stats::default();
    let mut time_err = Stats::default();
    let mut pairs: Vec<(f64, f64)> = Vec::new();
    for y in YEARS {
        let path = root.join(format!("tides_battery/{y}.json"));
        if !path.exists() {
            continue;
        }
        let raw: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;
        let events = raw["predictions"].as_array().context("predictions")?;
        let t_ref_utc = year_t_ref_utc(y);
        let Some(predictor) = STORE.tide_predictor(id, t_ref_utc) else {
            continue;
        };
        for ev in events {
            let t_utc = parse_dt(ev["t"].as_str().unwrap())?;
            let v_noaa: f64 = ev["v"].as_str().unwrap().parse()?;
            let v_ours = predictor.at(t_utc);
            raw_value.push(v_ours - v_noaa);
            pairs.push((v_ours, v_noaa));

            let want_high = ev["type"].as_str() == Some("H");
            let mut best_dm: i64 = 0;
            let mut best_v = predictor.at(t_utc);
            for dm in -120..=120_i64 {
                let v = predictor.at(t_utc + Duration::minutes(dm));
                let better = if want_high { v > best_v } else { v < best_v };
                if better {
                    best_v = v;
                    best_dm = dm;
                }
            }
            time_err.push(best_dm as f64);
        }
    }
    let bias: f64 = if pairs.is_empty() {
        0.0
    } else {
        pairs.iter().map(|(a, b)| a - b).sum::<f64>() / pairs.len() as f64
    };
    let mut detrended = Stats::default();
    for (ours, noaa) in &pairs {
        detrended.push((ours - bias) - noaa);
    }
    Ok((raw_value, detrended, time_err, bias))
}

fn collect_station_ids(root: &Path) -> Result<Vec<String>> {
    let dir = root.join("2025_currents_nyc");
    let mut ids = Vec::new();
    for entry in fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let name = entry?.file_name().to_string_lossy().to_string();
        if let Some(stem) = name.strip_suffix(".json") {
            ids.push(stem.to_string());
        }
    }
    ids.sort();
    Ok(ids)
}

/// Baseline accuracy target. Recorded from the pre-refactor validator run and
/// used here to catch regressions. Numbers are intentionally loose — the
/// per-memory mean errors are ~0.011 kt for harmonics and ~0.92 min for
/// subordinate time, so these thresholds give ~3× headroom.
const MAX_MEAN_CURRENT_ERR_KT: f64 = 0.05;
const MAX_MEAN_SUB_TIME_MIN: f64 = 3.0;
const MAX_MEAN_TIDE_ERR_DETRENDED_FT: f64 = 0.20;

#[test]
#[ignore]
fn offline_validation() -> Result<()> {
    let root = noaa_out_dir();
    if !root.exists() {
        panic!(
            "noaa_out directory not found at {}; set NOAA_OUT env var or populate it",
            root.display()
        );
    }
    let t0 = std::time::Instant::now();
    let ids = collect_station_ids(&root)?;
    println!(
        "Validating {} current stations × {} years from {}",
        ids.len(),
        YEARS.len(),
        root.display()
    );
    println!(
        "{:<10} {:>4} {:>10} {:>10} {:>10}",
        "station", "type", "n", "mean_kt", "max_kt"
    );

    let mut agg = Stats::default();
    let mut agg_t = Stats::default();
    let mut agg_a = Stats::default();
    let mut agg_unmatched = 0_usize;
    let mut per_station: Vec<(String, StationReport, &'static str)> = Vec::new();
    let mut missing: Vec<String> = Vec::new();

    for id in &ids {
        let typ = if STORE.subordinate(id).is_some() {
            "sub"
        } else if STORE.current_bins(id).is_some() {
            "harm"
        } else {
            missing.push(id.clone());
            continue;
        };
        match validate_current_station(&root, id)? {
            Some(rep) => {
                println!(
                    "{:<10} {:>4} {:>10} {:>10.4} {:>10.4}",
                    id,
                    typ,
                    rep.sample.n,
                    rep.sample.mean(),
                    rep.sample.max_abs
                );
                agg.merge(&rep.sample);
                if let Some(m) = &rep.matched {
                    agg_t.merge(&m.time_min);
                    agg_a.merge(&m.amp_kt);
                    agg_unmatched += m.unmatched;
                }
                per_station.push((id.clone(), rep, typ));
            },
            None => missing.push(id.clone()),
        }
    }

    println!("\n=== aggregate (NYC currents, all stations, all years) ===");
    println!(
        "sample-at-noaa-time:        n={} mean |err|={:.4} kt max |err|={:.4} kt",
        agg.n,
        agg.mean(),
        agg.max_abs
    );
    println!(
        "event-matched (subs only):  n={} time mean={:.2} min max={:.1} min, amp mean={:.4} kt max={:.4} kt, unmatched={agg_unmatched}",
        agg_t.n,
        agg_t.mean(),
        agg_t.max_abs,
        agg_a.mean(),
        agg_a.max_abs
    );
    if !missing.is_empty() {
        println!("stations with no data: {}", missing.len());
    }

    let (raw, detrended, tt, bias) = validate_battery_tides(&root)?;
    println!("\n=== tides at Battery (8518750) ===");
    println!(
        "height (raw):        n={} mean {:.4} ft  max {:.4} ft  (residual bias {:+.4} ft)",
        raw.n,
        raw.mean(),
        raw.max_abs,
        bias
    );
    println!(
        "height (bias-free):  n={} mean {:.4} ft  max {:.4} ft",
        detrended.n,
        detrended.mean(),
        detrended.max_abs
    );
    println!(
        "time:                n={} mean {:.4} min max {:.0} min",
        tt.n,
        tt.mean(),
        tt.max_abs
    );

    println!("\nelapsed: {:.2?}", t0.elapsed());

    // Regression guards. Loose, but will catch a serious regression.
    assert!(
        agg.mean() < MAX_MEAN_CURRENT_ERR_KT,
        "aggregate current mean {:.4} kt exceeds {:.4} kt",
        agg.mean(),
        MAX_MEAN_CURRENT_ERR_KT
    );
    assert!(
        agg_t.mean() < MAX_MEAN_SUB_TIME_MIN,
        "subordinate event-time mean {:.2} min exceeds {:.2} min",
        agg_t.mean(),
        MAX_MEAN_SUB_TIME_MIN
    );
    assert!(
        detrended.mean() < MAX_MEAN_TIDE_ERR_DETRENDED_FT,
        "Battery tide detrended mean {:.4} ft exceeds {:.4} ft",
        detrended.mean(),
        MAX_MEAN_TIDE_ERR_DETRENDED_FT
    );
    Ok(())
}
