//! Benchmark: canonical coarse/fine event detection vs a naive dense 1-min
//! sweep. The coarse/fine default is expected to be ~4× faster with zero
//! missed events and bit-identical match quality — see the comment on
//! [`noaa_tides::events::detect_events`].
//!
//! Run with: `cargo bench -p noaa_tides --bench event_detection`.

use std::collections::BTreeMap;
use std::time::Duration as StdDuration;

use chrono::Datelike;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::offset::Local;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use noaa_tides::CurrentPredictor;
use noaa_tides::STORE;
use noaa_tides::events::DEFAULT_COARSE_SEC;
use noaa_tides::events::detect_events_with_step;
use noaa_tides::util::CMS_PER_KNOT;

/// Naive baseline: dense 1-min sweep. Kept here as a bench baseline only;
/// production code uses [`detect_events_with_step`] with the default coarse
/// step.
fn detect_dense_1min(pred: &CurrentPredictor, start: NaiveDateTime, end: NaiveDateTime) -> usize {
    let step_sec = 60i64;
    let n = ((end - start).num_seconds() / step_sec) as usize + 1;
    let mut v: Vec<(NaiveDateTime, f64)> = Vec::with_capacity(n);
    for i in 0..n {
        let t = start + Duration::seconds(i as i64 * step_sec);
        v.push((t, pred.at(t).major / CMS_PER_KNOT));
    }
    let mut count = 0usize;
    for i in 1..v.len() - 1 {
        let (_, a) = v[i - 1];
        let (_, b) = v[i];
        let (_, c) = v[i + 1];
        let denom = a - 2.0 * b + c;
        if denom.abs() > 1e-12 && (b - a).signum() != (c - b).signum() {
            let x = 0.5 * (a - c) / denom;
            if x.abs() <= 1.0 {
                count += 1;
            }
        }
    }
    for i in 0..v.len() - 1 {
        let (_, a) = v[i];
        let (_, b) = v[i + 1];
        if a == 0.0 && b == 0.0 {
            continue;
        }
        if (a <= 0.0 && b > 0.0) || (a >= 0.0 && b < 0.0) {
            count += 1;
        }
    }
    std::hint::black_box(count)
}

fn unique_reference_predictors(t_ref: NaiveDateTime) -> Vec<CurrentPredictor> {
    let mut refs: BTreeMap<(String, i32), CurrentPredictor> = BTreeMap::new();
    for s in &STORE.subordinates {
        let key = (s.offsets.ref_id.clone(), s.offsets.ref_bin);
        if refs.contains_key(&key) {
            continue;
        }
        if let Some(p) = STORE.current_predictor(&s.offsets.ref_id, Some(s.offsets.ref_bin), t_ref)
        {
            refs.insert(key, p);
        }
    }
    refs.into_values().collect()
}

fn bench_event_detection(c: &mut Criterion) {
    let today = Local::now().date_naive();
    let t0 = NaiveDate::from_ymd_opt(today.year(), today.month(), 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let hours: i64 = 24 * 60;
    let pad = Duration::hours(3);
    let start = t0 - pad;
    let end = t0 + Duration::hours(hours) + pad;
    let t_ref = t0 + Duration::hours(hours / 2);
    let refs = unique_reference_predictors(t_ref);

    let mut g = c.benchmark_group("event_detection_60d");
    g.measurement_time(StdDuration::from_secs(15));
    g.sample_size(10);

    g.bench_function("dense_1min_all_refs", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for p in &refs {
                total += detect_dense_1min(p, start, end);
            }
            std::hint::black_box(total)
        })
    });

    for coarse_sec in [120i64, DEFAULT_COARSE_SEC, 600, 900] {
        g.bench_with_input(
            BenchmarkId::new("coarse_fine", coarse_sec),
            &coarse_sec,
            |b, &coarse| {
                b.iter(|| {
                    let mut total = 0usize;
                    for p in &refs {
                        total += detect_events_with_step(p, start, end, coarse).len();
                    }
                    std::hint::black_box(total)
                })
            },
        );
    }
    g.finish();
}

criterion_group!(benches, bench_event_detection);
criterion_main!(benches);
