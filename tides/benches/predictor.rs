//! Throughput benchmark for the offline predictor. Measures wall time to
//! generate 2-month current predictions for all harmonic + subordinate
//! stations in the bundled store, and per-call `CurrentPredictor::at` cost.
//!
//! Run with: `cargo bench -p noaa_tides --bench predictor`.

use std::time::Duration as StdDuration;

use chrono::Datelike;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::offset::Local;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use noaa_tides::STORE;

fn month_start() -> NaiveDateTime {
    let today = Local::now().date_naive();
    NaiveDate::from_ymd_opt(today.year(), today.month(), 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

/// Per-call cost of `CurrentPredictor::at`: isolates the harmonic-sum
/// hot path from event detection or offset application.
fn bench_predictor_at(c: &mut Criterion) {
    let t_ref = month_start();
    // Use a representative harmonic station (Hell Gate bin 7 — dense
    // constituent set, realistic constituent count).
    let pred = STORE
        .current_predictor("n03020", Some(7), t_ref)
        .expect("n03020 bin 7 is in the bundled store");
    let samples: Vec<NaiveDateTime> = (0..1440).map(|i| t_ref + Duration::minutes(i)).collect();

    let mut g = c.benchmark_group("predictor_at");
    g.throughput(Throughput::Elements(samples.len() as u64));
    g.bench_function("n03020_bin7_1d_1min", |b| {
        b.iter(|| {
            let mut sink = 0.0_f64;
            for t in &samples {
                let r = pred.at(*t);
                sink += r.speed + r.major + r.minor;
            }
            std::hint::black_box(sink)
        })
    });
    g.finish();
}

/// Batch throughput across all bundled stations, mirroring the historical
/// `bench_all_stations` binary but via criterion so we get statistical
/// variance. Expensive — the sample count is low and we let criterion set
/// measurement time.
fn bench_all_stations_month(c: &mut Criterion) {
    let t_ref = month_start();
    let hours = 24_i64 * 30 * 2;
    let samples: Vec<NaiveDateTime> = (0..(hours * 2))
        .map(|i| t_ref + Duration::minutes(i * 30))
        .collect();

    let harm_preds: Vec<_> = STORE
        .currents
        .iter()
        .filter_map(|e| STORE.current_predictor(&e.info.id, None, t_ref))
        .collect();
    let sub_preds: Vec<_> = STORE
        .subordinates
        .iter()
        .filter_map(|e| STORE.current_predictor(&e.offsets.ref_id, Some(e.offsets.ref_bin), t_ref))
        .collect();

    let mut g = c.benchmark_group("all_stations_2mo_30min");
    g.measurement_time(StdDuration::from_secs(20));
    g.sample_size(10);
    for (kind, preds) in [("harmonic", &harm_preds), ("subordinate", &sub_preds)] {
        g.throughput(Throughput::Elements((preds.len() * samples.len()) as u64));
        g.bench_with_input(BenchmarkId::from_parameter(kind), preds, |b, preds| {
            b.iter(|| {
                let mut sink = 0.0_f64;
                for p in preds.iter() {
                    for t in &samples {
                        let r = p.at(*t);
                        sink += r.major;
                    }
                }
                std::hint::black_box(sink)
            })
        });
    }
    g.finish();
}

criterion_group!(benches, bench_predictor_at, bench_all_stations_month);
criterion_main!(benches);
