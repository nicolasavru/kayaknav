//! Diagnostic tool for NOAA tide/current predictions from the bundled
//! harcon store. Subcommands:
//!
//! * `dst` — probe chrono's DST-ambiguity interpretation.
//! * `samples STATION BIN DATE` — 1-min major-axis samples + extrema.
//! * `harcon-diff STATION [BIN]` — diff bundled harcon vs NOAA live harcon.
//! * `dst-sweep STATION BIN DATE HOUR [MINUTES]` — 1-min samples labelled
//!   in both US Eastern wall-clock offsets and UTC.
//! * `subordinate-errors STATION [NOAA_OUT] [Y_MIN] [Y_MAX]` — dump
//!   per-event errors >0.2 kt vs cached NOAA JSON under `noaa_out/`.
//!
//! Run `cargo run --bin diag -- <subcommand> [args...]`.

use std::fs;

use bpaf::Bpaf;
use chrono::Datelike;
use chrono::Duration;
use chrono::Local;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use noaa_tides::Client;
use noaa_tides::Event;
use noaa_tides::STORE;
use noaa_tides::apply_offsets;
use noaa_tides::detect_events;
use noaa_tides::interp_events;
use noaa_tides::prelude::*;
use noaa_tides::util::CMS_PER_KNOT;
use noaa_tides::util::parse_date;
use noaa_tides::util::parse_dt;
use serde_json::Value;

#[derive(Bpaf, Debug, Clone)]
#[bpaf(options, version)]
enum Cmd {
    #[bpaf(command)]
    /// Probe chrono's DST-ambiguity interpretation.
    Dst,
    #[bpaf(command)]
    /// 1-min major-axis samples + extrema for a station/bin/date.
    Samples {
        #[bpaf(positional("STATION"), fallback("n03020".to_string()))]
        station: String,
        #[bpaf(positional("BIN"), fallback(7))]
        bin: i32,
        #[bpaf(positional::<String>("DATE"), parse(parse_date), fallback(NaiveDate::from_ymd_opt(2025, 2, 7).unwrap()))]
        date: NaiveDate,
    },
    #[bpaf(command("harcon-diff"))]
    /// Diff bundled harcon vs NOAA live harcon.
    HarconDiff {
        #[bpaf(positional("STATION"), fallback("n03020".to_string()))]
        station: String,
        #[bpaf(positional("BIN"), fallback(7))]
        bin: i32,
    },
    #[bpaf(command("dst-sweep"))]
    /// 1-min samples labelled in both US Eastern wall-clock offsets and UTC.
    DstSweep {
        #[bpaf(positional("STATION"), fallback("NYH1924".to_string()))]
        station: String,
        #[bpaf(positional("BIN"), fallback(6))]
        bin: i32,
        #[bpaf(positional::<String>("DATE"), parse(parse_date), fallback(NaiveDate::from_ymd_opt(2025, 11, 2).unwrap()))]
        date: NaiveDate,
        #[bpaf(positional("HOUR"), fallback(4))]
        hour: u32,
        #[bpaf(positional("MIN"), fallback(300))]
        minutes: i64,
    },
    #[bpaf(command("subordinate-errors"))]
    /// Dump per-event errors >0.2 kt vs cached NOAA JSON under `noaa_out/`.
    SubordinateErrors {
        #[bpaf(positional("STATION"), fallback("ACT3416".to_string()))]
        station: String,
        #[bpaf(positional("NOAA_OUT"), fallback("noaa_out".to_string()))]
        noaa_out: String,
        #[bpaf(positional("Y_MIN"), fallback(2025))]
        year_min: i32,
        #[bpaf(positional("Y_MAX"), fallback(2032))]
        year_max: i32,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match cmd().run() {
        Cmd::Dst => {
            cmd_dst();
            Ok(())
        },
        Cmd::Samples { station, bin, date } => cmd_samples(station, bin, date),
        Cmd::HarconDiff { station, bin } => cmd_harcon_diff(station, bin).await,
        Cmd::DstSweep {
            station,
            bin,
            date,
            hour,
            minutes,
        } => cmd_dst_sweep(station, bin, date, hour, minutes),
        Cmd::SubordinateErrors {
            station,
            noaa_out,
            year_min,
            year_max,
        } => cmd_subordinate_errors(station, noaa_out, year_min, year_max),
    }
}

// -------- dst --------

fn cmd_dst() {
    let t = NaiveDateTime::parse_from_str("2025-11-02 01:59:00", "%Y-%m-%d %H:%M:%S").unwrap();
    let lr = Local.from_local_datetime(&t);
    println!("LocalResult: {lr:?}");
    if let chrono::LocalResult::Ambiguous(a, b) = lr {
        println!("a (earliest) = {a:?} ({} UTC)", a.naive_utc());
        println!("b (latest)   = {b:?} ({} UTC)", b.naive_utc());
        println!("a < b: {}", a < b);
    }
    let t2 = NaiveDateTime::parse_from_str("2025-11-02 02:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
    println!(
        "\n2025-11-02 02:30 (non-existent?): {:?}",
        Local.from_local_datetime(&t2)
    );
    let t3 = NaiveDateTime::parse_from_str("2025-11-02 03:11:00", "%Y-%m-%d %H:%M:%S").unwrap();
    println!("2025-11-02 03:11: {:?}", Local.from_local_datetime(&t3));
    println!("system TZ offset now: {:?}", Local::now().offset());

    println!("\n--- 2028-11-05 fall-back probe ---");
    for hm in ["00:59", "01:00", "01:59", "02:00", "02:01", "03:00"] {
        let s = format!("2028-11-05 {hm}:00");
        let t = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").unwrap();
        println!("  {s} -> {:?}", Local.from_local_datetime(&t));
    }
}

// -------- samples --------

fn cmd_samples(station: String, bin: i32, date: NaiveDate) -> Result<()> {
    let t_ref = NaiveDate::from_ymd_opt(date.year(), 7, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let pred = STORE
        .current_predictor(&station, Some(bin), t_ref)
        .ok_or_else(|| anyhow!("no predictor for {station} bin {bin}"))?;

    let start = date.and_hms_opt(0, 0, 0).unwrap();
    let n = 24 * 60;
    let mut samples: Vec<(NaiveDateTime, f64)> = Vec::with_capacity(n + 1);
    for m in 0..=n {
        let t = start + Duration::minutes(m as i64);
        samples.push((t, pred.at(t).major / CMS_PER_KNOT));
    }

    println!("{station} bin {bin} extrema on {date} (UTC):");
    for i in 1..samples.len() - 1 {
        let a = samples[i - 1].1;
        let (tb, b) = samples[i];
        let c = samples[i + 1].1;
        let denom = a - 2.0 * b + c;
        if denom.abs() > 1e-9 && (b - a).signum() != (c - b).signum() {
            let x = 0.5 * (a - c) / denom;
            if x.abs() <= 1.0 {
                let peak = b - (a - c) * (a - c) / (8.0 * denom);
                let t_peak = tb + Duration::milliseconds((x * 60_000.0) as i64);
                let kind = if denom < 0.0 { "max+" } else { "max-" };
                println!("  {t_peak} {kind} {peak:+6.3} kt");
            }
        }
    }
    println!("\n{station} bin {bin} zero crossings on {date} (UTC):");
    for i in 0..samples.len() - 1 {
        let (ta, a) = samples[i];
        let (tb, b) = samples[i + 1];
        if (a <= 0.0 && b > 0.0) || (a >= 0.0 && b < 0.0) {
            let frac = -a / (b - a);
            let t =
                ta + Duration::milliseconds((frac * (tb - ta).num_milliseconds() as f64) as i64);
            let kind = if b > a { "↑" } else { "↓" };
            println!("  {t} {kind}");
        }
    }

    println!("\nHourly samples 11:00-14:00 UTC:");
    for h in 11..=14 {
        let t = start + Duration::hours(h);
        let s = pred.at(t);
        println!(
            "  {h:02}:00 major={:+7.3} kt  speed={:+6.3} kt  dir={:5.1}°",
            s.major / CMS_PER_KNOT,
            s.speed / CMS_PER_KNOT,
            s.direction
        );
    }
    Ok(())
}

// -------- harcon-diff --------

async fn cmd_harcon_diff(station: String, bin: i32) -> Result<()> {
    let ours = STORE
        .current_harcon(&station, Some(bin))
        .with_context(|| format!("no stored harcon for {station} bin {bin}"))?;
    println!(
        "OURS   {station} bin {}: azi={:.3}° major_mean={:+.3} minor_mean={:+.3} {} constituents",
        ours.bin_nbr,
        ours.azi,
        ours.major_mean,
        ours.minor_mean,
        ours.constituents.len(),
    );

    let client = Client::new().no_cache();
    let noaa = client.harcon(&station, Some(bin)).await?.expect_current()?;
    println!(
        "NOAA   {station} bin {}: azi={:.3}° major_mean={:+.3} minor_mean={:+.3} {} constituents\n",
        noaa.bin_nbr,
        noaa.azi,
        noaa.major_mean,
        noaa.minor_mean,
        noaa.constituents.len(),
    );

    println!(
        "  {:<8}  {:>10} {:>10}  {:>10} {:>10} {:>8}  {:>10} {:>10}  {:>10} {:>10} {:>8}",
        "name",
        "ours_mjA",
        "noaa_mjA",
        "ours_mjφ",
        "noaa_mjφ",
        "Δφ_mj",
        "ours_mnA",
        "noaa_mnA",
        "ours_mnφ",
        "noaa_mnφ",
        "Δφ_mn",
    );

    let mut worst: Vec<(f64, String, f64, f64, f64, f64, f64)> = Vec::new();
    for oc in &ours.constituents {
        let Some(nc) = noaa.constituents.iter().find(|c| c.name == oc.name) else {
            println!("  {:<8}  <missing in NOAA>", oc.name);
            continue;
        };
        let dphi_mj = ((oc.major_phase_gmt - nc.major_phase_gmt + 540.0).rem_euclid(360.0)) - 180.0;
        let dphi_mn = ((oc.minor_phase_gmt - nc.minor_phase_gmt + 540.0).rem_euclid(360.0)) - 180.0;
        let da_mj = oc.major_amplitude - nc.major_amplitude;
        let score = dphi_mj.abs() * nc.major_amplitude + da_mj.abs() * 10.0;
        worst.push((
            score,
            oc.name.clone(),
            oc.major_amplitude,
            nc.major_amplitude,
            oc.major_phase_gmt,
            nc.major_phase_gmt,
            dphi_mj,
        ));
        println!(
            "  {:<8}  {:>10.3} {:>10.3}  {:>10.2} {:>10.2} {:>+8.2}  {:>10.3} {:>10.3}  {:>10.2} {:>10.2} {:>+8.2}",
            oc.name,
            oc.major_amplitude,
            nc.major_amplitude,
            oc.major_phase_gmt,
            nc.major_phase_gmt,
            dphi_mj,
            oc.minor_amplitude,
            nc.minor_amplitude,
            oc.minor_phase_gmt,
            nc.minor_phase_gmt,
            dphi_mn,
        );
    }

    let noaa_only: Vec<_> = noaa
        .constituents
        .iter()
        .filter(|nc| !ours.constituents.iter().any(|oc| oc.name == nc.name))
        .collect();
    if !noaa_only.is_empty() {
        println!(
            "\nNOAA constituents not in our store ({}):",
            noaa_only.len()
        );
        for nc in noaa_only {
            println!(
                "  {:<8}  mjA={:>7.3} mjφ={:>7.2}  mnA={:>7.3} mnφ={:>7.2}",
                nc.name,
                nc.major_amplitude,
                nc.major_phase_gmt,
                nc.minor_amplitude,
                nc.minor_phase_gmt,
            );
        }
    }

    worst.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    println!("\nTop 8 constituents by amplitude-weighted major-axis phase error:");
    for (score, name, oa, na, op, np, dp) in worst.iter().take(8) {
        println!(
            "  {name:<8} score={score:>7.2}  ours(A={oa:>6.3} φ={op:>6.2}) noaa(A={na:>6.3} φ={np:>6.2}) Δφ={dp:+.2}°"
        );
    }

    Ok(())
}

// -------- dst-sweep --------

fn cmd_dst_sweep(
    station: String,
    bin: i32,
    date: NaiveDate,
    hour: u32,
    minutes: i64,
) -> Result<()> {
    let t_ref = NaiveDate::from_ymd_opt(date.year(), 7, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let pred = STORE
        .current_predictor(&station, Some(bin), t_ref)
        .ok_or_else(|| anyhow!("no predictor for {station} bin {bin}"))?;
    let start = date.and_hms_opt(hour, 0, 0).unwrap();
    println!("{station} bin {bin} major-axis speed (kt), UTC:");
    println!(" local(EDT) local(EST)  UTC        major_kt");
    for m in 0..=minutes {
        let t = start + Duration::minutes(m);
        let s = pred.at(t);
        let edt = t - Duration::hours(4);
        let est = t - Duration::hours(5);
        let kt = s.major / CMS_PER_KNOT;
        if m % 5 == 0 || kt.abs() < 0.1 {
            println!(
                "  {}  {}  {}  {kt:+7.3}",
                edt.format("%H:%M:%S"),
                est.format("%H:%M:%S"),
                t.format("%H:%M:%S"),
            );
        }
    }
    Ok(())
}

// -------- subordinate-errors --------

fn cmd_subordinate_errors(
    station: String,
    noaa_out: String,
    year_min: i32,
    year_max: i32,
) -> Result<()> {
    let entry = STORE
        .subordinate(&station)
        .ok_or_else(|| anyhow!("station {station} is not a subordinate in the bundled store"))?;
    let offsets = entry.offsets.clone();
    println!(
        "{station} offsets: ref={} bin={} mfc_t={} sbe_t={} mec_t={} sbf_t={} mfc_amp={} mec_amp={}",
        offsets.ref_id,
        offsets.ref_bin,
        offsets.mfc_time_min,
        offsets.sbe_time_min,
        offsets.mec_time_min,
        offsets.sbf_time_min,
        offsets.mfc_amp,
        offsets.mec_amp,
    );

    let mut worst: Vec<(f64, i32, NaiveDateTime, String, f64, f64, Event)> = Vec::new();

    for y in year_min..=year_max {
        let path = format!("{noaa_out}/{y}_currents_nyc/{station}.json");
        if !std::path::Path::new(&path).exists() {
            continue;
        }
        let raw: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;
        let events = raw["current_predictions"]["cp"].as_array().unwrap();
        let t_ref_utc = year_t_ref_utc(y);
        let ref_pred = STORE
            .current_predictor(&offsets.ref_id, Some(offsets.ref_bin), t_ref_utc)
            .ok_or_else(|| anyhow!("ref predictor missing"))?;
        let (t_start, t_end) = year_bounds_utc(y);
        let raw_events = detect_events(
            &ref_pred,
            t_start - Duration::hours(6),
            t_end + Duration::hours(6),
        );
        let our_events = apply_offsets(&raw_events, &offsets);

        for ev in events {
            let t_utc = parse_dt(ev["Time"].as_str().unwrap())?;
            let noaa_type = ev["Type"].as_str().unwrap_or("?").to_string();
            let noaa_major = ev["Velocity_Major"].as_f64().unwrap_or(0.0);
            let ours_kt = interp_events(&our_events, t_utc);

            let mut best: Option<Event> = None;
            let mut best_dt = Duration::days(9999);
            for e in &our_events {
                let d = (e.t - t_utc).abs();
                if d < best_dt {
                    best_dt = d;
                    best = Some(e.clone());
                }
            }
            let err = ours_kt - noaa_major;
            if err.abs() > 0.2 {
                worst.push((err, y, t_utc, noaa_type, noaa_major, ours_kt, best.unwrap()));
            }
        }
    }
    worst.sort_by(|a, b| b.0.abs().partial_cmp(&a.0.abs()).unwrap());

    println!("\ntop 15 worst events (|err| > 0.2 kt) — times in UTC:");
    for (err, y, t_utc, kind, noaa_kt, ours_kt, nearest) in worst.iter().take(15) {
        let dt_min = (nearest.t - *t_utc).num_seconds() as f64 / 60.0;
        println!(
            "  {y} {t_utc} [{kind:6}] noaa={noaa_kt:+6.3} ours={ours_kt:+6.3} err={err:+.3}  \
             nearest_ours: {} {:?} {:+.3} kt  Δt={dt_min:+.1} min",
            nearest.t, nearest.kind, nearest.speed_kt,
        );
    }
    println!("\ntotal outlier events (>0.2 kt): {}", worst.len());
    Ok(())
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
