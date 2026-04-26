//! Validate the offline predictor against NOAA's online `datagetter` across
//! several sampling strategies. Each subcommand fetches live and compares.
//!
//! Subcommands:
//!   tide STATION [DATE] [HOURS]      one tide station, NOAA hourly
//!   current STATION [DATE] [HOURS]   one harmonic current station, NOAA hourly
//!   bin STATION BIN [YEAR]           one harmonic current station + bin, full year
//!   nyc-tides                        10 NYC tide stations, hilo, 1 month
//!   nyc-harmonic [DATE] [HOURS]      every harmonic current station in NYC box
//!   nyc-subordinate [DATE] [HOURS]   every subordinate current station in NYC box
//!   random [SEED] [N_STATIONS] [N_MONTHS]  SplitMix64 sample across the whole store
//!
//! The offline validator against cached `noaa_out/` data lives in
//! `tests/offline.rs` as `#[test] #[ignore]` (run with `cargo test --
//! --ignored offline`).

use std::collections::BTreeMap;

use bpaf::Bpaf;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use futures::stream::StreamExt;
use futures::stream::{
    self,
};
use noaa_tides::Client;
use noaa_tides::CurrentPredictor;
use noaa_tides::Predictor;
use noaa_tides::STORE;
use noaa_tides::StationInfo;
use noaa_tides::SubordinateOffsets;
use noaa_tides::debug_astro;
use noaa_tides::debug_v0;
use noaa_tides::events::Event;
use noaa_tides::events::EventKind;
use noaa_tides::events::apply_offsets;
use noaa_tides::events::detect_events;
use noaa_tides::events::interp_events;
use noaa_tides::prelude::*;
use noaa_tides::util::CMS_PER_KNOT;
use noaa_tides::util::Product;
use noaa_tides::util::Stats;
use noaa_tides::util::datagetter_url;
use noaa_tides::util::in_nyc_box;
use noaa_tides::util::parse_date;
use noaa_tides::util::parse_dt;
use serde_json::Value;

const MAX_CONCURRENT: usize = 6;

fn fval(v: &Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0.0)
}

/// NOAA returns `"Available bin number of X: 15, 10, 5, "` when a bin is
/// invalid. Parse the first listed so callers can retry.
fn parse_available_bin(err: &str) -> Option<i32> {
    err.split("Available bin number")
        .nth(1)
        .and_then(|t| t.split(':').nth(1))
        .and_then(|t| t.split(',').next())
        .and_then(|t| t.trim().parse::<i32>().ok())
}

// -----------------------------------------------------------------------------
// tide STATION [DATE] [HOURS]
// -----------------------------------------------------------------------------

async fn cmd_tide(station_id: String, start_date: NaiveDate, hours: u32) -> Result<()> {
    println!("Validating tide station {station_id} from {start_date} for {hours} hours");

    let client = Client::new();
    let harcon = client.harcon(&station_id, None).await?.expect_tide()?;
    println!(
        "  {} constituents ({})",
        harcon.constituents.len(),
        harcon.units
    );

    let t_ref = start_date.and_hms_opt(0, 0, 0).unwrap() + Duration::hours(hours as i64 / 2);
    let predictor = Predictor::new(&harcon, t_ref);

    let end_date = start_date + Duration::hours(hours as i64);
    let url = datagetter_url(
        &station_id,
        start_date,
        end_date,
        &Product::TidePredictions { interval: "6" },
    );
    let resp: Value = client.fetch_json(&url, None).await?;
    let preds = resp["predictions"]
        .as_array()
        .ok_or_else(|| anyhow!("no predictions in response: {resp}"))?;

    let mut rows: Vec<(NaiveDateTime, f64, f64)> = Vec::with_capacity(preds.len());
    let (mut min_n, mut max_n) = (f64::INFINITY, f64::NEG_INFINITY);
    for p in preds {
        let t = parse_dt(p["t"].as_str().context("no t")?)?;
        let noaa_v: f64 = p["v"].as_str().context("no v")?.parse()?;
        rows.push((t, noaa_v, predictor.at(t)));
        min_n = min_n.min(noaa_v);
        max_n = max_n.max(noaa_v);
    }
    let noaa_mean = rows.iter().map(|(_, v, _)| v).sum::<f64>() / rows.len() as f64;
    let ours_mean = rows.iter().map(|(_, _, o)| o).sum::<f64>() / rows.len() as f64;
    let z0 = noaa_mean - ours_mean;
    let mut st = Stats::default();
    for (_, v, o) in &rows {
        st.push(v - (o + z0));
    }

    println!(
        "  samples={} NOAA range [{min_n:.2}, {max_n:.2}] ft inferred Z0 {z0:+.3} ft",
        rows.len()
    );
    println!("  RMS {:.3} ft  max |err| {:.3} ft", st.rms(), st.max_abs);

    let (t_deg, s, h, p, n, ps) = debug_astro(t_ref);
    println!("\nAstro at {t_ref}: T={t_deg:.3} s={s:.3} h={h:.3} p={p:.3} N={n:.3} ps={ps:.3}",);
    for name in ["M2", "S2", "N2", "K1", "O1"] {
        if let Some(v0) = debug_v0(name, t_ref) {
            println!("  V0({name}) = {v0:.3}°");
        }
    }
    let mut contribs = predictor.contributions(t_ref);
    contribs.sort_by(|a, b| b.1.abs().partial_cmp(&a.1.abs()).unwrap());
    println!("\nConstituent contributions at t_ref (top 10):");
    for (name, v) in contribs.iter().take(10) {
        println!("  {name:6}  {v:+7.4}");
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// current STATION [DATE] [HOURS]
// -----------------------------------------------------------------------------

async fn cmd_current(station_id: String, start_date: NaiveDate, hours: u32) -> Result<()> {
    println!("Validating current station {station_id} from {start_date} for {hours} hours");

    let client = Client::new();
    let base_id = station_id.split('_').next().unwrap();

    let probe_url = datagetter_url(
        &station_id,
        start_date,
        start_date,
        &Product::CurrentsHourly { bin: None },
    );
    let probe: Value = client.fetch_json(&probe_url, None).await?;
    let noaa_bin = probe["current_predictions"]["cp"][0]["Bin"]
        .as_str()
        .and_then(|s| s.parse::<i32>().ok());
    println!("  NOAA default bin: {noaa_bin:?}");

    let harcon = client.harcon(base_id, noaa_bin).await?.expect_current()?;
    let t_ref = start_date.and_hms_opt(0, 0, 0).unwrap() + Duration::hours(hours as i64 / 2);
    let predictor = CurrentPredictor::new(&harcon, t_ref);

    let end_date = start_date + Duration::hours(hours as i64);
    let url = datagetter_url(
        &station_id,
        start_date,
        end_date,
        &Product::CurrentsHourly { bin: None },
    );
    let resp: Value = client.fetch_json(&url, None).await?;
    let preds = resp["current_predictions"]["cp"]
        .as_array()
        .ok_or_else(|| anyhow!("no cp array: {resp}"))?;

    let variants = [
        (true, true, "major+minor+mean"),
        (true, false, "major+minor    "),
        (false, true, "major+mean     "),
        (false, false, "major only     "),
    ];
    let mut stats = [
        Stats::default(),
        Stats::default(),
        Stats::default(),
        Stats::default(),
    ];
    let mut noaa_peak = 0.0_f64;
    for p in preds {
        let t = parse_dt(p["Time"].as_str().context("no Time")?)?;
        let noaa_kt: f64 = p["Speed"].as_str().context("no Speed")?.parse()?;
        noaa_peak = noaa_peak.max(noaa_kt.abs());
        let our = [
            predictor.at_with(t, true, true).speed / CMS_PER_KNOT,
            predictor.at_with(t, true, false).speed / CMS_PER_KNOT,
            predictor.at_with(t, false, true).speed / CMS_PER_KNOT,
            predictor.at_with(t, false, false).speed / CMS_PER_KNOT,
        ];
        for (i, v) in our.iter().enumerate() {
            stats[i].push(noaa_kt - v);
        }
    }
    println!("  samples={} NOAA |max|={noaa_peak:.2} kt", stats[0].n);
    for (i, (_, _, label)) in variants.iter().enumerate() {
        println!(
            "  {label}: RMS {:.3} kt  max |err| {:.3} kt",
            stats[i].rms(),
            stats[i].max_abs
        );
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// bin STATION BIN [YEAR]
// -----------------------------------------------------------------------------

async fn cmd_bin(id: String, bin: i32, year: i32) -> Result<()> {
    let t_ref = NaiveDate::from_ymd_opt(year, 7, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let pred = STORE
        .current_predictor(&id, Some(bin), t_ref)
        .with_context(|| format!("no predictor for {id} bin {bin}"))?;

    let client = Client::new().no_cache();
    let start = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();

    let mut samples: Vec<(NaiveDateTime, f64, f64)> = Vec::new();
    let mut cur = start;
    while cur <= end {
        let chunk_end = (cur + Duration::days(30)).min(end);
        let url = datagetter_url(
            &id,
            cur,
            chunk_end,
            &Product::CurrentsHourly { bin: Some(bin) },
        );
        let v: Value = client.fetch_json(&url, None).await?;
        let arr = v["current_predictions"]["cp"]
            .as_array()
            .context("no cp array")?;
        eprintln!("  {cur} -> {chunk_end}: {} rows", arr.len());
        let azi_r = pred.azimuth().to_radians();
        for r in arr {
            let t = parse_dt(r["Time"].as_str().unwrap())?;
            let speed = fval(&r["Speed"]);
            let dir_r = fval(&r["Direction"]).to_radians();
            let vn = speed * dir_r.cos();
            let ve = speed * dir_r.sin();
            let noaa_major = vn * azi_r.cos() + ve * azi_r.sin();
            let ours_major = pred.at(t).major / CMS_PER_KNOT;
            samples.push((t, noaa_major, ours_major));
        }
        cur = chunk_end + Duration::days(1);
    }

    let n = samples.len() as f64;
    let mut st = Stats::default();
    for (_, noaa, ours) in &samples {
        st.push(ours - noaa);
    }
    let mean = samples.iter().map(|(_, n, o)| o - n).sum::<f64>() / n;
    let (worst_t, worst_n, worst_o) = samples
        .iter()
        .max_by(|a, b| (a.2 - a.1).abs().partial_cmp(&(b.2 - b.1).abs()).unwrap())
        .unwrap();
    println!(
        "\n{id} bin {bin} {year}: n={} mean {mean:+.4} RMS {:.4} max |err| {:.4} kt",
        st.n,
        st.rms(),
        st.max_abs,
    );
    println!(
        "  worst: {worst_t} noaa={worst_n:+.3} ours={worst_o:+.3} err={:+.3}",
        worst_o - worst_n
    );

    // Daily max-|err|.
    let mut daily: BTreeMap<NaiveDate, f64> = BTreeMap::new();
    for (t, nm, om) in &samples {
        let e = (om - nm).abs();
        daily
            .entry(t.date())
            .and_modify(|v| *v = v.max(e))
            .or_insert(e);
    }
    let mut by_err: Vec<_> = daily.into_iter().collect();
    by_err.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    println!("\ntop 10 worst days by max hourly |err|:");
    for (d, e) in by_err.iter().take(10) {
        println!("  {d}: {e:.4} kt");
    }

    // Best time-shift.
    let rms0 = st.rms();
    let mut best = (0i64, f64::INFINITY);
    for dt_m in (-180..=180).step_by(6) {
        let mut sse = 0.0;
        for sample in &samples {
            let t_target = sample.0 + Duration::minutes(dt_m);
            let o = pred.at(t_target).major / CMS_PER_KNOT;
            sse += (o - sample.1).powi(2);
        }
        let r = (sse / n).sqrt();
        if r < best.1 {
            best = (dt_m, r);
        }
    }
    println!(
        "\nbest time-shift for RMS minimum: {} min (RMS {:.4} kt) vs 0-shift RMS {rms0:.4} kt",
        best.0, best.1
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// nyc-tides
// -----------------------------------------------------------------------------

async fn cmd_nyc_tides() -> Result<()> {
    const STATIONS: &[&str] = &[
        "8467150", "8518750", "8516945", "8517847", "8514322", "8515186", "8516881", "8519483",
        "8531680", "8518668",
    ];
    let begin = NaiveDate::from_ymd_opt(2025, 6, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2025, 6, 30).unwrap();

    let client = Client::new();
    println!(
        "{:10} {:>5} {:>9} {:>9} {:>9} {:>8}  name",
        "station", "n", "mean", "max", "bias", "z0"
    );
    println!("{}", "-".repeat(90));

    let (mut agg_sum, mut agg_sum_abs, mut agg_n) = (0.0_f64, 0.0_f64, 0_usize);
    for id in STATIONS {
        let Some(info) = STORE.station_info(id) else {
            println!("{id:10} (missing from store)");
            continue;
        };
        let Some(harcon) = STORE.tide_harcon(id) else {
            println!("{id:10} (no harcon)");
            continue;
        };
        let z0 = harcon.z0_mllw;

        let url = datagetter_url(
            id,
            begin,
            end,
            &Product::TidePredictions { interval: "hilo" },
        );
        let key = format!(
            "nyc_tides/{id}_{}_{}_hilo.json",
            begin.format("%Y%m%d"),
            end.format("%Y%m%d"),
        );
        let v = match client.fetch_json(&url, Some(&key)).await {
            Ok(v) => v,
            Err(e) => {
                println!("{id:10} fetch error: {e}");
                continue;
            },
        };
        let arr = v["predictions"]
            .as_array()
            .ok_or_else(|| anyhow!("no predictions for {id}"))?;
        if arr.is_empty() {
            println!("{id:10} empty events");
            continue;
        }
        let mut events: Vec<(NaiveDateTime, f64)> = Vec::with_capacity(arr.len());
        for p in arr {
            let t = parse_dt(p["t"].as_str().unwrap_or(""))?;
            let h: f64 = p["v"].as_str().unwrap_or("0").parse()?;
            events.push((t, h));
        }
        let t_mid = events[events.len() / 2].0;
        let predictor = STORE
            .tide_predictor(id, t_mid)
            .ok_or_else(|| anyhow!("no predictor for {id}"))?;

        let (mut n, mut sum, mut sum_abs, mut max_abs) = (0_usize, 0.0_f64, 0.0_f64, 0.0_f64);
        for (t, noaa_h) in &events {
            let e = predictor.at(*t) - *noaa_h;
            n += 1;
            sum += e;
            sum_abs += e.abs();
            max_abs = max_abs.max(e.abs());
        }
        agg_sum += sum;
        agg_sum_abs += sum_abs;
        agg_n += n;
        println!(
            "{:10} {:5} {:>7.4} ft {:>6.4} ft {:>+7.4} ft {:>6.3} ft  {}",
            id,
            n,
            sum_abs / n as f64,
            max_abs,
            sum / n as f64,
            z0,
            info.name
        );
    }
    if agg_n > 0 {
        println!(
            "\naggregate: n={agg_n} mean |err|={:.4} ft overall bias={:+.4} ft",
            agg_sum_abs / agg_n as f64,
            agg_sum / agg_n as f64,
        );
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// nyc-harmonic and nyc-subordinate (batch via lat/lon box)
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
struct BatchResult {
    id: String,
    name: String,
    peak_kt: f64,
    rms_kt: f64,
    max_abs_kt: f64,
    pct: f64,
    bin: Option<i32>,
    error: Option<String>,
}

async fn validate_harmonic_one(
    client: &Client,
    info: &StationInfo,
    start: NaiveDate,
    hours: u32,
) -> BatchResult {
    let base_id = info.id.split('_').next().unwrap();
    let end = start + Duration::hours(hours as i64);

    let probe_url = datagetter_url(
        &info.id,
        start,
        start,
        &Product::CurrentsHourly { bin: None },
    );
    let probe: Value = match client.fetch_json(&probe_url, None).await {
        Ok(v) => v,
        Err(e) => return fail_batch(info, format!("probe: {e}")),
    };
    let noaa_bin = probe["current_predictions"]["cp"][0]["Bin"]
        .as_str()
        .and_then(|s| s.parse::<i32>().ok());
    let harcon = match client.harcon(base_id, noaa_bin).await {
        Ok(h) => match h.expect_current() {
            Ok(c) => c,
            Err(e) => return fail_batch(info, format!("expect_current: {e}")),
        },
        Err(e) => return fail_batch(info, format!("harcon: {e}")),
    };
    let t_ref = start.and_hms_opt(0, 0, 0).unwrap() + Duration::hours(hours as i64 / 2);
    let predictor = CurrentPredictor::new(&harcon, t_ref);

    let url = datagetter_url(&info.id, start, end, &Product::CurrentsHourly { bin: None });
    let resp: Value = match client.fetch_json(&url, None).await {
        Ok(v) => v,
        Err(e) => return fail_batch(info, format!("data: {e}")),
    };
    let Some(preds) = resp["current_predictions"]["cp"].as_array() else {
        return fail_batch(info, "no cp array".into());
    };

    let (mut st, mut peak) = (Stats::default(), 0.0_f64);
    for p in preds {
        let Some(t_str) = p["Time"].as_str() else {
            continue;
        };
        let Some(v_str) = p["Speed"].as_str() else {
            continue;
        };
        let Ok(t) = NaiveDateTime::parse_from_str(t_str, "%Y-%m-%d %H:%M") else {
            continue;
        };
        let Ok(noaa_kt) = v_str.parse::<f64>() else {
            continue;
        };
        let ours_kt = predictor.at_with(t, true, true).speed / CMS_PER_KNOT;
        st.push(noaa_kt - ours_kt);
        peak = peak.max(noaa_kt.abs());
    }
    if st.n == 0 {
        return fail_batch(info, "0 samples".into());
    }
    let pct = if peak > 0.0 {
        100.0 * st.rms() / peak
    } else {
        0.0
    };
    BatchResult {
        id: info.id.clone(),
        name: info.name.clone(),
        peak_kt: peak,
        rms_kt: st.rms(),
        max_abs_kt: st.max_abs,
        pct,
        bin: noaa_bin,
        error: None,
    }
}

fn fail_batch(info: &StationInfo, msg: String) -> BatchResult {
    BatchResult {
        id: info.id.clone(),
        name: info.name.clone(),
        error: Some(msg),
        ..Default::default()
    }
}

async fn cmd_nyc_harmonic(start: NaiveDate, hours: u32) -> Result<()> {
    let client = Client::new();
    let mut stations: Vec<StationInfo> = client
        .current_stations()
        .await?
        .into_iter()
        .filter(|s| s.station_type == "H" && in_nyc_box(s))
        .collect();
    stations.sort_by(|a, b| a.id.cmp(&b.id));
    eprintln!(
        "Validating {} harmonic stations from {start} for {hours} hours",
        stations.len()
    );

    let results: Vec<BatchResult> = stream::iter(stations.iter())
        .map(|s| {
            let client = client.clone();
            async move {
                let r = validate_harmonic_one(&client, s, start, hours).await;
                eprintln!(
                    "  {:<8} peak={:.2} rms={:.3} pct={:.2}% {}",
                    r.id,
                    r.peak_kt,
                    r.rms_kt,
                    r.pct,
                    r.error.as_deref().unwrap_or("")
                );
                r
            }
        })
        .buffer_unordered(MAX_CONCURRENT)
        .collect()
        .await;

    let mut ok: Vec<&BatchResult> = results.iter().filter(|r| r.error.is_none()).collect();
    let fails: Vec<&BatchResult> = results.iter().filter(|r| r.error.is_some()).collect();
    ok.sort_by(|a, b| b.pct.partial_cmp(&a.pct).unwrap());

    println!("\n# Accuracy within 1° of Battery NY ({start} to +{hours}h)");
    println!(
        "{:<8} {:>5} {:>6} {:>6} {:>6} {:>6}  name",
        "station", "bin", "peak", "rms", "max", "pct%"
    );
    for r in &ok {
        println!(
            "{:<8} {:>5} {:>6.2} {:>6.3} {:>6.3} {:>6.2}  {}",
            r.id,
            r.bin.map(|b| b.to_string()).unwrap_or_else(|| "-".into()),
            r.peak_kt,
            r.rms_kt,
            r.max_abs_kt,
            r.pct,
            r.name,
        );
    }
    if !fails.is_empty() {
        println!("\n# Failed ({}):", fails.len());
        for r in &fails {
            println!("  {}: {}", r.id, r.error.as_deref().unwrap_or(""));
        }
    }
    if !ok.is_empty() {
        let mean_pct = ok.iter().map(|r| r.pct).sum::<f64>() / ok.len() as f64;
        let mean_rms = ok.iter().map(|r| r.rms_kt).sum::<f64>() / ok.len() as f64;
        let worst = ok.first().unwrap();
        let best = ok.last().unwrap();
        println!(
            "\n# Summary: {} OK, {} failed. mean RMS {:.3} kt, mean err {:.2}%. Worst {} ({:.2}%), best {} ({:.2}%).",
            ok.len(),
            fails.len(),
            mean_rms,
            mean_pct,
            worst.id,
            worst.pct,
            best.id,
            best.pct
        );
    }
    Ok(())
}

// Subordinate batch: event-matched timeline via canonical detect_events.
#[derive(Debug, Default)]
struct SubBatchResult {
    id: String,
    name: String,
    ref_id: String,
    ref_bin: i32,
    n_matched: usize,
    time_rms_min: f64,
    time_max_min: f64,
    speed_rms_kt: f64,
    speed_max_kt: f64,
    noaa_peak_kt: f64,
    error: Option<String>,
}

async fn fetch_noaa_sub_events(
    client: &Client,
    station: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<Event>> {
    let url = datagetter_url(
        station,
        start,
        end,
        &Product::CurrentsMaxSlack { bin: None },
    );
    let resp: Value = client.fetch_json(&url, None).await?;
    let arr = resp["current_predictions"]["cp"]
        .as_array()
        .ok_or_else(|| anyhow!("no cp array"))?;

    let mut raw: Vec<(NaiveDateTime, EventKind, f64, String)> = Vec::new();
    for p in arr {
        let Some(t_str) = p["Time"].as_str() else {
            continue;
        };
        let t = NaiveDateTime::parse_from_str(t_str, "%Y-%m-%d %H:%M")?;
        let ty = p["Type"].as_str().unwrap_or("").to_string();
        let vel = p["Velocity_Major"].as_f64().unwrap_or(0.0);
        let kind = match ty.as_str() {
            "flood" => EventKind::MaxFlood,
            "ebb" => EventKind::MaxEbb,
            "slack" => EventKind::SlackBeforeEbb, // disambiguated below
            _ => continue,
        };
        raw.push((t, kind, vel, ty));
    }
    // Resolve slacks by looking at the next non-slack NOAA event.
    let mut resolved: Vec<Event> = Vec::with_capacity(raw.len());
    for i in 0..raw.len() {
        let (t, kind, vel, ty) = &raw[i];
        let final_kind = if ty == "slack" {
            let next_nonslack = raw[(i + 1)..].iter().find(|e| e.3 != "slack").map(|e| e.1);
            match next_nonslack {
                Some(EventKind::MaxEbb) => EventKind::SlackBeforeEbb,
                Some(EventKind::MaxFlood) => EventKind::SlackBeforeFlood,
                _ => {
                    let prev_nonslack = raw[..i].iter().rev().find(|e| e.3 != "slack").map(|e| e.1);
                    match prev_nonslack {
                        Some(EventKind::MaxFlood) => EventKind::SlackBeforeEbb,
                        Some(EventKind::MaxEbb) => EventKind::SlackBeforeFlood,
                        _ => EventKind::SlackBeforeEbb,
                    }
                },
            }
        } else {
            *kind
        };
        resolved.push(Event {
            kind: final_kind,
            t: *t,
            speed_kt: *vel,
        });
    }
    Ok(resolved)
}

fn match_events_by_kind(ours: &[Event], theirs: &[Event]) -> Vec<(Event, Event)> {
    let mut pairs = Vec::new();
    for t in theirs {
        let mut best: Option<(&Event, i64)> = None;
        for o in ours {
            if o.kind != t.kind {
                continue;
            }
            let dt = (o.t - t.t).num_seconds().abs();
            if best.is_none_or(|(_, bdt)| dt < bdt) {
                best = Some((o, dt));
            }
        }
        if let Some((o, dt)) = best
            && dt <= 90 * 60
        {
            pairs.push((o.clone(), t.clone()));
        }
    }
    pairs
}

async fn validate_subordinate_one(
    client: &Client,
    info: &StationInfo,
    start: NaiveDate,
    hours: u32,
) -> SubBatchResult {
    let fail = |msg: String| SubBatchResult {
        id: info.id.clone(),
        name: info.name.clone(),
        error: Some(msg),
        ..Default::default()
    };

    let offsets = match client.subordinate_offsets(&info.id, None).await {
        Ok(o) => o,
        Err(e) => return fail(format!("offsets: {e}")),
    };
    let harcon = match client.harcon(&offsets.ref_id, Some(offsets.ref_bin)).await {
        Ok(h) => match h.expect_current() {
            Ok(c) => c,
            Err(e) => return fail(format!("ref not current: {e}")),
        },
        Err(e) => return fail(format!("ref harcon: {e}")),
    };
    let t_ref = start.and_hms_opt(0, 0, 0).unwrap() + Duration::hours(hours as i64 / 2);
    let predictor = CurrentPredictor::new(&harcon, t_ref);

    let pad = Duration::hours(3);
    let sim_start = start.and_hms_opt(0, 0, 0).unwrap() - pad;
    let sim_end = start.and_hms_opt(0, 0, 0).unwrap() + Duration::hours(hours as i64) + pad;
    let raw_events = detect_events(&predictor, sim_start, sim_end);
    let ours = apply_offsets(&raw_events, &offsets);

    let end_date = start + Duration::hours(hours as i64);
    let theirs = match fetch_noaa_sub_events(client, &info.id, start, end_date).await {
        Ok(e) => e,
        Err(e) => return fail(format!("noaa events: {e}")),
    };

    let pairs = match_events_by_kind(&ours, &theirs);
    if pairs.is_empty() {
        return fail(format!(
            "no matches (ours={}, theirs={})",
            ours.len(),
            theirs.len()
        ));
    }
    let (mut t_sq, mut t_max, mut s_sq, mut s_max) = (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
    for (o, n) in &pairs {
        let dt_min = (o.t - n.t).num_seconds() as f64 / 60.0;
        t_sq += dt_min * dt_min;
        t_max = t_max.max(dt_min.abs());
        let ds = o.speed_kt - n.speed_kt;
        s_sq += ds * ds;
        s_max = s_max.max(ds.abs());
    }
    let nmatch = pairs.len() as f64;
    let noaa_peak = theirs
        .iter()
        .map(|e| e.speed_kt.abs())
        .fold(0.0_f64, f64::max);

    SubBatchResult {
        id: info.id.clone(),
        name: info.name.clone(),
        ref_id: offsets.ref_id,
        ref_bin: offsets.ref_bin,
        n_matched: pairs.len(),
        time_rms_min: (t_sq / nmatch).sqrt(),
        time_max_min: t_max,
        speed_rms_kt: (s_sq / nmatch).sqrt(),
        speed_max_kt: s_max,
        noaa_peak_kt: noaa_peak,
        error: None,
    }
}

async fn cmd_nyc_subordinate(start: NaiveDate, hours: u32) -> Result<()> {
    let client = Client::new();
    let mut stations: Vec<StationInfo> = client
        .current_stations()
        .await?
        .into_iter()
        .filter(|s| s.station_type == "S" && in_nyc_box(s))
        .collect();
    stations.sort_by(|a, b| a.id.cmp(&b.id));
    eprintln!(
        "Validating {} subordinate stations from {start} for {hours} hours",
        stations.len()
    );

    let results: Vec<SubBatchResult> = stream::iter(stations.iter())
        .map(|s| {
            let client = client.clone();
            async move {
                let r = validate_subordinate_one(&client, s, start, hours).await;
                if let Some(ref err) = r.error {
                    eprintln!("  {:<8} FAIL {err}  {}", r.id, r.name);
                } else {
                    eprintln!(
                        "  {:<8} n={} t_rms={:.1}min s_rms={:.3}kt peak={:.2}  {}",
                        r.id, r.n_matched, r.time_rms_min, r.speed_rms_kt, r.noaa_peak_kt, r.name
                    );
                }
                r
            }
        })
        .buffer_unordered(MAX_CONCURRENT)
        .collect()
        .await;

    let mut ok: Vec<&SubBatchResult> = results.iter().filter(|r| r.error.is_none()).collect();
    let fails: Vec<&SubBatchResult> = results.iter().filter(|r| r.error.is_some()).collect();
    ok.sort_by(|a, b| b.time_rms_min.partial_cmp(&a.time_rms_min).unwrap());

    println!("\n# Subordinate accuracy within 1° of Battery NY ({start} to +{hours}h)");
    println!(
        "{:<8} {:>8} {:>3} {:>4} {:>6} {:>6} {:>6} {:>6} {:>6}  name",
        "station", "ref", "bin", "n", "t_rms", "t_max", "s_rms", "s_max", "peak"
    );
    for r in &ok {
        println!(
            "{:<8} {:>8} {:>3} {:>4} {:>6.1} {:>6.1} {:>6.3} {:>6.3} {:>6.2}  {}",
            r.id,
            r.ref_id,
            r.ref_bin,
            r.n_matched,
            r.time_rms_min,
            r.time_max_min,
            r.speed_rms_kt,
            r.speed_max_kt,
            r.noaa_peak_kt,
            r.name,
        );
    }
    if !fails.is_empty() {
        println!("\n# Failed ({}):", fails.len());
        for r in &fails {
            println!("  {}: {}", r.id, r.error.as_deref().unwrap_or(""));
        }
    }
    if !ok.is_empty() {
        let mean_t = ok.iter().map(|r| r.time_rms_min).sum::<f64>() / ok.len() as f64;
        let mean_s = ok.iter().map(|r| r.speed_rms_kt).sum::<f64>() / ok.len() as f64;
        let worst = ok.first().unwrap();
        println!(
            "\n# Summary: {} OK, {} failed. mean t_rms {:.1} min, mean s_rms {:.3} kt. Worst time {} ({:.1} min).",
            ok.len(),
            fails.len(),
            mean_t,
            mean_s,
            worst.id,
            worst.time_rms_min,
        );
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// random — SplitMix64 sample across the entire bundled store
// -----------------------------------------------------------------------------

const RANDOM_YEAR_MIN: i32 = 2025;
const RANDOM_YEAR_MAX: i32 = 2032;
const RANDOM_DEFAULT_SEED: u64 = 0x7E57_1DE4_F00D_CAFE;
const RANDOM_DEFAULT_STATIONS: usize = 40;
const RANDOM_DEFAULT_MONTHS: usize = 4;

struct SplitMix64(u64);
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn sample_without_replacement(&mut self, n: usize, k: usize) -> Vec<usize> {
        let mut idx: Vec<usize> = (0..n).collect();
        let k = k.min(n);
        for i in 0..k {
            let j = i + (self.next() as usize) % (n - i);
            idx.swap(i, j);
        }
        idx.into_iter().take(k).collect()
    }
}

fn month_bounds(y: i32, m: u32) -> (NaiveDate, NaiveDate) {
    let start = NaiveDate::from_ymd_opt(y, m, 1).unwrap();
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    let end = NaiveDate::from_ymd_opt(ny, nm, 1).unwrap() - Duration::days(1);
    (start, end)
}

#[derive(Clone, Copy, Debug)]
enum StationKind {
    Tide,
    Harmonic,
    Subordinate,
}

#[derive(Clone, Debug)]
struct Pick {
    id: String,
    kind: StationKind,
}

async fn fetch_tide(client: &Client, id: &str, s: NaiveDate, e: NaiveDate) -> Result<Value> {
    let url = datagetter_url(id, s, e, &Product::TidePredictions { interval: "h" });
    client.fetch_json(&url, None).await
}

async fn fetch_current_h(
    client: &Client,
    id: &str,
    bin: i32,
    s: NaiveDate,
    e: NaiveDate,
) -> Result<Value> {
    let url = datagetter_url(id, s, e, &Product::CurrentsHourly { bin: Some(bin) });
    client.fetch_json(&url, None).await
}

async fn fetch_current_events(
    client: &Client,
    id: &str,
    bin: i32,
    s: NaiveDate,
    e: NaiveDate,
) -> Result<Value> {
    let url = datagetter_url(id, s, e, &Product::CurrentsMaxSlack { bin: Some(bin) });
    client.fetch_json(&url, None).await
}

fn tide_err(id: &str, raw: &Value, s: NaiveDate) -> Result<Stats> {
    let mut st = Stats::default();
    let arr = raw["predictions"].as_array().context("predictions")?;
    let t_ref = s.and_hms_opt(0, 0, 0).unwrap();
    let pred = STORE
        .tide_predictor(id, t_ref)
        .context("no tide predictor")?;
    for r in arr {
        let t = parse_dt(r["t"].as_str().unwrap())?;
        let v: f64 = r["v"].as_str().unwrap().parse()?;
        st.push(pred.at(t) - v);
    }
    Ok(st)
}

fn harm_current_err(id: &str, raw: &Value, s: NaiveDate) -> Result<Stats> {
    let mut st = Stats::default();
    let arr = raw["current_predictions"]["cp"].as_array().context("cp")?;
    if arr.is_empty() {
        return Ok(st);
    }
    let bin: i32 = arr[0]["Bin"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let t_ref = s.and_hms_opt(0, 0, 0).unwrap();
    let pred = STORE
        .current_predictor(id, Some(bin), t_ref)
        .context("no current predictor")?;
    let azi_r = pred.azimuth().to_radians();
    for r in arr {
        let t = parse_dt(r["Time"].as_str().unwrap())?;
        let speed = fval(&r["Speed"]);
        let dir_r = fval(&r["Direction"]).to_radians();
        let vn = speed * dir_r.cos();
        let ve = speed * dir_r.sin();
        let noaa_major = vn * azi_r.cos() + ve * azi_r.sin();
        let ours_major = pred.at(t).major / CMS_PER_KNOT;
        st.push(ours_major - noaa_major);
    }
    Ok(st)
}

fn sub_current_err(
    id: &str,
    raw: &Value,
    s: NaiveDate,
    e: NaiveDate,
) -> Result<(Stats, Stats, Stats)> {
    let mut sample = Stats::default();
    let mut t_err = Stats::default();
    let mut a_err = Stats::default();
    let arr = raw["current_predictions"]["cp"].as_array().context("cp")?;
    let entry = STORE.subordinate(id).context("no subordinate")?;
    let offsets: SubordinateOffsets = entry.offsets.clone();
    let t_ref = s.and_hms_opt(0, 0, 0).unwrap();
    let ref_pred = STORE
        .current_predictor(&offsets.ref_id, Some(offsets.ref_bin), t_ref)
        .context("no ref predictor")?;
    let start = s.and_hms_opt(0, 0, 0).unwrap() - Duration::hours(6);
    let end = e.and_hms_opt(23, 59, 0).unwrap() + Duration::hours(6);
    let raw_ev = detect_events(&ref_pred, start, end);
    let our_events = apply_offsets(&raw_ev, &offsets);

    for r in arr {
        let t = parse_dt(r["Time"].as_str().unwrap())?;
        let noaa = r["Velocity_Major"].as_f64().unwrap_or(0.0);
        let ours = interp_events(&our_events, t);
        sample.push(ours - noaa);

        let typ = r["Type"].as_str().unwrap_or("");
        let want: Option<EventKind> = match typ {
            "flood" => Some(EventKind::MaxFlood),
            "ebb" => Some(EventKind::MaxEbb),
            "slack" => None,
            _ => continue,
        };
        let mut best: Option<&Event> = None;
        let mut best_dt = Duration::days(9999);
        for ev in &our_events {
            let ok = match (want, ev.kind) {
                (Some(k), ek) => k == ek,
                (None, EventKind::SlackBeforeEbb | EventKind::SlackBeforeFlood) => true,
                (None, _) => false,
            };
            if !ok {
                continue;
            }
            let d = (ev.t - t).abs();
            if d < best_dt {
                best_dt = d;
                best = Some(ev);
            }
        }
        if let Some(ev) = best
            && best_dt <= Duration::minutes(180)
        {
            t_err.push((ev.t - t).num_seconds() as f64 / 60.0);
            a_err.push(ev.speed_kt - noaa);
        }
    }
    Ok((sample, t_err, a_err))
}

async fn cmd_random(seed: u64, n_stations: usize, n_months: usize) -> Result<()> {
    let mut rng = SplitMix64::new(seed);

    let mut pool: Vec<Pick> = Vec::new();
    for e in &STORE.tides {
        pool.push(Pick {
            id: e.info.id.clone(),
            kind: StationKind::Tide,
        });
    }
    for e in &STORE.currents {
        pool.push(Pick {
            id: e.info.id.clone(),
            kind: StationKind::Harmonic,
        });
    }
    for e in &STORE.subordinates {
        pool.push(Pick {
            id: e.info.id.clone(),
            kind: StationKind::Subordinate,
        });
    }
    pool.sort_by(|a, b| a.id.cmp(&b.id));
    println!(
        "pool: {} stations (tides + harmonic + subordinate)",
        pool.len()
    );

    let picks: Vec<Pick> = rng
        .sample_without_replacement(pool.len(), n_stations)
        .into_iter()
        .map(|i| pool[i].clone())
        .collect();
    let mut months: Vec<(i32, u32)> = Vec::new();
    while months.len() < n_months {
        let y =
            RANDOM_YEAR_MIN + rng.below((RANDOM_YEAR_MAX - RANDOM_YEAR_MIN + 1) as usize) as i32;
        let m = 1 + rng.below(12) as u32;
        let pair = (y, m);
        if !months.contains(&pair) {
            months.push(pair);
        }
    }
    println!("stations picked:");
    for p in &picks {
        println!("  {} {:?}", p.id, p.kind);
    }
    println!("months picked: {months:?}");

    let client = Client::new().no_cache();
    let jobs: Vec<(Pick, i32, u32)> = picks
        .iter()
        .flat_map(|p| months.iter().map(move |&(y, m)| (p.clone(), y, m)))
        .collect();
    println!("\nfetching {} (station × month) jobs…", jobs.len());

    let results: Vec<(
        Pick,
        i32,
        u32,
        Option<(Stats, Option<Stats>, Option<Stats>)>,
        bool,
    )> = stream::iter(jobs)
        .map(|(p, y, m)| {
            let client = client.clone();
            async move {
                let (s, e) = month_bounds(y, m);
                let res: Result<(Stats, Option<Stats>, Option<Stats>)> = async {
                    match p.kind {
                        StationKind::Tide => match fetch_tide(&client, &p.id, s, e).await {
                            Ok(raw) => Ok((tide_err(&p.id, &raw, s)?, None, None)),
                            Err(err) => {
                                let msg = err.to_string();
                                if msg.contains("No Predictions data") {
                                    return Err(anyhow!(
                                        "NOAA has no tide predictions for {}",
                                        p.id
                                    ));
                                }
                                Err(err)
                            },
                        },
                        StationKind::Harmonic => {
                            let probe = match fetch_current_h(&client, &p.id, 1, s, s).await {
                                Ok(v) => v,
                                Err(e1) => {
                                    let msg = e1.to_string();
                                    let Some(bin) = parse_available_bin(&msg) else {
                                        return Err(e1);
                                    };
                                    fetch_current_h(&client, &p.id, bin, s, s).await?
                                },
                            };
                            let bin: i32 = probe["current_predictions"]["cp"]
                                .as_array()
                                .and_then(|a| a.first())
                                .and_then(|r| r["Bin"].as_str())
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(1);
                            let raw = fetch_current_h(&client, &p.id, bin, s, e).await?;
                            Ok((harm_current_err(&p.id, &raw, s)?, None, None))
                        },
                        StationKind::Subordinate => {
                            let raw = match fetch_current_events(&client, &p.id, 1, s, e).await {
                                Ok(v) => v,
                                Err(e1) => {
                                    let msg = e1.to_string();
                                    let Some(bin) = parse_available_bin(&msg) else {
                                        return Err(e1);
                                    };
                                    fetch_current_events(&client, &p.id, bin, s, e).await?
                                },
                            };
                            let (sa, te, ae) = sub_current_err(&p.id, &raw, s, e)?;
                            Ok((sa, Some(te), Some(ae)))
                        },
                    }
                }
                .await;
                let (out, skipped) = match res {
                    Ok(t) => (Some(t), false),
                    Err(err) => {
                        let msg = err.to_string();
                        let skip = msg.contains("NOAA has no tide predictions");
                        if skip {
                            eprintln!("  SKIP {} {}-{:02}: {msg}", p.id, y, m);
                        } else {
                            eprintln!("  FAIL {} {}-{:02}: {err}", p.id, y, m);
                        }
                        (None, skip)
                    },
                };
                (p, y, m, out, skipped)
            }
        })
        .buffer_unordered(MAX_CONCURRENT)
        .collect()
        .await;

    let mut per_station: BTreeMap<String, (StationKind, Stats, Stats, Stats)> = BTreeMap::new();
    let mut agg_tide = Stats::default();
    let mut agg_harm = Stats::default();
    let mut agg_sub_sample = Stats::default();
    let mut agg_sub_t = Stats::default();
    let mut agg_sub_a = Stats::default();
    let mut fails = 0_usize;
    let mut skips = 0_usize;
    for (p, _y, _m, r, skipped) in &results {
        let Some((sample, te, ae)) = r else {
            if *skipped {
                skips += 1;
            } else {
                fails += 1;
            }
            continue;
        };
        let entry = per_station
            .entry(p.id.clone())
            .or_insert_with(|| (p.kind, Stats::default(), Stats::default(), Stats::default()));
        entry.1.merge(sample);
        if let Some(t) = te {
            entry.2.merge(t);
        }
        if let Some(a) = ae {
            entry.3.merge(a);
        }
        match p.kind {
            StationKind::Tide => agg_tide.merge(sample),
            StationKind::Harmonic => agg_harm.merge(sample),
            StationKind::Subordinate => {
                agg_sub_sample.merge(sample);
                if let Some(t) = te {
                    agg_sub_t.merge(t);
                }
                if let Some(a) = ae {
                    agg_sub_a.merge(a);
                }
            },
        }
    }

    println!(
        "\n=== per-station ({} stations, {fails} fails, {skips} skips) ===",
        per_station.len()
    );
    println!(
        "{:<10} {:>4} {:>8} {:>10} {:>10} {:>8} {:>8}",
        "station", "type", "n", "mean", "max", "t_max", "a_max"
    );
    for (id, (kind, s, t, a)) in &per_station {
        let k = match kind {
            StationKind::Tide => "tide",
            StationKind::Harmonic => "harm",
            StationKind::Subordinate => "sub",
        };
        let unit = if matches!(kind, StationKind::Tide) {
            "ft"
        } else {
            "kt"
        };
        println!(
            "{:<10} {:>4} {:>8} {:>10.4} {:>10.4} {:>8.1} {:>8.4}  ({unit})",
            id,
            k,
            s.n,
            s.mean(),
            s.max_abs,
            t.max_abs,
            a.max_abs
        );
    }
    println!("\n=== aggregate by type ===");
    println!(
        "tides:                 n={} mean={:.4} ft  max={:.4} ft",
        agg_tide.n,
        agg_tide.mean(),
        agg_tide.max_abs
    );
    println!(
        "harmonic currents:     n={} mean={:.4} kt  max={:.4} kt",
        agg_harm.n,
        agg_harm.mean(),
        agg_harm.max_abs
    );
    println!(
        "subordinate (sample):  n={} mean={:.4} kt  max={:.4} kt",
        agg_sub_sample.n,
        agg_sub_sample.mean(),
        agg_sub_sample.max_abs
    );
    println!(
        "subordinate (matched): n={} time mean={:.2} min max={:.1} min, amp mean={:.4} kt max={:.4} kt",
        agg_sub_t.n,
        agg_sub_t.mean(),
        agg_sub_t.max_abs,
        agg_sub_a.mean(),
        agg_sub_a.max_abs
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// entry
// -----------------------------------------------------------------------------

fn parse_hex_u64(s: String) -> std::result::Result<u64, String> {
    u64::from_str_radix(s.trim_start_matches("0x"), 16).map_err(|e| format!("bad hex '{s}': {e}"))
}

#[derive(Bpaf, Debug, Clone)]
#[bpaf(options, version)]
enum Cmd {
    #[bpaf(command)]
    /// Validate one tide station against NOAA hourly predictions.
    Tide {
        #[bpaf(positional("STATION"), fallback("8518750".to_string()))]
        station: String,
        #[bpaf(positional::<String>("DATE"), parse(parse_date), fallback(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()))]
        date: NaiveDate,
        #[bpaf(positional("HOURS"), fallback(168))]
        hours: u32,
    },
    #[bpaf(command)]
    /// Validate one harmonic current station, NOAA hourly.
    Current {
        #[bpaf(positional("STATION"), fallback("HUR0611".to_string()))]
        station: String,
        #[bpaf(positional::<String>("DATE"), parse(parse_date), fallback(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()))]
        date: NaiveDate,
        #[bpaf(positional("HOURS"), fallback(48))]
        hours: u32,
    },
    #[bpaf(command)]
    /// Validate one harmonic current station + bin for a full year.
    Bin {
        #[bpaf(positional("STATION"), fallback("n03020".to_string()))]
        station: String,
        #[bpaf(positional("BIN"), fallback(7))]
        bin: i32,
        #[bpaf(positional("YEAR"), fallback(2025))]
        year: i32,
    },
    #[bpaf(command("nyc-tides"))]
    /// 10 NYC tide stations, hilo, 1 month.
    NycTides,
    #[bpaf(command("nyc-harmonic"))]
    /// Every harmonic current station in the NYC box.
    NycHarmonic {
        #[bpaf(positional::<String>("DATE"), parse(parse_date), fallback(NaiveDate::from_ymd_opt(2025, 6, 1).unwrap()))]
        date: NaiveDate,
        #[bpaf(positional("HOURS"), fallback(720))]
        hours: u32,
    },
    #[bpaf(command("nyc-subordinate"))]
    /// Every subordinate current station in the NYC box.
    NycSubordinate {
        #[bpaf(positional::<String>("DATE"), parse(parse_date), fallback(NaiveDate::from_ymd_opt(2025, 6, 1).unwrap()))]
        date: NaiveDate,
        #[bpaf(positional("HOURS"), fallback(720))]
        hours: u32,
    },
    #[bpaf(command)]
    /// SplitMix64 sample across the whole store.
    Random {
        #[bpaf(positional::<String>("SEED_HEX"), parse(parse_hex_u64), fallback(RANDOM_DEFAULT_SEED))]
        seed: u64,
        #[bpaf(positional("N_STATIONS"), fallback(RANDOM_DEFAULT_STATIONS))]
        n_stations: usize,
        #[bpaf(positional("N_MONTHS"), fallback(RANDOM_DEFAULT_MONTHS))]
        n_months: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    match cmd().run() {
        Cmd::Tide {
            station,
            date,
            hours,
        } => cmd_tide(station, date, hours).await,
        Cmd::Current {
            station,
            date,
            hours,
        } => cmd_current(station, date, hours).await,
        Cmd::Bin { station, bin, year } => cmd_bin(station, bin, year).await,
        Cmd::NycTides => cmd_nyc_tides().await,
        Cmd::NycHarmonic { date, hours } => cmd_nyc_harmonic(date, hours).await,
        Cmd::NycSubordinate { date, hours } => cmd_nyc_subordinate(date, hours).await,
        Cmd::Random {
            seed,
            n_stations,
            n_months,
        } => cmd_random(seed, n_stations, n_months).await,
    }
}
