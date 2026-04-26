//! Measures (a) total on-disk/on-wire size of the harcon records required to
//! predict every station kayaknav uses within 1° of Battery NY, and (b) the
//! CPU time to compute one month of predictions at 6-minute resolution for
//! all of them.
//!
//! Sizes are reported as raw NOAA JSON and as a compact binary form
//! (constituent name + 5 f64 per constituent + a few scalars per station).

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Instant;

use chrono::Duration;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use futures::stream::StreamExt;
use futures::stream::{
    self,
};
use noaa_tides::Client;
use noaa_tides::CurrentHarconData;
use noaa_tides::CurrentPredictor;
use noaa_tides::HarconKind;
use noaa_tides::StationInfo;
use noaa_tides::prelude::*;
use noaa_tides::util::Product;
use noaa_tides::util::datagetter_url;
use noaa_tides::util::in_nyc_box;
use serde_json::Value;

fn in_box(s: &StationInfo) -> bool {
    (s.station_type == "H" || s.station_type == "S") && in_nyc_box(s)
}

async fn probe_default_bin(station: &str) -> Result<Option<i32>> {
    let d = NaiveDate::from_ymd_opt(2025, 6, 1).unwrap();
    let url = datagetter_url(station, d, d, &Product::CurrentsHourly { bin: None });
    let body = reqwest::get(&url).await?.text().await?;
    let v: Value = serde_json::from_str(&body)?;
    Ok(v["current_predictions"]["cp"][0]["Bin"]
        .as_str()
        .and_then(|s| s.parse::<i32>().ok()))
}

async fn fetch_raw_harcon(station: &str, bin: Option<i32>) -> Result<(Value, usize)> {
    let url = format!(
        "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{}/harcon.json{}",
        station,
        match bin {
            Some(b) => format!("?bin={b}"),
            None => String::new(),
        },
    );
    let body = reqwest::get(&url).await?.text().await?;
    let v: Value = serde_json::from_str(&body)?;
    Ok((v, body.len()))
}

/// Compact-binary estimate: name length + 6-byte name + 5 f64 + speed f64 =
/// 55 bytes/constituent. Fixed-size header: bin nbr + depth + units + azi +
/// two mean components.
fn compact_size_current(h: &CurrentHarconData) -> usize {
    let header = 4 + 4 + 8 + h.units.len() + 8 + 8 + 8;
    let per_const = 1 + 6 + 5 * 8 + 8;
    header + per_const * h.constituents.len()
}
fn compact_size_offsets() -> usize {
    60
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let client = Client::new();
    let stations: Vec<StationInfo> = client
        .current_stations()
        .await?
        .into_iter()
        .filter(in_box)
        .collect();
    let (h_list, s_list): (Vec<_>, Vec<_>) = stations
        .iter()
        .cloned()
        .partition(|s| s.station_type == "H");
    eprintln!(
        "Stations: {} harmonic, {} subordinate",
        h_list.len(),
        s_list.len()
    );

    // Probe NOAA's default bin per harmonic station.
    eprintln!("Probing default bin for harmonic stations...");
    let h_bins: Vec<(StationInfo, Option<i32>)> = stream::iter(h_list.iter().cloned())
        .map(|s| async move {
            let bin = probe_default_bin(&s.id).await.ok().flatten();
            (s, bin)
        })
        .buffer_unordered(8)
        .collect()
        .await;

    eprintln!("Fetching offsets for subordinate stations...");
    let s_refs: Vec<(StationInfo, Option<(String, i32)>, usize)> =
        stream::iter(s_list.iter().cloned())
            .map(|s| {
                let client = client.clone();
                async move {
                    match client.subordinate_offsets(&s.id, None).await {
                        Ok(o) => (s, Some((o.ref_id, o.ref_bin)), 0),
                        Err(_) => (s, None, 0),
                    }
                }
            })
            .buffer_unordered(8)
            .collect()
            .await;

    let mut need: HashSet<(String, Option<i32>)> = HashSet::new();
    for (s, bin) in &h_bins {
        need.insert((s.id.clone(), *bin));
    }
    for (_, r, _) in &s_refs {
        if let Some((id, b)) = r {
            need.insert((id.clone(), Some(*b)));
        }
    }
    let need: Vec<(String, Option<i32>)> = need.into_iter().collect();
    eprintln!("Unique (station, bin) harcons needed: {}", need.len());

    eprintln!("Fetching harcons...");
    let raw_and_compact: Vec<(String, Option<i32>, usize, usize, Option<CurrentHarconData>)> =
        stream::iter(need.clone().into_iter())
            .map(|(id, bin)| {
                let client = client.clone();
                async move {
                    let (_raw, raw_size) = match fetch_raw_harcon(&id, bin).await {
                        Ok(v) => v,
                        Err(_) => return (id, bin, 0, 0, None),
                    };
                    let typed: Option<CurrentHarconData> = match client.harcon(&id, bin).await {
                        Ok(HarconKind::Current(c)) => Some(c),
                        _ => None,
                    };
                    let compact = typed.as_ref().map(compact_size_current).unwrap_or(0);
                    (id, bin, raw_size, compact, typed)
                }
            })
            .buffer_unordered(8)
            .collect()
            .await;

    let raw_total: usize = raw_and_compact.iter().map(|r| r.2).sum();
    let compact_total: usize = raw_and_compact.iter().map(|r| r.3).sum();
    let offsets_compact: usize =
        s_refs.iter().filter(|(_, r, _)| r.is_some()).count() * compact_size_offsets();

    println!("\n=== Data size for the NYC-box station set ===");
    println!("Unique harcons needed:    {}", need.len());
    println!(
        "Raw NOAA harcon JSON:     {:>10} bytes ({:.2} MB)",
        raw_total,
        raw_total as f64 / 1e6
    );
    println!(
        "Compact binary harcons:   {:>10} bytes ({:.2} MB)",
        compact_total,
        compact_total as f64 / 1e6
    );
    println!(
        "Compact binary offsets:   {:>10} bytes ({:.2} KB)",
        offsets_compact,
        offsets_compact as f64 / 1e3
    );
    println!(
        "Compact total (harcon + offsets): {:.2} MB",
        (compact_total + offsets_compact) as f64 / 1e6
    );

    // Build predictors and run one month at 6-min resolution.
    let mut predictors: HashMap<(String, Option<i32>), CurrentPredictor> = HashMap::new();
    let t_ref: NaiveDateTime = NaiveDate::from_ymd_opt(2025, 6, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    for (id, bin, _, _, typed) in &raw_and_compact {
        if let Some(h) = typed {
            predictors.insert((id.clone(), *bin), CurrentPredictor::new(h, t_ref));
        }
    }
    let mut call_list: Vec<&CurrentPredictor> = Vec::new();
    for (s, bin) in &h_bins {
        if let Some(p) = predictors.get(&(s.id.clone(), *bin)) {
            call_list.push(p);
        }
    }
    for (_, r, _) in &s_refs {
        if let Some((id, b)) = r {
            if let Some(p) = predictors.get(&(id.clone(), Some(*b))) {
                call_list.push(p);
            }
        }
    }
    eprintln!(
        "Active predictors: {} unique, {} station-instances",
        predictors.len(),
        call_list.len()
    );

    let start: NaiveDateTime = NaiveDate::from_ymd_opt(2025, 6, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let samples: Vec<NaiveDateTime> = (0..(30 * 24 * 10))
        .map(|i| start + Duration::seconds((i as i64) * 6 * 60))
        .collect();

    let mut sink = 0.0f64;
    let t0 = Instant::now();
    for p in &call_list {
        for t in &samples {
            let r = p.at(*t);
            sink += r.speed + r.major + r.minor;
        }
    }
    let elapsed = t0.elapsed();
    let total_calls = call_list.len() * samples.len();
    println!("\n=== Prediction benchmark (30 days @ 6-min, both axes + mean) ===");
    println!("Stations evaluated:       {}", call_list.len());
    println!("Samples per station:      {}", samples.len());
    println!("Total predictor calls:    {}", total_calls);
    println!("Wall time:                {:.3} s", elapsed.as_secs_f64());
    println!(
        "Per-station month time:   {:.3} ms",
        elapsed.as_secs_f64() * 1000.0 / call_list.len() as f64
    );
    println!(
        "Per-call time:            {:.2} µs",
        elapsed.as_nanos() as f64 / total_calls as f64 / 1000.0
    );
    eprintln!("(sink = {sink})");

    Ok(())
}
