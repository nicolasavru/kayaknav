//! Re-fetch every NOAA JSON under `noaa_out/` using `time_zone=gmt` so
//! all on-disk data is UTC. The offline validator then parses timestamps
//! as UTC without DST disambiguation.
//!
//! Walks the existing directory layout (182 NYC current stations × 8
//! years + Battery tide H/L × 8 years), detects the file shape (hourly
//! speed/dir vs event-style max/slack) and bin from the existing file,
//! and overwrites it in place with the GMT-timestamped response.
//!
//! Default output directory is `noaa_out/` relative to the current working
//! directory (i.e., the workspace root when running via `cargo run`).
//! Override with the `NOAA_OUT` environment variable.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use futures::stream::StreamExt;
use futures::stream::{
    self,
};
use noaa_tides::Client;
use noaa_tides::prelude::*;
use serde_json::Value;

const YEARS: [i32; 8] = [2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032];
const BATTERY_ID: &str = "8518750";
const MAX_CONCURRENT: usize = 8;

fn noaa_out_dir() -> PathBuf {
    std::env::var("NOAA_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("noaa_out"))
}

#[derive(Debug, Clone)]
struct CurrentJob {
    year: i32,
    station: String,
    bin: i32,
    hourly: bool,
    path: PathBuf,
}

fn detect_shape(path: &Path) -> Result<Option<(bool, i32)>> {
    let raw: Value = serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("parse {}", path.display()))?;
    let Some(first) = raw["current_predictions"]["cp"]
        .as_array()
        .and_then(|a| a.first())
    else {
        return Ok(None);
    };
    let hourly = first.get("Speed").is_some();
    let bin: i32 = first["Bin"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow!("no Bin in {}", path.display()))?;
    Ok(Some((hourly, bin)))
}

fn currents_url(j: &CurrentJob) -> String {
    let begin = format!("{}0101", j.year);
    let end = format!("{}1231", j.year);
    let (interval, vel_type) = if j.hourly {
        ("h", "&vel_type=speed_dir")
    } else {
        ("max_slack", "")
    };
    format!(
        "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter\
         ?time_zone=gmt&units=english&application=kayaknav&format=json\
         &product=currents_predictions&interval={interval}{vel_type}\
         &station={station}&bin={bin}&begin_date={begin}&end_date={end}",
        station = j.station,
        bin = j.bin,
    )
}

fn tide_url(year: i32) -> String {
    format!(
        "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter\
         ?station={BATTERY_ID}&begin_date={year}0101&end_date={year}1231\
         &product=predictions&interval=hilo&datum=MLLW\
         &time_zone=gmt&units=english&format=json"
    )
}

fn file_is_valid(path: &Path) -> bool {
    let Ok(bytes) = fs::read(path) else {
        return false;
    };
    let Ok(v) = serde_json::from_slice::<Value>(&bytes) else {
        return false;
    };
    v["current_predictions"]["cp"]
        .as_array()
        .or_else(|| v["predictions"].as_array())
        .is_some_and(|a| a.len() > 10)
}

async fn fetch_and_write(
    client: &Client,
    url: &str,
    path: &Path,
    start: SystemTime,
) -> Result<bool> {
    if path.exists()
        && fs::metadata(path)
            .and_then(|m| m.modified())
            .ok()
            .is_some_and(|t| t >= start)
        && file_is_valid(path)
    {
        return Ok(false);
    }
    let mut last_err: Option<Error> = None;
    for attempt in 0..3 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(3 * attempt)).await;
        }
        match client.fetch_json(url, None).await {
            Ok(v) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).ok();
                }
                fs::write(path, serde_json::to_vec(&v)?)?;
                return Ok(true);
            },
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("retries exhausted")))
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let root = noaa_out_dir();
    println!("output directory: {}", root.display());

    let mut current_jobs: Vec<CurrentJob> = Vec::new();
    let ref_dir = root.join(format!("{}_currents_nyc", YEARS[0]));
    for entry in
        fs::read_dir(&ref_dir).with_context(|| format!("read_dir {}", ref_dir.display()))?
    {
        let name = entry?.file_name().to_string_lossy().to_string();
        let Some(station) = name.strip_suffix(".json") else {
            continue;
        };
        let ref_path = ref_dir.join(&name);
        let Some((hourly, bin)) = detect_shape(&ref_path)
            .with_context(|| format!("detect shape {}", ref_path.display()))?
        else {
            eprintln!("skip {station} (empty reference file)");
            continue;
        };
        for y in YEARS {
            let path: PathBuf = root.join(format!("{y}_currents_nyc/{name}"));
            current_jobs.push(CurrentJob {
                year: y,
                station: station.to_string(),
                bin,
                hourly,
                path,
            });
        }
    }
    let total = current_jobs.len() + YEARS.len();
    println!(
        "fetching {} currents + {} tide files with time_zone=gmt ({} total)",
        current_jobs.len(),
        YEARS.len(),
        total,
    );

    let client = Client::new();
    // Any file whose mtime is newer than the reference is considered a fresh
    // GMT fetch and skipped on retry. Default reference is `<root>/.fetch_cutoff`;
    // falls back to the fetcher binary mtime.
    let start = fs::metadata(root.join(".fetch_cutoff"))
        .and_then(|m| m.modified())
        .or_else(|_| {
            std::env::current_exe()
                .and_then(fs::metadata)
                .and_then(|m| m.modified())
        })
        .unwrap_or_else(|_| SystemTime::now());

    let done = std::sync::atomic::AtomicUsize::new(0);
    let skip = std::sync::atomic::AtomicUsize::new(0);
    let fail = std::sync::atomic::AtomicUsize::new(0);

    stream::iter(current_jobs.iter())
        .for_each_concurrent(MAX_CONCURRENT, |job| {
            let client = client.clone();
            let done = &done;
            let skip = &skip;
            let fail = &fail;
            async move {
                let url = currents_url(job);
                match fetch_and_write(&client, &url, &job.path, start).await {
                    Ok(true) => {
                        let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        if n.is_multiple_of(50) {
                            println!("  currents {n} fetched …");
                        }
                    },
                    Ok(false) => {
                        skip.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    },
                    Err(e) => {
                        fail.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        eprintln!("FAIL {} {}: {e}", job.year, job.station);
                    },
                }
            }
        })
        .await;

    for y in YEARS {
        let path: PathBuf = root.join(format!("tides_battery/{y}.json"));
        let url = tide_url(y);
        match fetch_and_write(&client, &url, &path, start).await {
            Ok(true) => {
                done.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                println!("  tides {y} ok");
            },
            Ok(false) => {
                skip.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
            Err(e) => {
                fail.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                eprintln!("FAIL tides {y}: {e}");
            },
        }
    }

    println!(
        "\ndone: {} fetched, {} skipped (already fresh), {} failed",
        done.load(std::sync::atomic::Ordering::Relaxed),
        skip.load(std::sync::atomic::Ordering::Relaxed),
        fail.load(std::sync::atomic::Ordering::Relaxed),
    );
    Ok(())
}
