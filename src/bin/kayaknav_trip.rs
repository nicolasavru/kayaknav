//! CLI tool that computes best-departure times for a trip defined by a
//! waypoints JSON file. Given a trip file (same schema as the UI's
//! Import/Export Waypoints panel), loads NOAA current-prediction
//! stations around the waypoint bounding box, runs the sweep
//! synchronously, and prints a JSON payload describing every surviving
//! "fast start" — start time, end time, per-waypoint arrivals, and
//! per-segment distance/duration/speed.
//!
//! Usage:
//!   kayaknav_trip --trip <path> [--date YYYY-MM-DD] [--hours N] [--speed KT]
//!
//! Loads predictions via `Station::in_area` with a fixed padding around
//! the trip's lat/lon bbox — no tile UI, no panning, no background
//! worker. Output goes to stdout.

use std::path::PathBuf;

use bpaf::Parser;
use chrono::Local;
use chrono::NaiveDate;
use kayaknav::WeekdayFlags;
use kayaknav::noaa::Station;
use kayaknav::prelude::*;
use kayaknav::scheduling::Trip;
use uom::si::f64::Velocity;
use uom::si::velocity::knot;

/// Degrees of latitude/longitude padding added to the waypoint bbox
/// before querying stations. At mid-latitudes 0.5° ≈ 55 km — enough
/// that a trip inside a small bay still picks up the nearest offshore
/// current stations without pulling in the entire coastline.
const BBOX_PAD_DEG: f64 = 0.5;

struct CliArgs {
    trip: PathBuf,
    date: Option<NaiveDate>,
    hours: u32,
    speed_kt: f64,
    weekdays: WeekdayFlags,
    daytime: bool,
    output: Option<PathBuf>,
}

/// Parse the `--weekdays` argument. Accepts three forms:
///   - `all` (default): every day of the week survives the filter
///   - `weekdays`: Mon–Fri (matching the common English idiom)
///   - `weekends`: Sat–Sun
///   - comma-separated day tokens: `mon,tue,fri` (case-insensitive,
///     accepts the same 3-letter abbreviations the UI uses)
///
/// An empty result is rejected — that would filter *every* start out
/// of the sweep and produce an empty export, which is almost certainly
/// a typo rather than an intentional request.
fn parse_weekdays(raw: String) -> std::result::Result<WeekdayFlags, String> {
    let trimmed = raw.trim().to_ascii_lowercase();
    match trimmed.as_str() {
        "all" => return Ok(WeekdayFlags::all()),
        "weekdays" => {
            return Ok(WeekdayFlags::Mon
                | WeekdayFlags::Tue
                | WeekdayFlags::Wed
                | WeekdayFlags::Thu
                | WeekdayFlags::Fri);
        },
        "weekends" => return Ok(WeekdayFlags::Sat | WeekdayFlags::Sun),
        _ => {},
    }
    let mut flags = WeekdayFlags::empty();
    for tok in trimmed.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        let day = match tok {
            "mon" => WeekdayFlags::Mon,
            "tue" => WeekdayFlags::Tue,
            "wed" => WeekdayFlags::Wed,
            "thu" => WeekdayFlags::Thu,
            "fri" => WeekdayFlags::Fri,
            "sat" => WeekdayFlags::Sat,
            "sun" => WeekdayFlags::Sun,
            other => {
                return Err(format!(
                    "unknown weekday `{other}`; expected one of mon,tue,wed,thu,fri,sat,sun \
                     (or all/weekdays/weekends)"
                ));
            },
        };
        flags |= day;
    }
    if flags.is_empty() {
        return Err("`--weekdays` parsed to an empty set — no starts would survive".into());
    }
    Ok(flags)
}

fn parse_args() -> CliArgs {
    let trip = bpaf::long("trip")
        .help("Path to a waypoints JSON file (schema matches the UI's Import/Export panel).")
        .argument::<PathBuf>("PATH");

    let date = bpaf::long("date")
        .help("Trip date window start (YYYY-MM-DD). Defaults to today (local time).")
        .argument::<String>("YYYY-MM-DD")
        .parse(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d"))
        .optional();

    let hours = bpaf::long("hours")
        .help("Prediction window length in hours. Defaults to 168 (one week).")
        .argument::<u32>("N")
        .fallback(168)
        .display_fallback();

    let speed_kt = bpaf::long("speed")
        .help("Base over-ground speed in knots. Defaults to 3.0.")
        .argument::<f64>("KT")
        .fallback(3.0)
        .display_fallback();

    let weekdays = bpaf::long("weekdays")
        .help(
            "Which weekdays a departure may start on. Accepts `all`, `weekdays` (Mon-Fri), \
             `weekends` (Sat-Sun), or a comma-separated list like `mon,tue,fri`. \
             Defaults to `all`.",
        )
        .argument::<String>("DAYS")
        .parse(parse_weekdays)
        .fallback(WeekdayFlags::all());

    let daytime = bpaf::long("daytime")
        .help(
            "Restrict to daytime trips: starts must be 08:00 or later, and the trip must \
             finish before 21:00. Off by default.",
        )
        .switch();

    let output = bpaf::long("output")
        .short('o')
        .help("Write the JSON export to this file instead of stdout.")
        .argument::<PathBuf>("PATH")
        .optional();

    bpaf::construct!(CliArgs {
        trip,
        date,
        hours,
        speed_kt,
        weekdays,
        daytime,
        output,
    })
    .to_options()
    .run()
}

/// Compute the lat/lon bounding box over the waypoints listed in a
/// waypoints JSON file. Parses only the fields we need (no projection,
/// no type handling) so the CLI can plan its station query before
/// constructing a full `Trip`.
fn waypoint_bbox(trip_json: &str) -> Result<((f64, f64), (f64, f64))> {
    let raw: serde_json::Value =
        serde_json::from_str(trip_json).context("failed to parse trip JSON")?;
    let arr = raw
        .get("waypoints")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("trip JSON missing `waypoints` array"))?;
    if arr.is_empty() {
        bail!("trip JSON contains zero waypoints");
    }
    let (mut lat_min, mut lat_max) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut lon_min, mut lon_max) = (f64::INFINITY, f64::NEG_INFINITY);
    for w in arr {
        let lat = w
            .get("lat")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| anyhow!("waypoint missing `lat`"))?;
        let lon = w
            .get("lon")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| anyhow!("waypoint missing `lon`"))?;
        lat_min = lat_min.min(lat);
        lat_max = lat_max.max(lat);
        lon_min = lon_min.min(lon);
        lon_max = lon_max.max(lon);
    }
    Ok((
        (lat_min - BBOX_PAD_DEG, lat_max + BBOX_PAD_DEG),
        (lon_min - BBOX_PAD_DEG, lon_max + BBOX_PAD_DEG),
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = parse_args();

    let trip_json = std::fs::read_to_string(&args.trip)
        .with_context(|| format!("failed to read trip file: {}", args.trip.display()))?;

    let (lat_range, lon_range) = waypoint_bbox(&trip_json)?;
    eprintln!(
        "bbox lat=[{:.4},{:.4}] lon=[{:.4},{:.4}]",
        lat_range.0, lat_range.1, lon_range.0, lon_range.1
    );

    let stations = Station::in_area(lat_range, lon_range, None)
        .await
        .context("failed to query stations in bbox")?;
    let mut stations: Vec<Station> = stations.into_iter().collect();
    // Sort by id so both prediction order and any downstream ties are
    // deterministic across runs — mirrors the regression test.
    stations.sort_by(|a, b| a.id.cmp(&b.id));
    eprintln!("loaded {} stations in bbox", stations.len());
    if stations.is_empty() {
        bail!("no stations found within {BBOX_PAD_DEG}° of waypoint bbox");
    }

    let start_date = args.date.unwrap_or_else(|| Local::now().date_naive());
    eprintln!(
        "computing {}h of predictions starting {}",
        args.hours, start_date
    );

    // Sequential `.await` per station — simple, and the whole bbox
    // typically has <200 stations so the wall-clock is fine for a CLI.
    // Stations that fail to predict are skipped rather than aborting
    // the whole run; matches the UI's loader tolerance.
    let mut predictions = Vec::with_capacity(stations.len());
    for s in &stations {
        match s.current_prediction(start_date, args.hours).await {
            Ok(p) => predictions.push(p),
            Err(e) => eprintln!("skipping station {}: {e}", s.id),
        }
    }
    eprintln!(
        "computed predictions for {}/{} stations",
        predictions.len(),
        stations.len()
    );

    let mut trip = Trip::new_headless(Velocity::new::<knot>(args.speed_kt), predictions)?;
    // Override the headless defaults (all weekdays, non-daytime) only
    // now that we've parsed the CLI flags — `new_headless` pre-seeds
    // `all()` so the `Trip::set_weekdays` short-circuit on an
    // unchanged value stays cheap.
    trip.set_weekdays(args.weekdays);
    trip.set_daytime(args.daytime);

    let count = trip
        .import_waypoints_json(&trip_json)
        .context("failed to import waypoints into Trip")?;
    eprintln!("imported {count} waypoints from {}", args.trip.display());

    let json = trip
        .export_best_departures_json()
        .context("failed to compute best departures")?;
    match &args.output {
        Some(path) => {
            std::fs::write(path, &json)
                .with_context(|| format!("failed to write output to {}", path.display()))?;
            eprintln!("wrote {} bytes to {}", json.len(), path.display());
        },
        None => println!("{json}"),
    }
    Ok(())
}
