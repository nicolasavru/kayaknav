//! Diagnostic: fetch a full year of NOAA online current predictions for a
//! station, compute our offline prediction at the same timestamps, and
//! project every signal (NOAA, ours, residual) onto each constituent
//! frequency. Prints a full per-constituent table for both major and
//! minor axes so any new accuracy spike can be traced to the offending
//! constituent.
//!
//! Usage: cargo run --bin spectrum_currents -- [station_id] [year]

use chrono::Days;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use noaa_tides::Client;
use noaa_tides::CurrentPredictor;
use noaa_tides::prelude::*;
use noaa_tides::util::CMS_PER_KNOT;
use noaa_tides::util::Product;
use noaa_tides::util::datagetter_url;
use serde_json::Value;

async fn fetch_month(
    station: &str,
    begin: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<(NaiveDateTime, f64, f64)>> {
    let url = datagetter_url(station, begin, end, &Product::CurrentsHourly { bin: None });
    let body = reqwest::get(&url).await?.text().await?;
    let resp: Value = serde_json::from_str(&body).map_err(|e| {
        anyhow!(
            "parse error {}..{}: {e}. body starts: {:?}",
            begin,
            end,
            &body.chars().take(200).collect::<String>()
        )
    })?;
    let preds = resp["current_predictions"]["cp"]
        .as_array()
        .ok_or_else(|| anyhow!("no predictions in {}..{}: {resp}", begin, end))?;
    let mut out = Vec::with_capacity(preds.len());
    for p in preds {
        let t_str = p["Time"].as_str().ok_or_else(|| anyhow!("no Time"))?;
        let v_str = p["Speed"].as_str().ok_or_else(|| anyhow!("no Speed"))?;
        let t = NaiveDateTime::parse_from_str(t_str, "%Y-%m-%d %H:%M")?;
        let speed: f64 = v_str.parse()?;
        let dir_deg = p["Direction"].as_f64().unwrap_or(0.0);
        out.push((t, speed, dir_deg));
    }
    Ok(out)
}

/// Fourier projection of series r(t) (sampled at arbitrary times, in hours)
/// onto exp(i·ω·t). Returns (sin coefficient, cos coefficient) — both
/// ×2/N so that |(sc + i·cc)| equals the classical single-frequency amplitude.
fn project(series: &[(f64, f64)], omega: f64) -> (f64, f64) {
    let n = series.len() as f64;
    let (mut sc, mut cc) = (0.0, 0.0);
    for (t, r) in series {
        sc += r * (omega * t).sin();
        cc += r * (omega * t).cos();
    }
    (2.0 * sc / n, 2.0 * cc / n)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args: Vec<String> = std::env::args().collect();
    let station_id = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "HUR0611".to_string());
    let year: i32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2024);

    println!("Fetching {} for {} (hourly, full year)", station_id, year);

    let client = Client::new();
    let base_id = station_id.split('_').next().unwrap();
    let probe_url = datagetter_url(
        &station_id,
        NaiveDate::from_ymd_opt(year, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(year, 1, 2).unwrap(),
        &Product::CurrentsHourly { bin: None },
    );
    let probe: Value = serde_json::from_str(&reqwest::get(&probe_url).await?.text().await?)?;
    let noaa_bin = probe["current_predictions"]["cp"][0]["Bin"]
        .as_str()
        .and_then(|s| s.parse::<i32>().ok());
    println!("NOAA online default bin: {:?}", noaa_bin);
    let harcon = client.harcon(base_id, noaa_bin).await?.expect_current()?;

    let t_ref = NaiveDate::from_ymd_opt(year, 7, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let predictor = CurrentPredictor::new(&harcon, t_ref);

    // Fetch the year in 31-day chunks (NOAA's per-request cap).
    let start = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();

    let mut our_mj: Vec<(f64, f64)> = Vec::new();
    let mut our_mn: Vec<(f64, f64)> = Vec::new();
    let mut noaa_mj: Vec<(f64, f64)> = Vec::new();
    let mut noaa_mn: Vec<(f64, f64)> = Vec::new();
    let mut res_mj: Vec<(f64, f64)> = Vec::new();
    let mut res_mn: Vec<(f64, f64)> = Vec::new();

    let azi_r = harcon.azi.to_radians();
    let midnight = start.and_hms_opt(0, 0, 0).unwrap();
    let mut cur = start;
    while cur <= end {
        let chunk_end = (cur + Days::new(30)).min(end);
        let chunk = fetch_month(&station_id, cur, chunk_end).await?;
        for (t, noaa_kt, dir_deg) in chunk {
            let dir_r = dir_deg.to_radians();
            let v_n = noaa_kt * dir_r.cos();
            let v_e = noaa_kt * dir_r.sin();
            let n_mj = v_n * azi_r.cos() + v_e * azi_r.sin();
            let n_mn = -v_n * azi_r.sin() + v_e * azi_r.cos();
            let ours = predictor.at(t);
            let o_mj = ours.major / CMS_PER_KNOT;
            let o_mn = ours.minor / CMS_PER_KNOT;
            let hours = (t - midnight).num_milliseconds() as f64 / 3_600_000.0;
            our_mj.push((hours, o_mj));
            our_mn.push((hours, o_mn));
            noaa_mj.push((hours, n_mj));
            noaa_mn.push((hours, n_mn));
            res_mj.push((hours, n_mj - o_mj));
            res_mn.push((hours, n_mn - o_mn));
        }
        cur = chunk_end + Days::new(1);
    }

    let n = res_mj.len() as f64;
    println!("Fetched {} hourly samples", n);

    let rms = |series: &[(f64, f64)]| -> f64 {
        (series.iter().map(|(_, r)| r * r).sum::<f64>() / n).sqrt()
    };
    let mean = |series: &[(f64, f64)]| -> f64 { series.iter().map(|(_, r)| r).sum::<f64>() / n };

    let noaa_max = noaa_mj
        .iter()
        .zip(noaa_mn.iter())
        .map(|((_, a), (_, b))| (a * a + b * b).sqrt())
        .fold(0.0_f64, f64::max);
    println!(
        "RMS residual: major {:.3} kt, minor {:.3} kt (NOAA |max speed| = {:.2})",
        rms(&res_mj),
        rms(&res_mn),
        noaa_max
    );
    println!(
        "DC bias: major {:+.4} kt, minor {:+.4} kt",
        mean(&res_mj),
        mean(&res_mn)
    );

    // Probe every constituent NOAA publishes for this station, plus a
    // handful of absent long-period probes so we can detect seasonal / node
    // signals the harcon omits. Amplitude = 2·|projection| is the classical
    // single-frequency amplitude.
    let mut probes: Vec<(String, f64, f64, f64, f64, f64)> = harcon
        .constituents
        .iter()
        .map(|c| {
            (
                c.name.clone(),
                c.speed,
                c.major_amplitude / CMS_PER_KNOT,
                c.major_phase_gmt,
                c.minor_amplitude / CMS_PER_KNOT,
                c.minor_phase_gmt,
            )
        })
        .collect();
    for (name, period_h) in &[
        ("SA", 365.2422 * 24.0),
        ("SSA", 182.6211 * 24.0),
        ("MSM", 31.812 * 24.0),
        ("MM", 27.5546 * 24.0),
        ("MSF", 14.7653 * 24.0),
        ("MF", 13.6608 * 24.0),
    ] {
        if !probes.iter().any(|(n, ..)| n == name) {
            probes.push((name.to_string(), 360.0 / period_h, 0.0, 0.0, 0.0, 0.0));
        }
    }
    probes.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    // Per-axis table: for each constituent we show
    //   A_pub : the amplitude NOAA publishes in harcon (0 for probes)
    //   A_noaa: amplitude projected out of NOAA's time series
    //   A_ours: amplitude projected out of our time series
    //   A_res : amplitude projected out of the residual
    //   φ_noaa, φ_ours (from the time series projection, degrees),
    //     and Δφ = φ_ours − φ_noaa, wrapped to (−180, 180].
    //   res_s, res_c: sin / cos coefficients of the residual (=2·N⁻¹ inner prod)
    // The two axes are printed as separate tables.
    let wrap_phase = |d: f64| {
        let w = d.rem_euclid(360.0);
        if w > 180.0 { w - 360.0 } else { w }
    };
    let print_axis = |label: &str, ours: &[(f64, f64)], noaa: &[(f64, f64)], res: &[(f64, f64)]| {
        println!("\n=== {label} axis ===");
        println!(
            "{:<7} {:>9} | {:>6} {:>7} {:>6} {:>7} | {:>6} {:>7} | {:>7} {:>7} {:>7}",
            "name",
            "period(h)",
            "A_pub",
            "A_noaa",
            "A_ours",
            "dA",
            "φ_noaa",
            "φ_ours",
            "dφ",
            "res_s",
            "res_c",
        );
        for (name, speed_deg_h, a_pub_mj, _phi_mj, a_pub_mn, _phi_mn) in &probes {
            let _ = (_phi_mj, _phi_mn);
            let omega = speed_deg_h.to_radians();
            let (sc_o, cc_o) = project(ours, omega);
            let (sc_n, cc_n) = project(noaa, omega);
            let (sc_r, cc_r) = project(res, omega);
            let a_ours = (sc_o * sc_o + cc_o * cc_o).sqrt();
            let a_noaa = (sc_n * sc_n + cc_n * cc_n).sqrt();
            let phi_ours = cc_o.atan2(sc_o).to_degrees();
            let phi_noaa = cc_n.atan2(sc_n).to_degrees();
            let dphi = wrap_phase(phi_ours - phi_noaa);
            let period_h = 360.0 / speed_deg_h;
            // A_pub column differs per axis: caller passes the right one
            // via closure capture: but we need both. Cheat by keying on label.
            let a_pub = if label == "major" {
                *a_pub_mj
            } else {
                *a_pub_mn
            };
            println!(
                "{:<7} {:>9.3} | {:>6.3} {:>7.4} {:>6.4} {:>+7.4} | {:>+6.1} {:>+7.1} | {:>+7.1} {:>+7.4} {:>+7.4}",
                name,
                period_h,
                a_pub,
                a_noaa,
                a_ours,
                a_ours - a_noaa,
                phi_noaa,
                phi_ours,
                dphi,
                sc_r,
                cc_r,
            );
        }
    };

    print_axis("major", &our_mj, &noaa_mj, &res_mj);
    print_axis("minor", &our_mn, &noaa_mn, &res_mn);

    // Parseval summary: how much of the residual variance lives at the
    // probed frequencies vs elsewhere (= unexplained).
    let var_mj: f64 = res_mj.iter().map(|(_, r)| r * r).sum::<f64>() / n;
    let var_mn: f64 = res_mn.iter().map(|(_, r)| r * r).sum::<f64>() / n;
    let mut explained_mj = 0.0;
    let mut explained_mn = 0.0;
    for (_name, speed_deg_h, _, _, _, _) in &probes {
        let omega = speed_deg_h.to_radians();
        let (sc_r, cc_r) = project(&res_mj, omega);
        explained_mj += 0.5 * (sc_r * sc_r + cc_r * cc_r);
        let (sc_r, cc_r) = project(&res_mn, omega);
        explained_mn += 0.5 * (sc_r * sc_r + cc_r * cc_r);
    }
    println!(
        "\nResidual variance: major {:.4e} kt² (explained by probes: {:.2}%), minor {:.4e} kt² ({:.2}%)",
        var_mj,
        100.0 * explained_mj / var_mj,
        var_mn,
        100.0 * explained_mn / var_mn,
    );

    Ok(())
}
