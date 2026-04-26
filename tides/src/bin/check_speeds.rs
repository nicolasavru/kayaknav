//! Cross-check the angular speed (°/hr) we derive from each constituent's
//! Doodson coefficients against the `speed` field NOAA publishes in
//! `harcon.json`. Any mismatch > ~1e-6 is a Doodson-coefficient bug.

use noaa_tides::Client;
use noaa_tides::constituent_speed;
use noaa_tides::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let station = std::env::args().nth(1).unwrap_or_else(|| "8518750".into());
    let client = Client::new();
    let harcon = client.harcon(&station, None).await?.expect_tide()?;

    println!(
        "Station {} ({} constituents):",
        station,
        harcon.constituents.len()
    );
    println!(
        "{:<8}  {:>12}  {:>12}  {:>12}",
        "name", "noaa_speed", "our_speed", "diff"
    );

    let mut max_abs = 0.0_f64;
    let mut unknown: Vec<String> = Vec::new();
    for c in &harcon.constituents {
        let Some(our) = constituent_speed(&c.name) else {
            unknown.push(c.name.clone());
            continue;
        };
        let diff = our - c.speed;
        if diff.abs() > max_abs {
            max_abs = diff.abs();
        }
        if diff.abs() > 1e-6 {
            println!(
                "{:<8}  {:>12.7}  {:>12.7}  {:>12.2e}  <-- MISMATCH",
                c.name, c.speed, our, diff
            );
        } else {
            println!(
                "{:<8}  {:>12.7}  {:>12.7}  {:>12.2e}",
                c.name, c.speed, our, diff
            );
        }
    }
    println!("\nmax |diff| = {:.2e}", max_abs);
    if !unknown.is_empty() {
        println!("Unknown constituents: {:?}", unknown);
    }
    Ok(())
}
