//! Smoke test for kayaknav's offline tide/current pipeline. Runs
//! Station::tide_prediction for Battery NY and Station::current_prediction
//! for one harmonic + one subordinate station in the NYC box. Verifies
//! DataFrame shapes match what the UI expects.

use chrono::Datelike;
use chrono::Local;
use chrono::NaiveDate;
use kayaknav::noaa::Station;
use kayaknav::noaa::StationType;
use kayaknav::prelude::*;
use noaa_tides::ApiProxy;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let _ = ApiProxy { url: String::new() };

    let today = Local::now().date_naive();
    let start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap();
    let hours: u32 = 24 * 7;

    let battery = Station::new("8518750", None).await?;
    println!("Battery: {} ({})", battery.name, battery.id);
    let tide_df = battery.tide_prediction(start, hours).await?;
    println!("tide df shape: {:?}", tide_df.shape());
    println!("tide df cols : {:?}", tide_df.get_column_names());
    println!("first 5 rows:\n{}", tide_df.head(Some(5)));

    let stations = Station::in_area((39.7, 41.7), (-75.0, -73.0), None).await?;
    println!("Found {} stations in NYC box", stations.len());

    let harmonic = stations
        .iter()
        .find(|s| matches!(s.type_, StationType::Harmonic))
        .cloned()
        .expect("at least one harmonic station");
    println!("Harmonic: {} ({})", harmonic.name, harmonic.id);
    let cp_h = harmonic.current_prediction(start, hours).await?;
    println!("harm df shape: {:?}", cp_h.df.shape());
    println!("harm df cols : {:?}", cp_h.df.get_column_names());
    println!("harm head:\n{}", cp_h.df.head(Some(5)));

    let subord = stations
        .iter()
        .find(|s| matches!(s.type_, StationType::Subordinate))
        .cloned()
        .expect("at least one subordinate station");
    println!("Subord: {} ({})", subord.name, subord.id);
    let cp_s = subord.current_prediction(start, hours).await?;
    println!("sub df shape: {:?}", cp_s.df.shape());
    println!("sub df cols : {:?}", cp_s.df.get_column_names());
    println!("sub head:\n{}", cp_s.df.head(Some(5)));

    Ok(())
}
