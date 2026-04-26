//! Offline tidal height and current predictor driven by NOAA harmonic
//! constants. The [`predictor`] module is pure math (no async, no HTTP) and
//! the [`noaa`] module is a download-and-cache client for
//! `api.tidesandcurrents.noaa.gov`.
//!
//! Basic usage:
//! ```no_run
//! use noaa_tides::{noaa, predictor::Predictor};
//! use chrono::{NaiveDate, Duration};
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let client = noaa::Client::default();
//! let harcon = client.harcon("8518750", None).await?.expect_tide()?;
//! let t_ref = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap().and_hms_opt(0, 0, 0).unwrap();
//! let predictor = Predictor::new(&harcon, t_ref);
//! println!("h(t) = {:.2}", predictor.at(t_ref + Duration::hours(3)));
//! # Ok(())
//! # }
//! ```

mod error_utils;

pub mod bundled;
pub mod events;
pub mod noaa;
pub mod predictor;
pub mod prelude;
pub mod store;
pub mod util;

pub use bundled::STORE;
pub use bundled::cached_reference_events;
pub use bundled::local_to_utc;
pub use bundled::round_to_30m;
pub use events::Event;
pub use events::EventKind;
pub use events::apply_offsets;
pub use events::detect_events;
pub use events::interp_events;
pub use noaa::ApiProxy;
#[cfg(not(target_arch = "wasm32"))]
pub use noaa::Client;
pub use noaa::HarconKind;
pub use noaa::StationInfo;
pub use noaa::SubordinateOffsets;
pub use predictor::CurrentHarconData;
pub use predictor::CurrentPredictor;
pub use predictor::Predictor;
pub use predictor::constituent_speed;
pub use predictor::debug_astro;
pub use predictor::debug_v0;
pub use store::HarconStore;
