//! Download-and-cache client for `api.tidesandcurrents.noaa.gov`.
//!
//! The client fetches harmonic constants (`harcon.json`) and subordinate
//! current-station offsets (`currentpredictionoffsets.json`) and persists
//! the raw JSON to disk so repeat runs don't re-hit NOAA.
//!
//! Default cache location is `~/.cache/noaa-tides/` (resolved via the
//! `dirs` crate). Use [`Client::with_cache_dir`] for a custom path, or
//! [`Client::no_cache`] to disable caching entirely.

#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
#[cfg(not(target_arch = "wasm32"))]
use serde_json::Value;

use crate::predictor::CurrentHarconData;
use crate::predictor::HarconData;
#[cfg(not(target_arch = "wasm32"))]
use crate::predictor::HarmonicConstituent;
#[cfg(not(target_arch = "wasm32"))]
use crate::predictor::VectorConstituent;
#[cfg(not(target_arch = "wasm32"))]
use crate::predictor::constituent_speed;
use crate::prelude::*;

/// Optional URL-wrapping proxy for NOAA API calls.
///
/// Some deployments (e.g. WASM in a browser) route NOAA requests through
/// a CORS-unwrapping proxy. The proxy takes the target URL as an
/// `apiurl=...` query parameter on its own base URL.
#[derive(Debug, Clone)]
pub struct ApiProxy {
    pub url: String,
}

impl ApiProxy {
    pub fn proxied_url(&self, url: &str) -> String {
        self.url.clone() + "?apiurl=" + &*urlencoding::encode(url)
    }
}

/// Entry from NOAA's station-list endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationInfo {
    pub id: String,
    pub name: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    /// "H" for harmonic current, "S" for subordinate current, "R" for tide, …
    pub station_type: String,
}

/// Time and amplitude offsets for a subordinate current station, plus its
/// reference harmonic station + bin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubordinateOffsets {
    pub ref_id: String,
    pub ref_bin: i32,
    pub mfc_time_min: f64,
    pub sbe_time_min: f64,
    pub mec_time_min: f64,
    pub sbf_time_min: f64,
    pub mfc_amp: f64,
    pub mec_amp: f64,
    pub mean_flood_dir: Option<f64>,
    pub mean_ebb_dir: Option<f64>,
}

/// Subset of NOAA `datums.json` needed to shift a zero-mean harmonic tide
/// prediction onto NOAA's published Mean-Lower-Low-Water datum. NOAA's online
/// harmonic predictor reports heights relative to MLLW; our harmonic sum is
/// MSL-relative (zero-mean cosine sum), so we add Z₀ = MSL − MLLW.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StationDatums {
    /// Mean Sea Level, in `units` (typically feet).
    pub msl: f64,
    /// Mean Lower-Low Water.
    pub mllw: f64,
}

impl StationDatums {
    /// Offset to add to an MSL-relative harmonic prediction to yield MLLW.
    pub fn z0_mllw(&self) -> f64 {
        self.msl - self.mllw
    }
}

/// Result of [`Client::harcon`]. Tide stations return a scalar constituent list;
/// current stations return the vector form plus the per-bin mean current and
/// major-axis direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HarconKind {
    Tide(HarconData),
    Current(CurrentHarconData),
}

impl HarconKind {
    pub fn expect_tide(self) -> Result<HarconData> {
        match self {
            Self::Tide(h) => Ok(h),
            Self::Current(_) => Err(anyhow!("station returned current harcon, want tide")),
        }
    }

    pub fn expect_current(self) -> Result<CurrentHarconData> {
        match self {
            Self::Current(h) => Ok(h),
            Self::Tide(_) => Err(anyhow!("station returned tide harcon, want current")),
        }
    }
}

/// Full harcon response — a single tide harcon or every bin of a current
/// station. Returned by [`Client::harcon_all_bins`] so one HTTP call
/// populates the cache for every bin without repeat requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MultiHarcon {
    Tide(HarconData),
    Current(Vec<CurrentHarconData>),
}

#[cfg(not(target_arch = "wasm32"))]
fn harcon_url(station_id: &str) -> String {
    format!(
        "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station_id}/harcon.json"
    )
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Deserialize)]
struct TideHarcon {
    units: Option<String>,
    #[serde(rename = "HarmonicConstituents")]
    constituents: Vec<TideHarconConst>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Deserialize)]
struct TideHarconConst {
    name: String,
    amplitude: f64,
    #[serde(rename = "phase_GMT")]
    phase_gmt: f64,
    speed: f64,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Deserialize)]
struct CurrentHarcon {
    units: Option<String>,
    #[serde(rename = "HarmonicConstituents")]
    constituents: Vec<CurrentHarconConst>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Deserialize)]
struct CurrentHarconConst {
    #[serde(rename = "binNbr")]
    bin_nbr: i32,
    #[serde(rename = "binDepth")]
    bin_depth: f64,
    #[serde(rename = "constituentName")]
    name: String,
    #[serde(rename = "majorAmplitude")]
    major_amplitude: f64,
    #[serde(rename = "majorPhaseGMT")]
    major_phase_gmt: f64,
    #[serde(default, rename = "minorAmplitude")]
    minor_amplitude: f64,
    #[serde(default, rename = "minorPhaseGMT")]
    minor_phase_gmt: f64,
    #[serde(default, rename = "majorMeanSpeed")]
    major_mean_speed: f64,
    #[serde(default, rename = "minorMeanSpeed")]
    minor_mean_speed: f64,
    #[serde(default)]
    azi: f64,
}

/// Configurable download-and-cache client for NOAA's Tides & Currents API.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    cache_dir: Option<PathBuf>,
    api_proxy: Option<ApiProxy>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for Client {
    fn default() -> Self {
        let cache_dir = dirs::cache_dir().map(|d| d.join("noaa-tides"));
        Self {
            http: reqwest::Client::builder()
                .user_agent("noaa_tides")
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            cache_dir,
            api_proxy: None,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Client {
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the disk cache directory. Default is `~/.cache/noaa-tides`.
    pub fn with_cache_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(dir.into());
        self
    }

    /// Disable disk caching; every request hits NOAA.
    pub fn no_cache(mut self) -> Self {
        self.cache_dir = None;
        self
    }

    /// Route requests through a URL-wrapping proxy.
    pub fn with_proxy(mut self, proxy: ApiProxy) -> Self {
        self.api_proxy = Some(proxy);
        self
    }

    /// Active cache directory, if caching is enabled.
    pub fn cache_dir(&self) -> Option<&Path> {
        self.cache_dir.as_deref()
    }

    /// Fetch harmonic constants for either a tide or a current station.
    ///
    /// `bin_hint` picks a specific current-station bin; if None, the
    /// shallowest (surface) bin is returned for current stations and the
    /// argument is ignored for tide stations. NOAA's online
    /// `currents_predictions` uses a station-specific default bin that
    /// is typically NOT the shallowest — validators should probe it
    /// and pass it explicitly.
    pub async fn harcon(&self, station_id: &str, bin_hint: Option<i32>) -> Result<HarconKind> {
        match self.harcon_all_bins(station_id).await? {
            MultiHarcon::Tide(h) => Ok(HarconKind::Tide(h)),
            MultiHarcon::Current(bins) => {
                Ok(HarconKind::Current(select_bin(station_id, bin_hint, bins)?))
            },
        }
    }

    /// Fetch every bin of a current station's harcon (or the single tide
    /// harcon). NOAA returns all bins in one response, so this is the
    /// primary fetch path; [`Client::harcon`] is a thin filter over it
    /// and shares the same cache key (`harcon/{id}.json`).
    pub async fn harcon_all_bins(&self, station_id: &str) -> Result<MultiHarcon> {
        let cache_key = format!("harcon/{station_id}.json");
        let raw = self
            .fetch_json(&harcon_url(station_id), Some(&cache_key))
            .await?;
        parse_harcon_all_bins(station_id, raw)
    }

    /// Fetch subordinate current-station offsets.
    ///
    /// NOAA's `currentpredictionoffsets.json` endpoint requires the station id
    /// to carry a bin suffix (plain id returns 404, and the wrong bin returns
    /// a payload with every field `null`). Pass the station's `currbin` from
    /// the stations listing as `bin_hint`; if the id already includes a bin
    /// suffix it's used verbatim, otherwise `bin_hint` (default 1) is
    /// appended.
    pub async fn subordinate_offsets(
        &self,
        station_id: &str,
        bin_hint: Option<i32>,
    ) -> Result<SubordinateOffsets> {
        let id_with_bin = if station_id.contains('_') {
            station_id.to_string()
        } else {
            format!("{station_id}_{}", bin_hint.unwrap_or(1))
        };
        let url = format!(
            "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{id_with_bin}/currentpredictionoffsets.json"
        );
        let key = format!("offsets/{id_with_bin}.json");
        let v = self.fetch_json(&url, Some(&key)).await?;
        Ok(SubordinateOffsets {
            ref_id: v["refStationId"]
                .as_str()
                .ok_or_else(|| anyhow!("no refStationId: {v}"))?
                .to_string(),
            ref_bin: v["refStationBin"]
                .as_i64()
                .ok_or_else(|| anyhow!("no refStationBin: {v}"))? as i32,
            mfc_time_min: v["mfcTimeAdjMin"].as_f64().unwrap_or(0.0),
            sbe_time_min: v["sbeTimeAdjMin"].as_f64().unwrap_or(0.0),
            mec_time_min: v["mecTimeAdjMin"].as_f64().unwrap_or(0.0),
            sbf_time_min: v["sbfTimeAdjMin"].as_f64().unwrap_or(0.0),
            mfc_amp: v["mfcAmpAdj"].as_f64().unwrap_or(1.0),
            mec_amp: v["mecAmpAdj"].as_f64().unwrap_or(1.0),
            mean_flood_dir: v["meanFloodDir"].as_f64(),
            mean_ebb_dir: v["meanEbbDir"].as_f64(),
        })
    }

    /// Fetch the MSL and MLLW datum values for a tide station. Both are
    /// expressed in the station's declared units (typically feet). Use
    /// [`StationDatums::z0_mllw`] to turn them into the Z₀ offset that
    /// converts our zero-mean harmonic prediction into NOAA-style MLLW
    /// heights.
    pub async fn station_datums(&self, station_id: &str) -> Result<StationDatums> {
        let url = format!(
            "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station_id}/datums.json"
        );
        let key = format!("datums/{station_id}.json");
        let v = self.fetch_json(&url, Some(&key)).await?;
        let arr = v["datums"]
            .as_array()
            .ok_or_else(|| anyhow!("no datums array for {station_id}"))?;
        let pick = |name: &str| -> Option<f64> {
            arr.iter()
                .find(|d| d["name"].as_str() == Some(name))
                .and_then(|d| d["value"].as_f64())
        };
        let msl = pick("MSL").ok_or_else(|| anyhow!("no MSL datum for {station_id}"))?;
        let mllw = pick("MLLW").ok_or_else(|| anyhow!("no MLLW datum for {station_id}"))?;
        Ok(StationDatums { msl, mllw })
    }

    /// Fetch the list of current-prediction stations (both harmonic "H" and
    /// subordinate "S"). Cached as a single file.
    pub async fn current_stations(&self) -> Result<Vec<StationInfo>> {
        let url = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=currentpredictions";
        let v = self
            .fetch_json(url, Some("stations/currentpredictions.json"))
            .await?;
        let arr = v["stations"]
            .as_array()
            .ok_or_else(|| anyhow!("no stations field"))?;
        let mut out = Vec::with_capacity(arr.len());
        for s in arr {
            let id = s["id"].as_str().unwrap_or_default().to_string();
            if id.is_empty() {
                continue;
            }
            out.push(StationInfo {
                id,
                name: s["name"].as_str().unwrap_or_default().to_string(),
                lat: s["lat"].as_f64(),
                lon: s["lng"].as_f64(),
                station_type: s["type"].as_str().unwrap_or_default().to_string(),
            });
        }
        Ok(out)
    }

    /// Raw GET of a JSON endpoint, with optional caching by relative key.
    pub async fn fetch_json(&self, url: &str, cache_key: Option<&str>) -> Result<Value> {
        if let (Some(dir), Some(key)) = (&self.cache_dir, cache_key) {
            let path = dir.join(key);
            if let Ok(bytes) = tokio::fs::read(&path).await
                && let Ok(v) = serde_json::from_slice::<Value>(&bytes)
            {
                debug!("cache hit {}", path.display());
                return Ok(v);
            }
        }

        let req_url = self
            .api_proxy
            .as_ref()
            .map(|p| p.proxied_url(url))
            .unwrap_or_else(|| url.to_string());
        info!("GET {req_url}");
        let resp = self
            .http
            .get(&req_url)
            .send()
            .await
            .with_context(|| format!("request {req_url}"))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .with_context(|| format!("read body from {req_url}"))?;
        if !status.is_success() {
            return Err(anyhow!(
                "{} from {}: {}",
                status,
                req_url,
                String::from_utf8_lossy(&bytes)
            ));
        }
        let json: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode JSON from {req_url}"))?;
        if json.get("error").is_some() {
            return Err(anyhow!("error field in response from {req_url}: {json}"));
        }

        if let (Some(dir), Some(key)) = (&self.cache_dir, cache_key) {
            let path = dir.join(key);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
            tokio::fs::write(&path, &bytes).await.ok();
        }

        Ok(json)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn parse_harcon_all_bins(station_id: &str, raw: Value) -> Result<MultiHarcon> {
    if raw["HarmonicConstituents"][0]
        .get("majorAmplitude")
        .is_some()
    {
        let parsed: CurrentHarcon = serde_json::from_value(raw)?;
        let units = parsed.units.unwrap_or_default();

        // Group by bin. Preserve NOAA's order of first appearance.
        let mut bins: Vec<CurrentHarconData> = Vec::new();
        for c in parsed.constituents {
            let name = c.name.trim().to_uppercase();
            let speed = constituent_speed(&name).unwrap_or(0.0);
            let vc = VectorConstituent {
                name,
                major_amplitude: c.major_amplitude,
                major_phase_gmt: c.major_phase_gmt,
                minor_amplitude: c.minor_amplitude,
                minor_phase_gmt: c.minor_phase_gmt,
                speed,
            };
            if let Some(existing) = bins.iter_mut().find(|b| b.bin_nbr == c.bin_nbr) {
                existing.constituents.push(vc);
            } else {
                bins.push(CurrentHarconData {
                    station_id: station_id.to_string(),
                    bin_nbr: c.bin_nbr,
                    bin_depth: c.bin_depth,
                    units: units.clone(),
                    azi: c.azi,
                    major_mean: c.major_mean_speed,
                    minor_mean: c.minor_mean_speed,
                    constituents: vec![vc],
                });
            }
        }
        Ok(MultiHarcon::Current(bins))
    } else {
        let parsed: TideHarcon = serde_json::from_value(raw)?;
        Ok(MultiHarcon::Tide(HarconData {
            station_id: station_id.to_string(),
            units: parsed.units.unwrap_or_default(),
            z0_mllw: 0.0,
            constituents: parsed
                .constituents
                .into_iter()
                .map(|c| HarmonicConstituent {
                    name: c.name.trim().to_uppercase(),
                    amplitude: c.amplitude,
                    phase_gmt: c.phase_gmt,
                    speed: c.speed,
                })
                .collect(),
        }))
    }
}

/// Pick one bin out of a current station's full bin list. `None` picks the
/// shallowest; `Some(n)` matches exactly and errors if absent.
pub fn select_bin(
    station_id: &str,
    bin_hint: Option<i32>,
    mut bins: Vec<CurrentHarconData>,
) -> Result<CurrentHarconData> {
    if let Some(wanted) = bin_hint {
        bins.into_iter()
            .find(|b| b.bin_nbr == wanted)
            .ok_or_else(|| anyhow!("bin {wanted} not found in harcon for {station_id}"))
    } else {
        bins.sort_by(|a, b| {
            a.bin_depth
                .partial_cmp(&b.bin_depth)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        bins.into_iter()
            .next()
            .ok_or_else(|| anyhow!("no bins in harcon for {station_id}"))
    }
}
