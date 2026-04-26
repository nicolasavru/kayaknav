//! Serializable collection of NOAA harmonic constants for every station.
//!
//! A [`HarconStore`] bundles station metadata, tide-station harcons,
//! current-station harcons (every bin), and subordinate-station offsets
//! into one structure that can be round-tripped through JSON (plaintext)
//! or Postcard (space-efficient binary).
//!
//! The typical flow:
//!
//! 1. [`HarconStore::download`] pulls every station from NOAA using the
//!    [`crate::Client`]'s disk cache and concurrency cap, and returns the
//!    assembled store.
//! 2. [`HarconStore::to_plaintext`] / [`HarconStore::to_binary`] serialize
//!    the store for checking into a repository or embedding with
//!    `include_bytes!`.
//! 3. [`HarconStore::from_plaintext`] / [`HarconStore::from_binary`]
//!    decode the store at runtime. Prediction helpers
//!    ([`HarconStore::tide_predictor`], [`HarconStore::current_predictor`])
//!    build predictors directly from the decoded structure — no network.

use chrono::NaiveDateTime;
use serde::Deserialize;
use serde::Serialize;

use crate::noaa::StationInfo;
use crate::noaa::SubordinateOffsets;
use crate::predictor::CurrentHarconData;
use crate::predictor::CurrentPredictor;
use crate::predictor::HarconData;
use crate::predictor::HarmonicConstituent;
use crate::predictor::Predictor;
use crate::predictor::VectorConstituent;
use crate::prelude::*;

/// Format-version tag written into every serialized store. Bump when the
/// wire format changes; decoders reject unknown versions so you don't
/// silently read a new store with an old binary.
///
/// History:
/// * v1 — postcard varint encoding.
/// * v2 — bitcode encoding (~2% smaller raw, ~20% smaller after zstd-22).
/// * v3 — per-tide-station Z₀ (MSL−MLLW) carried on `HarconData.z0_mllw`, so
///   `Predictor::at` returns MLLW-referenced heights that match NOAA.
pub const STORE_FORMAT_VERSION: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarconStore {
    pub format_version: u32,
    pub tides: Vec<TideEntry>,
    pub currents: Vec<CurrentEntry>,
    pub subordinates: Vec<SubordinateEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TideEntry {
    pub info: StationInfo,
    pub harcon: HarconData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentEntry {
    pub info: StationInfo,
    /// Every bin NOAA publishes for the station, in the order returned.
    pub bins: Vec<CurrentHarconData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubordinateEntry {
    pub info: StationInfo,
    pub offsets: SubordinateOffsets,
}

impl HarconStore {
    pub fn new() -> Self {
        Self {
            format_version: STORE_FORMAT_VERSION,
            tides: Vec::new(),
            currents: Vec::new(),
            subordinates: Vec::new(),
        }
    }

    // -------- Serialization --------

    /// Decode a store from its space-efficient binary form. Same bytes as
    /// produced by [`HarconStore::to_binary`]. The on-wire encoding is
    /// `bitcode` (via its serde adapter); earlier format versions used
    /// postcard and will fail the version check.
    pub fn from_binary(bytes: &[u8]) -> Result<Self> {
        let store: Self = bitcode::deserialize(bytes).context("decode bitcode HarconStore")?;
        store.check_version()?;
        Ok(store)
    }

    pub fn to_binary(&self) -> Result<Vec<u8>> {
        bitcode::serialize(self).context("encode bitcode HarconStore")
    }

    /// Decode a zstd-compressed binary store. Matches the output of
    /// [`HarconStore::to_binary_zstd`]; use this when embedding the store
    /// via `include_bytes!` on a compressed artifact to keep the shipped
    /// wasm/binary small. Decompression is pure-Rust (`ruzstd`) so this
    /// works on every target, including `wasm32-unknown-unknown`.
    pub fn from_binary_zstd(compressed: &[u8]) -> Result<Self> {
        let mut decoder = ruzstd::streaming_decoder::StreamingDecoder::new(compressed)
            .map_err(|e| anyhow!("init zstd decoder: {e}"))?;
        let mut out = Vec::with_capacity(compressed.len().saturating_mul(10));
        std::io::copy(&mut decoder, &mut out).context("zstd decompress")?;
        Self::from_binary(&out)
    }

    /// Encode the store as zstd-compressed postcard bytes. `level` is the
    /// zstd compression level; 22 (ultra) is appropriate for build-time
    /// artifacts since decompression speed is level-independent. Only
    /// available off-wasm because the C-backed `zstd` crate isn't built
    /// for `wasm32-unknown-unknown`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn to_binary_zstd(&self, level: i32) -> Result<Vec<u8>> {
        let bin = self.to_binary()?;
        zstd::encode_all(&bin[..], level).context("zstd compress")
    }

    /// Decode a store from its pretty-printed JSON form.
    pub fn from_plaintext(json: &str) -> Result<Self> {
        let store: Self = serde_json::from_str(json).context("decode JSON HarconStore")?;
        store.check_version()?;
        Ok(store)
    }

    pub fn to_plaintext(&self) -> Result<String> {
        serde_json::to_string_pretty(self).context("encode JSON HarconStore")
    }

    fn check_version(&self) -> Result<()> {
        if self.format_version != STORE_FORMAT_VERSION {
            return Err(anyhow!(
                "HarconStore format version {} not supported (expected {})",
                self.format_version,
                STORE_FORMAT_VERSION
            ));
        }
        Ok(())
    }

    // -------- Lookups --------

    pub fn tide_harcon(&self, id: &str) -> Option<&HarconData> {
        self.tides
            .iter()
            .find(|t| t.info.id == id)
            .map(|t| &t.harcon)
    }

    pub fn current_bins(&self, id: &str) -> Option<&[CurrentHarconData]> {
        self.currents
            .iter()
            .find(|c| c.info.id == id)
            .map(|c| c.bins.as_slice())
    }

    /// Pick one bin of a harmonic current station. `None` picks the
    /// shallowest (surface) bin; `Some(n)` matches exactly.
    pub fn current_harcon(&self, id: &str, bin_hint: Option<i32>) -> Option<&CurrentHarconData> {
        let bins = self.current_bins(id)?;
        if let Some(wanted) = bin_hint {
            bins.iter().find(|b| b.bin_nbr == wanted)
        } else {
            bins.iter().min_by(|a, b| {
                a.bin_depth
                    .partial_cmp(&b.bin_depth)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        }
    }

    pub fn subordinate(&self, id: &str) -> Option<&SubordinateEntry> {
        self.subordinates.iter().find(|s| s.info.id == id)
    }

    pub fn station_info(&self, id: &str) -> Option<&StationInfo> {
        self.tides
            .iter()
            .map(|e| &e.info)
            .chain(self.currents.iter().map(|e| &e.info))
            .chain(self.subordinates.iter().map(|e| &e.info))
            .find(|info| info.id == id)
    }

    /// All stations whose lat/lon fall inside the given half-open box.
    /// `lat` and `lon` accept (min, max) in either order.
    pub fn stations_in_box(&self, lat: (f64, f64), lon: (f64, f64)) -> Vec<&StationInfo> {
        let (lat_min, lat_max) = (lat.0.min(lat.1), lat.0.max(lat.1));
        let (lon_min, lon_max) = (lon.0.min(lon.1), lon.0.max(lon.1));
        self.tides
            .iter()
            .map(|e| &e.info)
            .chain(self.currents.iter().map(|e| &e.info))
            .chain(self.subordinates.iter().map(|e| &e.info))
            .filter(|info| match (info.lat, info.lon) {
                (Some(la), Some(lo)) => {
                    la >= lat_min && la <= lat_max && lo >= lon_min && lo <= lon_max
                },
                _ => false,
            })
            .collect()
    }

    // -------- Predictor factories --------

    pub fn tide_predictor(&self, id: &str, t_ref: NaiveDateTime) -> Option<Predictor> {
        self.tide_harcon(id).map(|h| Predictor::new(h, t_ref))
    }

    pub fn current_predictor(
        &self,
        id: &str,
        bin_hint: Option<i32>,
        t_ref: NaiveDateTime,
    ) -> Option<CurrentPredictor> {
        self.current_harcon(id, bin_hint)
            .map(|h| CurrentPredictor::new(h, t_ref))
    }

    // -------- Stats --------

    /// Rough estimate of the heap + stack footprint of this store, in bytes.
    /// Walks every entry and sums `capacity()` of inner `Vec`s and `String`s
    /// so the number reflects actual allocator use, not just `len()`.
    /// HashMap-style indirect overhead (per-node metadata in future
    /// refactors) is not counted; for the current all-`Vec` layout this is
    /// accurate to within a word per allocation.
    pub fn bytes_in_memory(&self) -> usize {
        use std::mem::size_of;
        let mut n = size_of::<Self>();
        n += self.tides.capacity() * size_of::<TideEntry>();
        for e in &self.tides {
            n += station_info_bytes(&e.info);
            n += harcon_bytes(&e.harcon);
        }
        n += self.currents.capacity() * size_of::<CurrentEntry>();
        for e in &self.currents {
            n += station_info_bytes(&e.info);
            n += e.bins.capacity() * size_of::<CurrentHarconData>();
            for b in &e.bins {
                n += current_harcon_bytes(b);
            }
        }
        n += self.subordinates.capacity() * size_of::<SubordinateEntry>();
        for e in &self.subordinates {
            n += station_info_bytes(&e.info);
            n += subordinate_offsets_bytes(&e.offsets);
        }
        n
    }

    /// Short one-line summary for logs, e.g. `"3600 tides, 150 currents
    /// (312 bins), 2400 subordinates"`.
    pub fn summary(&self) -> String {
        let bin_total: usize = self.currents.iter().map(|c| c.bins.len()).sum();
        format!(
            "{} tides, {} currents ({} bins), {} subordinates",
            self.tides.len(),
            self.currents.len(),
            bin_total,
            self.subordinates.len()
        )
    }
}

impl Default for HarconStore {
    fn default() -> Self {
        Self::new()
    }
}

fn station_info_bytes(info: &StationInfo) -> usize {
    info.id.capacity() + info.name.capacity() + info.station_type.capacity()
}

fn harcon_bytes(h: &HarconData) -> usize {
    use std::mem::size_of;
    let mut n = h.station_id.capacity()
        + h.units.capacity()
        + h.constituents.capacity() * size_of::<HarmonicConstituent>();
    for c in &h.constituents {
        n += c.name.capacity();
    }
    n
}

fn current_harcon_bytes(h: &CurrentHarconData) -> usize {
    use std::mem::size_of;
    let mut n = h.station_id.capacity()
        + h.units.capacity()
        + h.constituents.capacity() * size_of::<VectorConstituent>();
    for c in &h.constituents {
        n += c.name.capacity();
    }
    n
}

fn subordinate_offsets_bytes(o: &SubordinateOffsets) -> usize {
    // Only the reference-station id is dynamically sized; everything else is
    // inline floats already counted in size_of::<SubordinateEntry>.
    o.ref_id.capacity()
}

// -------- Downloader (non-wasm) --------

#[cfg(not(target_arch = "wasm32"))]
mod download {
    use std::collections::HashSet;

    use futures::stream::StreamExt;
    use futures::stream::{
        self,
    };
    use tracing::warn;

    #[allow(clippy::wildcard_imports)]
    use super::*;
    use crate::noaa::Client;
    use crate::noaa::MultiHarcon;

    impl HarconStore {
        /// Fetch every station and its harcons / offsets from NOAA.
        ///
        /// The client's disk cache is used — second and subsequent runs are
        /// filesystem-bound and do not hit the network. `max_concurrent`
        /// caps the number of in-flight HTTP requests; NOAA publishes no
        /// explicit rate limit but <= 6 is a safe default. `delay_ms`, if
        /// non-zero, inserts a sleep between launching each request
        /// (combined with the concurrency cap this gives a predictable
        /// request rate even on cold caches).
        pub async fn download(
            client: &Client,
            max_concurrent: usize,
            delay_ms: u64,
        ) -> Result<Self> {
            let max_concurrent = max_concurrent.max(1);

            // 1. Station metadata: currents (H + S) + tides (tidepredictions).
            //    For currents we also capture NOAA's `currbin` from the
            //    listing — subordinate stations whose default bin isn't 1
            //    (e.g. COR0301=5) need it to form the right offsets URL.
            let tide_stations = fetch_tide_stations(client).await?;
            let current_stations_with_bins = fetch_current_stations_with_bins(client).await?;

            // 2. Tide station harcons (concurrent).
            let mut tides = fetch_harcons(
                client,
                tide_stations,
                max_concurrent,
                delay_ms,
                "tide",
                |info, multi| match multi {
                    MultiHarcon::Tide(h) => Some(TideEntry { info, harcon: h }),
                    MultiHarcon::Current(_) => {
                        warn!(
                            "station {} listed as tide but returned current harcon",
                            info.id
                        );
                        None
                    },
                },
            )
            .await;

            // 2b. Per-station MLLW Z₀ offset (= MSL − MLLW). Populated into
            //     each TideEntry's harcon so Predictor::at returns heights
            //     on NOAA's published MLLW datum. Stations missing MSL or
            //     MLLW keep z0 = 0 and log a warning.
            fetch_tide_datums(client, &mut tides, max_concurrent, delay_ms).await;

            // 3. Partition current stations into H (harmonic) and S (subordinate).
            let (h_stations, s_stations): (Vec<_>, Vec<_>) = current_stations_with_bins
                .into_iter()
                .partition(|(info, _)| info.station_type == "H");
            let h_stations: Vec<StationInfo> = h_stations.into_iter().map(|(i, _)| i).collect();

            // 4. Harmonic-current harcons — one fetch per station, all bins.
            let currents = fetch_harcons(
                client,
                h_stations,
                max_concurrent,
                delay_ms,
                "current",
                |info, multi| match multi {
                    MultiHarcon::Current(bins) => Some(CurrentEntry { info, bins }),
                    MultiHarcon::Tide(_) => {
                        warn!(
                            "station {} listed as current but returned tide harcon",
                            info.id
                        );
                        None
                    },
                },
            )
            .await;

            // 5. Subordinate offsets — one fetch per station.
            let subordinates =
                fetch_subordinates(client, s_stations, max_concurrent, delay_ms).await;

            // 6. Ensure every subordinate's reference harmonic station has
            //    a harcon in the store. Log missing; don't fail — NOAA
            //    occasionally ships subordinates whose ref_id isn't public.
            let known: HashSet<String> = currents.iter().map(|c| c.info.id.clone()).collect();
            for s in &subordinates {
                if !known.contains(&s.offsets.ref_id) {
                    warn!(
                        "subordinate {} references harmonic station {} which is not in the store",
                        s.info.id, s.offsets.ref_id
                    );
                }
            }

            Ok(Self {
                format_version: STORE_FORMAT_VERSION,
                tides,
                currents,
                subordinates,
            })
        }
    }

    async fn fetch_current_stations_with_bins(
        client: &Client,
    ) -> Result<Vec<(StationInfo, Option<i32>)>> {
        let url = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=currentpredictions";
        let v = client
            .fetch_json(url, Some("stations/currentpredictions.json"))
            .await?;
        let arr = v["stations"]
            .as_array()
            .ok_or_else(|| anyhow!("no stations field in currents list"))?;
        let mut out = Vec::with_capacity(arr.len());
        for s in arr {
            let id = s["id"].as_str().unwrap_or_default().to_string();
            if id.is_empty() {
                continue;
            }
            let info = StationInfo {
                id,
                name: s["name"].as_str().unwrap_or_default().to_string(),
                lat: s["lat"].as_f64(),
                lon: s["lng"].as_f64(),
                station_type: s["type"].as_str().unwrap_or_default().to_string(),
            };
            let currbin = s["currbin"].as_i64().map(|n| n as i32);
            out.push((info, currbin));
        }
        Ok(out)
    }

    async fn fetch_tide_stations(client: &Client) -> Result<Vec<StationInfo>> {
        let url = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=tidepredictions";
        let v = client
            .fetch_json(url, Some("stations/tidepredictions.json"))
            .await?;
        let arr = v["stations"]
            .as_array()
            .ok_or_else(|| anyhow!("no stations field in tide list"))?;
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
                // NOAA returns no `type` field on the tide list; tag these
                // explicitly so consumers can filter.
                station_type: s["type"].as_str().unwrap_or("R").to_string(),
            });
        }
        Ok(out)
    }

    async fn fetch_harcons<T, F>(
        client: &Client,
        stations: Vec<StationInfo>,
        max_concurrent: usize,
        delay_ms: u64,
        label: &str,
        mut into_entry: F,
    ) -> Vec<T>
    where
        F: FnMut(StationInfo, MultiHarcon) -> Option<T> + Send,
        T: Send,
    {
        let total = stations.len();
        tracing::info!("fetching {} {} harcons", total, label);
        let results: Vec<Option<T>> = stream::iter(stations.into_iter().enumerate())
            .map(|(i, info)| {
                let client = client.clone();
                async move {
                    if delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                    match client.harcon_all_bins(&info.id).await {
                        Ok(multi) => {
                            if i % 100 == 0 {
                                tracing::info!("  {} harcons: {}/{}", label, i, total);
                            }
                            // fn closures aren't easily held across await; call
                            // via a local binding that captures into_entry by
                            // &mut. Since we're in buffer_unordered, we don't
                            // actually share this mutably — move a clone of
                            // the closure into each task would require Clone.
                            // Simpler: do the mapping in the collecting stage
                            // (return (info, multi)) and map synchronously.
                            Some((info, multi))
                        },
                        Err(e) => {
                            warn!("{} harcon {}: {}", label, info.id, e);
                            None
                        },
                    }
                }
            })
            .buffer_unordered(max_concurrent)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|opt| opt.and_then(|(info, multi)| into_entry(info, multi)))
            .collect();
        results.into_iter().flatten().collect()
    }

    async fn fetch_tide_datums(
        client: &Client,
        tides: &mut [TideEntry],
        max_concurrent: usize,
        delay_ms: u64,
    ) {
        let total = tides.len();
        tracing::info!("fetching {} tide-station datums", total);
        let pairs: Vec<(String, Option<f64>)> =
            stream::iter(tides.iter().map(|t| t.info.id.clone()).enumerate())
                .map(|(i, id)| {
                    let client = client.clone();
                    async move {
                        if delay_ms > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        }
                        let z0 = match client.station_datums(&id).await {
                            Ok(d) => Some(d.z0_mllw()),
                            Err(e) => {
                                warn!("datums {}: {}", id, e);
                                None
                            },
                        };
                        if i % 200 == 0 {
                            tracing::info!("  datums: {}/{}", i, total);
                        }
                        (id, z0)
                    }
                })
                .buffer_unordered(max_concurrent)
                .collect::<Vec<_>>()
                .await;
        let map: std::collections::HashMap<String, f64> = pairs
            .into_iter()
            .filter_map(|(id, z)| z.map(|v| (id, v)))
            .collect();
        for t in tides.iter_mut() {
            if let Some(&z) = map.get(&t.info.id) {
                t.harcon.z0_mllw = z;
            }
        }
    }

    async fn fetch_subordinates(
        client: &Client,
        stations: Vec<(StationInfo, Option<i32>)>,
        max_concurrent: usize,
        delay_ms: u64,
    ) -> Vec<SubordinateEntry> {
        let total = stations.len();
        tracing::info!("fetching {} subordinate offsets", total);
        stream::iter(stations.into_iter().enumerate())
            .map(|(i, (info, bin))| {
                let client = client.clone();
                async move {
                    if delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                    match client.subordinate_offsets(&info.id, bin).await {
                        Ok(offsets) => {
                            if i % 100 == 0 {
                                tracing::info!("  subordinate: {}/{}", i, total);
                            }
                            Some(SubordinateEntry { info, offsets })
                        },
                        Err(e) => {
                            warn!("subordinate offsets {}: {}", info.id, e);
                            None
                        },
                    }
                }
            })
            .buffer_unordered(max_concurrent)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect()
    }
}
