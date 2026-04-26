//! Offline tidal prediction using NOAA harmonic constituents.
//!
//! Tide height at a station is the scalar harmonic sum (Parker 2007 eq. 3.3):
//!
//! ```text
//! h(t) = H₀ + Σ_i f_i · A_i · cos(ω_i · t + {V₀+u}_i − κ'_i)
//! ```
//!
//! where {V₀+u} is the Greenwich equilibrium argument and κ' is the epoch of
//! constituent i relative to the local time meridian. NOAA's harcon.json
//! `phase_GMT` field is the Greenwich epoch G (see eq. 3.2), which is exactly
//! what we need when driving the sum off a UTC clock.
//!
//! Tidal currents are vectors. Following Parker 2007 §3.4.3 we predict the
//! two orthogonal major/minor axis components independently and then combine
//! them to produce speed and direction. Each current constituent has an
//! amplitude/phase pair for each axis (majorAmplitude/majorPhaseGMT and
//! minorAmplitude/minorPhaseGMT in NOAA's JSON); the signed scalar along the
//! major axis alone is NOT enough for rotary flows — at slack water the
//! minor axis contribution dominates. We also add the per-bin mean current
//! along each axis (majorMeanSpeed, minorMeanSpeed), which matches NOAA's
//! online currents_predictions output (Parker §5.3.5 third exception).
//!
//! V₀, f, u depend on the mean longitudes (s, h, p, N, p_s) of the Moon,
//! Sun, lunar perigee, lunar ascending node, and solar perigee. Formulas
//! follow Schureman (1941) "Manual of Harmonic Analysis and Prediction of
//! Tides", NOAA Special Publication 98.

use chrono::Datelike;
use chrono::NaiveDateTime;
use chrono::Timelike;
use serde::Deserialize;
use serde::Serialize;

use crate::prelude::*;

/// Scalar harmonic constituent for a tide station.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarmonicConstituent {
    pub name: String,
    /// Amplitude in the harcon.json `units` (typically feet for tides).
    pub amplitude: f64,
    /// Greenwich-referenced phase (G) in degrees.
    pub phase_gmt: f64,
    /// Angular speed ω in degrees per hour.
    pub speed: f64,
}

/// Full harmonic-constants payload for one tide station.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarconData {
    pub station_id: String,
    /// Units string from NOAA (e.g. "feet").
    pub units: String,
    /// Z₀ = MSL − MLLW in `units`. Added to the harmonic sum so predictions
    /// match NOAA's MLLW-referenced online output; 0.0 leaves the raw
    /// zero-mean prediction untouched.
    #[serde(default)]
    pub z0_mllw: f64,
    pub constituents: Vec<HarmonicConstituent>,
}

/// Vector harmonic constituent for a tidal-current station: amplitude and
/// Greenwich epoch on both the major and minor axes of the constituent
/// ellipse (Parker §3.4.3, §5.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConstituent {
    pub name: String,
    pub major_amplitude: f64,
    pub major_phase_gmt: f64,
    pub minor_amplitude: f64,
    pub minor_phase_gmt: f64,
    /// Angular speed ω in degrees per hour.
    pub speed: f64,
}

/// Full harmonic-constants payload for one tidal-current station bin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentHarconData {
    pub station_id: String,
    pub bin_nbr: i32,
    pub bin_depth: f64,
    /// NOAA units string (e.g. "meters, centimeters/second" — amplitudes are
    /// cm/s, depths are meters).
    pub units: String,
    /// Direction of the major axis of every constituent ellipse at this bin,
    /// clockwise from true north.
    pub azi: f64,
    /// Constant along-axis mean currents for the bin (Parker §5.3.5).
    pub major_mean: f64,
    pub minor_mean: f64,
    pub constituents: Vec<VectorConstituent>,
}

/// Derive the angular speed (°/hr) from Doodson coefficients using the rates of
/// the astronomical mean longitudes.
pub(crate) fn doodson_speed(d: Doodson) -> f64 {
    // Hourly rates of (T, s, h, p, N, p_s) in degrees per hour.
    const RATES: [f64; 6] = [
        15.0,        // T
        0.54901653,  // s (moon)
        0.04106864,  // h (sun)
        0.00464183,  // p (lunar perigee)
        -0.00220641, // N (lunar node)
        0.00000196,  // p_s (solar perigee)
    ];
    let coefs = [d.t, d.s, d.h, d.p, d.n, d.p_s];
    (0..6).map(|i| coefs[i] as f64 * RATES[i]).sum()
}

// -------- Astronomical arguments (Schureman, eqs. 1–5) --------

/// Mean longitudes of astronomical bodies, in degrees, at the given UTC instant.
#[derive(Debug, Clone, Copy)]
struct Astro {
    /// Moon mean longitude.
    s: f64,
    /// Sun mean longitude.
    h: f64,
    /// Mean longitude of lunar perigee.
    p: f64,
    /// Mean longitude of lunar ascending node (N, not -N).
    n: f64,
    /// Mean longitude of solar perigee.
    p_s: f64,
    /// Mean solar time at Greenwich, in degrees (= 15·UT_hours − 180, mod 360).
    t: f64,
}

fn julian_day(dt: &NaiveDateTime) -> f64 {
    // Meeus algorithm for Julian date in UT.
    let mut year = dt.year() as i64;
    let mut month = dt.month() as i64;
    let day = dt.day() as i64;
    if month <= 2 {
        year -= 1;
        month += 12;
    }
    let a = year.div_euclid(100);
    let b = 2 - a + a.div_euclid(4);
    let jd0 = (365.25 * (year as f64 + 4716.0)).floor()
        + (30.6001 * (month as f64 + 1.0)).floor()
        + day as f64
        + b as f64
        - 1524.5;
    let frac = (dt.hour() as f64 + dt.minute() as f64 / 60.0 + dt.second() as f64 / 3600.0) / 24.0;
    jd0 + frac
}

fn astro(dt: &NaiveDateTime) -> Astro {
    let jd = julian_day(dt);
    let tjc = (jd - 2451545.0) / 36525.0; // Julian centuries since J2000.0

    let wrap = |x: f64| x.rem_euclid(360.0);

    let s = wrap(218.3164591 + 481267.88134236 * tjc - 0.0013268 * tjc * tjc);
    let h = wrap(280.4664567 + 36000.7697489 * tjc + 0.0003032 * tjc * tjc);
    let p = wrap(83.3532430 + 4069.0137287 * tjc - 0.01032 * tjc * tjc);
    let n = wrap(125.0445479 - 1934.1362891 * tjc + 0.0020754 * tjc * tjc);
    let p_s = wrap(282.9373481 + 1.71945766 * tjc + 0.00045962 * tjc * tjc);

    let ut_hours = dt.hour() as f64 + dt.minute() as f64 / 60.0 + dt.second() as f64 / 3600.0;
    let t = wrap(15.0 * ut_hours - 180.0);

    Astro { s, h, p, n, p_s, t }
}

// -------- Constituent table (Doodson coefficients + node formula) --------

/// Coefficients (a, b, c, d, e, g) such that
///   V₀ = a·T + b·s + c·h + d·p + e·N + g·p_s + phase_offset  (mod 360°)
/// where T is Schureman's mean solar time in degrees (15·UT − 180).
/// Here N is the longitude of the lunar ASCENDING node (not the N' = −N used
/// in some references; sign is already folded into the e coefficient).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Doodson {
    t: i32,
    s: i32,
    h: i32,
    p: i32,
    n: i32,
    p_s: i32,
    /// Additive phase offset, in degrees.
    offset: i32,
}

#[derive(Debug, Clone, Copy)]
enum NodeKind {
    /// f = 1, u = 0.
    None,
    /// Lunar semidiurnal group (M2, N2, 2N2, MU2, NU2, LAM2).
    M2,
    /// Lunar diurnal (O1, Q1, 2Q1, RHO1).
    O1,
    /// Luni-solar diurnal (K1).
    K1,
    /// Luni-solar semidiurnal (K2).
    K2,
    /// Smaller lunar elliptic diurnal J1.
    J1,
    /// Smaller lunar elliptic diurnal OO1.
    OO1,
    /// Composite M1 with perigee-dependent Q/Q_a (Schureman §141).
    M1,
    /// Smaller lunar elliptic semidiurnal L2.
    L2,
    /// Lunar monthly MM.
    Mm,
    /// Lunar fortnightly MF.
    Mf,
    /// Lunar terdiurnal M3: f = f(M2)^(3/2), u = (3/2)·u(M2) (Schureman §86).
    M3,
    /// Shallow-water combinations expressed as (pow_M2, pow_K1, pow_O1, pow_K2).
    /// f = f_M2^a · f_K1^b · f_O1^c · f_K2^d; u = a·u_M2 + b·u_K1 + c·u_O1 + d·u_K2.
    /// Signed exponents are required to express compounds like 2SM2 = 2·S2 − M2
    /// (M2 power = −1) or 2MK3 = 2·M2 − K1 (K1 power = −1).
    Shallow(i8, i8, i8, i8),
}

pub(crate) struct ConstDef {
    pub(crate) name: &'static str,
    pub(crate) d: Doodson,
    node: NodeKind,
}

macro_rules! c {
    ($name:literal, $t:literal, $s:literal, $h:literal, $p:literal, $n:literal, $ps:literal, $off:literal, $node:expr) => {
        ConstDef {
            name: $name,
            d: Doodson {
                t: $t,
                s: $s,
                h: $h,
                p: $p,
                n: $n,
                p_s: $ps,
                offset: $off,
            },
            node: $node,
        }
    };
}

// Doodson coefficients in (T, s, h, p, N, p_s) form, phase offsets in degrees.
// Offsets follow Schureman: constituents with an i·90° factor get that added.
// Node kinds map each constituent to its Schureman f, u formulas (Table 14).
#[rustfmt::skip]
const CONSTITUENTS: &[ConstDef] = &[
    // Semidiurnal
    c!("M2",    2, -2,  2,  0,  0,  0,   0, NodeKind::M2),
    c!("S2",    2,  0,  0,  0,  0,  0,   0, NodeKind::None),
    c!("N2",    2, -3,  2,  1,  0,  0,   0, NodeKind::M2),
    c!("NU2",   2, -3,  4, -1,  0,  0,   0, NodeKind::M2),
    c!("MU2",   2, -4,  4,  0,  0,  0,   0, NodeKind::M2),
    c!("2N2",   2, -4,  2,  2,  0,  0,   0, NodeKind::M2),
    c!("LAM2",  2, -1,  0,  1,  0,  0, 180, NodeKind::M2),
    c!("L2",    2, -1,  2, -1,  0,  0, 180, NodeKind::L2),
    c!("T2",    2,  0, -1,  0,  0,  1,   0, NodeKind::None),
    c!("R2",    2,  0,  1,  0,  0, -1, 180, NodeKind::None),
    c!("K2",    2,  0,  2,  0,  0,  0,   0, NodeKind::K2),
    c!("2SM2",  2,  2, -2,  0,  0,  0,   0, NodeKind::Shallow(-1, 0, 0, 0)),

    // Diurnal
    c!("K1",    1,  0,  1,  0,  0,  0, -90, NodeKind::K1),
    c!("O1",    1, -2,  1,  0,  0,  0,  90, NodeKind::O1),
    c!("P1",    1,  0, -1,  0,  0,  0,  90, NodeKind::None),
    c!("Q1",    1, -3,  1,  1,  0,  0,  90, NodeKind::O1),
    c!("2Q1",   1, -4,  1,  2,  0,  0,  90, NodeKind::O1),
    c!("RHO",   1, -3,  3, -1,  0,  0,  90, NodeKind::O1),
    c!("S1",    1,  0,  0,  0,  0,  0,   0, NodeKind::None),
    c!("M1",    1, -1,  1,  1,  0,  0, -90, NodeKind::M1),
    c!("J1",    1,  1,  1, -1,  0,  0, -90, NodeKind::J1),
    c!("OO1",   1,  2,  1,  0,  0,  0, -90, NodeKind::OO1),

    // Long period
    c!("MM",    0,  1,  0, -1,  0,  0,   0, NodeKind::Mm),
    c!("MF",    0,  2,  0,  0,  0,  0,   0, NodeKind::Mf),
    c!("MSF",   0,  2, -2,  0,  0,  0,   0, NodeKind::None),
    c!("SA",    0,  0,  1,  0,  0,  0,   0, NodeKind::None),
    c!("SSA",   0,  0,  2,  0,  0,  0,   0, NodeKind::None),

    // Shallow water / overtides
    c!("M4",    4, -4,  4,  0,  0,  0,   0, NodeKind::Shallow(2, 0, 0, 0)),
    c!("M6",    6, -6,  6,  0,  0,  0,   0, NodeKind::Shallow(3, 0, 0, 0)),
    c!("M8",    8, -8,  8,  0,  0,  0,   0, NodeKind::Shallow(4, 0, 0, 0)),
    c!("M3",    3, -3,  3,  0,  0,  0,   0, NodeKind::M3),
    c!("S4",    4,  0,  0,  0,  0,  0,   0, NodeKind::None),
    c!("S6",    6,  0,  0,  0,  0,  0,   0, NodeKind::None),
    c!("MN4",   4, -5,  4,  1,  0,  0,   0, NodeKind::Shallow(2, 0, 0, 0)),
    c!("MS4",   4, -2,  2,  0,  0,  0,   0, NodeKind::Shallow(1, 0, 0, 0)),
    c!("MK3",   3, -2,  3,  0,  0,  0, -90, NodeKind::Shallow(1, 1, 0, 0)),
    c!("2MK3",  3, -4,  3,  0,  0,  0,  90, NodeKind::Shallow(2, -1, 0, 0)),
];

pub(crate) fn lookup(name: &str) -> Option<&'static ConstDef> {
    let up = name.trim().to_uppercase();
    CONSTITUENTS.iter().find(|c| c.name == up)
}

/// Speed (°/hour) of the constituent with the given name, or None if unknown.
pub fn constituent_speed(name: &str) -> Option<f64> {
    lookup(name).map(|def| doodson_speed(def.d))
}

// -------- Node factors f (amplitude) and u (phase), Schureman Table 14 --------

/// Returns (f, u_deg) for the given NodeKind. `n_deg` is the longitude of the
/// lunar ascending node; `p_deg` is the longitude of the lunar perigee (only
/// the composite M1 kind uses it — every other kind depends on N only).
fn node_factor(kind: NodeKind, n_deg: f64, p_deg: f64) -> (f64, f64) {
    let nrad = n_deg.to_radians();
    let (sn, s2n, s3n) = (nrad.sin(), (2.0 * nrad).sin(), (3.0 * nrad).sin());
    let (cn, c2n, c3n) = (nrad.cos(), (2.0 * nrad).cos(), (3.0 * nrad).cos());

    match kind {
        NodeKind::None => (1.0, 0.0),

        // M2 (also N2, 2N2, MU2, NU2, LAM2). Schureman eq. 78.
        NodeKind::M2 => {
            let f = 1.0004 - 0.0373 * cn + 0.0002 * c2n;
            let u = -2.14 * sn;
            (f, u)
        },

        // O1, Q1, 2Q1, RHO. Schureman eq. 75.
        NodeKind::O1 => {
            let f = 1.0089 + 0.1871 * cn - 0.0147 * c2n + 0.0014 * c3n;
            let u = 10.80 * sn - 1.34 * s2n + 0.19 * s3n;
            (f, u)
        },

        // K1. Polynomial approximation of Schureman.
        NodeKind::K1 => {
            let f = 1.0060 + 0.1150 * cn - 0.0088 * c2n + 0.0006 * c3n;
            let u = -8.86 * sn + 0.68 * s2n - 0.07 * s3n;
            (f, u)
        },

        // K2. Polynomial approximation of Schureman.
        NodeKind::K2 => {
            let f = 1.0241 + 0.2863 * cn + 0.0083 * c2n - 0.0015 * c3n;
            let u = -17.74 * sn + 0.68 * s2n - 0.04 * s3n;
            (f, u)
        },

        // J1. Schureman.
        NodeKind::J1 => {
            let f = 1.1029 + 0.1676 * cn - 0.0170 * c2n + 0.0016 * c3n;
            let u = -12.94 * sn + 1.34 * s2n - 0.19 * s3n;
            (f, u)
        },

        // OO1. Schureman.
        NodeKind::OO1 => {
            let f = 1.1027 + 0.6504 * cn + 0.0317 * c2n - 0.0014 * c3n;
            let u = -36.68 * sn + 4.02 * s2n - 0.57 * s3n;
            (f, u)
        },

        // Composite M1 (= lunar terms A16+A23). Schureman §122-124, p. 41-42,
        // formula (194): arg = (T-s+h+p-90°) - ν - Q_a, amplitude ∝ 1/Q_a.
        //   P = p - ξ, with ξ ≈ 11.87°·sinN - 1.34°·sin2N + 0.19°·sin3N.
        //   1/Q_a = sqrt(2.310 + 1.435·cos 2P)  at I=ω (eq. 197).
        //   Q_a   = atan2(sin 2P, 2.873 + cos 2P)  at I=ω (eq. 196).
        //   -ν    = u(J1) (Schureman Table 2: J1's u is -ν).
        // Doodson (1,-1,1,1,0,0,-90°) supplies V0 = T-s+h+p-90°, matching
        // NOAA's tabulated M1 speed of 14.4967°/hr. The unnormalized 1/Q_a
        // (peaks near 2 at perigee alignment) matches NOAA's H(M1).
        NodeKind::M1 => {
            let xi_deg = 11.87 * sn - 1.34 * s2n + 0.19 * s3n;
            let big_p_deg = p_deg - xi_deg;
            let big_p_rad = big_p_deg.to_radians();
            let c2p = (2.0 * big_p_rad).cos();
            let q_a_inv = (2.310 + 1.435 * c2p).sqrt();
            let q_deg = (0.483 * big_p_rad.sin())
                .atan2(big_p_rad.cos())
                .to_degrees();
            let (f_o1, _) = node_factor(NodeKind::O1, n_deg, p_deg);
            let (_, u_m2) = node_factor(NodeKind::M2, n_deg, p_deg);
            let u = (0.5 * u_m2 + q_deg - p_deg).rem_euclid(360.0);
            (f_o1 * q_a_inv, u)
        },

        // L2 (smaller lunar elliptic semidiurnal). Schureman §140, eqs
        // 213-215. Like M1, L2's node factor depends on lunar perigee p,
        // not just N, so needs_dynamic() must return true for it.
        //   P      = p - ξ,   ξ = 11.87°·sinN - 1.34°·sin2N + 0.19°·sin3N.
        //   1/R_a  = sqrt(1 - 12·tan²(½I)·cos 2P + 36·tan⁴(½I))   at I=ω
        //          ≈ sqrt(1.0668 - 0.5168·cos 2P)
        //   R      = atan2(sin 2P, 1/(6·tan²½I) - cos 2P)
        //          ≈ atan2(sin 2P, 3.869 - cos 2P)
        //   f(L2)  = f(M2) · (1/R_a)   (unnormalized; matches NOAA's H(L2)
        //                                by the same convention used for M1)
        //   u(L2)  = u(M2) - R
        NodeKind::L2 => {
            let xi_deg = 11.87 * sn - 1.34 * s2n + 0.19 * s3n;
            let big_p_rad = (p_deg - xi_deg).to_radians();
            let c2p = (2.0 * big_p_rad).cos();
            let s2p = (2.0 * big_p_rad).sin();
            let r_a_inv = (1.0668 - 0.5168 * c2p).sqrt();
            let r_deg = s2p.atan2(3.869 - c2p).to_degrees();
            let (f_m2, u_m2) = node_factor(NodeKind::M2, n_deg, p_deg);
            let f = f_m2 * r_a_inv;
            let u = (u_m2 - r_deg).rem_euclid(360.0);
            (f, u)
        },

        // MM. Schureman eq. 73.
        NodeKind::Mm => {
            let f = 1.0 - 0.1300 * cn + 0.0013 * c2n;
            (f, 0.0)
        },

        // M3 lunar terdiurnal: third harmonic of M2. Schureman §86: f and u
        // scale with the cube power of cos⁴(½I), giving f(M2)^(3/2) and
        // (3/2)·u(M2).
        NodeKind::M3 => {
            let (f_m2, u_m2) = node_factor(NodeKind::M2, n_deg, p_deg);
            (f_m2.powf(1.5), 1.5 * u_m2)
        },

        // MF. Schureman eq. 74.
        NodeKind::Mf => {
            let f = 1.0429 + 0.4135 * cn - 0.004 * c2n;
            let u = -23.74 * sn + 2.68 * s2n - 0.38 * s3n;
            (f, u)
        },

        NodeKind::Shallow(a, b, c, d) => {
            let (fm, um) = node_factor(NodeKind::M2, n_deg, p_deg);
            let (fk1, uk1) = node_factor(NodeKind::K1, n_deg, p_deg);
            let (fo1, uo1) = node_factor(NodeKind::O1, n_deg, p_deg);
            let (fk2, uk2) = node_factor(NodeKind::K2, n_deg, p_deg);
            let f =
                fm.powi(a as i32) * fk1.powi(b as i32) * fo1.powi(c as i32) * fk2.powi(d as i32);
            let u = a as f64 * um + b as f64 * uk1 + c as f64 * uo1 + d as f64 * uk2;
            (f, u)
        },
    }
}

fn equilibrium(d: Doodson, a: Astro) -> f64 {
    (d.t as f64 * a.t
        + d.s as f64 * a.s
        + d.h as f64 * a.h
        + d.p as f64 * a.p
        + d.n as f64 * a.n
        + d.p_s as f64 * a.p_s
        + d.offset as f64)
        .rem_euclid(360.0)
}

/// Static precomputed term: node factor f·A and (V0+u-κ) at t_ref, plus ω.
struct StaticTerm {
    omega: f64,
    fa: f64,
    phase_deg: f64,
}

/// Dynamic term: reevaluate f and u per sample from the constituent def.
/// Used for constituents whose node factor varies rapidly (M1's Q_a changes
/// ~20°/year because perigee drifts ~40°/year; L2's R_a likewise).
struct DynamicTerm {
    def: &'static ConstDef,
    amplitude: f64,
    kappa_deg: f64,
}

fn needs_dynamic(node: NodeKind) -> bool {
    matches!(node, NodeKind::M1 | NodeKind::L2)
}

/// Tide-height predictor for a single NOAA station.
pub struct Predictor {
    t_ref: NaiveDateTime,
    z0: f64,
    static_terms: Vec<(String, StaticTerm)>,
    dynamic_terms: Vec<(String, DynamicTerm)>,
}

impl Predictor {
    /// Build a predictor from NOAA harmonic constants, using the given
    /// reference time to evaluate astronomical arguments. Pick a time near
    /// the middle of the prediction window for best accuracy on f/u.
    pub fn new(harcon: &HarconData, t_ref: NaiveDateTime) -> Self {
        let a = astro(&t_ref);
        let mut static_terms = Vec::new();
        let mut dynamic_terms = Vec::new();
        for c in &harcon.constituents {
            let Some(def) = lookup(&c.name) else {
                warn!("Unknown constituent: {}", c.name);
                continue;
            };
            if needs_dynamic(def.node) {
                dynamic_terms.push((
                    def.name.to_string(),
                    DynamicTerm {
                        def,
                        amplitude: c.amplitude,
                        kappa_deg: c.phase_gmt,
                    },
                ));
            } else {
                let v0 = equilibrium(def.d, a);
                let (f, u) = node_factor(def.node, a.n, a.p);
                let phase = (v0 + u - c.phase_gmt).rem_euclid(360.0);
                static_terms.push((
                    def.name.to_string(),
                    StaticTerm {
                        omega: c.speed,
                        fa: f * c.amplitude,
                        phase_deg: phase,
                    },
                ));
            }
        }
        Self {
            t_ref,
            z0: harcon.z0_mllw,
            static_terms,
            dynamic_terms,
        }
    }

    /// Evaluate the predicted signal at the given UTC time.
    pub fn at(&self, t: NaiveDateTime) -> f64 {
        let dt_hours = (t - self.t_ref).num_milliseconds() as f64 / 3_600_000.0;
        let mut h: f64 = self.z0
            + self
                .static_terms
                .iter()
                .map(|(_, s)| s.fa * (s.phase_deg + s.omega * dt_hours).to_radians().cos())
                .sum::<f64>();
        if !self.dynamic_terms.is_empty() {
            let a = astro(&t);
            for (_, d) in &self.dynamic_terms {
                let v0 = equilibrium(d.def.d, a);
                let (f, u) = node_factor(d.def.node, a.n, a.p);
                let arg = (v0 + u - d.kappa_deg).to_radians();
                h += f * d.amplitude * arg.cos();
            }
        }
        h
    }

    /// Diagnostic: per-constituent contribution (name, signed value) at time t.
    pub fn contributions(&self, t: NaiveDateTime) -> Vec<(String, f64)> {
        let dt_hours = (t - self.t_ref).num_milliseconds() as f64 / 3_600_000.0;
        let mut out: Vec<(String, f64)> = self
            .static_terms
            .iter()
            .map(|(name, s)| {
                (
                    name.clone(),
                    s.fa * (s.phase_deg + s.omega * dt_hours).to_radians().cos(),
                )
            })
            .collect();
        if !self.dynamic_terms.is_empty() {
            let a = astro(&t);
            for (name, d) in &self.dynamic_terms {
                let v0 = equilibrium(d.def.d, a);
                let (f, u) = node_factor(d.def.node, a.n, a.p);
                let arg = (v0 + u - d.kappa_deg).to_radians();
                out.push((name.clone(), f * d.amplitude * arg.cos()));
            }
        }
        out
    }

    /// Evaluate the predicted signal at every time in `times` (UTC).
    pub fn predict(&self, times: &[NaiveDateTime]) -> Vec<f64> {
        times.iter().map(|t| self.at(*t)).collect()
    }
}

/// Predicted tidal-current sample: along-axis components Mj (along flood
/// direction `azi`) and Mn (90° clockwise), with derived speed/direction.
#[derive(Debug, Clone, Copy)]
pub struct CurrentSample {
    /// Component along the major (flood) axis, in the same units as the
    /// source harmonic constants (cm/s for NOAA).
    pub major: f64,
    /// Component 90° clockwise of the major axis.
    pub minor: f64,
    /// sqrt(major² + minor²).
    pub speed: f64,
    /// Compass direction (degrees clockwise from true north) of the flow.
    pub direction: f64,
}

/// Static per-axis term (same shape as tide Predictor's static terms, one pair
/// per axis to avoid redundant astro/node_factor work).
struct StaticAxisTerm {
    omega: f64,
    fa_major: f64,
    phase_major: f64,
    fa_minor: f64,
    phase_minor: f64,
}

/// Dynamic per-axis term: H and κ per axis, reevaluated with f,u at call time.
struct DynamicAxisTerm {
    def: &'static ConstDef,
    major_amp: f64,
    major_kappa: f64,
    minor_amp: f64,
    minor_kappa: f64,
}

/// Precomputed predictor for a tidal-current station. Holds one term list per
/// ellipse axis plus constant mean flows along each axis.
pub struct CurrentPredictor {
    t_ref: NaiveDateTime,
    azi: f64,
    major_mean: f64,
    minor_mean: f64,
    static_terms: Vec<StaticAxisTerm>,
    dynamic_terms: Vec<DynamicAxisTerm>,
}

impl CurrentPredictor {
    pub fn new(harcon: &CurrentHarconData, t_ref: NaiveDateTime) -> Self {
        let a = astro(&t_ref);
        let mut static_terms = Vec::new();
        let mut dynamic_terms = Vec::new();

        for c in &harcon.constituents {
            let Some(def) = lookup(&c.name) else {
                warn!("Unknown constituent: {}", c.name);
                continue;
            };
            if needs_dynamic(def.node) {
                dynamic_terms.push(DynamicAxisTerm {
                    def,
                    major_amp: c.major_amplitude,
                    major_kappa: c.major_phase_gmt,
                    minor_amp: c.minor_amplitude,
                    minor_kappa: c.minor_phase_gmt,
                });
            } else {
                let v0 = equilibrium(def.d, a);
                let (f, u) = node_factor(def.node, a.n, a.p);
                let phase_major = (v0 + u - c.major_phase_gmt).rem_euclid(360.0);
                let phase_minor = (v0 + u - c.minor_phase_gmt).rem_euclid(360.0);
                static_terms.push(StaticAxisTerm {
                    omega: c.speed,
                    fa_major: f * c.major_amplitude,
                    phase_major,
                    fa_minor: f * c.minor_amplitude,
                    phase_minor,
                });
            }
        }

        Self {
            t_ref,
            azi: harcon.azi,
            major_mean: harcon.major_mean,
            minor_mean: harcon.minor_mean,
            static_terms,
            dynamic_terms,
        }
    }

    pub fn at(&self, t: NaiveDateTime) -> CurrentSample {
        self.at_with(t, true, true)
    }

    /// Same as `at`, but with toggles for the minor-axis and mean-current
    /// contributions (useful to diagnose which terms NOAA's reference
    /// predictor uses).
    pub fn at_with(
        &self,
        t: NaiveDateTime,
        include_minor: bool,
        include_mean: bool,
    ) -> CurrentSample {
        let dt_hours = (t - self.t_ref).num_milliseconds() as f64 / 3_600_000.0;
        let mut major = if include_mean { self.major_mean } else { 0.0 };
        let mut minor = if include_mean && include_minor {
            self.minor_mean
        } else {
            0.0
        };
        for s in &self.static_terms {
            let arg = (s.phase_major + s.omega * dt_hours).to_radians();
            major += s.fa_major * arg.cos();
            if include_minor {
                let arg_mn = (s.phase_minor + s.omega * dt_hours).to_radians();
                minor += s.fa_minor * arg_mn.cos();
            }
        }
        if !self.dynamic_terms.is_empty() {
            let a = astro(&t);
            for d in &self.dynamic_terms {
                let v0 = equilibrium(d.def.d, a);
                let (f, u) = node_factor(d.def.node, a.n, a.p);
                let v0_u = v0 + u;
                major += f * d.major_amp * (v0_u - d.major_kappa).to_radians().cos();
                if include_minor {
                    minor += f * d.minor_amp * (v0_u - d.minor_kappa).to_radians().cos();
                }
            }
        }
        let speed = (major * major + minor * minor).sqrt();
        let azi_r = self.azi.to_radians();
        let n = major * azi_r.cos() - minor * azi_r.sin();
        let e = major * azi_r.sin() + minor * azi_r.cos();
        let direction = e.atan2(n).to_degrees().rem_euclid(360.0);
        CurrentSample {
            major,
            minor,
            speed,
            direction,
        }
    }

    pub fn predict(&self, times: &[NaiveDateTime]) -> Vec<CurrentSample> {
        times.iter().map(|t| self.at(*t)).collect()
    }

    /// Major-axis azimuth (degrees clockwise from true north) — flood direction.
    pub fn azimuth(&self) -> f64 {
        self.azi
    }
}

/// Diagnostic: returns the equilibrium argument V₀ (deg) for the named
/// constituent at the given UTC time, or None if the name isn't in the table.
pub fn debug_v0(name: &str, t: NaiveDateTime) -> Option<f64> {
    let def = lookup(name)?;
    let a = astro(&t);
    Some(equilibrium(def.d, a))
}

/// Diagnostic: returns astronomical arguments (T, s, h, p, N, p_s) in degrees
/// at the given UTC time.
pub fn debug_astro(t: NaiveDateTime) -> (f64, f64, f64, f64, f64, f64) {
    let a = astro(&t);
    (a.t, a.s, a.h, a.p, a.n, a.p_s)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn julian_day_j2000() {
        let dt = NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let jd = julian_day(&dt);
        assert!((jd - 2451545.0).abs() < 1e-6, "got {}", jd);
    }

    #[test]
    fn constituent_lookup() {
        assert!(lookup("M2").is_some());
        assert!(lookup("m2").is_some());
        assert!(lookup("  K1 ").is_some());
        assert!(lookup("NONSENSE").is_none());
    }

    #[test]
    fn predict_is_periodic_in_omega() {
        let harcon = HarconData {
            station_id: "test".into(),
            units: "feet".into(),
            z0_mllw: 0.0,
            constituents: vec![HarmonicConstituent {
                name: "M2".into(),
                amplitude: 1.0,
                phase_gmt: 0.0,
                speed: 28.9841042,
            }],
        };
        let t0 = NaiveDate::from_ymd_opt(2024, 6, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let p = Predictor::new(&harcon, t0);
        let period_hours = 360.0 / 28.9841042;
        let t1 = t0 + chrono::Duration::milliseconds((period_hours * 3_600_000.0) as i64);
        assert!((p.at(t0) - p.at(t1)).abs() < 1e-6);
    }
}
