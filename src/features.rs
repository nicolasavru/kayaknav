use std::sync::Arc;

use galileo::Color;
use galileo::Map;
use galileo::layer::feature_layer::Feature;
use galileo::layer::feature_layer::FeatureId;
use galileo::layer::feature_layer::FeatureLayer;
use galileo::layer::feature_layer::symbol::Symbol;
use galileo::render::LineCap;
use galileo::render::LinePaint;
use galileo::render::point_paint::PointPaint;
use galileo::render::render_bundle::RenderBundle;
use galileo_types::Segment;
use galileo_types::cartesian::CartesianPoint2d;
use galileo_types::cartesian::CartesianPoint3d;
use galileo_types::cartesian::Point2;
use galileo_types::cartesian::Point3;
use galileo_types::contour::Contour as ContourTrait;
use galileo_types::geo::Crs;
use galileo_types::geo::GeoPoint;
use galileo_types::geo::NewGeoPoint;
use galileo_types::geo::Projection;
use galileo_types::geo::impls::GeoPoint2d;
use galileo_types::geometry::CartesianGeometry2d;
use galileo_types::geometry::Geom;
use galileo_types::geometry::Geometry;
use galileo_types::geometry_type::CartesianSpace2d;
use galileo_types::impls::Contour;
use parking_lot::RwLock;

use crate::noaa::CurrentPrediction;
use crate::noaa::StationType;
use crate::prelude::*;
use crate::saturating::Saturating;
use crate::scheduling::Trip;

/// EPSG:3857 projection for lat/lon ↔ galileo-map coordinate conversions.
/// Galileo only implements `Projection` behind a `Box<dyn ...>`, so this
/// helper hides the `get_projection().unwrap()` dance and the type annotation.
fn epsg3857() -> Box<dyn Projection<InPoint = GeoPoint2d, OutPoint = Point2>> {
    Crs::EPSG3857.get_projection().unwrap()
}

pub fn clear_waypoint_features(
    layer: Arc<RwLock<FeatureLayer<Point2, Waypoint, WaypointSymbol, CartesianSpace2d>>>,
) {
    let mut feature_layer = layer.write();
    let ids: Vec<FeatureId> = feature_layer.features().iter().map(|(id, _)| id).collect();
    for id in ids {
        feature_layer.features_mut().remove(id);
    }
    feature_layer.update_all_features();
}

#[derive(Debug, Clone, Copy)]
pub enum WaypointType {
    Move,
    Pause,
}

#[derive(Debug, Clone, Copy)]
pub struct Waypoint {
    pub point: Point2,
    pub type_: WaypointType,
}

impl Feature for Waypoint {
    type Geom = Self;

    fn geometry(&self) -> &Self::Geom {
        self
    }
}

impl GeoPoint for Waypoint {
    type Num = f64;

    fn lat(&self) -> Self::Num {
        epsg3857().unproject(&self.point).unwrap().lat()
    }

    fn lon(&self) -> Self::Num {
        epsg3857().unproject(&self.point).unwrap().lon()
    }
}

impl CartesianPoint2d for Waypoint {
    type Num = f64;

    fn x(&self) -> Self::Num {
        self.point.x()
    }

    fn y(&self) -> Self::Num {
        self.point.y()
    }
}

impl Geometry for Waypoint {
    type Point = Point2;

    fn project<P: Projection<InPoint = Self::Point> + ?Sized>(
        &self,
        projection: &P,
    ) -> Option<Geom<P::OutPoint>> {
        self.point.project(projection)
    }
}

impl CartesianGeometry2d<Point2> for Waypoint {
    fn is_point_inside<Other: CartesianPoint2d<Num = f64>>(
        &self,
        point: &Other,
        tolerance: f64,
    ) -> bool {
        self.point.is_point_inside(point, tolerance)
    }

    fn bounding_rectangle(&self) -> Option<galileo_types::cartesian::Rect<f64>> {
        None
    }
}

fn zoom_scale(min_resolution: f64, ref_res: f64, min: f64, max: f64) -> f32 {
    (ref_res / min_resolution).clamp(min, max) as f32
}

pub struct WaypointSymbol {}

impl Symbol<Waypoint> for WaypointSymbol {
    fn render(
        &self,
        feature: &Waypoint,
        geometry: &Geom<Point3>,
        min_resolution: f64,
        bundle: &mut RenderBundle,
    ) {
        let Geom::Point(point) = geometry else {
            return;
        };

        let scale = zoom_scale(min_resolution, 10.0, 0.4, 2.0);
        let size = 2f32 * scale;

        bundle.add_point(
            point,
            &PointPaint::circle(Color::BLACK, size * 2.0 + 4.0 * scale),
            min_resolution,
        );
        bundle.add_point(
            point,
            &PointPaint::sector(
                match feature.type_ {
                    WaypointType::Move => Color::from_hex("#ff8000"),
                    WaypointType::Pause => Color::from_hex("#0080ff"),
                },
                size * 2.0,
                0f32.to_radians(),
                360f32.to_radians(),
            ),
            min_resolution,
        );
    }
}

pub fn add_waypoint(
    map: &mut Map,
    trip: Arc<RwLock<Trip>>,
    pos: Point2,
    waypoint_type: WaypointType,
) -> Result<()> {
    let view = map.view().clone();
    let map_pos = view.screen_to_map(pos).log()?;
    trip.write().add_waypoint(Waypoint {
        point: Point2::new(map_pos.x(), map_pos.y()),
        type_: waypoint_type,
    });

    map.redraw();
    Ok(())
}

/// Path connecting the trip's waypoints in visit order. Rendered as a
/// single feature (a polyline) so the symbol can iterate segments and
/// decorate each with a direction arrow without needing neighbor lookup.
#[derive(Debug, Clone)]
pub struct TripPath {
    pub contour: Contour<Point2>,
}

impl Feature for TripPath {
    type Geom = Contour<Point2>;

    fn geometry(&self) -> &Self::Geom {
        &self.contour
    }
}

/// Rebuild the single-feature content of the path layer from a slice of
/// waypoints. Fewer than two waypoints means no line to draw — we still
/// clear so stale segments disappear after a `remove_waypoint`.
pub fn set_trip_path_from_waypoints(
    layer: Arc<RwLock<FeatureLayer<Point2, TripPath, TripPathSymbol, CartesianSpace2d>>>,
    waypoints: &[Waypoint],
) {
    let mut layer = layer.write();
    let ids: Vec<FeatureId> = layer.features().iter().map(|(id, _)| id).collect();
    for id in ids {
        layer.features_mut().remove(id);
    }
    if waypoints.len() >= 2 {
        let points: Vec<Point2> = waypoints.iter().map(|w| w.point).collect();
        layer.features_mut().add(TripPath {
            contour: Contour::open(points),
        });
    }
    layer.update_all_features();
}

pub struct TripPathSymbol {}

impl Symbol<TripPath> for TripPathSymbol {
    fn render(
        &self,
        _feature: &TripPath,
        geometry: &Geom<Point3>,
        min_resolution: f64,
        bundle: &mut RenderBundle,
    ) {
        let Geom::Contour(contour) = geometry else {
            return;
        };

        let line_paint = LinePaint {
            color: Color::from_hex("#ff8800").with_alpha(210),
            width: 2.5,
            offset: 0.0,
            line_cap: LineCap::Round,
            dasharray: None,
        };
        bundle.add_line(contour, &line_paint, min_resolution);

        // Arrow chevrons at segment midpoints. Size follows zoom like
        // WaypointSymbol so the arrows don't dominate the map when
        // zoomed out, or disappear when zoomed in.
        let scale = zoom_scale(min_resolution, 10.0, 0.6, 1.6);
        let arrow_diameter = 18f32 * scale;
        // Narrow wedge — ±25° from the center angle is a recognisably
        // arrow-shaped chevron.
        let half_angle = 25f32.to_radians();
        let arrow_color = Color::from_hex("#ff5500");

        for Segment(p1, p2) in contour.iter_segments() {
            let dx = (p2.x() - p1.x()) as f32;
            let dy = (p2.y() - p1.y()) as f32;
            // Skip zero-length segments (overlapping waypoints) — atan2
            // of (0,0) is defined, but a sector of a degenerate segment
            // has no direction to indicate.
            if dx.abs() < 1e-6 && dy.abs() < 1e-6 {
                continue;
            }

            let midpoint = Point3::new(
                (p1.x() + p2.x()) / 2.0,
                (p1.y() + p2.y()) / 2.0,
                (p1.z() + p2.z()) / 2.0,
            );

            // Sector apex is at the midpoint; the wedge opens in its
            // center-angle direction. Galileo's screen axis flips vs.
            // world y (+y world = north, +y screen = down), so we add
            // 180° the same way `CurrentPredictionSymbol` does — the
            // wedge then visually "points" along the direction of
            // travel on screen.
            let center_angle = dy.atan2(dx) + std::f32::consts::PI;

            bundle.add_point(
                &midpoint,
                &PointPaint::sector(
                    arrow_color,
                    arrow_diameter,
                    center_angle - half_angle,
                    center_angle + half_angle,
                ),
                min_resolution,
            );
        }
    }
}

pub fn remove_waypoints(map: &mut Map, trip: Arc<RwLock<Trip>>, pos: Point2) -> Result<()> {
    let view = map.view().clone();
    let map_pos = view.screen_to_map(pos).log()?;

    let ids_to_remove: Vec<FeatureId> = {
        let trip = trip.read();
        let layer = trip.waypoint_layer.read();
        layer
            .get_features_at(&map_pos, map.view().resolution() * 10.0)
            .map(|(id, _)| id)
            .collect()
    };

    {
        let mut trip = trip.write();
        for id in ids_to_remove {
            trip.remove_waypoint_by_id(id);
        }
    }

    map.redraw();
    Ok(())
}

/// Lightweight "this station exists" marker rendered for every station
/// in the embedded harcon store at startup. Carries only the location and
/// type — no time series — so building 4000+ of them is essentially free
/// and the map can show the global station distribution before the
/// prediction loader has touched a single one.
#[derive(Debug, Clone, Copy)]
pub struct StationMarker {
    pub loc: GeoPoint2d,
    pub type_: StationType,
}

impl Feature for StationMarker {
    type Geom = GeoPoint2d;

    fn geometry(&self) -> &Self::Geom {
        &self.loc
    }
}

pub struct StationMarkerSymbol {}

impl Symbol<StationMarker> for StationMarkerSymbol {
    fn render(
        &self,
        feature: &StationMarker,
        geometry: &Geom<Point3>,
        min_resolution: f64,
        bundle: &mut RenderBundle,
    ) {
        let Geom::Point(point) = geometry else {
            return;
        };

        // Pixel-space dot: stays a constant ~3 px regardless of zoom so
        // the overall pattern of stations remains visible when zoomed out
        // to a regional or continental view (the whole point of having
        // markers — current-prediction arrows are world-space and shrink
        // to sub-pixel at low zoom). The clamp lets us nudge it slightly
        // larger when zoomed in close, so the dot doesn't disappear
        // entirely under the eventual arrow shaft.
        let scale = zoom_scale(min_resolution, 10.0, 0.8, 1.4);
        let dot_diameter = 3.0 * scale;
        // Thin white halo behind the colored fill — keeps the dot
        // legible over both bright land tiles and dark water tiles
        // without needing a per-tile contrast heuristic.
        let halo_diameter = dot_diameter + 2.0;

        let fill = match feature.type_ {
            StationType::Harmonic => Color::BLUE,
            StationType::Subordinate => Color::RED,
        };

        bundle.add_point(
            point,
            &PointPaint::circle(Color::WHITE, halo_diameter),
            min_resolution,
        );
        bundle.add_point(
            point,
            &PointPaint::circle(fill, dot_diameter),
            min_resolution,
        );
    }
}

impl<const R: u8> Feature for CurrentPrediction<R> {
    type Geom = GeoPoint2d;

    fn geometry(&self) -> &Self::Geom {
        &self.station.loc
    }
}

impl<const R: u8> GeoPoint for CurrentPrediction<R> {
    type Num = f64;

    fn lat(&self) -> Self::Num {
        self.station.loc.lat()
    }

    fn lon(&self) -> Self::Num {
        self.station.loc.lon()
    }
}

/// Project a station's lat/lon into the EPSG:3857 map plane. Called by
/// both `CartesianPoint2d::x` and `::y` — the trait splits the axes into
/// separate methods, so each call pays one `Box<dyn Projection>` alloc
/// plus the projection. Shared here so the body lives in one place.
fn project_current_prediction<const R: u8>(p: &CurrentPrediction<R>) -> Point2 {
    epsg3857()
        .project(&GeoPoint2d::latlon(p.lat(), p.lon()))
        .unwrap()
}

impl<const R: u8> CartesianPoint2d for CurrentPrediction<R> {
    type Num = f64;

    fn x(&self) -> Self::Num {
        project_current_prediction(self).x()
    }

    fn y(&self) -> Self::Num {
        project_current_prediction(self).y()
    }
}

pub struct CurrentPredictionSymbol {
    pub time_idx: Arc<RwLock<Saturating<usize>>>,
}

impl<const R: u8> Symbol<CurrentPrediction<R>> for CurrentPredictionSymbol {
    fn render(
        &self,
        feature: &CurrentPrediction<R>,
        geometry: &Geom<Point3>,
        min_resolution: f64,
        bundle: &mut RenderBundle,
    ) {
        let Geom::Point(point) = geometry else {
            return;
        };

        let idx = self.time_idx.read().val();

        let mut heading = feature.direction[idx] as f32;
        let mut speed = feature.speed[idx] as f32;
        if speed < 0.0 {
            speed *= -1.0;
            heading += 180.0;
        }

        // Draw the arrow as a line in *world* coordinates (EPSG:3857
        // meters) rather than a pixel-sized sector. Galileo's vertex
        // shader transforms world coords through the view matrix, so a
        // world-space line naturally scales with zoom — a 1 kt arrow
        // always covers the same real-world distance on the map.
        //
        // A pixel-sized sector can't do this: `min_resolution` here is
        // the *LOD's* configured resolution (a constant set by
        // `FeatureLayer::new`, which creates a single LOD at 1.0), not
        // the current view's resolution — so any formula that tries to
        // derive a zoom-aware pixel size from it is reading a constant.
        //
        // EPSG:3857 world units are meters at the equator and
        // `1/cos(lat)` meters at latitude L, hence the lat_cos divisor.
        // 0.2 mi per knot ≈ 10 min of drift at that current — visually
        // large enough to read at a glance while still letting nearby
        // stations stay distinguishable.
        const MILES_PER_KNOT: f32 = 0.2;
        const METERS_PER_MILE: f32 = 1609.344;
        let lat_cos = (feature.station.loc.lat().to_radians() as f32)
            .cos()
            .max(0.01);
        let arrow_world_len = (MILES_PER_KNOT * speed * METERS_PER_MILE / lat_cos) as f64;

        // Compass heading (0°=N, CW) → math angle (0°=+x, CCW). World
        // +y is north; galileo's view matrix handles the "north-up"
        // flip when projecting to the screen, so no extra 180° turn is
        // needed here (contrast `TripPathSymbol`'s chevrons, which are
        // pixel-space sectors in y-flipped screen coords and so do
        // need the flip).
        let math_angle_rad = (90.0 - heading).to_radians() as f64;
        let dx = math_angle_rad.cos() * arrow_world_len;
        let dy = math_angle_rad.sin() * arrow_world_len;

        let tip = Point3::new(point.x() + dx, point.y() + dy, point.z());

        // Arrowhead: a V-shape at the tip, also drawn in world coords
        // so the chevron scales with the shaft. The head points back
        // toward the base — its arms are `back_angle ± head_half_angle`
        // (both offsets from "opposite of travel"). Length is a fixed
        // fraction of the shaft so short arrows don't get a dispropor-
        // tionately large head at low currents.
        let head_len = arrow_world_len * 0.3;
        let head_half_angle = 25f64.to_radians();
        let back_angle = math_angle_rad + std::f64::consts::PI;
        let head_left = Point3::new(
            tip.x() + (back_angle - head_half_angle).cos() * head_len,
            tip.y() + (back_angle - head_half_angle).sin() * head_len,
            tip.z(),
        );
        let head_right = Point3::new(
            tip.x() + (back_angle + head_half_angle).cos() * head_len,
            tip.y() + (back_angle + head_half_angle).sin() * head_len,
            tip.z(),
        );

        let shaft = Contour::open(vec![*point, tip]);
        let head = Contour::open(vec![head_left, tip, head_right]);

        let color = match feature.station.type_ {
            StationType::Harmonic => Color::BLUE,
            StationType::Subordinate => Color::RED,
        };
        let paint = LinePaint {
            color,
            width: 3.0,
            offset: 0.0,
            line_cap: LineCap::Round,
            dasharray: None,
        };

        bundle.add_line(&shaft, &paint, min_resolution);
        bundle.add_line(&head, &paint, min_resolution);
    }
}
