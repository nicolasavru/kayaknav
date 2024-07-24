use std::sync::Arc;
use std::sync::RwLock;

use galileo::layer::feature_layer::symbol::Symbol;
use galileo::layer::feature_layer::Feature;
use galileo::layer::feature_layer::FeatureLayer;
use galileo::render::point_paint::PointPaint;
use galileo::render::render_bundle::RenderPrimitive;
use galileo::Color;
use galileo::Map;
use galileo_types::cartesian::CartesianPoint2d;
use galileo_types::cartesian::CartesianPoint3d;
use galileo_types::cartesian::Point2d;
use galileo_types::geo::impls::GeoPoint2d;
use galileo_types::geo::Crs;
use galileo_types::geo::GeoPoint;
use galileo_types::geo::NewGeoPoint;
use galileo_types::geo::Projection;
use galileo_types::geometry::CartesianGeometry2d;
use galileo_types::geometry::Geom;
use galileo_types::geometry::Geometry;
use galileo_types::impls::Contour;
use galileo_types::impls::Polygon;
use num_traits::AsPrimitive;

use crate::noaa::CurrentPrediction;
use crate::noaa::StationType;
use crate::prelude::*;
use crate::saturating::Saturating;
use crate::scheduling::Trip;

pub fn clear_features<P, F, S, SP>(layer: Arc<RwLock<FeatureLayer<P, F, S, SP>>>) -> Vec<F>
where
    F: Feature,
    F::Geom: Geometry<Point = P>,
    S: Symbol<F>,
{
    let mut feature_layer = layer.write().unwrap();
    let len = feature_layer.features().iter().count();
    let feature_store = feature_layer.features_mut();

    let mut features = vec![];
    for i in (0..len).rev() {
        features.push(feature_store.remove(i));
    }

    features
}

#[derive(Debug, Clone, Copy)]
pub enum WaypointType {
    Move,
    Pause,
}

#[derive(Debug, Clone, Copy)]
pub struct Waypoint {
    pub point: Point2d,
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
        let crs = Crs::EPSG3857;
        let proj: Box<dyn Projection<InPoint = GeoPoint2d, OutPoint = Point2d>> =
            crs.get_projection().unwrap();
        proj.unproject(&self.point).unwrap().lat()
    }

    fn lon(&self) -> Self::Num {
        let crs = Crs::EPSG3857;
        let proj: Box<dyn Projection<InPoint = GeoPoint2d, OutPoint = Point2d>> =
            crs.get_projection().unwrap();
        proj.unproject(&self.point).unwrap().lon()
    }
}

impl CartesianPoint2d for Waypoint {
    type Num = f64;

    fn x(&self) -> Self::Num {
        self.point.x
    }

    fn y(&self) -> Self::Num {
        self.point.y
    }
}

impl Geometry for Waypoint {
    type Point = Point2d;

    fn project<P: Projection<InPoint = Self::Point> + ?Sized>(
        &self,
        projection: &P,
    ) -> Option<Geom<P::OutPoint>> {
        self.point.project(projection)
    }
}

impl CartesianGeometry2d<Point2d> for Waypoint {
    fn is_point_inside<
        Other: galileo_types::cartesian::CartesianPoint2d<
            Num = <Point2d as galileo_types::cartesian::CartesianPoint2d>::Num,
        >,
    >(
        &self,
        point: &Other,
        tolerance: <Point2d as galileo_types::cartesian::CartesianPoint2d>::Num,
    ) -> bool {
        self.point.is_point_inside(point, tolerance)
    }

    fn bounding_rectangle(
        &self,
    ) -> Option<
        galileo_types::cartesian::Rect<
            <Point2d as galileo_types::cartesian::CartesianPoint2d>::Num,
        >,
    > {
        None
    }
}

pub struct WaypointSymbol {}

impl Symbol<Waypoint> for WaypointSymbol {
    fn render<'a, N, P>(
        &self,
        feature: &Waypoint,
        geometry: &'a Geom<P>,
        _min_resolution: f64,
    ) -> Vec<RenderPrimitive<'a, N, P, Contour<P>, Polygon<P>>>
    where
        N: AsPrimitive<f32>,
        P: CartesianPoint3d<Num = N> + Clone,
    {
        let size = 10f32;
        let mut primitives = vec![];
        let Geom::Point(point) = geometry else {
            return primitives;
        };

        primitives.push(RenderPrimitive::new_point_ref(
            point,
            PointPaint::circle(Color::BLACK, size * 2.0 + 4.0),
        ));
        primitives.push(RenderPrimitive::new_point_ref(
            point,
            PointPaint::sector(
                match feature.type_ {
                    WaypointType::Move => Color::from_hex("#ff8000"),
                    WaypointType::Pause => Color::from_hex("#0080ff"),
                },
                size * 2.0,
                0f32.to_radians(),
                360f32.to_radians(),
            ),
        ));

        primitives
    }
}

pub fn add_waypoint(
    map: &mut Map,
    trip: Arc<RwLock<Trip>>,
    pos: Point2d,
    waypoint_type: WaypointType,
) -> Result<()> {
    let view = map.view().clone();
    let map_pos = view.screen_to_map(pos).log()?;
    trip.write().unwrap().add_waypoint(Waypoint {
        point: Point2d::new(map_pos.x, map_pos.y),
        type_: waypoint_type,
    });

    map.redraw();
    Ok(())
}

pub fn remove_waypoints(map: &mut Map, trip: Arc<RwLock<Trip>>, pos: Point2d) -> Result<()> {
    let view = map.view().clone();
    let map_pos = view.screen_to_map(pos).log()?;

    let mut trip = trip.write().unwrap();
    let mut indices_to_remove = {
        let layer = trip.waypoint_layer.read().unwrap();
        let matching_features: Vec<_> = layer
            .get_features_at(&map_pos, map.view().resolution() * 10.0)
            .collect();

        let mut indices_to_remove = vec![];
        for feature_container in matching_features {
            indices_to_remove.push(feature_container.index());
        }

        indices_to_remove
    };

    indices_to_remove.sort();
    indices_to_remove.reverse();

    for index in indices_to_remove {
        trip.remove_waypoint(index);
    }

    map.redraw();
    Ok(())
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

impl<const R: u8> CartesianPoint2d for CurrentPrediction<R> {
    type Num = f64;

    fn x(&self) -> Self::Num {
        let crs = Crs::EPSG3857;
        let proj: Box<dyn Projection<InPoint = GeoPoint2d, OutPoint = Point2d>> =
            crs.get_projection().unwrap();
        proj.project(&GeoPoint2d::latlon(self.lat(), self.lon()))
            .unwrap()
            .x
    }

    fn y(&self) -> Self::Num {
        let crs = Crs::EPSG3857;
        let proj: Box<dyn Projection<InPoint = GeoPoint2d, OutPoint = Point2d>> =
            crs.get_projection().unwrap();
        proj.project(&GeoPoint2d::latlon(self.lat(), self.lon()))
            .unwrap()
            .y
    }
}

/// Heading 0 degrees is North while sector 0 degrees is right, and they
/// increase in opposite directions..
fn heading_degrees_to_polar_degrees(heading: f32) -> f32 {
    heading * -1.0 + 90.0
}

pub struct CurrentPredictionSymbol {
    pub time_idx: Arc<RwLock<Saturating<usize>>>,
}

impl<const R: u8> Symbol<CurrentPrediction<R>> for CurrentPredictionSymbol {
    fn render<'a, N, P>(
        &self,
        feature: &CurrentPrediction<R>,
        geometry: &'a Geom<P>,
        _min_resolution: f64,
    ) -> Vec<RenderPrimitive<'a, N, P, Contour<P>, Polygon<P>>>
    where
        N: AsPrimitive<f32>,
        P: CartesianPoint3d<Num = N> + Clone,
    {
        let base_size = 30f32;
        let mut primitives = vec![];
        let Geom::Point(point) = geometry else {
            return primitives;
        };

        let mut rev_heading = heading_degrees_to_polar_degrees(
            (feature.df["direction"]
             .f64()
             .unwrap()
             .get(self.time_idx.read().unwrap().val())
             .unwrap()) as f32)
        // Reverse it so the the sector "arrow" points in the right
        // direction.
            + 180.0;

        let mut speed = (feature.df["speed"]
            .f64()
            .unwrap()
            .get(self.time_idx.read().unwrap().val())
            .unwrap()) as f32;

        if speed < 0.0 {
            speed *= -1.0;
            rev_heading += 180.0;
        }

        primitives.push(RenderPrimitive::new_point_ref(
            point,
            PointPaint::sector(
                match feature.station.type_ {
                    StationType::Harmonic => Color::BLUE,
                    StationType::Subordinate => Color::RED,
                },
                base_size * speed,
                (rev_heading - 15.0).to_radians(),
                (rev_heading + 15.0).to_radians(),
            ),
        ));

        primitives
    }
}
