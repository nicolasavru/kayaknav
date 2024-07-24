use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;

use galileo::control::EventProcessor;
use galileo::control::EventPropagation;
use galileo::control::MapController;
// TODO: make public
// use galileo::control::map::MapControllerParameters;
use galileo::control::MouseButton;
use galileo::control::MouseEvent;
use galileo::control::UserEvent;
use galileo::layer::feature_layer::FeatureLayer;
use galileo::render::WgpuRenderer;
use galileo::tile_scheme::TileIndex;
use galileo::winit::WinitInputHandler;
use galileo::winit::WinitMessenger;
use galileo::Map;
use galileo::MapBuilder;
use galileo::MapView;
use galileo::TileSchema;
use galileo_types::cartesian::Point2d;
use galileo_types::cartesian::Size;
use galileo_types::geo::impls::GeoPoint2d;
use galileo_types::geometry_type::GeoSpace2d;
use galileo_types::latlon;
use wgpu::Device;
use wgpu::Queue;
use wgpu::Surface;
use wgpu::SurfaceConfiguration;
use winit::dpi::PhysicalSize;
use winit::event::WindowEvent;
use winit::window::Window;

use crate::features;
use crate::features::CurrentPredictionSymbol;
use crate::features::WaypointType;
use crate::noaa::CurrentPrediction;
use crate::prelude::*;
use crate::scheduling::Trip;
use crate::state::WaypointClickAction;
use crate::state::WgpuFrame;

pub struct GalileoState {
    input_handler: WinitInputHandler,
    event_processor: EventProcessor,
    renderer: Arc<RwLock<WgpuRenderer>>,
    map: Rc<RwLock<Map>>,
    pointer_position: Arc<RwLock<Point2d>>,
    current_prediction_layer: Arc<
        RwLock<
            FeatureLayer<GeoPoint2d, CurrentPrediction<30>, CurrentPredictionSymbol, GeoSpace2d>,
        >,
    >,
}

impl GalileoState {
    pub fn new(
        window: Arc<Window>,
        device: Arc<Device>,
        surface: Arc<Surface<'static>>,
        queue: Arc<Queue>,
        config: SurfaceConfiguration,
        waypoint_mode: Arc<RwLock<WaypointClickAction>>,
        current_prediction_layer: Arc<
            RwLock<
                FeatureLayer<
                    GeoPoint2d,
                    CurrentPrediction<30>,
                    CurrentPredictionSymbol,
                    GeoSpace2d,
                >,
            >,
        >,
        trip: Arc<RwLock<Trip>>,
    ) -> Self {
        let messenger = WinitMessenger::new(window);
        let trip_clone = trip.clone();

        let renderer = WgpuRenderer::new_with_device_and_surface(device, surface, queue, config);
        let renderer = Arc::new(RwLock::new(renderer));

        let input_handler = WinitInputHandler::default();

        let pointer_position = Arc::new(RwLock::new(Point2d::default()));
        let pointer_position_clone = pointer_position.clone();

        let mut event_processor = EventProcessor::default();
        event_processor.add_handler(move |ev: &UserEvent, map: &mut Map| {
            match (ev, &*waypoint_mode.read().unwrap()) {
                (
                    UserEvent::PointerMoved(MouseEvent {
                        screen_pointer_position,
                        ..
                    }),
                    _,
                ) => {
                    *pointer_position_clone.write().expect("poisoned lock") =
                        *screen_pointer_position;
                },

                (
                    UserEvent::Click(
                        MouseButton::Left,
                        MouseEvent {
                            screen_pointer_position,
                            ..
                        },
                    ),
                    WaypointClickAction::Move,
                ) => {
                    features::add_waypoint(
                        map,
                        trip.clone(),
                        *screen_pointer_position,
                        WaypointType::Move,
                    )
                    .unwrap();
                },

                (
                    UserEvent::Click(
                        MouseButton::Middle,
                        MouseEvent {
                            screen_pointer_position,
                            ..
                        },
                    ),
                    _,
                )
                | (
                    UserEvent::Click(
                        MouseButton::Left,
                        MouseEvent {
                            screen_pointer_position,
                            ..
                        },
                    ),
                    WaypointClickAction::Pause,
                ) => {
                    features::add_waypoint(
                        map,
                        trip.clone(),
                        *screen_pointer_position,
                        WaypointType::Pause,
                    )
                    .unwrap();
                },

                (
                    UserEvent::Click(
                        MouseButton::Right,
                        MouseEvent {
                            screen_pointer_position,
                            ..
                        },
                    ),
                    _,
                )
                | (
                    UserEvent::Click(
                        MouseButton::Left,
                        MouseEvent {
                            screen_pointer_position,
                            ..
                        },
                    ),
                    WaypointClickAction::Remove,
                ) => {
                    features::remove_waypoints(map, trip.clone(), *screen_pointer_position)
                        .unwrap();
                },

                _ => {},
            }

            EventPropagation::Propagate
        });

        // event_processor.add_handler(MapController {
        //     parameters: MapControllerParameters {
        //         max_rotation_x: 0.0
        //         ..MapControllerParameters::default()
        //     }
        // });

        event_processor.add_handler(MapController::default());

        let view = MapView::new(
            &latlon!(40.7127, -74.0059),
            TileSchema::web(18).lod_resolution(12).unwrap(),
        );

        let tile_source = |index: &TileIndex| {
            // TODO: These are cached to .tile_cache, figure out a way to redirect it.
            format!(
                "https://tile.openstreetmap.org/{}/{}/{}.png",
                index.z, index.x, index.y
            )
        };

        let layer = Box::new(MapBuilder::create_raster_tile_layer(
            tile_source,
            TileSchema::web(18),
        ));

        let map = Rc::new(RwLock::new(Map::new(view, vec![layer], Some(messenger))));

        // TODO: make layer indices constants.
        map.write()
            .unwrap()
            .layers_mut()
            .insert(1, trip_clone.read().unwrap().waypoint_layer.clone());

        map.write()
            .unwrap()
            .layers_mut()
            .insert(2, current_prediction_layer.clone());

        Self {
            input_handler,
            event_processor,
            renderer,
            map,
            pointer_position,
            current_prediction_layer,
        }
    }

    pub fn about_to_wait(&self) {
        self.map.write().unwrap().animate();
    }

    #[instrument(level = "debug", skip_all)]
    pub fn redraw_map(&self) {
        let features = features::clear_features(self.current_prediction_layer.clone());
        let mut feature_layer = self.current_prediction_layer.write().unwrap();
        let feature_store = feature_layer.features_mut();

        for feature in features.iter() {
            feature_store.insert(feature.to_owned());
        }

        // TODO: can we do anything better than removing and re-adding all the
        // features? Does re-creating the layer make sense? Maybe that will fix
        // the gradual slowdown?

        // for mut feature_container in feature_layer.features_mut().iter_mut() {
        //   // let _ = feature_container.edit_style();
        // }

        self.map.read().unwrap().redraw();
    }

    pub fn resize(&self, size: PhysicalSize<u32>) {
        self.renderer
            .write()
            .expect("poisoned lock")
            .resize(Size::new(size.width, size.height));
        self.map
            .write()
            .expect("poisoned lock")
            .set_size(Size::new(size.width as f64, size.height as f64));
    }

    #[instrument(level = "debug", skip_all)]
    pub fn render(&self, wgpu_frame: &WgpuFrame<'_>) {
        let galileo_map = self.map.read().unwrap();
        galileo_map.load_layers();

        self.renderer
            .write()
            .expect("poisoned lock")
            .render_to_texture_view(&galileo_map, wgpu_frame.texture_view);
    }

    pub fn handle_event(&mut self, event: &WindowEvent) {
        let scale = 1.0;

        if let Some(raw_event) = self.input_handler.process_user_input(event, scale) {
            let mut map = self.map.write().expect("poisoned lock");
            self.event_processor.handle(raw_event, &mut map);
        }
    }

    pub fn pointer_position(&self) -> Option<GeoPoint2d> {
        let pointer_position = *self.pointer_position.read().expect("poisoned lock");
        let view = self.map.read().expect("poisoned lock").view().clone();
        view.screen_to_map_geo(pointer_position)
    }
}
