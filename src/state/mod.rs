use std::iter;
use std::mem;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;

use chrono::offset::Local;
use chrono::Datelike;
use chrono::NaiveDate;
use futures::future;
use galileo::layer::feature_layer::FeatureLayer;
use galileo_types::geo::Crs;
use polars::prelude::*;
use uom::si::f64::Velocity;
use uom::si::velocity::knot;
use wgpu::Backends;
use wgpu::CommandEncoder;
use wgpu::CommandEncoderDescriptor;
use wgpu::Device;
use wgpu::DeviceDescriptor;
use wgpu::Features;
use wgpu::Instance;
use wgpu::InstanceDescriptor;
use wgpu::Limits;
use wgpu::PowerPreference;
use wgpu::Queue;
use wgpu::RequestAdapterOptions;
use wgpu::Surface;
use wgpu::SurfaceConfiguration;
use wgpu::SurfaceError;
use wgpu::TextureAspect;
use wgpu::TextureFormat;
use wgpu::TextureUsages;
use wgpu::TextureView;
use wgpu::TextureViewDescriptor;
use winit::dpi::PhysicalSize;
use winit::event::ElementState;
use winit::event::KeyEvent;
use winit::event::WindowEvent;
use winit::keyboard::Key;
use winit::keyboard::NamedKey;
use winit::window::Window;

use crate::features::CurrentPredictionSymbol;
use crate::features::WaypointSymbol;
use crate::http::ApiProxy;
use crate::noaa::Station;
use crate::prelude::*;
use crate::run_ui::run_ui;
use crate::run_ui::UiState;
use crate::saturating::Saturating;
use crate::scheduling::Trip;
use crate::state::egui_state::EguiState;
use crate::state::galileo_state::GalileoState;
use crate::Config;

mod egui_state;
pub mod galileo_state;

pub struct WgpuFrame<'frame> {
    device: &'frame Device,
    queue: &'frame Queue,
    encoder: &'frame mut CommandEncoder,
    window: &'frame Window,
    texture_view: &'frame TextureView,
    size: PhysicalSize<u32>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WaypointClickAction {
    Move,
    Pause,
    Remove,
}

pub struct State {
    pub surface: Arc<Surface<'static>>,
    pub device: Arc<Device>,
    pub queue: Arc<Queue>,
    pub surface_config: SurfaceConfiguration,
    pub size: PhysicalSize<u32>,
    pub window: Arc<Window>,
    pub egui_state: EguiState,
    pub galileo_state: Rc<RwLock<GalileoState>>,
    pub ui_state: UiState,
    pub time_idx: Arc<RwLock<Saturating<usize>>>,
}

impl State {
    pub async fn new(window: Arc<Window>, config: Config) -> Result<Self> {
        let size = window.inner_size();

        let instance = Instance::new(InstanceDescriptor {
            backends: Backends::all(),
            ..Default::default()
        });

        let surface = instance.create_surface(window.clone()).log()?;

        let adapter = instance
            .request_adapter(&RequestAdapterOptions {
                power_preference: PowerPreference::HighPerformance,
                compatible_surface: Some(&surface),
                force_fallback_adapter: false,
            })
            .await
            .log()?;

        let limits = if cfg!(target_arch = "wasm32") {
            Limits::downlevel_webgl2_defaults()
        } else {
            Limits::default()
        }
        .using_resolution(adapter.limits());

        let (device, queue) = adapter
            .request_device(
                &DeviceDescriptor {
                    label: None,
                    required_features: Features::empty(),
                    required_limits: limits,
                },
                None,
            )
            .await
            .log()?;

        let surface_caps = surface.get_capabilities(&adapter);
        let surface_format = surface_caps
            .formats
            .iter()
            .copied()
            .find(TextureFormat::is_srgb)
            .unwrap_or(surface_caps.formats[0]);
        let surface_config = SurfaceConfiguration {
            usage: TextureUsages::RENDER_ATTACHMENT,
            format: surface_format,
            width: size.width,
            height: size.height,
            present_mode: surface_caps.present_modes[0],
            desired_maximum_frame_latency: 2,
            alpha_mode: surface_caps.alpha_modes[0],
            view_formats: vec![],
        };
        surface.configure(&device, &surface_config);

        let egui_state = EguiState::new(&device, surface_config.format, None, 1, &window);

        let surface = Arc::new(surface);
        let device = Arc::new(device);
        let queue = Arc::new(queue);

        let api_proxy = if config.use_api_proxy {
            Some(ApiProxy {
                url: config.api_proxy_url,
            })
        } else {
            None
        };

        let battery = Station::new("8518750", api_proxy.clone()).await.log()?;

        let today = Local::now().date_naive();
        // https://tidesandcurrents.noaa.gov/noaacurrents/Faq#07
        // TODO: make a function
        // let start_month = match today.month() {
        //     2..=4 => 2,
        //     5..=7 => 5,
        //     8..=10 => 8,
        //     11 | 12 | 1 => 11,
        //     _ => unreachable!()
        // };

        // let start_date = NaiveDate::from_ymd_opt(today.year(),
        //                                          start_month,
        //                                          1)
        //     .log()?;

        let start_date = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).log()?;

        let duration_hours = 24 * 30 * 2;
        let nyc_lat_range = (39.0, 42.0);
        let nyc_lon_range = (-73.0, -75.0);

        let battery_tide_predictions = battery
            .tide_prediction(start_date, duration_hours)
            .await
            .log()?;

        let time_vec = battery_tide_predictions["time"]
            .datetime()
            .log()?
            .to_vec_null_aware()
            .unwrap_left();

        let mut max_time_idx = time_vec.len() - 1;

        let stations = Station::in_area(nyc_lat_range, nyc_lon_range, api_proxy)
            .await
            .log()?;
        info!("Found stations: {:?}", stations);

        let mut current_prediction_futures = Vec::new();
        for station in stations.iter() {
            current_prediction_futures.push(station.current_prediction(start_date, duration_hours))
        }

        let mut current_predictions: Vec<_> = future::join_all(current_prediction_futures)
            .await
            .into_iter()
            .flatten()
            .collect();

        for pred in &mut current_predictions {
            pred.df = mem::take(&mut pred.df)
                .lazy()
                .filter(col("time").gt_eq(time_vec[0]))
                .collect()
                .log()?;
            let max_idx = pred.df.height() - 1;
            if max_idx < max_time_idx {
                max_time_idx = max_idx;
            }
        }

        let time_idx = Arc::new(RwLock::new(Saturating::new(0, 0, max_time_idx)));

        let current_prediction_layer = FeatureLayer::new(
            current_predictions.clone(),
            CurrentPredictionSymbol {
                time_idx: time_idx.clone(),
            },
            Crs::EPSG3857,
        );
        let current_prediction_layer = Arc::new(RwLock::new(current_prediction_layer));

        let waypoint_layer = FeatureLayer::new(vec![], WaypointSymbol {}, Crs::EPSG3857);
        let waypoint_layer = Arc::new(RwLock::new(waypoint_layer));

        let trip = Arc::new(RwLock::new(Trip::new(
            Velocity::new::<knot>(3.0),
            waypoint_layer,
            current_predictions,
        )?));

        let waypoint_mode = Arc::new(RwLock::new(WaypointClickAction::Move));

        let galileo_state = GalileoState::new(
            Arc::clone(&window),
            Arc::clone(&device),
            Arc::clone(&surface),
            Arc::clone(&queue),
            surface_config.clone(),
            waypoint_mode.clone(),
            current_prediction_layer,
            trip.clone(),
        );
        let galileo_state = Rc::new(RwLock::new(galileo_state));

        let ui_state = UiState::new(
            time_idx.clone(),
            battery_tide_predictions,
            waypoint_mode,
            trip,
            galileo_state.clone(),
        );

        Ok(Self {
            surface,
            device,
            queue,
            surface_config,
            size,
            window,
            egui_state,
            galileo_state,
            ui_state,
            time_idx,
        })
    }

    pub fn window(&self) -> &Window {
        &self.window
    }

    pub fn about_to_wait(&mut self) {
        self.galileo_state.read().unwrap().about_to_wait();
    }

    pub fn resize(&mut self, new_size: PhysicalSize<u32>) {
        self.galileo_state.read().unwrap().resize(new_size);
        if new_size.width > 0 && new_size.height > 0 {
            self.size = new_size;
            self.surface_config.width = new_size.width;
            self.surface_config.height = new_size.height;
            self.surface.configure(&self.device, &self.surface_config);
        }
    }

    pub fn handle_event(&mut self, event: &WindowEvent) {
        // TODO: pass through other keys, e.g., F5 to refresh
        match event {
            WindowEvent::KeyboardInput {
                event:
                    KeyEvent {
                        state: ElementState::Pressed,
                        logical_key: Key::Named(NamedKey::ArrowRight),
                        ..
                    },
                ..
            } => {
                if self.time_idx.write().unwrap().inc() {
                    self.galileo_state.read().unwrap().redraw_map();
                }
            },
            WindowEvent::KeyboardInput {
                event:
                    KeyEvent {
                        state: ElementState::Pressed,
                        logical_key: Key::Named(NamedKey::ArrowLeft),
                        ..
                    },
                ..
            } => {
                if self.time_idx.write().unwrap().dec() {
                    self.galileo_state.read().unwrap().redraw_map();
                }
            },
            _ => (),
        }

        let res = self.egui_state.handle_event(&self.window, event);

        if !res.consumed {
            self.galileo_state.write().unwrap().handle_event(event);
        }

        self.window().request_redraw();
    }

    pub fn render(&mut self) -> Result<(), SurfaceError> {
        self.ui_state.pointer_position = self.galileo_state.read().unwrap().pointer_position();

        let texture = self.surface.get_current_texture()?;

        let texture_view = texture.texture.create_view(&TextureViewDescriptor {
            label: None,
            format: None,
            dimension: None,
            aspect: TextureAspect::All,
            base_mip_level: 0,
            mip_level_count: None,
            base_array_layer: 0,
            array_layer_count: None,
        });

        let mut encoder = self
            .device
            .create_command_encoder(&CommandEncoderDescriptor {
                label: Some("Render Encoder"),
            });

        {
            let mut wgpu_frame = WgpuFrame {
                device: &self.device,
                queue: &self.queue,
                encoder: &mut encoder,
                window: &self.window,
                texture_view: &texture_view,
                size: self.size,
            };

            self.galileo_state.read().unwrap().render(&wgpu_frame);

            self.egui_state
                .render(&mut wgpu_frame, |ui| run_ui(&mut self.ui_state, ui));
        }

        self.queue.submit(iter::once(encoder.finish()));

        texture.present();

        Ok(())
    }
}
