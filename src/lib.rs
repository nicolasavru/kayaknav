use std::io;
use std::panic;
use std::sync::Arc;

use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::format::Pretty;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
#[cfg(target_arch = "wasm32")]
use tracing_web::performance_layer;
#[cfg(target_arch = "wasm32")]
use tracing_web::MakeWebConsoleWriter;
#[cfg(target_arch = "wasm32")]
use winit::dpi::PhysicalSize;
use winit::event::Event;
use winit::event::KeyEvent;
use winit::event::WindowEvent;
use winit::event_loop::ControlFlow;
use winit::event_loop::EventLoop;
#[cfg(target_arch = "wasm32")]
use winit::platform::web::WindowExtWebSys;
use winit::window::Window;
#[cfg(target_arch = "wasm32")]
use winit::window::WindowBuilder;

mod error_utils;
mod features;
mod http;
mod noaa;
pub mod prelude;
mod run_ui;
mod saturating;
pub mod scheduling;
pub mod state;

use crate::state::State;

#[cfg(target_arch = "wasm32")]
pub mod html_panic_hook;

fn configure_tracing() {
    let mut layers = Vec::new();

    #[cfg(not(target_arch = "wasm32"))]
    {
        let layer = tracing_subscriber::fmt::layer()
            .with_writer(io::stderr)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_filter(EnvFilter::from_default_env());

        layers.push(layer.boxed());
    }

    #[cfg(target_arch = "wasm32")]
    {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_ansi(false) // Only partially supported across browsers
            .without_time()   // std::time is not available in browsers
            .with_level(false)
            .with_writer(MakeWebConsoleWriter::new()
                         .with_pretty_level())
            .with_filter(LevelFilter::WARN);
        layers.push(fmt_layer.boxed());

        let perf_layer = performance_layer().with_details_from_fields(Pretty::default());
        layers.push(perf_layer.boxed());
    }

    tracing_subscriber::registry().with(layers).init();
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Config {
    pub use_api_proxy: bool,
    pub api_proxy_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            use_api_proxy: false,
            api_proxy_url: "https://kayaknav.com/proxy".to_string(),
        }
    }
}

pub async fn run(window: Window, event_loop: EventLoop<()>, config: Config) {
    #[cfg(target_arch = "wasm32")]
    panic::set_hook(Box::new(html_panic_hook::hook));

    configure_tracing();

    let window = Arc::new(window);

    let mut state = State::new(Arc::clone(&window), config).await.unwrap();

    let _ = event_loop.run(move |event, ewlt| {
        ewlt.set_control_flow(ControlFlow::Wait);

        match &event {
            Event::AboutToWait => {
                state.about_to_wait();
            },
            Event::WindowEvent { event, window_id } if *window_id == state.window().id() => {
                match event {
                    WindowEvent::CloseRequested
                    | WindowEvent::KeyboardInput {
                        event:
                            KeyEvent {
                                logical_key:
                                    winit::keyboard::Key::Named(winit::keyboard::NamedKey::Escape),
                                ..
                            },
                        ..
                    } => ewlt.exit(),
                    WindowEvent::Resized(physical_size) => {
                        state.resize(*physical_size);
                    },
                    WindowEvent::RedrawRequested => match state.render() {
                        Ok(_) => {},
                        Err(wgpu::SurfaceError::Lost | wgpu::SurfaceError::Outdated) => {
                            state.resize(state.size)
                        },
                        Err(wgpu::SurfaceError::OutOfMemory) => ewlt.exit(),
                        Err(wgpu::SurfaceError::Timeout) => {},
                    },
                    other => {
                        state.handle_event(other);
                        window.request_redraw();
                        return;
                    },
                };
                state.handle_event(event);
                window.request_redraw();
            },
            _ => {},
        }
    });
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

#[cfg(target_arch = "wasm32")]
pub async fn set_up() -> (Window, EventLoop<()>) {
    let event_loop = EventLoop::new().unwrap();
    let window = WindowBuilder::new().build(&event_loop).unwrap();
    let window = window;

    web_sys::window()
        .and_then(|win| win.document())
        .and_then(|doc| {
            let dst = doc.get_element_by_id("map")?;
            let canvas = web_sys::Element::from(window.canvas()?);
            dst.append_child(&canvas).ok()?;

            Some(())
        })
        .expect("Couldn't create canvas");

    let web_window = web_sys::window().unwrap();
    let scale = web_window.device_pixel_ratio();

    let _ = window.request_inner_size(PhysicalSize::new(
        web_window.inner_width().unwrap().as_f64().unwrap() * scale,
        web_window.inner_height().unwrap().as_f64().unwrap() * scale,
    ));

    sleep(10).await;

    (window, event_loop)
}

#[cfg(target_arch = "wasm32")]
async fn sleep(duration: i32) {
    let mut cb = |resolve: js_sys::Function, _reject: js_sys::Function| {
        web_sys::window()
            .unwrap()
            .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, duration)
            .unwrap();
    };

    let p = js_sys::Promise::new(&mut cb);

    wasm_bindgen_futures::JsFuture::from(p).await.unwrap();
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub async fn init() {
    let config = Config {
        use_api_proxy: option_env!("KAYAKNAV_USE_API_PROXY")
            .map(|s| {
                s.parse::<bool>()
                    .expect("KAYAKNAV_USE_API_PROXY must be 'true' or 'false'")
            })
            .unwrap_or(true),
        api_proxy_url: option_env!("KAYAKNAV_API_PROXY_URL")
            .map(str::to_string)
            .unwrap_or_else(|| Config::default().api_proxy_url),
    };
    let (window, event_loop) = set_up().await;
    run(window, event_loop, config).await;
}
