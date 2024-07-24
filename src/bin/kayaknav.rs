use bpaf::Parser;
use kayaknav::run;
use kayaknav::Config;
use winit::event_loop::EventLoop;
use winit::window::WindowBuilder;

fn parse_args() -> Config {
    let default_config = Config::default();

    let use_api_proxy = bpaf::long("use-api-proxy")
        .help("Whether to use an api proxy. The NOAA API is slow and returns 504s when too many concurrent requests are made.")
        .argument::<bool>("BOOL")
        .fallback(default_config.use_api_proxy)
        .display_fallback();

    let api_proxy_url = bpaf::long("api-proxy-url")
        .help("The api proxy url to use. The proxy must accept the url-encodd url to query as an `apiurl` query parameter. An implementation of a proxy is provided in web/functions/proxy.js.")
        .argument::<String>("URL")
        .fallback(default_config.api_proxy_url)
        .display_fallback();

    bpaf::construct!(Config {
        use_api_proxy,
        api_proxy_url
    })
    .to_options()
    .run()
}

#[tokio::main]
async fn main() {
    let config = parse_args();
    let event_loop = EventLoop::new().unwrap();
    let window = WindowBuilder::new()
        .with_title("KayakNav")
        .build(&event_loop)
        .unwrap();

    run(window, event_loop, config).await;
}
