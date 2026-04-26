use std::path::PathBuf;

use bpaf::Parser;
use kayaknav::Config;
use kayaknav::launch;
use kayaknav::setup;

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

    // `display_fallback` would show the default in the `--help` text, but
    // bpaf requires `Display` on the fallback value and `PathBuf` only
    // implements `Debug`. Silent fallback is fine here — the help text
    // spells the default out.
    let tile_cache_dir = bpaf::long("tile-cache-dir")
        .help("Directory to use for the raster tile cache. Defaults to a CWD-relative `.tile_cache`; pass an absolute path if launching from an unusual directory.")
        .argument::<PathBuf>("PATH")
        .fallback(default_config.tile_cache_dir);

    bpaf::construct!(Config {
        use_api_proxy,
        api_proxy_url,
        tile_cache_dir,
    })
    .to_options()
    .run()
}

fn main() -> eframe::Result {
    env_logger::init();

    let config = parse_args();

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let bundle = rt
        .block_on(setup::build(config))
        .expect("failed to set up app");

    launch(bundle)
}
