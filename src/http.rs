#[cfg(not(target_arch = "wasm32"))]
use http_cache_reqwest::CACacheManager;
#[cfg(not(target_arch = "wasm32"))]
use http_cache_reqwest::Cache;
#[cfg(not(target_arch = "wasm32"))]
use http_cache_reqwest::CacheMode;
#[cfg(not(target_arch = "wasm32"))]
use http_cache_reqwest::HttpCache;
#[cfg(not(target_arch = "wasm32"))]
use http_cache_reqwest::HttpCacheOptions;
use once_cell::sync::Lazy;
use reqwest::Client;
use reqwest::Response;
#[cfg(not(target_arch = "wasm32"))]
use reqwest_middleware::ClientBuilder;
#[cfg(not(target_arch = "wasm32"))]
use reqwest_middleware::ClientWithMiddleware;
#[cfg(not(target_arch = "wasm32"))]
use reqwest_retry::policies::ExponentialBackoff;
#[cfg(not(target_arch = "wasm32"))]
use reqwest_retry::RetryTransientMiddleware;
use serde_json::Value;

use crate::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
pub static CLIENT: Lazy<ClientWithMiddleware> = Lazy::new(|| {
    ClientBuilder::new(Client::new())
        .with(RetryTransientMiddleware::new_with_policy(
            ExponentialBackoff::builder().build_with_max_retries(3),
        ))
        .with(Cache(HttpCache {
            mode: CacheMode::IgnoreRules,
            manager: CACacheManager {
                path: "/tmp/kayaknav_cache".into(),
            },
            options: HttpCacheOptions::default(),
        }))
        .build()
});

#[cfg(target_arch = "wasm32")]
pub static CLIENT: Lazy<Client> = Lazy::new(Client::new);

pub async fn error_for_status(resp: Response) -> Result<Response> {
    let status = resp.status();
    let url = resp.url().clone();
    if status.is_client_error() || status.is_server_error() {
        Err(anyhow!("{}, {}, {}", status, url, resp.text().await.log()?))
    } else {
        Ok(resp)
    }
}

pub async fn fetch_json(url: &str) -> Result<Value> {
    info!("Fetching url {url:?}");

    let bytes = error_for_status(CLIENT.get(url).send().await.log()?)
        .await
        .log()?
        .bytes()
        .await
        .log()?;

    // TODO: add retires (e.g., 504 Gateway timeout)
    // let resp = { || async { http::CLIENT.get(url.clone()).send().await } }
    //     .retry(&ExponentialBuilder::default())
    //     .await
    //     .log()?;
    // // .retry(&ExponentialBuilder::default())

    debug!("Got response from {url:?}: {bytes:?}");

    let json: Value = serde_json::from_slice(&bytes)
        .map_err(|err| anyhow!("Error decoding response from {url:?}: {err:?}, {bytes:?}"))
        .log()?;

    if json.get("error").is_some() {
        Err(anyhow!(
            "Response from {url:?} contained an error: {json:?}"
        ))
        .log()?
    }

    Ok(json)
}

#[derive(Debug, Clone)]
pub struct ApiProxy {
    pub url: String,
}

impl ApiProxy {
    pub fn proxied_url(&self, url: &str) -> String {
        self.url.clone() + "?apiurl=" + &*urlencoding::encode(url)
    }
}
