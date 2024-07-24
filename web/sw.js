const staticCacheName = "kayaknav-v%DATE%";
const dynamicCacheName = "kayaknav-dynamic";

const contentToCache = new Set([
  "/",
  "/apple-touch-icon.png",
  "/favicon.ico",
  "/index.html",
  "/kayaknav.svg",
  "/kayaknav-192.png",
  "/kayaknav-512.png",
  "/pkg/kayaknav.js",
  "/pkg/kayaknav_bg.wasm",
]);

const pathsToIgnore = new Set([
  "/sw.js",
  "/manifest.webmanifest",
]);

const urlsToIgnore = new Set([
  "https://static.cloudflareinsights.com/beacon.min.js",
]);

self.addEventListener("install", (e) => {
  console.log("[Service Worker] Install");
  e.waitUntil(
    (async () => {
      const cache = await caches.open(staticCacheName);
      console.log("[Service Worker] Caching all content.");
      await cache.addAll(contentToCache);
    })(),
  );
});

addEventListener("message", messageEvent => {
  if (messageEvent.data === "skipWaiting") {
    return skipWaiting();
  }
});

self.addEventListener("fetch", (e) => {
  e.respondWith(
    (async () => {
      if (e.request.mode === "navigate" &&
          e.request.method === "GET" &&
          registration.waiting) {
        if ((await clients.matchAll()).length < 2) {
          registration.waiting.postMessage("skipWaiting");
          return new Response("", {headers: {"Refresh": "0"}});
        } else {
          console.log("[Service Worker] More than one client open, not refreshing service worker.");
        }
      }

      const request = e.request;

      let response = await caches.match(request);
      let cache_hit = false;

      if (!response) {
        console.log(
          `[Service Worker] Response for request url: ${request.url} not present in cache. Fetching and caching request.`
        );

        response = await fetch(request);
        response = new Response(response.body, response);
      } else {
        console.log(`[Service Worker] Cache hit for: ${request.url}.`);
        cache_hit = true;
      }

      if (!response.ok) {
        console.log(`[Service Worker] Response was not ok, not caching: ${response}`);
        await caches.delete(request);
        return response;
      }

      // try {
      //   const resp_json = await response.clone().json();
      //   if (Object.hasOwn(resp_json, "error")) {
      //     console.log(`[Service Worker] Response contained an api error, not caching: ${resp_json}`);
      //     await caches.delete(request);
      //     return response;
      //   }
      // } catch(err) {
      //   // response wasn't json
      // }

      if (!cache_hit
          && !(urlsToIgnore.has(request.url)
               || (// request.mode === "same-origin"
                   // &&
                   (pathsToIgnore.has(request.url.path)
                       || contentToCache.has(request.url.path))))) {
        let cacheName;
        if (contentToCache.has(request.url.path)) {
          cacheName = staticCacheName;
        } else {
          cacheName = dynamicCacheName;
          response.headers.append("x-sw-cache-timestamp", Date.now())
        }

        const cache = await caches.open(cacheName);
        console.log(`[Service Worker] Caching new resource: ${request.url}`);
        cache.put(request, response.clone());
      }

      return response;
    })(),
  );
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    (async () => {
      console.log("[Service Worker] Activate");
      let dynamic_cache = await caches.open(dynamicCacheName);
      let dynamic_cache_keys = await dynamic_cache.keys();
      // console.log(dynamic_cache_keys);
      dynamic_cache_keys.forEach(async (request, index, array) => {
        let resp = await dynamic_cache.match(request);
        // console.log(resp.headers.entries().toArray())
        // console.log(Date.now() - resp.headers.get("x-sw-cache-timestamp"));
        if (Date.now() - resp.headers.get("x-sw-cache-timestamp") > 30 * 24 * 3600) {
          await dynamic_cache.delete(request);
        }
      });

      let cache_names = await caches.keys();
      cache_names.forEach(async (key) => {
        if (key === staticCacheName || key === dynamicCacheName) {

        } else {
          await caches.delete(key);
        }
      });
    })(),
  );
});
