# KayakNav

A kayak (or other human-powered watercraft) navigation application.

Currently focused on New York City and surrounding areas.

Try it out at <https://kayaknav.com/>.


## Features

* Current predictions sourced from [NOAA](https://tidesandcurrents.noaa.gov/web_services_info.html) overlayed on a map using [OpenStreetMap](https://www.openstreetmap.org/) tiles.
* Place movement waypoints and pause (e.g., lunch) waypoints. Calculates:
    * Distance, duration, speed, and cumulative trip time for each trip leg (segment between waypoints).
    * Total trip time and duration.
    * Best times to begin the trip (20th percentile duration).


## Current Limitations

### New York City, 2 Month Time Horizon

Only New York City +-2° lat/lon is currently supported. The blocker for
expanding this range is coming up with a good way of fetching data from more
NOAA stations. An obvious idea is fetching data from stations in the currently
visible map area, but zooming out should not suddenly fetch data for the entire
world.

Fetching the data is an issue because the NOAA APIs are somewhat slow and return
504 Gateway Timeout errors when making too many ~concurrent calls. An idea to
mitigate this is fetching a year or two of data at build-time and bundling it
with the deployment, but first we need to figure out how to handle that much
data in the UI; for example, the time slider becomes quite unwieldy when it
represents that long of a time and trip departure time calculations take longer
to run because there are more possible departures to leave. We could probably
keep a couple years of data but limit the UI/calculations to the next couple
months, but that's not yet implemented.

This is also the reason why KayakNav currently displays data only for the
current month and the next month.

Adding retries for API failues is planned but is... annoying on the web.

### Weather

Currents at the surface can deviate significantly based on local (or
not-so-local) weather. For example, heavy rains can result in a large amount of
water that needs to drain out of a river system, resulting in a stronger ebb and
weaker flood, and wind's influence is obvious. KayakNav does not take weather
into account for the currents it displays or for trip calculations. It is
unclear if KayakNav will ever include this information due to the complexity
involved and the poor accuracy of weather forecasts.

### Mobile

Placing waypoints (and consequently doing trip planning) on mobile devices
doesn't currently work due to <https://github.com/Maximkaaa/galileo/issues/79>.
This will hopefully be resolved relatively soon. Viewing current predictions
works fine.

### Waypoint UI

The waypoint UI (and I'm sure other parts of the UI) is currently a bit awkward.
It is annoying to figure out which waypoint on the map corresponds to which
waypoint in the UI and it would be nice to show numbers for the waypoints on the
map (possibly blocked by <https://github.com/Maximkaaa/galileo/issues/48>).
Another possible approach may be to draw lines connecting waypoints with small
arrows indicating direction of motion. It would also be nice to be able to add
waypoints in the middle of a trip instead of only at the end.

There is currently no way of saving or copy/pasting trips (sequences of
waypoints) for reuse.


### Current UI

A tooltip for the current arrows that displays the name of the station and
direction/speed would be nice.


### Sunrise/Sunset

Displaying sunrise/sunset times and a filter for scheduling trips only during
daylight hours (in addition to the current 8am-9pm filter) is planned and
shouldn't be difficult.


## Usage

### Local

KayakNav can be built and run locally using `cargo`. It has only been tested on
Linux.

Map tiles are cached at .tile_cache/ in the CWD. This is not currently
customizable, making that customizable needs to be done in
[Galileo](https://github.com/Maximkaaa/galileo).

Current prediction data is cached at `/tmp/kayaknav_cache/`. This is also not
currently customizable and should be improved.


### Web

KayakNav can be compiled to WASM using `build_web.sh`. This requires `wasm-pack`
to be installed.

KayakNav can also be installed as a progressive web application.

In both cases, loading a new version may require multiple refreshes due to the
way service workers work.


#### Proxy

Due to the NOAA API issue mentioned above, and the fact that the 504 errors are
missing CORS headers (apparently a common bug), a Cloudflare Worker proxy is
[provided](web/functions/proxy.js)) that adds CORS headers and inserts NOAA API
responses into the Cloudflare Cache.

By default, a WASM build uses the kayaknav.com proxy by default. To disable the
proxy, set the `KAYAKNAV_USE_API_PROXY` environment variable to `false` when
running build_web.sh. To change the URL of the proxy to use, set the
`KAYAKNAV_API_PROXY_URL` environment variable. The proxy must accept the
url-encodd url to query as an `apiurl` query parameter.

A local KayakNav does not use the proxy by default. It can be enabled when
launching KayakNav with `--use-api-proxy=true` and the proxy URL can be
configured with `--api-proxy-url=URL`.
