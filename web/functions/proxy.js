const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,HEAD,POST,OPTIONS",
  "Access-Control-Max-Age": "86400",
};

// The rest of this snippet for the demo page
function rawHtmlResponse(html) {
  return new Response(html, {
    headers: {
      "content-type": "text/html;charset=UTF-8",
    },
  });
}

async function handleRequest(context) {
  let request = context.request;
  const url = new URL(request.url);
  let apiUrl = url.searchParams.get("apiurl");

  const cache = caches.default;

  request = new Request(apiUrl, request);
  //add the correct Origin header to make the API server think that this request
  // is not cross-site.
  request.headers.set("Origin", new URL(apiUrl).origin);

  let response = await cache.match(request);
  let cache_hit = false;

  if (!response) {
    console.log(
      `Response for request url: ${request.url} not present in cache. Fetching and caching request.`
    );

    response = await fetch(request);
    response = new Response(response.body, response);
    // response.headers.set("Access-Control-Allow-Origin", url.origin);
    response.headers.set("Access-Control-Allow-Origin", '*');
    // Append to/Add Vary header so browser will cache response correctly
    response.headers.append("Vary", "Origin");
  } else {
    console.log(`Cache hit for: ${request.url}.`);
    cache_hit = true;
    response = new Response(response.body, response);
  }

  if (!response.ok) {
    console.log(`Response was not ok, not caching: ${response}`);
    context.waitUntil(cache.delete(request));
    return response;
  }

  // // TODO: consider letting the client check this and tell the proxy to clear
  // // the cache.
  // try {
  //   const resp_json = await response.clone().json();
  //   if (Object.hasOwn(resp_json, 'error')) {
  //     console.log(`Response contained an api error, not caching: ${resp_json}`);
  //     context.waitUntil(cache.delete(request));
  //     return response;
  //   }
  // } catch(err) {
  //   // response wasn't json
  // }

  response.headers.append("Cache-Control", `s-maxage=${30 * 24 * 3600}`);
  if (!cache_hit) {
    context.waitUntil(cache.put(request, response.clone()));
  }
  return response;
}

async function handleOptions(request) {
  if (
    request.headers.get("Origin") !== null &&
    request.headers.get("Access-Control-Request-Method") !== null &&
    request.headers.get("Access-Control-Request-Headers") !== null
  ) {
    // Handle CORS preflight requests.
    return new Response(null, {
      headers: {
        ...corsHeaders,
        "Access-Control-Allow-Headers": request.headers.get(
          "Access-Control-Request-Headers"
        ),
      },
    });
  } else {
    // Handle standard OPTIONS request.
    return new Response(null, {
      headers: {
        Allow: "GET, HEAD, POST, OPTIONS",
      },
    });
  }
}

export function onRequest(context) {
  let request = context.request;
  if (request.method === "OPTIONS") {
    // Handle CORS preflight requests
    return handleOptions(request);
  } else if (
    request.method === "GET" ||
      request.method === "HEAD" ||
      request.method === "POST"
  ) {
    // Handle requests to the API server
    return handleRequest(context);
  } else {
    return new Response(null, {
      status: 405,
      statusText: "Method Not Allowed",
    });
  }
}
