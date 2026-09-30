# elagoht/indexnow

A collage plugin that tells search engines which URLs changed, through the
[IndexNow](https://www.indexnow.org) protocol — Bing, Yandex, Seznam, Naver and the
others that share what one of them is told.

```go
app, err := collage.New(&collage.Config{
	Plugins: []collage.Plugin{indexnow.New(indexnow.Options{
		Key:     "4f1c2e9a7b3d4c8e",
		BaseURL: "https://example.com",
	})},
})
```

Requires collage v0.23.0 or later.

## What is sent, and when

What changed is what the cache let go of. When a tag is invalidated, collage names
the paths of the cached pages and documents it dropped — the URLs whose content is
about to be different — and the plugin tells IndexNow:

```go
app.InvalidateTags(ctx, "post:hello") // /blog/hello and / were cached with it
```

```json
{
  "host": "example.com",
  "key": "4f1c2e9a7b3d4c8e",
  "keyLocation": "https://example.com/4f1c2e9a7b3d4c8e.txt",
  "urlList": ["https://example.com/", "https://example.com/blog/hello"]
}
```

- **Batched.** URLs are collected for `window` — ten seconds — after the first one,
  then sent together, so publishing a post that invalidates ten pages, or a burst
  of edits, is one request. The window is not pushed back by the URLs that follow,
  so a site that invalidates constantly still sends. A request carries at most
  10,000 URLs, IndexNow's limit, or `batchSize`.
- **In the background.** Sending happens on a goroutine of the plugin's own. The
  code that invalidated — a webhook, an action, a command — returns at once and
  never waits on a search engine.
- **Retried.** A `429` or `5xx`, or an endpoint that cannot be reached, is tried
  again `retries` times, two seconds later and twice as long each time after, or
  when `Retry-After` says. A `400`, `403` or `422` — a malformed request, a key the
  endpoint could not verify, URLs not on the host — is logged and dropped: trying
  again would not change it.
- **Flushed on shutdown.** What is still waiting when the application stops is
  sent before it does, for as long as the shutdown's deadline allows.

## The key

IndexNow proves the site is yours by a key served from the site itself. The plugin
serves it at `/<key>.txt`, and a static build writes it there too. Any 8 to 128
letters, digits and dashes will do — `openssl rand -hex 16` makes one — but keep it
the same across deployments: an endpoint that has seen one key for a host expects
it again.

## Development

A development server sends nothing: what changes on a laptop is not news to a
search engine. `inDevelopment` sends anyway, for trying the plugin against a test
endpoint.

## Configuration

```json
{
  "elagoht/indexnow": {
    "key": "4f1c2e9a7b3d4c8e",
    "baseURL": "https://example.com",
    "endpoint": "https://api.indexnow.org/indexnow",
    "window": "10s",
    "batchSize": 10000,
    "retries": 3,
    "exclude": ["/api/", "/search"],
    "inDevelopment": false
  }
}
```

`key` is required; `baseURL` is an origin with no path, and may be left empty to take
the application's `Config.BaseURL` (collage v0.39.0). The application does not start
when a required value is missing. `endpoint` may be a search engine's own —
`https://www.bing.com/indexnow` — which shares with the rest the same way.
`exclude` lists path prefixes never sent. A negative `retries` tries once. In Go,
`Window` and `Backoff` are `time.Duration`s and `Client` the `*http.Client` that
sends.

## Limitations

- Only what the cache dropped is sent. A page that was not cached when its tag was
  invalidated — never requested since the last restart, or dynamic — is not in the
  invalidation, and a brand-new page is in none. A dynamic page's changes are
  invisible to the plugin.
- A static site has no server to invalidate anything, so a static build sends
  nothing; tell IndexNow from your deploy step instead.
- What is waiting is held in memory: a process killed without a shutdown loses it,
  and each instance of a site running on several sends its own invalidations.
- A document's path is sent like a page's, a feed or a sitemap included; list the
  ones a search engine has no use for in `exclude`.
