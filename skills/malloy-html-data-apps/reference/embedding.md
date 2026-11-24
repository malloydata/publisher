<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Embedding an HTML Data App

> `Publisher.embed(selector, { src })` drops a package page into a host page as a sandboxed, auto-resizing iframe. Same-origin embeds authenticate with the browser's cookies; cross-origin embeds need a signed token.

> **A cross-origin embed also needs the server to permit it.** Publisher sends `Content-Security-Policy: frame-ancestors 'self'` by default, so a page embedded from another origin is refused by the browser and renders blank -- with nothing logged server-side, which makes it look like a broken page rather than a policy. The deployment must set `PUBLISHER_FRAME_ANCESTORS` to the host page's origin (space-separated for several, or `*` to allow any). Check this first when an embed is blank: the browser console names `frame-ancestors`.

## The host-page pattern

```html
<script src="https://your-publisher/sdk/publisher.js"></script>
<div id="dashboard"></div>
<script>
  const handle = Publisher.embed("#dashboard", {
    src: "https://your-publisher/environments/demo/packages/sales/index.html",
  });
  // handle.destroy() removes the iframe and detaches its listeners.
</script>
```

`embed(selector, options)` returns `{ iframe, destroy(), send(type, payload), on(type, fn) }`. Options: `src` (required, with an http(s) origin), `getToken` (a function returning the bearer token, or a promise of it, for cross-origin auth), `token` (deprecated: a signed token appended as `embed_token`), `height` (omit to auto-size; a number is treated as pixels), and `allow` (the iframe permissions policy).

`send(type, payload)` is a request to the page that resolves with its reply; `on(type, fn)` listens for the page's events (`query.ran`, `givens.changed`, `version.unavailable`, ...) and returns an unsubscribe. Both work only once the page has answered the host's handshake; `send` rejects after 5 seconds without a reply. The page must have registered a handler for what you send (`handle.send("givens.set", { givens })` needs the page to call `Publisher.onGivensChange`; see `reference/runtime.md`), or the reply is an `unhandled` error. The server enables the channel only for hosts named in `PUBLISHER_FRAME_ANCESTORS` as an exact origin or a leading-label wildcard (`https://*.example.com`); with `*` there is none, and the embed still sizes itself.

## Sizing and the resize contract

Omit `height` and the frame auto-sizes. The embedded page measures its real content height and posts a `publisher:resize` message to the host, which resizes the iframe and accepts that message only from the iframe it created. You write none of this; it ships in `/sdk/publisher.js`, so the embedded page only has to load that script.

Do not rely on `body { min-height: 100vh }` to drive the frame height. The runtime deliberately measures the content's bottom edge, not the viewport, to avoid a grow-forever loop.

## Inside the frame there is no viewport

The host scrolls; the frame does not. Inside an auto-sized frame `window.innerHeight` is the whole document, `window.scrollY` stays 0, and `position: fixed` means the top of the document rather than the top of the reader's screen. Two consequences, both invisible when the page is opened directly:

- **An auto-sized frame must never become scrollable.** Anything whose box reaches past the content's bottom edge (a sentinel, an absolutely positioned helper, an overlay) is scrollable overflow: the document grows past the height the host sized, and the reader's first wheel turns scroll the app inside its frame before the host moves. Keep every added element within the content, and check it with the harness rather than hiding overflow: a fixed-height frame (`height` above, or `publisher:fit`) needs the app to scroll inside it.
- **Anything that floats over the page needs placing.** A drawer, a compare tray, a toast: built with `position: fixed` it opens at the top of the document, far above the reader, and focusing it scrolls the host page up to meet it. `reference/overlays.md` is the recipe: track the slice of the page the reader can see, place one clipped layer over it, contain its scroll, and focus without scrolling.

Test embedded, not just standalone: the Publisher console renders a package's app at `/<env>/<package>/data-apps/<path>`, which exercises the same resize contract a host does (`reference/verification-harness.md`).

## Auth

- Same-origin or same-tenant: pass no token. The browser's cookies authenticate the iframe.
- Cross-origin: mint a short-lived signed token server-side and return it from `options.getToken`. The runtime sends it to the page over the host channel once the page is ready and returns it when a query gets a 401 (the page asks with a request, so a host with no `getToken` answers `unhandled` and the query fails at once), and attaches it to the page's queries; the embedded page does nothing. Keep it short-lived and scoped to that one embed, and never put a long-lived or admin token in client HTML.
- The older `options.token` appends the token to the iframe URL as `embed_token`, and the page must read it (from `location.search`) and call `Publisher.setToken(token)`. It is deprecated: a URL token lands in browser history, Referer headers, and server logs.

## Guardrails (v1)

- The iframe is sandboxed (`allow-scripts allow-same-origin allow-forms`). Design for that: no top-level navigation, no popups.
- Every embed adds a random `#publisher-nonce=` fragment to `src`. The page reads it once, removes it from its URL, and echoes it in every message; the host rejects any message without it. Do not strip or reuse it, and reload the frame by setting its `src` again (a reload the page did not start loses the nonce).
- The page's `query.ran` event hands the host the text and givens of every query the page runs, so embed a page only in a host you would show those to.
- Embedded author JavaScript runs with the viewing user's data authority, so treat everything under `public/` as strictly first-party code: do not load untrusted third-party scripts, and do not move query results off to other hosts. Tighter per-embed isolation is planned.
