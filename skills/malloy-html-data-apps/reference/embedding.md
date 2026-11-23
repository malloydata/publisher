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

`embed(selector, options)` returns `{ iframe, destroy() }`. Options: `src` (required), `token` (a signed token for cross-origin auth, appended as `embed_token`), `height` (omit to auto-size; a number is treated as pixels), and `allow` (the iframe permissions policy).

## Sizing and the resize contract

Omit `height` and the frame auto-sizes. The embedded page measures its real content height and posts a `publisher:resize` message to the host, which resizes the iframe and accepts that message only from the iframe it created. You write none of this; it ships in `/sdk/publisher.js`, so the embedded page only has to load that script.

Do not rely on `body { min-height: 100vh }` to drive the frame height. The runtime deliberately measures the content's bottom edge, not the viewport, to avoid a grow-forever loop.

## Inside the frame there is no viewport

The host scrolls; the frame does not. Inside an auto-sized frame `window.innerHeight` is the whole document, `window.scrollY` stays 0, and `position: fixed` means the top of the document rather than the top of the reader's screen. Two consequences, both invisible when the page is opened directly:

- **An auto-sized frame must never become scrollable.** Anything whose box reaches past the content's bottom edge (a sentinel, an absolutely positioned helper, an overlay) is scrollable overflow: the document grows past the height the host sized, and the reader's first wheel turns scroll the app inside its frame before the host moves. Keep every added element within the content, and check it with the harness rather than hiding overflow: a fixed-height frame (`height` above, or `publisher:fit`) needs the app to scroll inside it.
- **Anything that floats over the page needs placing.** A drawer, a compare tray, a toast: built with `position: fixed` it opens at the top of the document, far above the reader, and focusing it scrolls the host page up to meet it. `reference/overlays.md` is the recipe: track the slice of the page the reader can see, place one clipped layer over it, contain its scroll, and focus without scrolling.

A frame as tall as its content changes more than these two. Each is invisible standalone:

- **`100vh` is the whole frame.** Size nothing to the viewport; a block sized to `100vh` grows to the host's height cap.
- **Sticky never sticks**, because the frame never scrolls. A sticky header or scope bar is fine as long as nothing
  depends on it staying in view.
- **`scrollTo`, `scrollBy` and `scrollY` do nothing**, while `scrollIntoView` scrolls the host. To land a reader at
  the top of a new view, call `scrollIntoView({ block: 'start' })` on its header, as the frame template does.
- **`IntersectionObserver` ignores `rootMargin` in a cross-origin frame**, so a lazy tile starts loading as it
  enters the screen rather than before (`reference/lazy-load.md`).
- **The runtime measures the bottoms of `body`'s children**, so a margin on the last child sits outside the
  measurement and gives the frame an inner scrollbar. Put the space inside, as padding.
- **The host may cap the frame's height**, for example at 20,000 px. A view runs taller at phone width than at
  desktop, by half again or more, so keep each view under about 5,000 px at desktop width; split a long view into two
  views rather than growing one.
- **What the frame refuses fails silently.** Publisher's frame sandbox allows scripts, same-origin requests and
  forms, so a download (a CSV button), a new tab and an `alert` do nothing inside it, though each works standalone.
  A clipboard write and full screen work in the same-origin Console, and are refused in a frame on another origin
  unless the host delegates them. A host may allow more. Check the frame template's "Download CSV" and "Copy link"
  in the frame your app will live in.

Test embedded, not just standalone: the Publisher console renders a package's app at `/<env>/<package>/data-apps/<path>`, which exercises the same resize contract a host does (`reference/verification-harness.md`).

## Auth

- Same-origin or same-tenant: pass no token. The browser's cookies authenticate the iframe.
- Cross-origin: mint a short-lived signed token server-side and pass it as `options.token`. The runtime appends it to the iframe URL as `embed_token`; the embedded page must read it (from `location.search`) and call `Publisher.setToken(token)`. Because it rides in the URL, it can land in browser history, Referer headers, and server logs, so keep it short-lived and scoped to that one embed, and never put a long-lived or admin token in client HTML.

## Guardrails (v1)

- The iframe is sandboxed (`allow-scripts allow-same-origin allow-forms`). Design for that: no top-level navigation, no popups.
- Embedded author JavaScript runs with the viewing user's data authority, so treat everything under `public/` as strictly first-party code: do not load untrusted third-party scripts, and do not move query results off to other hosts. Tighter per-embed isolation is planned.
