---
name: malloy-html-data-app-embedding
description: Embed an in-package HTML data app into a host page or another application, including auto-sizing and auth. Read when embedding a Publisher page via Publisher.embed.
---

# Embedding an HTML Data App

> `Publisher.embed(selector, { src })` drops a package page into a host page as a sandboxed, auto-resizing iframe. Same-origin embeds authenticate with the browser's cookies; cross-origin embeds need a signed token.

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

## Auth

- Same-origin or same-tenant: pass no token. The browser's cookies authenticate the iframe.
- Cross-origin: mint a short-lived signed token server-side and pass it as `options.token`. The runtime appends it to the iframe URL as `embed_token`; the embedded page must read it (from `location.search`) and call `Publisher.setToken(token)`. Because it rides in the URL, it can land in browser history, Referer headers, and server logs, so keep it short-lived and scoped to that one embed, and never put a long-lived or admin token in client HTML.

## Guardrails (v1)

- The iframe is sandboxed (`allow-scripts allow-same-origin allow-forms`). Design for that: no top-level navigation, no popups.
- Embedded author JavaScript runs with the viewing user's data authority, so treat everything under `public/` as strictly first-party code: do not load untrusted third-party scripts, and do not move query results off to other hosts. Tighter per-embed isolation is planned.
