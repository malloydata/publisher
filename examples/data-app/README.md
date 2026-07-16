# data-app — the React SDK example

A Vite + React app built on the [`@malloy-publisher/sdk`](../../packages/sdk). It
shows how to embed live Malloy results from a Publisher package into your own
application UI. It reads from the `storefront` package in the `examples`
environment.

> This is the **advanced / internal** path. Most apps should use an in-package
> [HTML data app](../html-data-app) instead — no build step, no framework. Reach
> for the React SDK only when you need to compose Publisher results inside an
> existing React app. See [docs/embedded-data-apps.md](../../docs/embedded-data-apps.md).

## What it demonstrates

The left nav switches between four patterns, each a small, self-contained example:

| View | Component | Pattern |
| --- | --- | --- |
| **Storefront dashboard** | `StorefrontDashboard` | A fixed grid of SDK tiles (`business_overview`, top products, trend, …) over the storefront model. |
| **Single Embed** | `SingleEmbedDashboard` | One `EmbeddedQueryResult` rendering the monthly sales line chart from a serialized query. |
| **Dynamic Dashboard** | `DynamicDashboard` | An editable grid — add and arrange widgets at runtime. |
| **Interactive** | `InteractiveDashboard` | Hand-rolled `fetch` to the query API, rendered with Recharts, to show the raw data path. |

## Run it

You need a Publisher server running with the `storefront` package loaded — that's
the default when you start Publisher (`bun run start` from the repo root), which
serves the `examples` environment on `http://localhost:4000`.

Then, from this directory:

```bash
bun install          # or: npm install (run once at the repo root)
bun run dev          # Vite dev server on http://localhost:5173
```

Vite proxies `/api/v0` to `http://localhost:4000` (see
[`vite.config.ts`](vite.config.ts)), so the SDK's queries reach your local
Publisher. Open <http://localhost:5173>.

## How embedding works

The SDK addresses Publisher resources with a `publisher://environments/<env>/packages/<pkg>/...`
URI. For example, the storefront model is
`publisher://environments/examples/packages/storefront/models/storefront.malloy`.
Components take a `resourceUri` (and a query or a named view) and render the
result; `EmbeddedQueryResult` accepts a serialized query string so a dashboard
can persist its widgets. See the SDK components in
[`packages/sdk/src/components`](../../packages/sdk/src/components).

## Learn more

- [docs/embedded-data-apps.md](../../docs/embedded-data-apps.md) — the React SDK guide.
- [docs/html-data-apps.md](../../docs/html-data-apps.md) — the simpler, no-build alternative.
- [examples/storefront](../storefront) — the model this app reads from.
