# React SDK (internal / advanced)

> **Heads up:** `@malloy-publisher/sdk` is the React component library the **Publisher App is built
> from** — it's an internal building block, not a supported path for external integration, and its
> component API can change between releases without notice. For putting analytics in front of users,
> prefer the two supported paths below. This page is kept for the curious and for advanced users
> who accept that tradeoff.

## Ways to surface analytics (start here)

| Path | Use it when | Doc |
| --- | --- | --- |
| **Publisher App** | You want zero-code exploration and sharing — the no-code Explorer, notebooks, and dashboards, out of the box. | [publisher-app.md](publisher-app.md) |
| **HTML data apps** | You want a custom dashboard with no build step, shipped inside a package and served by Publisher. | [html-data-apps.md](html-data-apps.md) |
| **REST / MCP APIs** | You're building your own application or agent against the data programmatically. | [api-overview.md](api-overview.md) · [ai-agents.md](ai-agents.md) |
| _React SDK (this page)_ | _You specifically need React components and accept the internal/unstable-API tradeoff._ | — |

## What the SDK is

A React component library (`ServerProvider`, `QueryResult`, `Notebook`, `Model`, page components, filter
widgets) that talks to Publisher's REST API and renders results with Malloy Render. The
[`Publisher App`](../packages/app) is composed entirely from it. Because it's the App's internal
toolkit, breaking changes ride along with App redesigns.

## Advanced reference: `examples/data-app`

[`examples/data-app`](../examples/data-app) is a Vite + React app that reads from the bundled
`storefront` package. It's kept as an advanced reference — each sidebar view maps to a component
showing one SDK pattern:

| Component | Pattern |
| --- | --- |
| [`SingleEmbedDashboard.tsx`](../examples/data-app/src/components/SingleEmbedDashboard.tsx) | Embed one saved analysis with `<EmbeddedQueryResult>`. |
| [`StorefrontDashboard.tsx`](../examples/data-app/src/components/StorefrontDashboard.tsx) | A fixed grid of `<EmbeddedQueryResult>` tiles. |
| [`DynamicDashboard.tsx`](../examples/data-app/src/components/DynamicDashboard.tsx) | Add/remove tiles at runtime. |
| [`InteractiveDashboard.tsx`](../examples/data-app/src/components/InteractiveDashboard.tsx) | Drive queries from React state via the `useRawQueryData` hook. |

Entry point: [`src/main.tsx`](../examples/data-app/src/main.tsx) wraps everything in one
`<ServerProvider>` and renders [`AppShell.tsx`](../examples/data-app/src/AppShell.tsx).
