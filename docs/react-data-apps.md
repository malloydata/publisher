# React data apps with the Publisher SDK

> What this is: how to build a **React data app** with `@malloy-publisher/sdk` — the component
> library that gives a React application Malloy's built-in renderer, notebooks, and dashboard
> types, the same ones the [Publisher Console](console.md) is built from.

## Ways to surface analytics

| Path | Use it when | Doc |
| --- | --- | --- |
| **Publisher Console** | You want zero-code exploration and sharing — the no-code Explorer, notebooks, and dashboards, out of the box. | [console.md](console.md) |
| **HTML data apps** | You want a custom page with no build step, shipped inside a package and served by Publisher. | [html-data-apps.md](html-data-apps.md) |
| **React data apps** (this page) | You're building a React app and want Malloy's renderer, notebooks, and dashboards as components, via the Publisher SDK. | — |
| **REST / MCP APIs** | You're building your own application or agent against the data programmatically. | [api-overview.md](api-overview.md) · [ai-agents.md](ai-agents.md) |

See [choosing-a-surface.md](choosing-a-surface.md) for how the in-package surfaces compare.

## What the SDK is

A React component library (`ServerProvider`, `QueryResult`, `Notebook`, `Model`, page components,
filter widgets) that talks to Publisher's REST API and renders results with Malloy Render. The
[Publisher Console](../packages/app) is composed entirely from it — so every component has a
production consumer, and anything the Console renders (query results, `# dashboard` grids,
notebooks, the givens Parameters panel) your app can render too.

## Reference app: `examples/data-app`

[`examples/data-app`](../examples/data-app) is a Vite + React app that reads from the bundled
`storefront` package. Each sidebar view maps to a component showing one SDK pattern:

| Component | Pattern |
| --- | --- |
| [`SingleEmbedDashboard.tsx`](../examples/data-app/src/components/SingleEmbedDashboard.tsx) | Embed one saved analysis with `<EmbeddedQueryResult>`. |
| [`StorefrontDashboard.tsx`](../examples/data-app/src/components/StorefrontDashboard.tsx) | A fixed grid of `<EmbeddedQueryResult>` tiles. |
| [`DynamicDashboard.tsx`](../examples/data-app/src/components/DynamicDashboard.tsx) | Add/remove tiles at runtime. |
| [`InteractiveDashboard.tsx`](../examples/data-app/src/components/InteractiveDashboard.tsx) | Drive queries from React state via the `useRawQueryData` hook. |

Entry point: [`src/main.tsx`](../examples/data-app/src/main.tsx) wraps everything in one
`<ServerProvider>` and renders [`AppShell.tsx`](../examples/data-app/src/AppShell.tsx).
