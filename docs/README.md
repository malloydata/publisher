# Publisher documentation

> Start at the [project README](../README.md) for the 60-second quick start. This folder holds the
> deeper reference. If you're an AI agent, read [AGENTS.md](../AGENTS.md) first — it's the canonical
> guide to running Publisher and connecting over MCP.

## Examples

Three runnable packages ship in the default `examples` environment, plus one standalone React app —
every doc below points back to one of them, and each example's README points back to the docs.

| Example                                              | What it shows                                                                                                                |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| [storefront](../examples/storefront)                 | A complete ecommerce model — joins, measures, a notebook, four dashboards, and a no-build HTML data app. The flagship first-open package. |
| [governed-analytics](../examples/governed-analytics) | Givens, `#(authorize)`, row-level access, and discovery curation in one small package.                                                    |
| [data-app](../examples/data-app)                     | A standalone React data app built on the SDK, reading from `storefront`. Not a served package — run it with Vite.            |

## Concepts

| Doc                                | Read it when you want to…                                                                           |
| ---------------------------------- | --------------------------------------------------------------------------------------------------- |
| [architecture.md](architecture.md) | Understand how Malloy, Render, Publisher, and the SDK fit together.                                 |
| [api-overview.md](api-overview.md) | Understand the REST + MCP surfaces and the resource hierarchy.                                      |
| [packages.md](packages.md)         | Understand the package format: `publisher.json`, models, data files, and how a package gets served. |

## Use it

| Doc                                            | Read it when you want to…                                                                         |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| [console.md](console.md)                       | Navigate the Publisher Console — the built-in web UI — and see how the constructs surface.        |
| [explorer.md](explorer.md)                     | Build queries with the no-code visual query builder.                                              |
| [ai-agents.md](ai-agents.md)                   | Connect an AI agent, over MCP or (unattended) over REST, and ground it in your models.            |
| [choosing-a-surface.md](choosing-a-surface.md) | Decide between a notebook, a dashboard, and an HTML data app — pros, cons, and decision rules.    |
| [dashboards.md](dashboards.md)                 | Write a `dashboards/*.malloy` — filter controls, grid layout, composites, and `# drill`.          |
| [html-data-apps.md](html-data-apps.md)         | Ship a no-build HTML dashboard **inside a package**, hosted by Publisher.                         |
| [react-data-apps.md](react-data-apps.md)       | Build a React data app with the SDK — Malloy's renderer, notebooks, and dashboards as components. |

## Model & govern

**Runtime parameters and access control all build on one mechanism — [givens](givens.md).** Start
there for the primitive, then follow the application you need.

| Doc                                                | Read it when you want to…                                                                              |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| [givens.md](givens.md)                             | Learn the base mechanism — declare runtime parameters, drive filter widgets, and reach access control. |
| [row-level-access.md](row-level-access.md)         | Restrict _which rows_ a caller sees (given-scoped `where:` + `#(authorize)`).                          |
| [authorize.md](authorize.md)                       | Gate _who_ can query a whole source with `#(authorize)`.                                               |
| [discovery-and-access.md](discovery-and-access.md) | Control _what_ is discoverable and queryable (`explores` / `queryableSources`) — the visibility axis.  |
| [security-posture.md](security-posture.md)         | Understand what Publisher does and does not defend against, before deploying it or adding a feature.   |

## Deploy & operate

| Doc                                                        | Read it when you want to…                                                                                                                           |
| ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| [deployment.md](deployment.md)                             | Run a built server via npx, Docker, or Docker Compose.                                                                                              |
| [connections.md](connections.md)                           | Connect BigQuery, Snowflake, Postgres, DuckDB, and more.                                                                                            |
| [materialization.md](materialization.md)                   | Persist Malloy sources into tables — the publish-gate rules, on-demand + scheduled builds, the `malloy-pub` CLI, and standalone-vs-hosted behavior. |
| [ducklake.md](ducklake.md)                                 | Attach a DuckLake catalog (read-only), understand catalog-format compatibility, and run offline / air-gapped.                                       |
| [persist-storage-tutorial.md](persist-storage-tutorial.md) | Materialize a `#@ persist` source into a DuckDB/DuckLake store and serve queries from it (the `storage=` tier + the `PERSIST_STORAGE_MODE` switch). |
| [theming.md](theming.md)                                   | Customize colors, fonts, and light/dark mode.                                                                                                       |
| [configuration.md](configuration.md)                       | Look up an env var / CLI flag, or tune the OOM guards.                                                                                              |

## Develop & contribute

| Doc                                                            | Read it when you want to…                                                               |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| [development.md](development.md)                               | Build and hack on Publisher from a clone.                                               |
| [agent-skills/](agent-skills/)                                 | Author or contribute the bundled agent skills.                                          |
| [malloyyo-dashboards-design.md](malloyyo-dashboards-design.md) | _Design doc:_ the grammar and architecture behind native `dashboards/*.malloy` support. |

## Full public docs

The complete user guide lives at
**[docs.malloydata.dev/documentation/user_guides/publishing](https://docs.malloydata.dev/documentation/user_guides/publishing/publishing)**.
