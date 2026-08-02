# Malloyyo vs. Publisher

A comparison of [Publisher](https://github.com/malloydata/publisher) (this repo) and
[Malloyyo](https://github.com/malloydata/malloyyo), with recommendations on which Malloyyo
features Publisher should adopt and which would let teams switch from Malloyyo to Publisher
without a rewrite.

*Written July 2026. Sources: this repo's* `docs/` *and* `AGENTS.md`*; Malloyyo's* `README.md`*,*
`docs/` *(*`explore-surface.md`*,* `creating-dashboards.md`*,* `authentication.md`*), and*
`packages/cli/README.md`*.*

## Positioning

The two projects barely overlap in their strengths.

**Publisher** is the full, self-hosted semantic model server: multiple environments and
packages, REST + MCP + a web app, ten connection types, the Explorer visual query builder,
notebooks, no-build HTML data apps, materialization with scheduling, and authorize/givens
governance. It is deliberately unauthenticated (network isolation or a gateway provides
security) and has no model lifecycle beyond "reload a directory." Strong in **breadth and
depth of serving**.

**Malloyyo** is thin by design: one Next.js app serving published model versions to AI over
MCP, with a web UI for humans. One dataset = one published model. It nails exactly the
things Publisher lacks: real auth (OAuth 2.1 for claude.ai and the CLI, Google/Okta/Entra
sign-in for humans), a compile-gated versioned publish workflow with git provenance,
dashboards as lint-checked `.malloy` files, and a query log where every AI-run query is
browsable, shareable, and re-runnable. Strong in **lifecycle, auth, and the consumer loop**.

## Feature comparison


| Area                        | Publisher                                                                                               | Malloyyo                                                                                                                                                |
| --------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Positioning**             | Full semantic model server: multi-environment, multi-package, REST + MCP + web app, self-hosted         | Thin hosted layer: one Next.js app serving published models to AI over MCP, web UI for humans                                                           |
| **API surfaces**            | REST API on `:4000` (full OpenAPI spec) plus MCP on `:4040` (5 tools) and an agent MCP on `:4041`       | MCP only (`list_sources`, `describe_source`, `query`, `open_share_link`, `yo_help`); no general REST API                                                |
| **MCP discovery**           | `malloy_getContext`: plain-English BM25 ranking over model text, progressive env → package → source     | One "fat" `describe_source`: typed columns, measures, views, reachable joins with fan-out flagged                                                       |
| **Query tools**             | `malloy_executeQuery` plus a separate `malloy_compile` for diagnostics                                  | `query` with `execute:false` for compile + SQL preview in one tool; byte-budgeted rows; every run mints a share link                                    |
| **Agent guidance**          | ~29 bundled skills, exposed as MCP prompts; `malloy_searchDocs` for language docs                       | `yo_help` topic tool; every error carries a `help_topic` pointer; one load-bearing how-to doc                                                           |
| **User auth**               | None — bind to localhost or put a gateway in front; identity-bound givens documented as planned         | NextAuth v5: Google, Okta, Microsoft Entra ID; email allow list; admin roles                                                                            |
| **Agent / MCP auth**        | Unauthenticated MCP endpoint                                                                            | Full OAuth 2.1 provider — claude.ai remote MCP and CLI login (Authorization Code + PKCE)                                                                |
| **Model lifecycle**         | Packages loaded from local / GitHub / GCS / S3; reload endpoint; watch mode with live reload            | Compile-gated versioned publish via CLI (git provenance), or GitHub pull with webhook auto-refresh                                                      |
| **Model versioning**        | None in OSS — packages are directories on disk (`scope: version` exists for control-plane use)          | Every publish stores a new version in Postgres with the git commit; a failed compile rejects the push                                                   |
| **Governance**              | `#(authorize)` gates, givens-based row-level access, discovery curation, query/byte/concurrency caps    | Restricted compile mode (no imports, no raw table/SQL, no new givens) — the model is the hard boundary                                                  |
| **Databases**               | 10 connection types incl. DuckLake, publisher proxy, SSH bastion, MotherDuck, Databricks, Trino         | DuckDB built in (Parquet over HTTP, no warehouse); attach BigQuery, Snowflake, MotherDuck, Databricks, MySQL, Postgres, Trino/Presto                    |
| **Connection config**       | `publisher.config.json` (env-level) + per-package built-in DuckDB                                       | Standard `malloy-config.json` at model root, secrets via `{ "env": "VAR" }` indirection                                                                 |
| **Dashboards**              | `.malloynb` notebooks, `# dashboard` renderer, no-build HTML data apps in `public/`, theming, embed SDK | `dashboards/*.malloy` files: `# artifact` + grid layout, auto-rendered given controls, `# drill` navigation, sandboxed JSX components, a `lint` command |
| **Query history & sharing** | None                                                                                                    | Every query logged; browse / edit / favorite / re-run / share in the browser (ltool); "Explore further with Claude" handoff                             |
| **Human UI**                | Publisher Console: Explorer visual query builder, notebooks, README browsing, theme editor              | Datasets list, ltool query browser, dashboard pages                                                                                                     |
| **Materialization**         | Malloy Persistence: `#@ persist`, cron scheduler, CLI (`malloy-pub materialize` / `schedule`)           | None                                                                                                                                                    |
| **Embedding & SDKs**        | HTML data-app runtime (`Publisher.query` / `embed`), embed tokens, React SDK, generated Python client   | Dashboard iframe embedding                                                                                                                              |
| **Deployment**              | npx, Docker, Bun monorepo; state on disk, no external DB required                                       | One-click Vercel or Docker; requires a Postgres (Neon) metadata database                                                                                |
| **Authoring loop**          | `malloy_compile` + `malloy_reloadPackage` + watch mode; skills guide the agent                          | `malloyyo init` wires a repo into Claude author mode; `malloyyo test` is a faithful dress rehearsal of the hosted surface                               |




## Malloyyo features worth adopting in Publisher

Ranked by impact. Items marked **(migration enabler)** also make a future Malloyyo →
Publisher switch easy — the two lists overlap heavily, so building the top items serves
both goals.

### 1. Query log + share links (ltool) — high priority (migration enabler)

Malloyyo logs every query and makes each one addressable: browse, edit, favorite, re-run,
share, and hand off to "Explore further with Claude." Publisher has no query history at
all. This turns ephemeral agent answers into auditable, reusable artifacts — and since a
Malloy dashboard is a single query, a saved query doubles as a saved report. It complements
notebooks rather than competing with them.

### 2. OAuth 2.1 on MCP + OIDC sign-in — high priority (migration enabler)

Malloyyo ships NextAuth (Google, Okta, Entra ID), email allow lists, admin roles, and a
full OAuth 2.1 provider so claude.ai's remote MCP and the CLI both authenticate. This is
Publisher's biggest documented gap — [authorize.md](authorize.md) calls givens
caller-asserted and identity-bound givens "planned." Real identity is exactly what makes
givens-based row-level access trustworthy, and it's what lets claude.ai connect to a
non-localhost Publisher at all.

### 3. Compile-gated, versioned publish with git provenance — high priority (migration enabler)

`malloyyo publish` bundles the model, records the git commit, and the server compiles
before accepting — a bad push is rejected and the live model is untouched; the CLI exits
non-zero so CI can gate on it. `malloyyo status` shows what's live. Publisher's reload is
transactional too, but there's no push workflow, no stored versions, and no provenance. A
`malloy-pub publish` / `status` pair would give packages a real deployment story.

### 4. Restricted compile mode on the serving surface — high priority

Malloyyo compiles explore queries in a restricted mode: no imports, no raw table or SQL
access, no new givens — and surfaces the restriction reactively, only when a query trips
it. Publisher's own [authorize.md](authorize.md) lists the `/compile` raw-SQL path as a
known schema-oracle gap; this is the fix, proven in the field.

### 5. GitHub webhook refresh — medium priority (migration enabler)

Point Malloyyo at a GitHub repo and a webhook endpoint refreshes the model on every push.
Publisher can load packages from GitHub but only re-fetches on `--init`. A webhook (or
poll) that triggers the existing reload path closes the continuous-deployment loop for
git-hosted packages.

### 6. Reactive help: error → `help_topic` — medium priority

Malloyyo's load-bearing design insight (see its `explore-surface.md`): MCP `instructions`,
prompts, and resources are unreliable channels — only tool descriptions and tool results
are guaranteed to be seen. So errors carry a `help_topic` and a `yo_help` tool serves the
guidance exactly when needed. Publisher's skills depend on the host loading them; adding a
help tool plus help pointers on compile/query errors would make guidance reach every
client.

### 7. One fat, typed describe — medium priority (migration enabler)

`describe_source` returns a locally-complete map of one source — typed columns, measures,
views, reachable joins with fan-out flagged — on the stance that one fat describe beats
many drill-down round-trips for an agent. It complements `malloy_getContext`'s search
ranking: search to find the source, describe to ground the query.

### 8. `# drill` navigation + a lint verb — medium priority (migration enabler)

Malloyyo dashboards get click-through navigation (`# drill` on a dimension opens another
dashboard seeded with the clicked value) and `malloyyo lint` verifies every drill target,
tile query, and custom component reference resolves — dashboards fail loudly at build
time, not at click time. Publisher notebooks have no cross-artifact navigation and no
package-wide lint; a `malloy-pub lint` that compiles every notebook/page reference would
catch rot before deploy.

### 9. Dress-rehearsal mode — low priority

`malloyyo test` launches Claude wired to exactly the curated production surface — a
faithful preview of what a claude.ai consumer will see, distinct from author mode.
Publisher's analog: a flag that serves only the curated discovery surface (`explores` +
declared sources) locally so authors can verify what agents get before shipping.

### 10. Byte-budgeted results — low priority

Publisher caps rows and response bytes for safety; Malloyyo budgets bytes specifically so
oversized results spill gracefully instead of flooding an LLM's context window. A small
refinement to the MCP result path.

## The migration path: Malloyyo → Publisher

What Publisher would add so a team that started on Malloyyo can switch without rewriting
their model repo, CI, dashboards, or agent workflows.


| Compatibility feature                       | Why it unlocks the switch                                                                                                                                                                                          |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Read** `malloy-config.json`               | Accept the standard Malloy connection file (with `{ "env": "VAR" }` secret indirection) as an alternative to `publisher.config.json` connections, so a Malloyyo model repo loads as a Publisher package unchanged. |
| **Honor the** `index.malloy` **convention** | Treat an `index.malloy`'s exports as the curated surface — mapping directly onto `explores` + `queryableSources: "declared"` — so a Malloyyo repo's curation carries over without a `publisher.json` rewrite.      |
| **MCP tool parity / aliases**               | Offer `list_sources` / `describe_source` / `query` (with `execute:false`) semantics alongside the `malloy_`* tools, so prompts, saved agent workflows, and habits built on Malloyyo work on day one.               |
| `malloy-pub publish`                        | Same login → publish → status UX (OAuth + PKCE, compile-gated push, git provenance), so a Malloyyo CI pipeline repoints to Publisher with a URL change.                                                            |
| **Render** `dashboards/*.malloy`            | Support `# artifact`, grid layout, auto given controls, and `# drill` natively — or ship a converter to `.malloynb` / HTML data apps. Dashboards are the artifact users would most fear losing in a switch.        |
| **Import saved queries**                    | Once Publisher has a query log, an importer for Malloyyo's Postgres metadata (`datasets`, `malloy_models`, saved/favorited queries) preserves users' accumulated artifacts.                                        |


**The shape of the migration story:** a Malloyyo deployment is a model repo
(`index.malloy` + `malloy-config.json` + `dashboards/`), a publish pipeline, an
OAuth-connected agent, and a pile of saved queries. Make each of those land unchanged on
Publisher — read the same repo layout, accept the same publish call, speak the same tool
names, render the same dashboards — and switching becomes a URL change instead of a
rewrite. Because both projects live in the malloydata org, converging on shared conventions
(`malloy-config.json`, `index.malloy`, the dashboard file format) is cheaper than building
converters later.

## What not to copy

Malloyyo's Postgres metadata dependency and Vercel-first deployment fit its hosted,
thin-by-design shape but would complicate Publisher's zero-dependency npx/Docker story. Its
MCP-only surface is a subset of what Publisher already serves — REST, embedding, and
materialization are Publisher advantages to keep, not gaps to regret. And Malloyyo's
single-model-per-dataset simplicity is a product stance, not a feature Publisher lacks.