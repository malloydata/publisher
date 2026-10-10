<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The package format

> What this is: what a Malloy Publisher package is on disk. The files, the `publisher.json`
> manifest fields, where a package's data comes from, and how a package gets served. For the
> server's own config file, see [configuration.md](configuration.md); for running a server, see
> [deployment.md](deployment.md).

## A package is a directory with two files

A package is a directory holding a `publisher.json` manifest and at least one `.malloy` model.
That is the whole format. Data files sit alongside the models:

```
sales/
  publisher.json        # the manifest ({} is valid)
  sales.malloy          # a Malloy model
  data/
    sales.csv           # data the model reads through the built-in duckdb connection
```

A working minimal pair:

```json
{ "name": "sales", "description": "Sales by region" }
```

```malloy
source: sales is duckdb.table('data/sales.csv') extend {
  measure: total_rows is count()

  view: by_region is {
    group_by: region
    aggregate: total_rows
  }
}
```

The manifest must exist and parse as JSON: a directory without a `publisher.json` is not a
package, and a manifest that fails to parse fails the package load. Beyond that, every field is
optional; `{}` is a valid manifest.

One filename is special. If the package root holds an `index.malloy`, that file is the package's
published surface: listings return it and whatever it `export`s, the other models become building
blocks, and a direct query against a source it does not export is refused. It is the
no-configuration way to curate a package, and it stays entirely optional. See
[discovery-and-access.md](discovery-and-access.md).

## The manifest: publisher.json

The fields the server reads:

| Field | Purpose |
| --- | --- |
| `name` | Conventionally the package's name, but the server never surfaces it: the registered name (config entry or API call) wins in API URLs and responses. |
| `description` | Shown in the Publisher UI and in API responses. |
| `explores` | **Deprecated, still honored.** Names the files whose exports are the surface; those files are listed and queryable. A tagged dashboard listed here reads the surface and adds nothing to it. Every use gets a load warning: an `index.malloy` that imports those files replaces it. Dashboards never need to be listed. See [discovery-and-access.md](discovery-and-access.md). |
| `queryableSources` | `"declared"` (the default) or `"all"`. Deprecated. `"declared"` does nothing, and writing it gets a warning. `"all"` still works with no warning, because nothing replaces it: the surface then decides listings only, and every source stays queryable by name. Use it to hide an `#(authorize)`-gated source from listings while authorized callers still query it. See [discovery-and-access.md](discovery-and-access.md). |
| `materialization` | Persisted-source build policy (`schedule`, `freshness`). Package root only. See [materialization.md](materialization.md). |
| `scope` | `"package"` (the default) or `"version"`. Any other value fails the package load. |
| `agents` | Named agents the package declares, keyed by agent name. See [Agents](#agents). |
| `retrieval` | How `get_context` searches and indexes this package: `representation` (`single` or `facets`), `keyphrases` (`auto`, `never`, `always`), `refine`, `rerank`, `sourceMatch` and `sourceSummary` (each `{ "enabled": "auto" \| true \| false }`, with `minLevel` on `refine` and `topSources` on `rerank`), and `prompts` (a file path inside the package for each of `keyphrase`, `refine`, `rerank`, `sourceMatch`, `sourceSummary`). Any other key under `retrieval`, or an invalid value, fails the package load. See [get-context-pipeline.md](get-context-pipeline.md) and [configuration.md](configuration.md). |

Unknown top-level keys are ignored and preserved. Inside `retrieval` they are not: an unknown key fails the
package load with a message naming the valid ones. The bundled examples carry a `version` field as a
convention, but nothing reads it. (One more field, `manifestLocation`, exists for orchestrated
control-plane deployments; a locally authored package never needs it.)

## Skills

A package can carry agent skills of its own in a `skills/` directory at its root: one directory per skill, each holding a `SKILL.md` in the [agentskills.io](https://agentskills.io) format (a `name` and `description` in the frontmatter, then the body) and optional `reference/*.md` files. Publisher serves them next to the bundled skills over the `get_skill` MCP tool and `GET …/packages/{pkg}/skills`, scoped to that package, so an agent learns how to work with this package's data from the package itself.

- A skill whose `name` matches a bundled skill replaces it for this package. Nothing is merged. Every entry carries an `origin` (`package` or `bundled`) saying which one you are reading.
- Every file is read through its real path, and one that resolves outside the package is not served, so a link cannot pull in a file from elsewhere on the machine. A `skills/` directory that is itself a link out of the package is skipped with a load warning.
- A file over 256 KB is not served. A skill whose `SKILL.md` cannot be read, or that repeats another skill's `name`, is skipped; one with no `description` is served but flagged, since nothing tells a caller when to read it. Each case is a load warning naming the fix, and the package still loads.

## Agents

A package can declare agents: a named brief for working with this package, made of instructions, the agent's own skills, and optional scheduled tasks. Publisher parses and serves the definition. It does not run the agent. A client adopts one by fetching it (`get_agent`, or `GET …/packages/{pkg}/agents/{name}`) and following it in its own session, with its own tools and permissions; the bundled `malloy-run-agent` skill is the recipe. Adopt an agent only when the user names it: nothing advertises one in discovery.

```json
{
  "name": "storefront",
  "agents": {
    "analyst": {
      "description": "Answers revenue questions using the storefront conventions",
      "model": "inherit",
      "instructions": "agents/analyst/instructions.md",
      "skills": ["agents/analyst/skills"],
      "schedules": [
        { "cron": "0 13 * * MON", "task": "agents/analyst/tasks/weekly-report.md" }
      ]
    }
  }
}
```

| Key | Meaning |
| --- | --- |
| `description` | Required. One line saying what the agent is for. |
| `instructions` | Required. Package-relative path of a Markdown file (no frontmatter): the brief the agent works from. At most 16 KB. |
| `model` | Optional. The model the agent asks for; `inherit` (the default) means the caller's own. |
| `skills` | Optional. Package-relative directories, each holding `<skill>/SKILL.md` as in [Skills](#skills). Two directories holding the same `<skill>/SKILL.md` collide and drop the agent. All of an agent's skill files together may total at most 256 KB. |
| `schedules` | Optional. A list of `{ "cron", "task" }`: a 5-field UNIX cron in UTC and the package-relative path of a Markdown task file (at most 16 KB). **Schedules are parsed, validated and shown. Nothing executes them yet.** A task runs when someone asks for it. |

The agent's name is its key: 1 to 64 characters of lowercase letters, digits and single hyphens.

**Strict allowlist.** An agent or schedule entry with a key this server does not know is dropped, because a key it does not understand might limit the agent, and an unknown key never runs. `tools`, `mcp-servers`, `hooks`, `permission-mode` and `base` are not part of the schema, so they are dropped too: a definition adds words to a session and never widens what the session may do. A key starting `x-` is ignored, for notes and tool metadata.

**A broken agent never fails the package.** An agent that fails validation (a bad name, a missing file, a path that escapes the package or resolves outside it through a link, a nested `publisher.json` under an agent path, an over-cap file, a bad cron) is left out and reported in the package's load warnings with a `Fix:` line. The model and the other agents keep serving. A package with no `agents` key is unchanged, and an older Publisher serves a package that has one as before, without the agents.

Two hashes say which bytes a definition came from. `sourceContentSha` on the package covers the models, the package skill files, the `agents` object and every file an agent reads; a `version` bump or another manifest key does not move it. `definitionSha` on the resolved agent covers only that agent's configuration, instructions, skills and task files, so editing one agent or a model leaves another agent's `definitionSha` alone. `GET …/agents/{name}` returns both under `source`, with `servedRevision`.

## Where the data comes from

Every loaded package automatically gets a DuckDB connection named `duckdb`, rooted at the package
directory. `duckdb.table('data/sales.csv')` works with zero configuration, and so do
`.parquet`, `.json`, `.ndjson`, and `.xlsx` files read the same way. Relative paths resolve
against the package root. It is also the default connection, so a model that names no connection
gets it.

There is no preprocessing step for any of these: DuckDB reads them in place, so a file never needs
converting to CSV, and inspecting one with a script instead of querying it is always the slower
path. JSON carries no schema, so a value written as `"90"` arrives as a string where CSV would
infer a number; cast it in the source with `::number`. Anything needing read options (an Excel
sheet name, a JSON format hint) goes through `duckdb.sql("""SELECT * FROM read_json_auto(...)""")`
instead of `table()`.

A package cannot declare its own warehouse connection. Connections to BigQuery, Snowflake,
Postgres, and the rest are defined per environment, in the server's config; the name `duckdb` is
reserved for the per-package sandbox. See [connections.md](connections.md).

## Serving a package

There is no directory scan: a package on disk serves only once it is registered. Two main ways:

- A `{ "name": "...", "location": "..." }` entry under an environment in `publisher.config.json`.
  The `location` can be a local path (absolute, `~/`, or relative to the config file, written
  `./sales`) or a `https://github.com/...`, `gs://`, or `s3://` URL. See
  [configuration.md](configuration.md#bring-your-own-config) for the full recipe.
- On a running server, `POST /api/v0/environments/{env}/packages` with
  `{"name": "...", "location": "/absolute/path"}`. See [api-overview.md](api-overview.md). Like
  the rest of the API this endpoint is unauthenticated, so keep the server on localhost or behind
  a gateway.

## Lifecycle: the served copy

When an environment is first created, Publisher copies each configured package into its own
storage at `publisher_data/<env>/<pkg>/` (in the server root: the directory the server was
launched from, unless `--server_root` set another) and compiles it, then serves that copy.
Consequences:

- Editing your original source directory afterwards changes nothing, unless the environment runs
  in watch mode (`--watch-env <env>`), which mounts local packages in place as symlinks instead of
  copying them.
- After editing the served copy, reload the package to recompile it:
  `GET /api/v0/environments/{env}/packages/{pkg}?reload=true` over REST, or the
  `reload_package` MCP tool. The reload is in place for a package that came from
  `publisher.config.json`; a package the server installed from a location (below) is re-fetched
  from it instead, which overwrites local edits. A reload that fails to compile leaves the files
  alone and keeps serving the previous model.
- `--init` deletes `publisher_data/` and re-copies everything from the configured locations. Keep
  your source of truth outside `publisher_data/`.

### Installed packages

A package can also arrive through the API: a `POST` to an environment's `packages` with a
`location`, or a `PATCH` on a package that names one. The server downloads it, compiles the new
copy beside the one that is serving (if any), and swaps it in; the previous copy answers queries
until the swap. Four things follow from how that is recorded:

- The server writes where it fetched the package from into its own file outside the package
  directory, `publisher_data/<env>/.install-records/<pkg>.json`. It never writes that into
  `publisher.json`, and nothing inside the package is read as one, neither a `location` an author
  puts in `publisher.json` nor a record file shipped with the content: a reload re-fetches from
  the recorded location, so only the server may set it.
- A `PATCH` whose `location` equals the recorded one is a metadata update. Nothing is fetched or
  recompiled beyond what a new `manifestLocation` requires. A `PATCH` with a different `location`
  installs from it. A `PATCH` never changes the recorded location by itself. To fetch the same
  location again, reload the package.
- A `PATCH` that arrives while a load of the package is in progress waits for it, and is applied
  to whichever copy that load leaves resident. So it lands on the copy being installed, or, if the
  install failed, on the copy that is still serving.
- A reload of an installed package re-fetches from the recorded location and re-applies the
  manifest binding it had. Everything else, the description, `explores`, policy, is what the
  re-fetched `publisher.json` declares.

While a package is loading, reinstalling or recompiling, its `status` reports `loading: true`
alongside `serving`, which stays true for as long as a previous copy answers queries. A package
loading for the first time is listed only by `GET /api/v0/status?includeLoading=true`; every other
listing shows packages that can serve. See [api-overview.md](api-overview.md).

The agent workflow built on this lifecycle is in [AGENTS.md](../AGENTS.md); load failures and how
to read them are in [deployment.md](deployment.md#serving-does-not-mean-everything-loaded).

## See also

- A `public/` directory of HTML pages makes the package a data app, no build step:
  [html-data-apps.md](html-data-apps.md).
- Runtime parameters and access control, declared in the models:
  [givens.md](givens.md), [authorize.md](authorize.md), [row-level-access.md](row-level-access.md).
