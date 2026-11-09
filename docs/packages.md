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

| Field              | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`             | Conventionally the package's name, but the server never surfaces it: the registered name (config entry or API call) wins in API URLs and responses.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `description`      | Shown in the Publisher UI and in API responses.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `explores`         | **Deprecated, still honored.** Names the files whose exports are the surface; those files are listed and queryable. A tagged report listed here reads the surface and adds nothing to it. Every use gets a load warning: an `index.malloy` that imports those files replaces it. Reports never need to be listed. See [discovery-and-access.md](discovery-and-access.md).                                                                                                                                                                                                                                                                                                                                     |
| `queryableSources` | `"declared"` (the default) or `"all"`. Deprecated. `"declared"` does nothing, and writing it gets a warning. `"all"` still works with no warning, because nothing replaces it: the surface then decides listings only, and every source stays queryable by name. Use it to hide an `#(authorize)`-gated source from listings while authorized callers still query it. See [discovery-and-access.md](discovery-and-access.md).                                                                                                                                                                                                                                                                                 |
| `materialization`  | Persisted-source build policy (`schedule`, `freshness`, `scope`). Package root only. Its `queryMetadata` is deprecated; declare `queryMetadata` at the root. See [materialization.md](materialization.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `scope`            | Deprecated at the root: declare it as `materialization.scope`. `"package"` (the default) or `"version"`. Any other value fails the package load.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `agents`           | Named agents the package declares, keyed by agent name. See [Agents](#agents).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `retrieval`        | How `get_context` searches and indexes this package: `fcj` (the engine's settings, such as `perTarget` and `judge`), `sourceSummary` (`{ "enabled": "auto" \| true \| false }`), and `prompts` (a file path inside the package for each of `judge`, `docParts`, `questions`, `sourceSummary`). Any other key under `retrieval`, or an invalid value, fails the package load, except `engine`, `representation`, `keyphrases`, `refine`, `rerank`, `sourceMatch` and the prompts `keyphrase`, `refine`, `rerank` and `sourceMatch`: those no longer do anything, are ignored, and are named in the package's `warnings`. See [configuration.md](configuration.md#package-settings-retrieval-in-publisherjson). |

Unknown top-level keys are ignored and preserved. Inside `retrieval` they are not: an unknown key fails the
package load with a message naming the valid ones. `version` is read when the package is published
from a location (`POST …/packages` with a `location`): it numbers the immutable version that publish
creates, and a publish without one is refused. A package in `publisher.config.json`, or one an
environment created over the API lists, is published the same way, unless its environment is
watched (`--watch-env`), which mounts it in place and ignores it. See
[package-versions.md](package-versions.md). (One more field, `manifestLocation`, exists for orchestrated
control-plane deployments; a locally authored package never needs it. It is deprecated: bind a
version's build manifest with `PUT …/versions/{v}/manifest` instead. See
[#1540](https://github.com/malloydata/publisher/issues/1540).)

## Skills

A package can carry agent skills of its own in a `skills/` directory at its root: one directory per skill, each holding a `SKILL.md` in the [agentskills.io](https://agentskills.io) format (a `name` and `description` in the frontmatter, then the body) and optional `reference/*.md` files. Publisher serves them next to the bundled skills over the `get_skill` MCP tool and `GET …/packages/{pkg}/skills`, scoped to that package, so an agent learns how to work with this package's data from the package itself.

- A skill whose `name` matches a bundled skill replaces it for this package. Nothing is merged. Every entry carries an `origin` (`package` or `bundled`) saying which one you are reading.
- Every file is read through its real path, and one that resolves outside the package is not served, so a link cannot pull in a file from elsewhere on the machine. A `skills/` directory that is itself a link out of the package is skipped with a load warning.
- A file over 256 KB is not served. A skill whose `SKILL.md` cannot be read, or whose frontmatter `name` differs from its directory name, is skipped; one with no `description` is served but flagged, since nothing tells a caller when to read it. A `reference/` that cannot be listed or read is skipped the same way, without dropping its skill. Each case is a load warning naming the fix, and the package still loads.

## Agents

A package can declare agents: a named brief for working with this package, made of instructions, the agent's own skills, optional scheduled tasks and an optional run budget. Publisher parses and serves the definition. It does not run the agent. A client adopts one by fetching it (`get_agent`, or `GET …/packages/{pkg}/agents/{name}`) and following it in its own session, with its own tools and permissions; the bundled `malloy-run-agent` skill is the recipe, and `malloy-define-agent` covers writing a definition. A large skill set can be read in two steps: `skills=summary` (MCP `skills: "summary"`) lists each file's `relative_filepath`, `bytes`, and for a `SKILL.md` its `name` and `description`, and `GET …/agents/{name}/skills/{relative_filepath}` (MCP `skill_path`) returns one file. Both are views of the same resolved definition and carry the same `source`. Adopt an agent only when the user names it: nothing advertises one in discovery.

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
        {
          "cron": "0 13 * * MON",
          "task": "agents/analyst/tasks/weekly-report.md"
        }
      ],
      "budget": { "turns": 30, "wall_clock_minutes": 60 }
    }
  }
}
```

| Key            | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `description`  | Required. One line saying what the agent is for.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `instructions` | Required. Package-relative path of a Markdown file (no frontmatter): the brief the agent works from. At most 16 KB.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `model`        | Optional. The model the agent asks for; `inherit` (the default) means the caller's own.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `skills`       | Optional. Package-relative directories, each holding `<skill>/SKILL.md` as in [Skills](#skills). Two directories holding the same `<skill>/SKILL.md` collide and drop the agent. All of an agent's skill files together may total at most 256 KB.                                                                                                                                                                                                                                                                                                                      |
| `schedules`    | Optional. A list of `{ "cron", "task" }`: a 5-field UNIX cron in UTC and the package-relative path of a Markdown task file (at most 16 KB). **Schedules are parsed, validated and shown. Nothing executes them yet.** A task runs when someone asks for it.                                                                                                                                                                                                                                                                                                            |
| `budget`       | Optional. Run limits as an object whose optional fields are `turns`, `wall_clock_minutes`, `llm_tokens` and `job_minutes`, each a positive integer. Set only the fields you care about (`{ "wall_clock_minutes": 60 }` alone is fine); an unset field keeps the platform's default. An unknown sub-key or any other value drops the agent. Publisher serves it (`get_agent`, the agent endpoints) and enforces nothing; a platform that runs the agent may clamp each field to its own maximum. A Publisher release that predates the key drops an agent that sets it. |

The agent's name is its key: 1 to 64 characters of lowercase letters, digits and single hyphens.

**Strict allowlist.** An agent, schedule or budget entry with a key this server does not know is dropped, because a key it does not understand might limit the agent, and an unknown key never runs. `tools`, `mcp-servers`, `hooks`, `permission-mode` and `base` are not part of the schema, so they are dropped too: a definition adds words to a session and never widens what the session may do. A key starting `x-` is ignored, for notes and tool metadata.

**A broken agent never fails the package.** An agent that fails validation (a bad name, a missing file, a path that escapes the package or resolves outside it through a link, a nested `publisher.json` under an agent path, an over-cap file, a bad cron, a bad `budget`) is left out and reported in the package's load warnings with a `Fix:` line. The model and the other agents keep serving. An agent skill with no `description`, a `reference/` that cannot be read, or a skills directory with no skills is not a failure: the agent is served and the finding appears in its `warnings`. A package with no `agents` key is unchanged, and an older Publisher serves a package that has one as before, without the agents.

### Skill assets

A skill an agent serves may ship `<skill>/assets/**`: text files the agent copies into an app it builds and never reads. Publisher validates them, lists them without their contents in the agent definition (`assets`, with `asset_roots` for each skill's package-relative directory; the summary view carries only `asset_counts`), and serves one on request through the same skill-file route (`GET …/agents/{name}/skills/<skill>/assets/…`, MCP `skill_path`). Assets never count against the 256 KB Markdown cap. Only a skill an agent names under `skills` ships assets: a package's own `skills/<x>/assets/` is ignored.

| Limit                                                                                                                             | Why                                                                                                     |
| --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Markdown: 256 KB per file and per agent                                                                                           | The context budget: the agent reads these.                                                              |
| One asset under 1 MiB (a file of 1,048,576 bytes is out)                                                                          | The draft store refuses a file of 1 MiB or more, so a larger asset could never be written back.         |
| 4 MiB and 200 files per agent                                                                                                     | Keeps copying every asset inside one agent turn's write-back.                                           |
| 8 MiB per package, each package path counted once                                                                                 | Bounds load-time hashing. Agents are charged in manifest order, so a path two agents share counts once. |
| Path segments `[A-Za-z0-9_][A-Za-z0-9._-]*`, listed path at most 200 characters, depth at most 8, at most 1,000 entries per agent | Names that survive any filesystem and any URL, and a walk that cannot be made to run long.              |
| UTF-8 text only, no NUL byte                                                                                                      | The route returns JSON text. Ship images as SVG.                                                        |

Names ending `.malloy` or `.malloynb`, and files named `publisher.json` or `SKILL.md` (any case), are refused. The `.malloy` refusal is a warning, not a protection: the package loader compiles a `.malloy` file anywhere in the package, an asset included.

Problems fall in two classes. **Security** problems drop the whole agent, with a `Package agent '<name>' is not served:` warning naming the file and the fix: a symlink anywhere under `assets/`, an `assets/` that is itself a link or resolves outside the skill, a nested `publisher.json` or `SKILL.md`, `..` in a name, or two paths that differ only by case. **Everything else** skips that one file, leaves it out of the listing and warns naming the limit: not UTF-8 or holds a NUL, 1 MiB or more, over the per-agent or per-package budget, a bad segment name or a path over 200 characters, a Malloy extension, or a walk past the depth or entry limit. An agent that ships a PNG or a very large `assets/` is still served. The caps bound what is listed, not every byte examined: a file under the size cap is read to check it, though a NUL in its first 8 KiB (a binary) rejects it without the full read, and each file is read and hashed at most once per load however many agents share its skill; a skipped file is never charged to a budget. Dotfiles are ignored, and an empty `assets/` is no assets.

**Trust.** An asset is copied into an app unread and runs with the viewing user's data authority wherever that app opens, previews while it is being built included. Review assets as code before adopting an agent that ships them.

Two hashes say which bytes a definition came from. `sourceContentSha`, in the package's serving state (`GET …/packages/{pkg}/status`), covers the models, the package skill files, the `agents` object and every file an agent reads, assets included; a `version` bump or another manifest key does not move it. `definitionSha` on the resolved agent covers only that agent's configuration, instructions, skills, asset listing (paths and sha256s) and task files, so editing one agent or a model leaves another agent's `definitionSha` alone. `GET …/agents/{name}` returns both under `source`, with `servedRevision`.

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

Every package a server serves is a published [version](package-versions.md): when an environment
is first created, Publisher publishes each configured package from its location as the version its
`publisher.json` declares, into its own storage at `publisher_data/<env>/<pkg>/<version>/` (in the
server root: the directory the server was launched from, unless `--server_root` set another), and
serves that immutable copy. Consequences:

- A configured package needs a semantic `version` in its `publisher.json`; one without fails to
  load, and its error is in `GET /api/v0/status`.
- Editing your original source directory afterwards changes nothing, and a reload answers the
  version unchanged. To serve new content, bump the `version` and publish it again
  (`POST …/packages` with its location).
- To edit and reload in place, run the environment in watch mode (`--watch-env <env>`), which
  mounts local packages as symlinks instead. A save recompiles the package, and
  `GET /api/v0/environments/{env}/packages/{pkg}?reload=true` (or the `reload_package` MCP tool)
  recompiles it on demand. A reload that fails to compile leaves the files alone and keeps serving
  the previous model.
- `--init` deletes `publisher_data/` and publishes everything again from the configured
  locations. Keep your source of truth outside `publisher_data/`.

### Published packages

A package can also arrive through the API: a `POST` to an environment's `packages` with a
`location` publishes the version its `publisher.json` declares, and the versions already published
keep serving. The server records where it fetched each version from, and a version whose files go
missing is fetched again from there. Nothing inside the package is read as that location, neither a
`location` an author puts in `publisher.json` nor a file shipped with the content.

While a package is loading or recompiling, the `status` on its entry in
`GET /api/v0/status` reports `loading: true`
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
