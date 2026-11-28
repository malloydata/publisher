# Materialization developer experience (design)

Status: design for review. Nothing here is implemented. Every new setting, contract value or
behaviour change is marked **[sign-off]** and collected in [Decisions](#decisions-for-kyle) at the
end.

This doc has three parts:

1. [A local dev loop for materialization](#part-1-a-local-dev-loop-for-materialization). It lets an
   author see "served from table X" or "live, because ..." on their own machine, without publishing.
2. [The skills work](#part-2-what-the-skills-should-teach). What each Malloy skill should say about
   materialization, what is stale, and which pull request carries each fix.
3. [A pass over the authoring journey](#part-3-the-authoring-journey-end-to-end). The five steps
   from annotating to iterating, locally and hosted, ranked by how much each one hurts an author.

The doc gives phases, dependencies and the critical path. It gives no effort or calendar
estimates.

Author-facing examples follow the authoring rule:

- `#(materialize)` for sources;
- `#@ persist` only for named queries and views;
- `#(rollup)` for metrics;
- never `#@ preaggregate`.

Citations are `file:line` against `origin/main` at `eef7d0ca2`. The packages are
`packages/server/src/...` (shortened to `server/...`) and `packages/cli/src/...` (shortened to
`cli/...`).

---

## Part 1: a local dev loop for materialization

### The problem

A hosted control plane gives authors a local dev command. It runs a standalone Publisher over the
author's working tree. Every connection in that Publisher is a `type: "publisher"` proxy to the
control plane's query gateway. In that setup an author cannot learn whether a source would be
served from its table, or why it would answer live, without publishing. Three things block it.

**1. A `storage=` build from a proxy source is refused by design.**
`server/service/materialization/domain/source_connection_type.ts:13-17` lists the connection types
a build can pass through: `bigquery`, `snowflake` and `postgres`. A `publisher` connection gets its
own refusal at `:47-58`: "A 'storage=' build runs on that host, not on this server. The source is
answered live here." The plan therefore refuses every stored source with
`storage_source_unsupported`, and that refusal hides any other reason the host might refuse it.

**2. The gateway allows a database only a single SELECT.**
The OSS `sqlQuery` route enforces the same rule on DuckDB-backed connections (`api-doc.yaml:865-886`).
A database-tier `#(materialize)` build runs a CTAS in the warehouse, and the gateway refuses that.

**3. A managed DuckLake is an empty shell locally.**
It reaches the local Publisher read-only with no catalog behind it, so there is nowhere to build.

The fix has two Publisher pieces:

- **Phase 1:** a local destination that needs no Postgres, plus a dev-only override that sends
  every materialization to it.
- **Phase 2:** a passthrough that feeds a build's source SELECT through the proxy's `sqlQuery`.

With both pieces, plans, builds, serve verdicts, rollups, joins and the served copy all run on the
author's machine against a sample of real data.

### Phase 1a: a local destination with no Postgres

Today a storage destination may be declared only as `ducklake`
(`DECLARABLE_STORAGE_DESTINATION_TYPES`, `server/service/connection_config.ts:420-421`). Every other
type is rejected when the list loads (`:571-578`). A DuckLake catalog must be Postgres:
`server/service/dialects/ducklake.ts:153-156` throws "PostgreSQL connection configuration is
required for DuckLake catalog". An author therefore cannot declare a destination without running
Postgres.

There are two ways to fix this.

**Option A: declare a plain DuckDB-file destination (`type: "duckdb"`).**

Most of this path already exists, unreachable from configuration:

- `storageDestinationFile` (`connection_config.ts:493-512`) names the file
  `.storage-destinations/<name>.duckdb` under the environment root, with a traversal check.
- `destinationFileNameRefusal` (`:469-478`) and `namesItsFile` (`:484-486`) already run inside
  `validateStorageDestinations` for a type whose registry entry has no destination dialect.
- The build's own type list admits `duckdb` (`STORAGE_DESTINATION_TYPES`,
  `server/service/materialization/run/materialization_build_session.ts:141`). Its comment says that
  branch "is not reachable from configuration".
- The build attaches the file read-write, creating the root if needed (`:1923-1947`), and the serve
  path attaches it `READ_ONLY` (`:1898-1919`).
- `storage_admin.ts:157` lists and drops its tables.

The change is to add `duckdb` to `DECLARABLE_STORAGE_DESTINATION_TYPES` and accept the
`duckdbConnection` shape for a destination. That shape is the file path, which is derived and never
configured; `setupSQL` and attached databases are refused.

**Option B: accept a DuckLake catalog held in a DuckDB file, with a local data path.**

This adds a catalog arm, `ducklakeConnection.catalog.duckdbFile`, beside `postgresConnection`, and
relaxes `ducklake.ts:153`.

|                                         | A: plain DuckDB file                                                                                                                                                                                                       | B: DuckLake on a DuckDB-file catalog                                                                                                              |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Code that exists                        | Build, serve and admin paths for `duckdb` destinations. Only the gate at `connection_config.ts:420` and its shape check are missing.                                                                                       | None for the catalog arm. Every catalog function assumes Postgres (`ducklake.ts:153-195`, the catalog-format preflight, `managed_catalog_scope`). |
| What it exercises                       | The storage tier's planning, build, column contract, served copy, rollups and path report. Not DuckLake-specific behaviour such as inlining (`dialects/ducklake_inlining.ts`), partition layouts, file sizes or snapshots. | Everything A does, plus DuckLake's attach, inlining and layouts, so it matches a hosted DuckLake destination more closely.                        |
| Wire contract                           | One more value in an existing enum.                                                                                                                                                                                        | A new catalog arm in `ducklakeConnection`, which every client that reads connections must learn.                                                  |
| Concurrency                             | DuckDB allows one writer per file. A build's read-write attach and a query's read-only attach in one process need a proof (see Risks).                                                                                     | Same limit: DuckLake on a DuckDB catalog allows one client.                                                                                       |
| Type fidelity against a hosted DuckLake | Plain DuckDB holds types DuckLake does not. A dev table can hold, and serve, a column a hosted DuckLake build would refuse.                                                                                                | Matches DuckLake's type set.                                                                                                                      |

**Recommendation: A in Phase 1, and B only if fidelity gaps show up in practice.** A unblocks the
loop through code that already exists. To narrow A's fidelity gap, the plan should judge a
`duckdb`-type destination's column types against DuckLake's type set when the dev override is on,
and report a mismatch as a plan warning rather than a refusal (code
`dev-destination-type-divergence`, **[sign-off]**).

- **[sign-off]** `duckdb` becomes a declarable storage destination type.
- **[sign-off]** `duckdb` is declarable only while the dev override below is set. Without that rule
  a self-hosted operator could use it as a production destination: one file, one process, no
  object storage. A stricter alternative is to leave it declarable everywhere and document it as
  single-process. The recommendation is the dev-only rule, so nothing becomes production
  configuration by accident.

### Phase 1b: the dev destination override

#### What it does

A new setting names one configured storage destination. While it is set, every materialization in
the watched environment(s) is planned, built and served in that destination. That covers:

- each `#(materialize)` source, database tier included (a source with no `storage=`);
- each `#@ persist` view and named query;
- each `#(rollup)` rollup.

The rest of the pipeline runs unchanged: the plan, the column contract, strict builds, chain
builds, serve verdicts, the served copy with its joins and extensions, the rollup leg, and the path
report.

**Settings registry entry [sign-off]:**

- **Key:** `persist.devDestination`. Per CLAUDE.md the key fixes every name: the variable is
  `PUBLISHER_PERSIST_DEV_DESTINATION` and the flag is `--persist-dev-destination`. The brief's
  `PERSIST_DEV_DESTINATION` is not a shipped name, so it gets no alias.
- **Kind:** launch-style. It is a variable and a flag only, never a `publisher.config.json` key.
  Startup ignores it in the file and says so, as it does for `root`, `config` and
  `watchEnvironments` (`server/settings/registry/launch.ts`). A config file that travels with
  packages to a hosted cell can therefore never carry it. The `launch` kind today means "decides
  which file is read", so this either widens that meaning or adds a `fileForbidden: true` property.
  **[sign-off]** on which.
- **Not secret.**
- **Docs:** run `bun run generate-settings-reference`. The flag takes a value, so it goes into
  `SERVER_VALUE_FLAGS` in `packages/create-malloy-package/src/scaffold.ts`.

**What the override changes, precisely:**

- **The plan.** Each planned item takes the dev destination as its `destination`. The build plan
  records what it replaced on each item as `devOverride: { from: "database" | "storage:<name>" }`
  **[sign-off]**, so a reader can tell an overridden plan from an authored one.
- **The content address.** The dev destination is always folded into every address, as
  `persist.addressIncludesDestination` does for `storage=` sources
  (`server/settings/registry/storage.ts:128-134`). A dev table can then never share an address, or
  a manifest entry, with a table built for real.
- **Storage mode.** The override implies `PERSIST_STORAGE_MODE=on` for the dev destination, so its
  tables are served. An explicit `PERSIST_STORAGE_MODE=off` or `write-only` alongside it fails
  startup rather than being silently overridden **[sign-off]**.
- **Visibility:**
  - `GET /status` gains `devDestination: { name, environments }` **[sign-off]**.
  - Every serve verdict on an overridden item gains `devOverride` with the same `from` value.
  - The path report's `servedBy` gains `devDestination: true`.
  - Each materialization run's `metadata` records `devDestination`.
  - Startup prints one line to stderr:
    `PUBLISHER_DEV_DESTINATION name=<n> environments=<list> storage_mode=on`.

**Reasons that appear only because of the override.** The served copy will report some reasons
that exist only because a database-tier source was moved into a destination. Examples:

- `cross_connection_join`, where a join that the warehouse would serve in place now crosses from
  the dev file to a live proxy connection;
- `sql_embed_cross_connection`, for a `.sql()` source that embeds a stored source.

Each such reason must be labelled `dev_override: true` in the verdict's `notServedIn` entry. Its
message must say "on the database tier this source would be served in its own warehouse; the dev
destination moved it". An author must never chase a refusal the host would not give
**[sign-off]**.

**Gates.** A database-tier gated source is judged under the database tier's gate rules, not the
storage tier's stricter `row_level_gate` rules (`docs/materialization.md:1215-1230`). That way the
local verdict predicts the hosted one. The table holds the author's own sample, on the author's own
machine, read with the author's own permissions. **[sign-off]** The alternative, judging by the dev
tier, makes every gated source answer live locally, so the author learns nothing about it.

**What the override does not exercise.** Startup prints this list and the docs page repeats it:

- warehouse CTAS dialects;
- `connection_cannot_hold_table`;
- BigQuery `needs_qualified_name`;
- warehouse collations on the database tier;
- incremental `MERGE` on a warehouse;
- DuckLake-specific behaviour under option A.

#### How "never alongside a manifest, never in a hosted cell" is enforced

Open-source Publisher has no "hosted" flag, and it should not grow one. The guarantee is built from
mechanical conditions, each of which a hosted cell fails. Each check sits where a control plane
drives a worker.

**1. Startup refuses.** Startup prints `PUBLISHER_INIT_FAILED` and exits non-zero when the override
is set and any of the following holds:

- `frozenConfig` is on (`server/settings/registry/server.ts:11`). A hosted cell freezes its
  configuration.
- `watchEnvironments` is empty. Watch mode is a launch setting documented as dev only
  (`launch.ts:59-70`), and it mounts local directories in place. A hosted cell receives packages by
  publish and never watches. Starting watch mode later through the admin API's `PUT /watch-mode`
  does not satisfy this check: the check reads the launch setting at startup.
- Either listener binds a non-loopback host, meaning `PUBLISHER_HOST` or `MCP_HOST` is anything
  other than `127.0.0.1`, `::1` or `localhost`. A hosted cell binds to be reachable from its
  gateway. **[sign-off]** This has a cost: a developer running Publisher in Docker binds `0.0.0.0`
  inside the container. The escape is to run the dev loop with `npx` or a clone, which is what the
  control plane's dev command does.
- The named destination is not declared in each watched environment's `storageDestinations`.
- The named destination is not local. That means a `duckdb` destination, or, under option B, a
  DuckLake destination whose catalog is a DuckDB file and whose data path is a local directory. A
  `gs://`, `s3://` or Postgres catalog fails. This bounds the damage of any mistake: an override can
  only ever write a file under the server root.
- `PERSIST_STORAGE_MODE` is explicitly `off` or `write-only`.

**2. The override is scoped to watched environments.** It applies only to packages mounted in
place in a watched environment. A package added by publish, including a published version
(`docs/package-versions.md`), is planned without it, even on the same server.

**3. Manifests and build instructions are refused while the override is set:**

- A watched package whose `publisher.json` declares a `manifestLocation` fails to load, code
  `dev-destination-manifest` in `loadErrors`, with a message naming both settings. A load failure
  is right here, not a warning, because serving half from a host manifest and half from local
  tables would make every verdict wrong.
- `PATCH …/packages/{pkg}` binding a manifest answers 409 `DEV_DESTINATION_ACTIVE`.
- `POST …/materializations` with `buildInstructions`
  (`server/controller/materialization.controller.ts:200-240`) answers 409 `DEV_DESTINATION_ACTIVE`.
  An instructed build is how a control plane drives a worker.
- Error code `DEV_DESTINATION_ACTIVE` is **[sign-off]**.

**4. The scheduler is unaffected.** The standalone scheduler may run, since it is local by
definition. Its runs carry `devDestination` like any other.

**What the hosted control plane must also do:** never set the variable in a cell image. Condition 1
makes a mistake there fatal at startup rather than silent.

#### Risks to prove in Phase 1

**One process, two attaches.** A build attaches the destination file read-write
(`materialization_build_session.ts:1944-1946`) while query sessions may hold it read-only
(`:1915-1918`). This happens in one process, and possibly on separate pooled DuckDB instances. That
combination needs a test before anything ships.

If DuckDB's file lock does not let them coexist safely, the design falls back to generational
files:

- each build writes `<name>.<generation>.duckdb`;
- the bind switches readers to the new generation;
- old generations are swept after the last reader detaches.

**Pooled instance identity.** A destination's working directory is kept apart from connections on
purpose (`connection_config.ts:423-441`). A `duckdb` destination must keep that separation.

### Phase 2: a `publisher` passthrough source

#### What it does

While the dev override is set, a materialization whose source reads a `publisher` connection is
built through the proxy:

1. The build's source SELECT is sent to the proxy's `sqlQuery`. This is the same SELECT the plan
   records as the source's build SQL, in the remote connection's dialect.
2. The rows that come back are written into the dev destination.
3. The table's column contract is recorded.
4. Everything downstream builds locally from that table: chained sources, rollups and persisted
   views.

Without the override, a `publisher` source keeps its refusal (`source_connection_type.ts:47-58`),
because a hosted build happens on the host **[sign-off]**.

The gateway already allows a single SELECT on `sqlQuery`, so it needs no new capability. It needs
limits that Publisher can state and the gateway can check.

#### Request shape

Publisher calls the proxy connection's `connectionUri` + `/sqlQuery` itself. It does not go through
`@malloydata/db-publisher`'s `runSQL`, for three reasons:

- That client sets `options.abortSignal = undefined` (`publisher_connection.js:115`), so a build
  could not be timed out or cancelled.
- Its streaming form buffers the whole response (`:131-150`).
- A build must also set a query class and metadata.

One request per source per build:

```
POST <connectionUri>/sqlQuery
Authorization: Bearer <the proxy connection's accessToken>
Content-Type: application/json

{
  "sqlStatement": "SELECT * FROM (<the source's build SELECT>) AS dev_build LIMIT <cap + 1>",
  "options": "{\"rowLimit\": <cap + 1>}",
  "queryClass": "materialize",
  "queryMetadata": {
    "run_id": "<materialization id>",
    "source": "<model path>#<source name>",
    "dev_build": "true"
  }
}
```

- The body follows the published contract (`api-doc.yaml:904-930`): `sqlStatement`, `options` as a
  JSON string forwarded as `RunSQLOptions`, `queryMetadata` and `queryClass`.
- `materialize` is an existing `QueryClass` value (`api-doc.yaml:8513-8521`), so the gateway's
  attribution separates dev builds from interactive traffic with no new enum value.
- `dev_build` is a metadata property, not a contract change.
- Requests run one at a time per server.
- The response is `QueryData` (`api-doc.yaml:10175-10189`). Its `data` is a JSON string holding the
  rows.

#### Limits Publisher needs, so the gateway's can be checked against them

Three new settings, all **[sign-off]**, read only while the override is set:

| Setting key (variable)                                                                | Default               | Meaning                                                                                                                                                 |
| ------------------------------------------------------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `persist.devProxyRowCap` (`PUBLISHER_PERSIST_DEV_PROXY_ROW_CAP`)                      | `100000`              | Most rows one source may bring through the proxy. Matches the OSS `sqlQuery` default cap (`PUBLISHER_MAX_QUERY_ROWS`, 100,000; `api-doc.yaml:874-879`). |
| `persist.devProxyTimeoutMs` (`PUBLISHER_PERSIST_DEV_PROXY_TIMEOUT_MS`)                | `300000`              | Wall clock for one request, enforced with `AbortSignal.timeout` on Publisher's own fetch.                                                               |
| `persist.devProxyMaxResponseBytes` (`PUBLISHER_PERSIST_DEV_PROXY_MAX_RESPONSE_BYTES`) | `268435456` (256 MiB) | Publisher counts body bytes as it reads and aborts above this. One JSON payload is held in memory, so this protects a laptop.                           |

The gateway must:

- allow a SELECT wrapped as a subselect with `LIMIT`, in the remote dialect;
- honour `rowLimit`, and answer 413 above its own cap rather than truncating silently;
- allow a request time of at least Publisher's timeout, and a response size of at least Publisher's
  byte limit, or publish its own lower values so Publisher's defaults can be lowered to match;
- attribute `queryClass: "materialize"` traffic to the developer's token.

Whichever cap is lower governs. Publisher sends `cap + 1` so that it can tell "exactly at the cap"
from "over it".

#### Above the cap: refuse, or sample

**Default: refuse.** The source's build fails strictly, code `proxy_rows_over_cap`, with this
message:

> Source 'orders' returned more than 100000 rows through the proxy connection 'warehouse', so it
> was not built. Build a sample with `malloy-pub materialize --sample 100000`, or narrow the source
> with a `where:` while you develop it.

If the gateway answers 413 or times out, the same code is used and the message says which.

**Opt in: sample.** A build request may carry `sample: { rows: N }` on `POST …/materializations`.
The CLI form is `--sample N`. Both are **[sign-off]**.

- The build takes the first N rows the gateway returns. That is an unordered `LIMIT`, so it is not
  reproducible.
- The table is marked `sampled: true` in its manifest entry and column contract.
- Every downstream build over it is marked sampled too.
- Its serve verdict is `serves`, with a `sampled` flag.
- Every answer served from it carries `path.servedBy.sampled: true`, and MCP `execute_query`
  prepends a one-line warning that the numbers come from a sample.

A sampled table is never served without saying so. Sampling does not go through annotation keys:
those keys are a closed set, and they ship with the model.

#### Rows to table

- The table's columns come from the plan's output columns: the compiled Malloy schema. They never
  come from the JSON.
- Rows are decoded per column type, with explicit handling for values JSON flattens: 64-bit and
  128-bit integers, decimals, timestamps with and without zone, and bytes. A value that does not
  decode fails the build strictly with the column's name.
- The column contract records `compare` as the destination's own, which is `exact`.
- The contract never records `gateExact`, so a dev table is never admitted past a row-level gate on
  the storage tier's rules. Under the gate decision above it is judged on the original tier.

### The control plane's side, and what Publisher offers it

The service side has four parts:

- a CLI publish cadence;
- the dev command writing a `local` destination and turning storage on;
- a `--build` option on the dev command, via `POST …/materializations` or `malloy-pub materialize
--wait` (the CLI published as `@malloy-publisher/cli` by #2062);
- printing each source's serve report.

Publisher must offer it the following:

1. **Configuration.** A `duckdb` storage destination entry, the `PUBLISHER_PERSIST_DEV_DESTINATION`
   variable, and `--watch-env`. All of it is documented in `docs/configuration.md` and refused
   loudly when combined wrongly (Phase 1b).
2. **A build trigger.** `POST …/materializations` with `sourceNames` (it exists:
   `api-doc.yaml:10814-10819`) and `sample`. In the CLI, `malloy-pub materialize` gains `--source`
   (repeatable) and `--sample`. Today the CLI sends only `forceRefresh` and `reseed`
   (`cli/commands/materializations.ts:113-119`).
3. **A readable per-source outcome.** The CLI needs one command that prints each materialized
   source with its verdict, tier, table, reason, `fixes` and `notServedIn` (Part 3, step 4). The
   dev command prints it after each build. "Each source's path report" in the brief is this
   report: the path report itself is per query (`api-doc.yaml:8806-8830`).
4. **A stable status block.** `GET /status` carries `devDestination`, so the dev command can
   confirm it is talking to a server in dev mode before it builds anything.
5. **Proxy limits.** The three settings above, with defaults the gateway's limits are checked
   against.

### Phasing and critical path

```
Phase 1a  duckdb destination declarable (dev-gated)  ─┐
                                                       ├─> Phase 1b override + enforcement ─> Phase 2 proxy passthrough
Part 3 P0: verdicts in compile_model/reload, CLI       ─┘            (concurrency proof first)       (needs gateway limits)
          `verdicts`, CLI --source
```

- The critical path is 1a, then the attach-concurrency proof, then 1b, then 2.
- Phase 1 alone is useful: an author who has a warehouse connection locally (not a proxy) gets the
  whole loop.
- Phase 2 depends on the gateway confirming its limits against the table above.
- The Part 3 P0 items do not depend on Part 1. They should land first, because they also help
  hosted authors.

---

## Part 2: what the skills should teach

### Sources of truth

**The v2 plan's skill items:**

- a migration recipe in phase 3 (`docs/materialization-v2-plan.md:132`);
- offer reference joins and child-grain sources before an inlined `join_many`, and flatten parent
  attributes, not parent numbers (`:301`, `:307`, repeated at `:1465` and `:1520`);
- an `estimating` reference and an updated tuning guide in phase 6 (`:363`, `:1449-1455`). These
  cover running `what_if` and showing a tradeoff table without applying anything, flagging fan-out
  above 10, a rollup that cuts rows by less than 10x, a size cap, and an extension that keeps most
  of its parent's rows, then running `compare` one window after a build.

**`docs/materialization.md`** links only to `skills/malloy-materialization/reference/tuning.md`
(`:34`, `:1899`).

**The authoring rule (2026-11-27, #2065):**

- `#(materialize)` on sources;
- `#@ persist` only on views and named queries, with `#@ -persist` still opting a reader out;
- `#(rollup)` on measures;
- no `#@ preaggregate` except where a page documents migration.

**Decided since:** #2069 reads `#@ preaggregate` as a synonym of `#(rollup)` with a deprecation
warning. The authoring rule still says to write `#(rollup)`, so skills show `#@ preaggregate` only
inside the migration note.

**The named-base rule (a hosted control plane's S3 ask).** Until the chain's string-read check no
longer needs a named base, the skill must say:

> A stored downstream over a stored upstream that groups, orders or filters on strings builds over
> a NAMED base, not an inline `extend`.

Today the check fails closed with "its string reads were not recorded"
(`server/service/materialization/domain/string_reads.ts:99`), or with an `upstream_table_unusable`
collation reason (`:116-121`).

### Per skill

All paths are under `skills/`. "Main" is `origin/main`, and "#2065" is branch
`docs/authoring-materialize-only`.

**`malloy-materialization/SKILL.md`**

- **Stale on main:**
  - v1 spelling throughout: `#@ persist` on sources at `:12`, `:18`, `:72-80`, `:104`, `:139-147`;
  - `:28` says "everything else in this skill describes v1";
  - `#@ preaggregate` at `:28`, `:35`, `:70`, `:156`.
- **#2065 carries:**
  - a full v2-first rewrite and a "Which annotation to write" table;
  - "never put `#@ persist` on a source";
  - a root-first migration note (`inherited_persist`);
  - confirming through serve verdicts and the path report;
  - debugging `materialize-not-built` and `served-copy-not-served`.
- **Still missing, which a new skills PR must carry:**
  1. The named-base rule above, with a two-line example of each shape (below).
  2. A short "why does it answer live" ladder:
     - the `refused` reason and its `refusedSources` record;
     - `awaiting_build`;
     - `served_copy` with its `notServedIn` reasons;
     - `upstream_table_unusable`;
     - collation refusals (`compared by equality only`, `unlike live`);
     - `storage_source_unsupported`, including the `publisher` proxy case ("the host builds it; use
       the dev destination to see it locally");
     - each with the field to read and the fix.
  3. The local dev loop: the dev destination, `--sample`, and how to read `devOverride` and
     `dev_override` reasons.
  4. A local build route besides the CLI: `POST …/materializations` with `sourceNames`. No skill
     names it today.
  5. One line on the `#@ preaggregate` synonym (#2069): it is read as `#(rollup)` and warns, so
     write `#(rollup)`.

**`malloy-materialization/reference/chaining.md`**

- **Stale on main:** v1 examples (`:11-15`), and rule 2's advice to use `-> { select: * } extend`
  (`:23`).
- **#2065 carries:** the move to `#(materialize)`, `inherited_persist`, the cross-destination
  exception, and `upstream_table_unusable` for a database extension of a `storage=` parent.
- **Still missing:**
  - `:29` names the `compare` refusal but gives no remedy. Add the named-base rule there, as the
    remedy.
  - Say that "its string reads were not recorded" means the same thing, until that fix lands.

**`malloy-materialization/reference/annotation.md`, `storage.md`, `refresh.md`, `tuning.md`,
`access-filter.md`, `policy.md`**

- **Stale on main:** every `#@ persist` on a source and every `#@ preaggregate`.
- **#2065 fixes all of them.**
- **Still missing:**
  - `storage.md` says nothing about collation. Add the destination's byte-exact compare and what it
    means for an upstream's string columns.
  - `tuning.md` has none of the v2 plan's `estimating` items (phase 6). Leave those to phase 6's
    PR, not this one, since `what_if` and `compare` do not exist yet.

**`malloy-model/SKILL.md`**

- **Stale on main:** `:338` says "`storage=` and `#@ preaggregate` refuse a gated source outright".
- **#2065 fixes this.**
- **Still missing:** a one-line pointer: "to make a source fast, read `malloy-materialization`; a
  materialized source can still answer live, and its serve verdict says why".

**`malloy-modeling/SKILL.md`**

- No materialization content, and #2065 does not touch it.
- **Should say:** one routing line to `malloy-materialization`, beside its routing to the other
  skills. Nothing more: this skill is the entry point, not the reference.

**`malloy-source-unreachable/SKILL.md`**

- No materialization content, and #2065 does not touch it.
- **Should say:** a source that answers but slowly, or whose `servedFrom` is null, is not
  unreachable. It is answering live. Send the reader to `malloy-materialization`'s ladder.
- It should also tell apart "Access denied" from an inherited `where:` that could not be verified
  (`INHERITED_FILTER_UNVERIFIED`, #2070) and an `#(authorize)` denial. Since #2070 the first has
  its own code and reason.

**Generated artifacts**

- The MCP skills bundle (`server/service/skills/skills_bundle.json`) and
  `server/mcp/testdata/tools_list.golden.json` regenerate with every skills change.
- The unpushed fix commit `369573945` on #2065's worktree regenerates the golden and updates the
  `persist.storageMode` description to say `#(materialize) storage=`. It changes nothing under
  `skills/`.
- Its message says `docs/configuration.md` was regenerated, but that file is not in the commit's
  stat. Rerun `bun run generate-settings-reference` before pushing, or `reference.spec.ts` fails.

### The named-base rule, as the skill should show it

```malloy
// Builds: the downstream reads a NAMED stored base.
#(materialize) storage="lake"
source: orders_base is warehouse.table('sales.orders') extend {
  dimension: region_name is upper(region)
}

#(materialize) storage="lake"
source: orders_by_region is orders_base -> {
  group_by: region_name
  aggregate: order_count is count()
}
```

```malloy
// Refused today: the same downstream over an inline extend of the stored base.
#(materialize) storage="lake"
source: orders_by_region is (orders_base extend { dimension: r is lower(region_name) }) -> {
  group_by: r
  aggregate: order_count is count()
}
```

The skill should say why. The build runs in the destination, which compares strings byte for byte.
Publisher checks that each string the downstream groups, orders or filters compares there as it
does live. It can follow that string only to a named stored upstream.

### Which PR carries what

| Item                                                                                                                                                                                               | PR                                                           |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| v2 spelling across skills, docs and SDK; root-first migration; verdict and path-report debugging                                                                                                   | #2065 (open), plus fix commit `369573945` to push            |
| `#@ preaggregate` read as `#(rollup)` with a warning                                                                                                                                               | #2069 (open). Skills change only by the one-line note below. |
| Named-base rule; "answers live" ladder; collation in `storage.md`; REST build route; `#@ preaggregate` synonym note; pointers in `malloy-model`, `malloy-modeling` and `malloy-source-unreachable` | **A new skills PR, stacked on #2065**                        |
| Dev-destination loop in the skill                                                                                                                                                                  | Lands with Phase 1b                                          |
| `estimating` reference, tuning thresholds, `what_if` and `compare`                                                                                                                                 | Phase 6 of the v2 plan (separate owners)                     |

---

## Part 3: the authoring journey end to end

Each step lists:

- what the author sees today;
- where it is confusing, silent, or needs a publish;
- the fix.

Priorities at the end rank them by how much they hurt an author. The examples come from one
production package on a hosted deployment, cited by the PR that fixed or is fixing each one.

### Step 1: annotate

**Today**

- Four spellings are live:
  - `#(materialize)` on a source;
  - `#@ persist` on a source (v1, planned natively by v2 per #2069's design);
  - `#(rollup)`;
  - `#@ preaggregate` (read as `#(rollup)` with `deprecated-preaggregate-annotation`).
- A misspelled route or directive fails the load (`materialization-near-miss`,
  `docs/materialization.md:1022`).
- Every key set is closed (`:1017`).

**Pain**

- The skill on main teaches v1 first (Part 2). An agent that follows it writes the spelling the
  authoring rule forbids.
- The shape rules are learned only from a refusal, after a build. The two worst:
  - root-first migration (`inherited_persist`);
  - the named base for string reads.

**Fix**

- #2065, plus the skills PR in Part 2.
- In the planner, the string-read refusal should name the shape and the remedy. See step 3.

### Step 2: compile and see findings

**Today**

- `compile_model` at scope `file` reports what the load would refuse or leave unbuilt when the
  text names a materialization annotation (`server/mcp/tools/compile_tool.ts:56`).
- At scope `package` it reports those refusals and unbuilt items for the whole package (`:57`).
- `reload_package` returns the package's `warnings` (`server/mcp/tools/reload_package_tool.ts:122-126`).
  Its description says that every package warning except the compile, render-tag and annotation
  findings "appears only here" (`:49`).

**Pain**

1. **Serve verdicts appear nowhere in the compile loop.** A source can compile, plan and build, and
   still answer live:
   - `served-copy-not-served`;
   - `notServedIn` reasons such as `cross_connection_join` and `fields_not_carried`;
   - a dry-run `servePath` of `live`.

   Neither `compile_model` nor `reload_package` returns the verdict or the dry-run `servePath`. The
   machinery exists: `POST …/packages?dryRun=true` answers `serveVerdicts` with `servePath`,
   compiled over bindings declared from the plan's output columns, with no table built
   (`api-doc.yaml:1501-1512`, `5149-5172`). An author reaches it only over REST, with a `location`.
   That is the "refusals that only showed up after publish" pain.

2. **Under a proxy connection, the plan refuses every `storage=` source for one reason.** The
   refusal is `storage_source_unsupported` with "the host builds it"
   (`source_connection_type.ts:47-58`). It hides any reason the host itself would refuse the
   source: author SQL inside a catalog (`catalogAuthorSqlMessage`, `:86-101`), a gate, a collation.
   The local answer is always "live, the host decides", so the author learns the real answer only
   after publishing.
3. **A frozen-config server refuses a dry run** (`api-doc.yaml:1516-1518`). A hosted cell cannot
   preview a publish on the worker that will serve it.

**Fix**

- **P0 [sign-off]:** `compile_model` at scope `package` returns `serveVerdicts` with `servePath`
  and `notServedIn`, computed by the dry-run's compile-only served copy. This is the same function
  `?dryRun=true` calls, run over the on-disk tree with the optional replacement file. It answers
  "will it serve?" before a save.
- **P0 [sign-off]:** `reload_package` and `get_status` gain a compact `materialization` block per
  package:
  - counts of `serves`, `awaiting_build` and `live`;
  - each `live` source with its `reason`, its first `fixes` entry, and its top `notServedIn` reason.

  An agent then sees "built but not served" without a REST call.

- **P1 [sign-off]:** when a source is refused because of the proxy, the plan still runs the host's
  other refusal checks, using the remote connection's type (the proxy resolves its dialect at
  runtime, `server/service/dialects/publisher.ts:1-8`). It reports them as `predictedRefusals` on
  the refused record: "the host would also refuse this: ...". A source with no predicted refusal
  says "the host would build it". With Phase 2 the author can then build it locally.
- **P2:** the hosted CLI's publish should call `?dryRun=true` first and print the verdicts. That is
  service-side; Publisher's part is decision 7 (let a frozen-config server run a dry run with a
  setting).

### Step 3: see serve verdicts and why a source answers live

**Today**

- `GET …/packages/{pkg}/status` carries `serveVerdicts`
  (`docs/materialization.md:1183-1260`).
- A query's REST response carries `servedFrom` and the path report `path`, with `considered`,
  `notConsidered` and `warnings` (`api-doc.yaml:8735-8860`).
- Live reasons are a documented table (`docs/materialization.md:1215-1230`).

**Pain**

1. **MCP `execute_query` returns neither `servedFrom` nor `path`.** Nothing in `server/mcp/`
   mentions either. An agent cannot tell a stored answer from a live one.
2. **Some messages give a reason that is not actionable:**
   - "its string reads were not recorded" (`string_reads.ts:99`) names no field, no upstream and
     no shape. A fix is in progress.
   - Collation refusals ("groups or compares 'x', which its upstream's table records as compared by
     equality only", `:116-121`) name the column but not the upstream source, the destination, or
     the remedy. #2073, open, adds join-key collation judging.
3. **Silent live answers have happened**, and each was a bug:
   - An import-of-import base answered live with no warning. #2071 fixed it.
   - "Access denied" came back with no reason. #2070 added `INHERITED_FILTER_UNVERIFIED`.

   The catch-all `notServedIn` reason `not_rerooted` ("when no warning names one",
   `api-doc.yaml:5225-5227`) is where the next silent case will hide.

**Fix**

- **P0 [sign-off]:** MCP `execute_query` returns `servedFrom` and a compact `path`:
  - `servedBy` (tier and table, or `live`);
  - for a live answer, the first `skipped` candidate's reason.

  Each is one short field, never the whole report.

- **P0:** every refusal message names three things:
  - **what:** the source, the column, and the upstream source by name;
  - **why:** the compare the destination needs and the compare recorded;
  - **what to do:** for example, "build it over a named base", "declare a collation on the
    upstream's column", or "materialize the upstream into the same destination".

  Specifically, replace "its string reads were not recorded" with a message that names the
  downstream, says the strings could not be followed to a named stored upstream, and gives the
  named-base remedy. Do this until the planner records reads through inline extends.

- **P1:** treat `not_rerooted` as a defect signal:
  - count it on `publisher_served_copy_bind_total`;
  - log it at WARN with the source;
  - add a conservation test asserting that every bound, materialized source in the test corpus gets
    either `serves` or a named reason.

  The pattern behind #2071 and #2070 is a path that answered live with no named reason. That test
  catches the next one before an author does.

### Step 4: build

**Today**

- `malloy-pub materialize --environment E --package P [--wait] [--force-refresh] [--reseed]`
  (`cli/index.ts:423-470`).
- It builds every source in the package.
- With `--wait` it polls (default timeout 120 s, `cli/commands/materializations.ts:138`), prints the
  whole run record as JSON (`:146`), and exits:
  - 0 when it reaches `MANIFEST_FILE_READY`;
  - 2 when the manifest did not load (`:150-162`);
  - 1 on failure.
- REST: `POST …/materializations`, which also takes `sourceNames`.
- There is no MCP build tool.

**Pain**

1. **No `--source`.** One edited source rebuilds, or at least re-plans, the package. Unchanged
   sources are reused by content address, but the author cannot narrow the run.
2. **The 120 s default timeout is short for a warehouse build.** On timeout the CLI says the build
   "may still be running", and the author re-runs it.
3. **"Built but not served" is invisible to the exit code** unless the manifest load failed.
   Suppose a table builds `READY` and the served copy declines it (verdict `live`, reason
   `served_copy`, which `docs/materialization.md:1233-1240` calls out). The CLI exits 0 and prints
   success.
4. **No per-source outcome.** The JSON record is the run's, not the sources'.
5. **No command to read the plan or the verdicts.** An author reaches for `curl`.

**Fix**

- **P0 [sign-off]:** `malloy-pub materialize` gains `--source <name>` (repeatable, sent as
  `sourceNames`) and, after Phase 2, `--sample <n>`.
- **P0 [sign-off]:** after a successful `--wait`, the CLI reads `serveVerdicts` and prints one line
  per source: verdict, tier, table, and reason with its first fix. It exits 2 when any source built
  in the run is `live`. That widens exit 2 from "manifest not loaded" to "built but not served".
- **P1 [sign-off]:** new `malloy-pub verdicts --environment E --package P`. It prints the same
  per-source table from `GET …/status` and adds `notServedIn`. With `--plan` it prints the build
  plan's sources and refusals instead. This is the "per-source report" the control plane's dev
  command prints.
- **P1 [sign-off]:** raise `--wait`'s default timeout to 900 s, the value `reference/tuning.md`
  already advises, or make it unlimited with Ctrl-C to stop waiting.
- **No MCP build tool for now.** A build spends warehouse money, and an agent can call the CLI or
  REST. Revisit once the dev destination exists, where a build is local and capped (decision 9).

### Step 5: iterate

**Today**

- In watch mode a save recompiles the package. A save that fails to compile keeps the previous
  model, marked `stale: true` (AGENTS.md section 6).
- An edit that moves a source's content address leaves it `awaiting_build`.
- A table left behind by a rename or tier move is reported `table-kept`.
- Hosted, every iteration is a publish.

**Pain**

- Locally the loop stops at `awaiting_build` unless the author builds, and under a proxy connection
  the author cannot build at all. That is Part 1.
- Hosted, an author pays a publish per iteration to see a verdict. Step 2's fixes remove most of
  those publishes.
- A watch-mode recompile reuses the columns it already has, so an upstream column change appears
  only after a reload (AGENTS.md section 6). Verdicts that depend on a column contract can lag
  behind a warehouse change until then.

**Fix**

- With Parts 1 and 2 in place, the local loop is:
  1. save;
  2. compile, with verdicts (step 2);
  3. `malloy-pub materialize --source X --wait`;
  4. read the per-source verdict, and query with `servedFrom` in the answer.

  No publish is needed until the author is satisfied.

- A table the dev destination holds is a local file. `table-kept` in the dev destination should be
  swept automatically on the next build, since no host owns it **[sign-off]**.

### Priorities, by author pain

| Priority | Item                                                                                                                                  | Step | Depends on                              |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------- | ---- | --------------------------------------- |
| P0       | Serve verdicts and `servePath` in `compile_model` (package scope), and a `materialization` block in `reload_package` and `get_status` | 2    | none                                    |
| P0       | `servedFrom` and a compact `path` in MCP `execute_query`                                                                              | 3    | none                                    |
| P0       | Refusal messages that name what, why and what to do (string reads, collation)                                                         | 3    | the string-reads fix in progress; #2073 |
| P0       | CLI `--source`; a per-source verdict after `--wait`; exit 2 for built but not served                                                  | 4    | none                                    |
| P0       | Skills PR (named base, answers-live ladder, pointers)                                                                                 | 1    | #2065 merged                            |
| P1       | Dev destination: Phase 1a, then 1b                                                                                                    | 2-5  | attach-concurrency proof                |
| P1       | `predictedRefusals` under a proxy connection                                                                                          | 2    | none                                    |
| P1       | `not_rerooted` as a defect signal and conservation test                                                                               | 3    | none                                    |
| P1       | `malloy-pub verdicts`; a longer `--wait` default                                                                                      | 4    | none                                    |
| P2       | Proxy passthrough (Phase 2)                                                                                                           | 4    | Phase 1b, gateway limits                |
| P2       | Dry run under a frozen config, behind a setting                                                                                       | 2    | decision 7                              |
| P3       | Option B (DuckLake on a DuckDB-file catalog)                                                                                          | 2-5  | evidence that A's fidelity gap matters  |

---

## Decisions for Kyle

Each decision lists its options. The recommendation is in bold.

1. **Local destination type.**
   - (a) **A plain DuckDB file (`type: "duckdb"`): most of the path exists.**
   - (b) DuckLake on a DuckDB-file catalog: closer to hosted, with a new catalog arm on the wire.
   - (c) Both now.
2. **Who may declare a `duckdb` destination.**
   - (a) **Only while `PUBLISHER_PERSIST_DEV_DESTINATION` is set.**
   - (b) Anyone, documented as single-process.
3. **The override setting's name and kind.**
   - (a) **Key `persist.devDestination` (`PUBLISHER_PERSIST_DEV_DESTINATION`,
     `--persist-dev-destination`), variable and flag only, by a new `fileForbidden` property.**
   - (b) The same, filed under `kind: "launch"`.
   - (c) Readable from the file like any setting.
4. **The hosted guard.** The override requires `watchEnvironments` at launch and a local
   destination, refuses `frozenConfig`, and is refused alongside a `manifestLocation`, a manifest
   PATCH or `buildInstructions` (409 `DEV_DESTINATION_ACTIVE`). Also:
   - (a) **Require both listeners on loopback.**
   - (b) Do not check the bind, so a developer can run it in Docker.
5. **Gates under the override.**
   - (a) **Judge a database-tier gated source by the database tier's rules, so the local verdict
     predicts the hosted one.**
   - (b) Judge by the storage tier's rules: every gated source answers live locally.
6. **Above the proxy row cap.**
   - (a) **Refuse by default (`proxy_rows_over_cap`), with an explicit `--sample N` that marks
     every table and answer `sampled`.**
   - (b) Always sample at the cap.
   - (c) Refuse only, with no sampling.
7. **Dry run on a frozen-config server.**
   - (a) **Add a setting that lets a frozen server answer `?dryRun=true`, so a hosted cell can
     preview on the worker that will serve.**
   - (b) Keep refusing, and the control plane dry-runs on an unfrozen worker.
8. **"Built but not served" in the CLI.**
   - (a) **Exit 2 when any source the run built is `live`, widening exit 2's meaning.**
   - (b) A new exit code 3 for it, and exit 2 keeps its current meaning.
9. **An MCP build tool.**
   - (a) **Not now: CLI and REST only. Revisit once the dev destination makes builds local and
     capped.**
   - (b) Add `materialize_package`, limited to the dev destination.
10. **Serve verdicts in the compile loop.**
    - (a) **`compile_model` at scope `package` returns `serveVerdicts` with `servePath`, and
      `reload_package` and `get_status` return a compact block.**
    - (b) Only the compact block on reload and status.
11. **Proxy refusals.**
    - (a) **Run the host's other refusal checks and report them as `predictedRefusals`.**
    - (b) Keep the single `storage_source_unsupported` refusal.
12. **Where the new skills content goes.**
    - (a) **One new skills PR stacked on #2065: named base, answers-live ladder, collation in
      `storage.md`, the REST build route, the `#@ preaggregate` synonym note, and pointers. The dev
      loop is added with Phase 1b.**
    - (b) Fold all of it into #2065.
