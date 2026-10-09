<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Package versions

A package published from a location is a series of immutable versions, each numbered by the `version` in its own `publisher.json`. Every published version keeps serving the content it was published with. A request names the version it wants with `versionId`, and a request that names none is served from the package's `latest`. Publishing a version, moving `latest` back to roll back, and archiving a version nobody uses any more are all API calls. None of them edits a version's files.

A package registered from a directory without a location, or loaded from `publisher.config.json`, has no versions. It is the single mutable slot it has always been, and nothing on this page applies to it.

## Publish a version

`POST /api/v0/environments/{env}/packages` with a `location` publishes a version. The request takes no version: the version is the `version` field of the package's `publisher.json`, the way `npm publish` reads `package.json`, so bumping that field is the release. A package scaffolded with `npm create @malloy-publisher/malloy-package` starts at `0.1.0`.

```bash
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages \
  -H 'content-type: application/json' \
  -d '{"name": "sales", "location": "/srv/packages/sales"}'
```

| What you publish                                                | Answer                                                                                                                                                             |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| A new version                                                   | 200. It is recorded, loaded and checked like any publish. A version that fails the checks is refused with 400, and the versions already published keep serving.    |
| A version already published, with the same content              | 200, and nothing is written. Placing a version onto a server that already holds it is safe to retry. A `manifestLocation` on that request binds the version to it. |
| A version already published, with different content             | 409 `VERSION_CONFLICT`. Bump the version.                                                                                                                          |
| A version that differs from a published one only by letter case | 409 `VERSION_CONFLICT`: a case-insensitive filesystem cannot keep the two apart.                                                                                   |
| No `version`, or one that is not a semantic version             | 400 `MANIFEST_VERSION_MISSING` or `MANIFEST_VERSION_INVALID`.                                                                                                      |
| An archived version, again                                      | 410 `VERSION_ARCHIVED`. Unarchive it instead.                                                                                                                      |
| Into a package watch mode mounts in place                       | 400. A watch mount is your source directory, which is never immutable.                                                                                             |
| A `manifestLocation` that is not a `gs://` or `s3://` URI       | 400.                                                                                                                                                               |

"The same content" is a hash of the package tree: every file's path and bytes, and every symlink's target text, with `.git` skipped.

A version is a semantic version: `1.2.0`, `1.2.0-rc.1` and `1.2.0+build.7` are all valid. On disk it lives at `publisher_data/<env>/<package>/<version>/`, with `+` written as `_`. The publish downloads into a staging folder, places the tree, compiles and checks it there, and records it in one database transaction; a failure removes what it placed.

The first versioned publish of a package that had an unversioned tree moves that tree aside, and puts it back if the publish fails. After that the package is versioned: an unversioned publish, a model or dashboard write, and installing over it answer 409 `PACKAGE_IS_VERSIONED`, and `?reload=true` (or the MCP `reload_package` tool) returns the version unchanged.

## Which version a request gets

Every route that reaches into a package takes `versionId`: the package itself, models, queries, compile, dashboards, notebooks, databases, data apps, events, the package's own connections, static files and materializations. An omitted or empty `versionId` means `latest`; empty is what a proxy forwarding a page's query string may send.

| Request                                                       | Answer                                                                                   |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| A published version                                           | That version.                                                                            |
| Nothing, or an empty value                                    | `latest`.                                                                                |
| A value that is not a semantic version                        | 400 `VERSION_ID_INVALID`.                                                                |
| A version the package does not have                           | 404 `VERSION_NOT_FOUND`.                                                                 |
| An archived version                                           | 410 `VERSION_ARCHIVED`.                                                                  |
| A versioned package with no `latest` yet, naming nothing      | 404 `VERSION_NOT_FOUND`.                                                                 |
| Any version, of a package with no versions                    | 404 `VERSION_NOT_FOUND`. Its one tree never answers as if it were the version asked for. |
| A version whose files are missing and cannot be fetched again | 424.                                                                                     |

A query takes `versionId` in its body; one in the URL's query string is read too, and when both name a version they must agree (400 otherwise). Build metadata must be percent-encoded in a URL: `1.2.0+build.5` is sent as `1.2.0%2Bbuild.5`, since a bare `+` decodes to a space. The SDK does this for you.

The package resource says which version answered: `versionId` is the version described, and `latestVersion` is the package's `latest`. A package listing names every package once, as its `latest`, and refuses a non-empty `versionId` with 400.

`GET /api/v0/status` lists every version each package holds, loaded or not, each with its `versionId`, `archiveStatus` and whether it is loaded, so a reconcile never mistakes a version that is not in memory for one that is missing. It also reports `packageVersioning` (`on`) and `versionPromotion`.

`GET …/packages/{pkg}/versions` lists the versions, highest first, each with its `latest` flag, `archiveStatus`, content hash, location, description, manifest binding and the times it was promoted to and demoted from `latest`. `GET …/versions/{version}` reads one.

### Static files and data apps

A data app's page is opened at `…/packages/{pkg}/index.html?versionId=1.2.0`, and its links carry the version the list was read from. The bundled runtime pins the page's queries to that version.

Only the URL pins a version. The page's own relative requests (`./app.js`, a stylesheet's `url()`) carry no query string, so they are served from `latest`. A page that must stay on one version end to end puts `?versionId=` on those URLs itself.

A `versionId` on a static file pins only a version the package has; anything else serves `latest` rather than 404. A proxy that resolves versions itself may forward a page's query string unchanged, with a `versionId` of its own, and refusing it would break every page it serves. A pinned version that is archived answers 410.

## Latest

`latest` is a single pointer per package, moved by compare-and-swap. A version that stops being `latest` stays loaded; only archiving it, or deleting the package, unloads it.

`versionPromotion` decides what a publish does to it. It is set in `publisher.config.json`, or with `PUBLISHER_VERSION_PROMOTION`, which wins; a value other than these two fails startup.

- **`on-publish` (the default).** A publish moves `latest` to the new version unless a higher version already is `latest`. A version that differs from `latest` only by build metadata (`1.2.0+b2` after `1.2.0+b1`) takes it, because it is the later build. A re-publish of identical content moves `latest` only to a strictly higher version, never back to an older build.
- **`explicit`.** A publish never moves `latest`. A version is servable by name as soon as it is published, and a request that names none gets 404 until something sets `latest`. This is the mode for an orchestrator that decides when a version is ready.

`PUT …/packages/{pkg}/latest` with `{"version": "1.1.0"}` sets it, in either mode. The version must exist (404) and must not be archived (410), and it is loaded before the pointer moves, so a version that cannot load never becomes `latest`. Pointing `latest` at an earlier version is a rollback; under `on-publish`, the next publish of a higher version moves it forward again.

## Bind a version to a build manifest

`PUT …/versions/{version}/manifest` with `{"manifestLocation": "gs://…/manifest.json"}` binds that one version's persist references to an externally computed build manifest, and `null` clears the binding so the version serves live. Only `gs://` and `s3://` URIs are accepted. The binding is recorded with the version and survives an unload or a restart. An archived version is refused before anything is written. The response is the version's package resource, which reports the outcome in `manifestBindingStatus` and `boundManifestUri`.

### The deprecated PATCH, on a versioned package

`PATCH …/packages/{pkg}` is deprecated. On a versioned package it keeps working for the changes that are not content, for clients that rebind through it:

- `manifestLocation` binds the package's `latest` version, as `PUT …/versions/{latest}/manifest` would.
- `description` sets the package's own description. It reads back on the package and in listings; with none set, a package reads as its `latest` version's description. Each version keeps its own.

Clients send whole objects, so a body that echoes the package back is accepted: read-only fields are ignored, and a field that is unset (null, absent, an empty list or object) or carries the value `latest` has now is not a change. A field that would change `latest`'s content is refused with 409 `PACKAGE_IS_VERSIONED`: publish a new version instead.

## Archive and unarchive

`PATCH …/versions/{version}` with `{"archiveStatus": "archive"}` takes a version out of service. Reads that name it answer 410 `VERSION_ARCHIVED`, and it is unloaded. `"unarchive"` puts it back; it loads on its next read, and serves live until it is built again. Sending the state a version is already in changes nothing.

An archive is refused with 409 when:

- the version is the package's `latest` (`VERSION_IS_LATEST`). Move `latest` first;
- it is the package's last version in service (`VERSION_IS_LAST_ACTIVE`). To take the whole package out of service, delete it;
- a materialization of that version is running (`VERSION_BUILDING`), since the archive would reclaim the tables it is writing. A run starts under the version's lock, so either the run is refused (410) or the archive is.

**The version's files stay on disk.** See [Disk growth](#disk-growth).

There is no error state: a version that cannot load is simply refused as a `latest` target, and an orchestrator that tracks failures keeps its own.

## Materializations

A materialization of a versioned package builds one version: the one `versionId` names (in the create body), or `latest`. The run records it, in `metadata.versionId`, with the package's materialization scope in `metadata.scope`. The list, get, stop and delete routes take `versionId` too:

- A list holds one version's runs (or `latest`'s), together with the runs from before the package's first versioned publish. A versioned package with no `latest` yet lists every run.
- A run's id is unique, so get, stop and delete find a run whichever version built it. Naming a version also requires the run to be that version's, or one from before any version.

Who owns the tables a run builds is the package's materialization scope (`"materialization": { "scope": … }` in `publisher.json`).

**`scope: "version"`.** Each version builds into tables of its own: a self-assigned name gains the version (`order_summary__v1_2_0`, plus 8 hex of the version's hash for a pre-release or build version), skip-if-unchanged reuses only that version's earlier runs, and each version serves from its own tables, including after a restart. A long name is cut and given a short hash so its table segment stays within 50 characters, so a dialect that truncates identifiers (Postgres, at 63) never folds two versions into one table.

Archiving the version reclaims the tables its auto-runs built, in the background, keeping any table another run still references, and deletes those runs. A run whose table could not be dropped (an unreachable warehouse, say) is kept, marked `FAILED` and holding only the tables still owed; the reclaim is tried again at the next archive and every time the server starts. A version's package-local `duckdb` is in memory, so its tables went with the unload and there is nothing to drop there.

**`scope: "package"` (the default).** The versions share the package's tables under their usual names.

- An auto-run builds `latest`. An auto-run of another version is refused with 400 `VERSION_NOT_LATEST`, because it would rebuild the table `latest` serves. To build another version, pass `buildInstructions` with table names of your own.
- A version binds a stored table only when its own definition built it: same source and same content address. A version whose source is defined differently serves that source live, never another version's rows.
- Before `latest`'s run rebuilds a shared table, it marks every older run's entry for that table as superseded (`metadata.supersededTables`), then rebinds every other loaded version. From then on no version binds that table from an older run, whatever happens to this run: it may fail half-way, be stopped, or have its record deleted. After it commits, the other versions are rebound to it. A version that cannot be rebound is unloaded, and binds from the store when it next loads.

**Active runs.** An auto-run under `scope: "package"` writes the shared tables, so one runs per package. A run under `scope: "version"`, or one with `buildInstructions`, holds its own version's slot, so versions build side by side. Two runs never write one table at once: a run with instructions names its tables when it starts, an auto-run of shared tables claims them before it builds, and a run that would write a table another active run writes answers 409. A run with instructions may not name the tables the versions share, either. The tables of a run with instructions are the caller's: the publisher never reclaims them, and another version never reads that run.

**Schedules.** A `materialization.schedule` is legal only under `scope: "version"`, so each version is scheduled on its own. The standalone scheduler (`PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER`) sweeps every version in service, reading its schedule from its `publisher.json` without loading it, and loads a version (through the memory governor, like any load) only when its cron comes due. Archiving a version takes it out of the sweep; unarchiving it arms it again. A version bound to a build manifest is left to whoever built that manifest.

## Disk growth

A published version's tree is never deleted while the package exists. Archive takes a version out of memory and reclaims its `scope: version` tables, but its files stay, so an archived version can be unarchived without being published again. The only thing that removes version trees is deleting the package, which removes all of them, with its version rows and materialization records. It does not drop the tables those versions built, as deleting an unversioned package never has: once the records are gone nothing names them, so reclaim a package's tables by archiving its versions, or with `DELETE …/materializations/{id}?dropTables=true`, before deleting it.

So a package's disk use grows by one tree per version published, and nothing shrinks it:

> disk per package ≈ (size of one version's tree) × (versions published since the package was created)

For one package on one server:

| Tree size                    | Publishes | After 30 days | After a year |
| ---------------------------- | --------- | ------------- | ------------ |
| 2 MB (models only)           | 5 a day   | 300 MB        | 3.7 GB       |
| 20 MB (models and some data) | 5 a day   | 3 GB          | 37 GB        |
| 200 MB (bundled data files)  | 1 a day   | 6 GB          | 73 GB        |

An orchestrator that archives versions after a period (30 days, say) bounds memory and materialized tables, not disk: a server that has held a package for a year holds every version published in that year. A server whose `publisher_data/` does not survive a restart starts empty each time and fetches versions again as they are named.

Size the volume under `publisher_data/` for the retention you need, and keep large data out of package trees: a connection to a warehouse, or a DuckLake destination, holds data once, while a bundled file is copied into every version. Delete and re-publish a package to start its history over.

## Limits worth knowing

- **Loaded versions.** A version stays loaded from its first read until it is archived or its package is deleted, whether or not it is `latest`. Nothing evicts a version that is rarely read. The memory governor's 503 (see [configuration.md](configuration.md)) is what bounds the number loaded at once.
- **Symlinks in a published tree.** The content hash records a symlink's target text, not what it points at. A tree copied from a local directory has its relative symlinks rewritten as absolute ones pointing back into that directory, so content behind a link can change after publish without changing the hash. Do not publish trees that rely on symlinks.
- **Legacy `/projects` routes** do not take versions: a `versionId` on them answers 501. Use the `/environments` routes.
- **One process per store.** The checks that keep two runs from writing one table, and a run from overlapping an archive, are held in the server's memory. Run one publisher per `publisher.db`.

## Restart

Versions, `latest`, archive state, manifest bindings and the package's own description are recorded in the publisher's database, and read back before anything loads. At startup the publisher removes a version folder no row owns (a publish that never committed) and restores an unversioned tree a first versioned publish had moved aside. A version whose tree has gone missing is fetched again from the location it was published from, and served only if it hashes to what was published; if it cannot be, the version answers 424, and the fetch is not retried on every request.

## What this does not do yet

- **Migrations.** The `versions` table and the two new columns are added in place, the way every other table is created; there is no migration runner to step through.
- **A compiled-model cache keyed by version.** Each loaded version is compiled once, in memory, when it loads; a restart compiles it again.
