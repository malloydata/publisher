<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Package versions

A package can be published as a series of immutable versions, each numbered by the `version` in its own `publisher.json`. Every published version keeps serving the content it was published with. A request names the version it wants with `versionId`, and a request that names none is served from the package's `latest`. Publishing a new version, moving `latest` back to roll back, and archiving a version nobody uses any more are all API calls. None of them edits a version's files.

The feature is off by default. With it off, a package is the single mutable slot it has always been, and nothing on this page applies to it.

## Turn it on

Two settings, each read from `publisher.config.json` and overridden by an environment variable. A value other than the ones listed fails startup rather than falling back.

| Setting | Env var | Values | Default |
| --- | --- | --- | --- |
| `packageVersioning` | `PUBLISHER_PACKAGE_VERSIONING` | `off`, `on` | `off` |
| `versionPromotion` | `PUBLISHER_VERSION_PROMOTION` | `on-publish`, `explicit` | `on-publish` |

```json
{
  "packageVersioning": "on",
  "versionPromotion": "on-publish",
  "environments": [ ... ]
}
```

`packageVersioning` decides only what a publish does. Reads resolve versions whatever it is set to, so a server restarted with it off still serves every version it published.

## Publish a version

With versioning on, `POST /api/v0/environments/{env}/packages` with a `location` publishes a version. The request takes no version: the version is the `version` field of the package's `publisher.json`, the way `npm publish` reads `package.json`, so bumping that field is the release.

```bash
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages \
  -H 'content-type: application/json' \
  -d '{"name": "sales", "location": "/srv/packages/sales"}'
```

| What you publish | Answer |
| --- | --- |
| A new version | 200. It is recorded, loaded and checked like any publish. A version that fails the checks is refused with 400, and the versions already published keep serving. |
| A version already published, with the same content | 200, and nothing is written. Re-loading a version onto a server that already holds it is safe to retry. |
| A version already published, with different content | 409 `VERSION_CONFLICT`. Bump the version. |
| A version that differs from a published one only by letter case | 409 `VERSION_CONFLICT`: a case-insensitive filesystem cannot keep the two apart. |
| No `version`, or one that is not a semantic version | 400 `MANIFEST_VERSION_MISSING` or `MANIFEST_VERSION_INVALID`. |
| An archived version, again | 410 `VERSION_ARCHIVED`. Unarchive it instead. |
| Into a package watch mode mounts in place | 400. A watch mount is your source directory, which is never immutable. |

"The same content" is a hash of the package tree: every file's path and bytes, and every symlink's target text, with `.git` skipped.

The first versioned publish of a package that was published unversioned moves the old tree aside, and puts it back if the publish fails. After that, the package is versioned: an unversioned publish, `PATCH …/packages/{pkg}`, model and dashboard writes, and `?reload=true` either refuse with 409 `PACKAGE_IS_VERSIONED` or, for a reload, return the version unchanged.

A version is a semantic version: `1.2.0`, `1.2.0-rc.1` and `1.2.0+build.7` are all valid, by the same pattern the Credible control plane checks. On disk a version lives at `publisher_data/<env>/<package>/<version>/`, with `+` written as `_`.

## Which version a request gets

Every route that reaches into a package takes `versionId`: queries (in the body), models, dashboards, notebooks, data apps, compile, the package's own connections, and materializations. An omitted or empty `versionId` means `latest`.

| Request | Answer |
| --- | --- |
| A published version | That version. |
| Nothing | `latest`. |
| A version the package does not have | 404 `VERSION_NOT_FOUND`. |
| An archived version | 410 `VERSION_ARCHIVED`. |
| Any version, of a package with no versions | 404 `VERSION_NOT_FOUND`. Its one tree never answers as if it were the version you asked for. |

The package resource says which version answered: `versionId` is the version described, and `latestVersion` is the package's `latest`.

`GET /api/v0/status` lists every version each package holds, not only `latest`. A loaded version has its full entry; one that is not loaded (the version that stopped being `latest`, or an archived one) is listed with `loaded: false` and its `archiveStatus`, because the server still holds it and serves it when it is next named. The status also reports the two settings, `packageVersioning` and `versionPromotion`, so an orchestrator can tell which servers take native versions.

`GET …/packages/{pkg}/versions` lists the versions, highest first, each with its `latest` flag, `archiveStatus`, content hash, location and manifest binding. `GET …/versions/{versionId}` reads one.

### Static files and data apps

A data app's page is opened at `…/packages/{pkg}/index.html?versionId=1.2.0`, and its links carry the version the list was read from. The page's own relative requests (`./app.js`) do not carry a query string, so a request without `versionId` takes the version from its `Referer` when that Referer is a page of the same package. Those responses are sent with `Vary: Referer` and `Cache-Control: no-cache`.

That covers what the page itself loads. A file loaded by another file, such as a stylesheet's `url()` or an ES module's `import`, has that file as its Referer, which carries no version, so it is served from `latest`. A page that must stay on one version end to end should put `?versionId=` on those URLs itself, or keep its assets in files the page loads directly.

For a package with no versions, `versionId` on a static file is ignored rather than refused. A proxy that resolves versions itself, and serves each one as its own package, forwards the page's query string unchanged, and refusing it would break every page that proxy serves.

## Latest

`latest` is a single pointer, moved by compare-and-swap.

- Under `versionPromotion: "on-publish"`, a publish moves `latest` to the new version unless a higher version already is `latest`. A version that differs from `latest` only by build metadata (`1.2.0+b2` after `1.2.0+b1`) takes it, because it is the later build. A re-publish of identical content moves `latest` only to a strictly higher version, never back to an older build.
- Under `"explicit"`, a publish never moves `latest`. A version is servable by name as soon as it is published, and a request that names none gets 404 until something sets `latest`. This is the mode for an orchestrator, such as the Credible control plane, that decides when a version is ready.

`PUT …/packages/{pkg}/latest` with `{"versionId": "1.1.0"}` sets it, in either mode. The version must exist and must not be archived. It is loaded before the pointer moves, so a version that cannot load never becomes `latest`. Pointing `latest` at an earlier version is a rollback; under `on-publish`, the next publish of a higher version moves it forward again.

The version that stops being `latest` is unloaded, and loads again the next time a request names it.

## Bind a version to a build manifest

`PUT …/versions/{versionId}/manifest` with `{"manifestLocation": "gs://…/manifest.json"}` binds that one version's persist references to an externally computed build manifest, and `null` clears the binding so the version serves live. The binding is recorded with the version and survives an unload or a restart. The response is the version's package resource, which reports the outcome in `manifestBindingStatus` and `boundManifestUri`.

This replaces `PATCH …/packages/{pkg}` for a versioned package. That route is deprecated, and refuses a versioned package with 409.

## Archive and unarchive

`PATCH …/versions/{versionId}` with `{"archiveStatus": "archive"}` takes a version out of service. Reads that name it answer 410 `VERSION_ARCHIVED`, and it is unloaded. `"unarchive"` puts it back; it loads on its next read.

- The package's `latest` cannot be archived (409 `VERSION_IS_LATEST`). Move `latest` first.
- An archive is refused with 409 `VERSION_BUILDING` while a materialization of that version is running. A run that starts just after the archive fails, and what it built is reclaimed once it ends.
- Sending the state a version is already in changes nothing.
- **The version's files stay on disk.** See [Disk growth](#disk-growth).

## Materializations

A materialization of a versioned package builds one version: the one `versionId` names, or `latest`. The run records it in `metadata.versionId`, along with the package's materialization scope in `metadata.scope`. The materialization list, get, stop and delete routes take `versionId` too. A list without one shows `latest`'s runs. Every version's list also shows the runs from before the package's first versioned publish, which still drive what a `scope: "package"` version serves.

Who owns the tables a run builds is the package's materialization scope (`"materialization": { "scope": … }` in `publisher.json`):

- **`scope: "version"`.** Each version builds into tables of its own: a self-assigned name gains the version (`order_summary__v1_2_0`), skip-if-unchanged reuses only that version's earlier runs, and each version serves from its own tables after a restart. Archiving the version reclaims the tables its auto-runs built, in the background, keeping any table another run still references. A run whose table could not be dropped (an unreachable warehouse, say) is kept, marked `FAILED` and holding only the tables still owed, and the reclaim is tried again at the next archive and every time the server starts. A long name is cut and given a short hash so that its table segment, suffix included, stays within 50 characters (leaving room for the 13-character staging suffix a build adds), so a dialect that truncates identifiers (Postgres, at 63) never folds two versions into one table. The package's other versions never read these runs: they are this version's alone.
- **`scope: "package"` (the default).** The versions share the package's tables under their usual names. An auto-run builds `latest`, and an auto-run of another version is refused with 400, because it would rebuild the table `latest` serves. To build another version, pass `buildInstructions` with table names of your own. After `latest` builds, every other loaded version is rebound to the new run. A version whose source is defined the same way keeps the shared table, and one whose source is defined differently serves live, never another version's table. The same holds for a version that loads after `latest` built: it binds only the tables its own definitions produce. While `latest`'s run is rebuilding shared tables, and for good if it fails or is stopped, the other versions serve those tables live: the run records which tables it is rebuilding before it starts, and until a run commits, no version binds a table whose content that record leaves in doubt. The next run rebuilds such a table rather than reusing it.

One run is active per package at a time, whichever version it builds. A schedule builds only `latest`.

The tables of an orchestrated run (`buildInstructions`) are never reclaimed by the publisher: their names, and when they go, belong to the caller that assigned them.

## Disk growth

A published version's tree is never deleted while the package exists. Archive takes a version out of memory and reclaims its `scope: version` tables, but its files stay, so an archived version can be unarchived without being published again. The only thing that removes version trees is deleting the package, which removes all of them.

So a package's disk use grows by one tree per version published, and nothing shrinks it:

> disk per package ≈ (size of one version's tree) × (versions published since the package was created)

Examples, for one package on one server:

| Tree size | Publishes | After 30 days | After a year |
| --- | --- | --- | --- |
| 2 MB (models only) | 5 a day | 300 MB | 3.7 GB |
| 20 MB (models and some data) | 5 a day | 3 GB | 37 GB |
| 200 MB (bundled data files) | 1 a day | 6 GB | 73 GB |

The Credible control plane archives versions automatically after 30 days (`autoArchiveTtl`). On the worker that bounds memory and materialized tables, but not disk: a worker that has held a package for a year holds every version published in that year.

Size the volume under `publisher_data/` for the retention you need, and keep large data out of package trees: a connection to a warehouse, or a DuckLake destination, holds data once, while a bundled file is copied into every version. Delete and re-publish a package to start its history over.

## Limits worth knowing

- **Loaded versions.** Any version a request names is loaded and stays loaded until it is archived or the package is deleted. Only the version that stops being `latest` is unloaded on its own. The memory governor's 503 (see [configuration.md](configuration.md)) is what bounds the number loaded at once.
- **Symlinks in a published tree.** The content hash records a symlink's target text, not what it points at. A tree copied from a local directory has its relative symlinks rewritten as absolute ones pointing back into that directory, so content behind a link can change after publish without changing the hash. Do not publish trees that rely on symlinks.

## Restart

Versions, `latest`, archive state and manifest bindings are recorded in the publisher's database, and are read back before anything loads. A version whose tree has gone missing from disk is fetched again from the location it was published from, and served only if it hashes to what was published.
