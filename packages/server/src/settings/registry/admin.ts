// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { setting } from "../setting";
import { integer, oneOf, string } from "../types";

/** The configuration design's `admin` table: the admin API's settings. */
export const admin = [
   setting({
      key: "admin.port",
      type: integer({ min: 0, max: 65535 }),
      description: "Port of the admin API; unset turns the listener off.",
      notes: "Port for the admin API (`admin-api-doc.yaml`): the operator's surface: the DuckLake catalogs in `PUBLISHER_ADMIN_CATALOG_DATABASE` as resources (create a catalog, load a file into a table, list and drop tables, reclaim old snapshots' storage), and watch mode as one resource, `GET`/`PUT`/`DELETE /watch-mode` (the main API's `/watch-mode/status`, `start` and `stop` are deprecated). Off unless set. Unauthenticated, like the main API. See [ducklake.md](ducklake.md#managing-ducklake-catalogs-the-admin-api).",
      defaultText: "_unset_ (off)",
      aliases: {
         env: ["PUBLISHER_ADMIN_PORT"],
         flags: ["--admin_port"],
      },
   }),
   setting({
      key: "admin.host",
      type: string(),
      // Not host: moving the main API onto every interface must not expose
      // the admin operations with it.
      default: "127.0.0.1",
      description: "Host the admin API binds to; {host} does not move it.",
      notes: "Host binding for the admin API. Deliberately not moved by `PUBLISHER_HOST`: putting the main API on every interface does not expose the admin operations. Widen it only inside the network the orchestrator calls from, never behind a public ingress.",
      aliases: { env: ["PUBLISHER_ADMIN_HOST"], flags: ["--admin_host"] },
   }),
   setting({
      key: "admin.requestTimeoutMs",
      type: integer({ min: 1 }),
      default: 600_000,
      description:
         "Ms the admin API keeps a request, its headers and an idle keep-alive open.",
      defaultText: "`600000` (10 min)",
      notes: "How long the admin listener lets a request run before closing its connection. A table copy or a file load answers only when the table is written, so a large table needs longer than the main API's ten minutes; raise this for the caller's largest copy, and keep the caller's own timeout above it. Applies to the admin listener alone: the main API and MCP listeners keep ten minutes.",
   }),
   // Strings because DuckDB parses them; assertDuckDBResourceConfig checks them at boot.
   setting({
      key: "admin.memoryLimit",
      type: string(),
      default: "1.5GB",
      description:
         "Most memory one admin table load or copy session may use, as a DuckDB size (1.5GB).",
      notes: "The `memory_limit` of the DuckDB session an admin table load or copy runs in, as a flat DuckDB size (`1.5GB`, `512MB`). The session uses the lower of this and `PUBLISHER_DUCKDB_MEMORY_LIMIT`, so a pod whose general limit is smaller is never raised by the default: to give a load more than 1.5GB, raise both. A keyed load sizes its comparison passes to half of this. Validated at startup.",
   }),
   setting({
      key: "admin.maxTempDirectorySize",
      type: string(),
      default: "6GiB",
      description:
         "Most spill one admin table load or copy session may write, as a DuckDB size (6GiB).",
      notes: "The `max_temp_directory_size` of the DuckDB session an admin table load or copy runs in, as a flat DuckDB size (`6GiB`, `20GB`). Keep it under the pod's ephemeral storage. A load that outgrows it answers `500` and a retry is safe. Validated at startup.",
   }),
   setting({
      key: "admin.catalogDatabase",
      type: string(),
      secret: true,
      description:
         "libpq connection string of the database the admin API finds DuckLake catalogs in.",
      notes: "A libpq connection string (keyword or `postgres://` form) for the Postgres database holding the catalogs the admin API manages. Unset, the admin API's catalog routes answer `503 CATALOG_DATABASE_NOT_CONFIGURED`. The string also opens the connection that holds a table load's advisory lock, so it must name one host (no `service`, `hostaddr` or list of hosts, and no `PGSERVICE` or `PGHOSTADDR` in the environment; either fails startup). It also refuses what the lock connection cannot honor: `channel_binding=require`, `gssencmode=require`, any `require_auth`, `sslcrl`, `sslcrldir`, an `ssl_min_protocol_version` of `TLSv1.3` (from the string or the matching `PG*` variable), a missing or unreadable `sslrootcert`, `sslcert` or `sslkey` file, and `sslmode=verify-ca` with no `sslrootcert` while `NODE_EXTRA_CA_CERTS` is unset. It must reach Postgres directly or through a session-mode pooler, not a transaction-mode one such as PgBouncer.",
      aliases: { env: ["PUBLISHER_ADMIN_CATALOG_DATABASE"] },
   }),
   setting({
      key: "admin.storage.provider",
      type: oneOf(["s3", "s3_credential_chain", "gcs"]),
      description:
         "Object store the catalogs' data paths are read and written with.",
      notes: "The credential the admin API reads and writes catalogs' data paths with: `s3` (with `PUBLISHER_ADMIN_STORAGE_KEY_ID`, `PUBLISHER_ADMIN_STORAGE_SECRET`, and optionally `PUBLISHER_ADMIN_STORAGE_REGION` and `PUBLISHER_ADMIN_STORAGE_ENDPOINT`), `s3_credential_chain` (the host's identity; optional region), or `gcs` (an HMAC key pair in the key id and secret variables). An unknown value or missing key fails startup.",
      defaultText: "_unset_ (local paths)",
      aliases: { env: ["PUBLISHER_ADMIN_STORAGE_PROVIDER"] },
   }),
   setting({
      key: "admin.storage.keyId",
      type: string(),
      description: "Access key id (s3) or HMAC key id (gcs).",
      // Not a secret (the design gives it a flag), but kept out of the log.
      masked: true,
      aliases: { env: ["PUBLISHER_ADMIN_STORAGE_KEY_ID"] },
   }),
   setting({
      key: "admin.storage.secret",
      type: string(),
      secret: true,
      description: "Secret access key (s3) or HMAC secret (gcs).",
      aliases: { env: ["PUBLISHER_ADMIN_STORAGE_SECRET"] },
   }),
   setting({
      key: "admin.storage.region",
      type: string(),
      description: "Region of the s3 bucket.",
      aliases: { env: ["PUBLISHER_ADMIN_STORAGE_REGION"] },
   }),
   setting({
      key: "admin.storage.endpoint",
      type: string(),
      description: "Endpoint of an s3-compatible store.",
      aliases: { env: ["PUBLISHER_ADMIN_STORAGE_ENDPOINT"] },
   }),
];
