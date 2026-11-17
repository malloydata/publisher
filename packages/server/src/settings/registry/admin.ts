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
   }),
   setting({
      key: "admin.host",
      type: string(),
      // Not host: moving the main API onto every interface must not expose
      // the admin operations with it.
      default: "127.0.0.1",
      description: "Host the admin API binds to; {host} does not move it.",
      notes: "Host binding for the admin API. Deliberately not moved by `PUBLISHER_HOST`: putting the main API on every interface does not expose the admin operations. Widen it only inside the network the orchestrator calls from, never behind a public ingress.",
   }),
   setting({
      key: "admin.catalogDatabase",
      type: string(),
      secret: true,
      description:
         "libpq connection string of the database the admin API finds DuckLake catalogs in.",
      notes: "A libpq connection string (keyword or `postgres://` form) for the Postgres database holding the catalogs the admin API manages. Unset, the admin API's catalog routes answer `503 CATALOG_DATABASE_NOT_CONFIGURED`.",
   }),
   setting({
      key: "admin.storage.provider",
      type: oneOf(["s3", "s3_credential_chain", "gcs"]),
      description:
         "Object store the catalogs' data paths are read and written with.",
      notes: "The credential the admin API reads and writes catalogs' data paths with: `s3` (with `PUBLISHER_ADMIN_STORAGE_KEY_ID`, `PUBLISHER_ADMIN_STORAGE_SECRET`, and optionally `PUBLISHER_ADMIN_STORAGE_REGION` and `PUBLISHER_ADMIN_STORAGE_ENDPOINT`), `s3_credential_chain` (the host's identity; optional region), or `gcs` (an HMAC key pair in the key id and secret variables). An unknown value or missing key fails startup.",
      defaultText: "_unset_ (local paths)",
   }),
   setting({
      key: "admin.storage.keyId",
      type: string(),
      description: "Access key id (s3) or HMAC key id (gcs).",
      // Not a secret (the design gives it a flag), but kept out of the log.
      masked: true,
   }),
   setting({
      key: "admin.storage.secret",
      type: string(),
      secret: true,
      description: "Secret access key (s3) or HMAC secret (gcs).",
   }),
   setting({
      key: "admin.storage.region",
      type: string(),
      description: "Region of the s3 bucket.",
   }),
   setting({
      key: "admin.storage.endpoint",
      type: string(),
      description: "Endpoint of an s3-compatible store.",
   }),
];
