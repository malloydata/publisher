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
      description: "Host the admin API binds to; host does not move it.",
      aliases: { env: ["PUBLISHER_ADMIN_HOST"], flags: ["--admin_host"] },
   }),
   setting({
      key: "admin.catalogDatabase",
      type: string(),
      secret: true,
      description:
         "libpq connection string of the database the admin API finds DuckLake catalogs in.",
      aliases: { env: ["PUBLISHER_ADMIN_CATALOG_DATABASE"] },
   }),
   setting({
      key: "admin.storage.provider",
      type: oneOf(["s3", "s3_credential_chain", "gcs"]),
      description:
         "Object store the catalogs' data paths are read and written with.",
      aliases: { env: ["PUBLISHER_ADMIN_STORAGE_PROVIDER"] },
   }),
   setting({
      key: "admin.storage.keyId",
      type: string(),
      description: "Access key id (s3) or HMAC key id (gcs).",
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
