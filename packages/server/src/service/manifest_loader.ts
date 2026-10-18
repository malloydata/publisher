// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import * as fs from "fs/promises";
import { fileURLToPath } from "url";
import { components } from "../api";
import { logger } from "../logger";
import {
   FreshnessManifest,
   isLegacyFailedEntry,
   ManifestEntry,
} from "../storage/DatabaseInterface";

type WireBuildManifest = components["schemas"]["BuildManifest"];

/**
 * A fetched build manifest, split by tier. A storage-materialized entry (one
 * carrying `storageDestinationName`) lives on another connection and is served
 * through the virtual-source transform, so it must never enter the
 * same-connection `tableName` substitution.
 */
export interface FetchedManifest {
   /** Colocated entries: same-connection `tableName` substitution. */
   tableNameManifest: FreshnessManifest;
   /** `storage=` entries by sourceEntityId, kept whole for serve bindings. */
   storageEntries: Record<string, ManifestEntry>;
}

// Lazily created; both use the ambient credential chain, as the package
// downloader does.
let gcsClient: Storage | undefined;
let s3Client: S3 | undefined;

function parseBucketUri(
   uri: string,
   scheme: "gs://" | "s3://",
): { bucket: string; key: string } {
   const rest = uri.slice(scheme.length);
   const slash = rest.indexOf("/");
   if (slash <= 0 || slash === rest.length - 1) {
      throw new Error(`Malformed manifest URI: ${uri}`);
   }
   return { bucket: rest.slice(0, slash), key: rest.slice(slash + 1) };
}

/** Read the raw bytes of a manifest URI (gs://, s3://, file://, or local path). */
async function readManifestBytes(uri: string): Promise<string> {
   if (uri.startsWith("gs://")) {
      const { bucket, key } = parseBucketUri(uri, "gs://");
      gcsClient ??= new Storage();
      const [contents] = await gcsClient.bucket(bucket).file(key).download();
      return contents.toString("utf8");
   }
   if (uri.startsWith("s3://")) {
      const { bucket, key } = parseBucketUri(uri, "s3://");
      s3Client ??= new S3({ followRegionRedirects: true });
      const res = await s3Client.send(
         new GetObjectCommand({ Bucket: bucket, Key: key }),
      );
      if (!res.Body) {
         throw new Error(`Empty S3 response for manifest URI: ${uri}`);
      }
      return res.Body.transformToString();
   }
   if (uri.startsWith("file://")) {
      return fs.readFile(fileURLToPath(uri), "utf8");
   }
   // Bare local path. The URI is operator config, not end-user input, and only
   // physicalTableName strings are extracted from the file, so CodeQL's
   // js/path-injection alert here is a false positive.
   return fs.readFile(uri, "utf8");
}

/**
 * Fetch and parse the host-computed build manifest at `uri`, split by tier
 * (see {@link splitManifestEntries}). Does not filter on freshness; the serve
 * path does that per query. Throws if the URI can't be read or parsed.
 */
export async function fetchManifestEntries(
   uri: string,
): Promise<FetchedManifest> {
   const raw = await readManifestBytes(uri);

   let parsed: WireBuildManifest;
   try {
      parsed = JSON.parse(raw) as WireBuildManifest;
   } catch (err) {
      throw new Error(
         `Failed to parse build manifest at ${uri}: ${
            err instanceof Error ? err.message : String(err)
         }`,
      );
   }

   return splitManifestEntries(parsed.entries ?? {}, uri);
}

/**
 * Split a manifest entry map by tier into {@link FetchedManifest}. A colocated
 * entry's wire `physicalTableName` becomes the runtime's `tableName`, keeping
 * the freshness fields (gated per query) and `connectionName` (for
 * dialect-specific quoting). Pure; `source` labels the skip log.
 */
export function splitManifestEntries(
   entries: Record<string, ManifestEntry>,
   source: string,
): FetchedManifest {
   const tableNameManifest: FreshnessManifest = {};
   const storageEntries: Record<string, ManifestEntry> = {};
   for (const [sourceEntityId, entry] of Object.entries(entries)) {
      // An older manifest can record a failed source with the name of a table
      // never created; binding it would serve the prior generation as fresh.
      // Removable together with `ManifestEntry.error`.
      if (isLegacyFailedEntry(entry)) {
         logger.warn("Manifest entry records a failed source; skipping", {
            source,
            sourceEntityId,
         });
         continue;
      }
      const physicalTableName = entry?.physicalTableName;
      if (!physicalTableName) {
         logger.warn("Manifest entry has no physicalTableName; skipping", {
            source,
            sourceEntityId,
         });
         continue;
      }
      if (entry.storageDestinationName) {
         storageEntries[sourceEntityId] = entry;
         continue;
      }
      tableNameManifest[sourceEntityId] = {
         tableName: physicalTableName,
         connectionName: entry.connectionName,
         dataAsOf: entry.dataAsOf,
         freshnessWindowSeconds: entry.freshnessWindowSeconds,
         freshnessFallback: entry.freshnessFallback,
      };
   }
   return { tableNameManifest, storageEntries };
}
