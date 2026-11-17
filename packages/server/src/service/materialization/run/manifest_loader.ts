// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A host-computed build manifest, fetched and split by tier
 * ({@link fetchManifestEntries}, {@link splitManifestEntries}): colocated
 * entries become the same-connection `tableName` substitution, `storage=`
 * entries are kept whole for their serve bindings. Read by environment.ts when
 * it binds a package's manifest, and by the manifest committer.
 *
 * Reads the manifest from a file, GCS or S3 location; logs and skips an entry
 * that recorded a failed source or names no table. Never filters on freshness,
 * which the serve path does per query.
 */

import { boundNativeSchemaOf, boundSchemaOf } from "../stored_column_types";
import { isUsableTableName as quotingIsUsableTableName } from "../../quoting";
import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import * as fs from "fs/promises";
import { fileURLToPath } from "url";
import { components } from "../../../api";
import { logger } from "../../../logger";
import {
   FreshnessManifest,
   isLegacyFailedEntry,
   type KeyUniqueness,
   ManifestEntry,
} from "../domain/types";

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
 * Whether a table name a host or the run store supplies (a manifest entry,
 * a build instruction, a reference, a ledger entry) is one this server puts
 * in SQL, in whatever dialect its connection turns out to be: the one
 * predicate for the build route and the bind, so neither accepts a name the
 * other refuses. A quoted name has real writers: an author's own
 * `manifestLocation` manifest, whose entries no `name=` rule checks, and the
 * manifests of Publisher v0.0.229 to v0.2.4. The control plane never sends
 * one. See {@link quotingIsUsableTableName}.
 */
export function isUsableTableName(name: string): boolean {
   return quotingIsUsableTableName(name);
}

/**
 * Split a manifest entry map by tier into {@link FetchedManifest}. A colocated
 * entry's wire `physicalTableName` becomes the runtime's `tableName`, keeping
 * the freshness fields (gated per query) and `connectionName` (for
 * dialect-specific quoting). Pure; `source` labels the skip log, which
 * `quiet` leaves out for a split that only compares and binds nothing.
 */
export function splitManifestEntries(
   entries: Record<string, ManifestEntry>,
   source: string,
   // Where the entries came from. Only Publisher's own store is read for a
   // key check record (withRollupKeyRecords); a host's manifest is never
   // trusted with one.
   from: "store" | "host" = "host",
   { quiet = false }: { quiet?: boolean } = {},
): FetchedManifest {
   const readKeyUniqueness = from === "store";
   const tableNameManifest: FreshnessManifest = {};
   const storageEntries: Record<string, ManifestEntry> = {};
   for (const [sourceEntityId, entry] of Object.entries(entries)) {
      // An older manifest can record a failed source with the name of a table
      // never created; binding it would serve the prior generation as fresh.
      // Removable together with `ManifestEntry.error`.
      if (isLegacyFailedEntry(entry)) {
         if (!quiet) {
            logger.warn("Manifest entry records a failed source; skipping", {
               source,
               sourceEntityId,
            });
         }
         continue;
      }
      const physicalTableName = entry?.physicalTableName;
      if (!physicalTableName) {
         if (!quiet) {
            logger.warn("Manifest entry has no physicalTableName; skipping", {
               source,
               sourceEntityId,
            });
         }
         continue;
      }
      // A name this server would not put in SQL (isUsableTableName): a
      // backslash or control character, quotes that do not form whole quoted
      // parts, or an empty part. A quoted name that parses (an author's own manifestLocation
      // manifest, or a legacy one) is kept.
      if (!isUsableTableName(physicalTableName)) {
         if (!quiet) {
            logger.warn(
               "Manifest entry names a table this server cannot read safely (a backslash or control character, quotes that do not parse, or an empty part); skipping, so its source answers live",
               { source, sourceEntityId },
            );
         }
         continue;
      }
      if (entry.storageDestinationName) {
         // The internal key record only from the local store
         // (withRollupKeyRecords); a host's entry never brings one.
         const { keyUniqueness: _unread, ...plain } = entry as ManifestEntry & {
            keyUniqueness?: unknown;
         };
         const keyUniqueness = readKeyUniqueness
            ? keyUniquenessOnEntry(entry)
            : undefined;
         storageEntries[sourceEntityId] = keyUniqueness
            ? ({ ...plain, keyUniqueness } as ManifestEntry)
            : plain;
         continue;
      }
      tableNameManifest[sourceEntityId] = {
         tableName: physicalTableName,
         connectionName: entry.connectionName,
         dataAsOf: entry.dataAsOf,
         freshnessWindowSeconds: entry.freshnessWindowSeconds,
         freshnessFallback: entry.freshnessFallback,
         ...boundSchemaOf(entry.schema),
         ...boundNativeSchemaOf(entry.nativeSchema),
         // Set only by a bind from the local store (withRollupKeyRecords);
         // a host's manifest never carries it.
         ...(readKeyUniqueness && keyUniquenessOnEntry(entry)
            ? { keyUniqueness: keyUniquenessOnEntry(entry) }
            : {}),
      };
   }
   return { tableNameManifest, storageEntries };
}

/** The internal rollup key record a local-store entry may carry. */
function keyUniquenessOnEntry(entry: ManifestEntry): KeyUniqueness | undefined {
   const k = (entry as { keyUniqueness?: Partial<KeyUniqueness> })
      .keyUniqueness;
   if (!k) return undefined;
   const own =
      typeof k.keyColumn === "string" && typeof k.unique === "boolean"
         ? { keyColumn: k.keyColumn, unique: k.unique }
         : undefined;
   const parentKeys =
      k.parentKeys && typeof k.parentKeys === "object"
         ? Object.fromEntries(
              Object.entries(k.parentKeys).filter(
                 ([, unique]) => typeof unique === "boolean",
              ),
           )
         : {};
   const anyParent = Object.keys(parentKeys).length > 0;
   return own || anyParent
      ? { ...(own ?? {}), ...(anyParent ? { parentKeys } : {}) }
      : undefined;
}
