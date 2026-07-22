import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import * as fs from "fs/promises";
import { fileURLToPath } from "url";
import { components } from "../api";
import { logger } from "../logger";
import { FreshnessManifest, ManifestEntry } from "../storage/DatabaseInterface";

type WireBuildManifest = components["schemas"]["BuildManifest"];

/**
 * A fetched build manifest, split by tier. A storage-materialized entry (one
 * that carries `storageConnectionName`) is served cross-connection via the
 * virtual-source transform, so it becomes a serve BINDING — it must never enter
 * the same-connection `tableName` substitution. Everything else is an
 * in-warehouse (path-C) entry the Malloy runtime resolves by substituting its
 * `tableName` at compile time.
 */
export interface FetchedManifest {
   /** In-warehouse (path-C) entries: same-connection `tableName` substitution. */
   tableNameManifest: FreshnessManifest;
   /**
    * `storage=` entries keyed by sourceEntityId, carried as full
    * {@link ManifestEntry}s (with `storageConnectionName` + captured `schema` +
    * `sourceName`) so the bind step can derive cross-connection serve bindings.
    */
   storageEntries: Record<string, ManifestEntry>;
}

// Lazily-created clients reused across fetches. Both use the ambient credential
// chain (ADC for GCS; the default provider chain for S3) — the same auth the
// package downloader already relies on.
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
   // Bare local path (used by tests and local/mounted deployments). The URI is
   // operator-supplied config (publisher.json or an authenticated PATCH), not
   // end-user input, and the file's contents are never returned to the caller —
   // only physicalTableName strings are extracted — so the CodeQL
   // js/path-injection alert here is a dismissed false positive.
   return fs.readFile(uri, "utf8");
}

/**
 * Fetch and parse the control-plane-computed build manifest at `uri`, split by
 * tier into {@link FetchedManifest}. The wire manifest keys physical tables
 * under `physicalTableName`; for an in-warehouse (path-C) entry the Malloy
 * runtime consumes `tableName`, so that field is translated here and the entry
 * lands in `tableNameManifest`, carrying the freshness fields verbatim (so the
 * serve path can gate `age vs window` per query) and `connectionName` (so the
 * bind step can quote the path for that connection's dialect — see
 * Package.quoteBoundTableNames).
 *
 * A `storage=`-materialized entry (one carrying `storageConnectionName`) is
 * routed instead to `storageEntries` as its full {@link ManifestEntry}: it lives
 * on a DIFFERENT connection and is served through the virtual-source transform
 * from its captured `schema`, so it must never become a same-connection
 * `tableName` substitution. Entries missing a physical table are skipped. This
 * is a pure fetch + parse: it does **not** filter on freshness (that happens per
 * query on the serve path). Throws if the URI can't be read or parsed.
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

   const tableNameManifest: FreshnessManifest = {};
   const storageEntries: Record<string, ManifestEntry> = {};
   for (const [sourceEntityId, entry] of Object.entries(parsed.entries ?? {})) {
      const physicalTableName = entry?.physicalTableName;
      if (!physicalTableName) {
         logger.warn("Manifest entry has no physicalTableName; skipping", {
            uri,
            sourceEntityId,
         });
         continue;
      }
      if (entry.storageConnectionName) {
         // Cross-connection storage tier: keep the full entry (schema +
         // sourceName + storageConnectionName) for the serve-binding derivation;
         // never enter the same-connection tableName manifest.
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
