import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import * as fs from "fs/promises";
import { fileURLToPath } from "url";
import { components } from "../api";
import { logger } from "../logger";
import { BuildManifest } from "../storage/DatabaseInterface";

type WireBuildManifest = components["schemas"]["BuildManifest"];

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
 * Fetch and parse the control-plane-computed build manifest at `uri`, returning
 * the Malloy-runtime binding map (`buildId -> { tableName }`). The wire manifest
 * keys physical tables under `physicalTableName`; the Malloy runtime consumes
 * `tableName`, so we translate here. Entries missing a physical table are
 * skipped. Throws if the URI can't be read or parsed.
 */
export async function fetchManifestEntries(
   uri: string,
): Promise<BuildManifest["entries"]> {
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

   const entries: BuildManifest["entries"] = {};
   for (const [buildId, entry] of Object.entries(parsed.entries ?? {})) {
      const physicalTableName = entry?.physicalTableName;
      if (!physicalTableName) {
         logger.warn("Manifest entry has no physicalTableName; skipping", {
            uri,
            buildId,
         });
         continue;
      }
      entries[buildId] = { tableName: physicalTableName };
   }
   return entries;
}
