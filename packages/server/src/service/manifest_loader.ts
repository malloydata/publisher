import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import * as fs from "fs/promises";
import { fileURLToPath } from "url";
import { components } from "../api";
import { logger } from "../logger";
import { recordManifestStaleEntry } from "../materialization_metrics";
import { BuildManifest } from "../storage/DatabaseInterface";

type WireBuildManifest = components["schemas"]["BuildManifest"];
type WireManifestEntry = components["schemas"]["ManifestEntry"];

/**
 * What the manifest binding should do with a persist source's materialized
 * table. Decided when the publisher (re)binds the manifest (package load /
 * reload), not per query — the table substitution is compiled into the model at
 * bind time, so the decision stands until the next rebind. The gate comes from
 * `materialization.freshness`, threaded by the control plane onto each entry as
 * `builtAt` + `freshnessWindowSeconds` + `freshnessFallback`:
 *   - `serve_table`: the table is fresh (or un-gated) — bind it.
 *   - `serve_live`: the table is stale and the policy is `live` — drop the
 *     binding so the source serves live.
 *   - `fail`: the table is stale and the policy is `fail`. The bound model has
 *     no per-query error hook, so the binding is still dropped (serve live);
 *     the value is kept distinct only for logging and for if/when the Malloy
 *     runtime gains a query-time error hook.
 */
export type FreshnessDecision = "serve_table" | "serve_live" | "fail";

/**
 * Evaluate an entry's freshness against `now` (the bind timestamp). Best-effort
 * and fail-open: a missing window, missing/unparseable `builtAt`, or unknown
 * fallback resolves to `serve_table` so a malformed gate never blocks serving.
 * A stale table applies its fallback (default `live`).
 */
export function evaluateManifestFreshness(
   entry: Pick<
      WireManifestEntry,
      "builtAt" | "freshnessWindowSeconds" | "freshnessFallback"
   >,
   now: Date,
): FreshnessDecision {
   const windowSeconds = entry.freshnessWindowSeconds;
   // No gate configured -> always serve the table (today's behavior).
   if (windowSeconds == null || windowSeconds <= 0) {
      return "serve_table";
   }
   if (!entry.builtAt) {
      return "serve_table";
   }
   const builtAtMs = Date.parse(entry.builtAt);
   if (Number.isNaN(builtAtMs)) {
      return "serve_table";
   }
   const ageSeconds = (now.getTime() - builtAtMs) / 1000;
   if (ageSeconds <= windowSeconds) {
      return "serve_table";
   }
   // Stale: apply the fallback (default live).
   switch ((entry.freshnessFallback ?? "live").toLowerCase()) {
      case "stale_ok":
         return "serve_table";
      case "fail":
         return "fail";
      case "live":
      default:
         return "serve_live";
   }
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

   const now = new Date();
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
      // Freshness gate, evaluated at (re)bind time: a stale table is dropped
      // from the binding so the source serves live (Malloy serves live for any
      // buildId absent from the manifest, since we bind with strict:false).
      // `stale_ok` keeps the binding. A fresh or un-gated entry binds as before.
      // The gate is not re-checked per query — only on the next rebind.
      const decision = evaluateManifestFreshness(entry, now);
      if (decision === "serve_table") {
         entries[buildId] = { tableName: physicalTableName };
         continue;
      }
      // `fail` has no per-query enforcement point (the table substitution is
      // compiled into the model at bind time), so a stale `fail` source drops
      // its binding and serves live rather than serving stale data; per-query
      // erroring would need Malloy-runtime support. `live` and `fail` therefore
      // both drop the binding, differing only in the metric label.
      const fallback = decision === "fail" ? "fail" : "live";
      recordManifestStaleEntry(fallback);
      // Per-entry detail at debug to keep a large stale manifest from flooding
      // info logs on every bind; the counter is the operational signal.
      logger.debug("Manifest entry is stale; dropping binding (serving live)", {
         uri,
         buildId,
         physicalTableName,
         freshnessFallback: fallback,
         builtAt: entry.builtAt ?? null,
         freshnessWindowSeconds: entry.freshnessWindowSeconds ?? null,
      });
   }
   return entries;
}
