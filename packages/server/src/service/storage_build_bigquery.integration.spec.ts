import { describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import { mkdtempSync } from "node:fs";
import os from "node:os";
import path from "node:path";

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { storageDestinationRoot } from "./connection_config";
import { buildSourceIntoStorage } from "./materialization_build_session";
import { MaterializationService } from "./materialization_service";

/**
 * The `storage=` build's warehouse read, against a REAL BigQuery project.
 *
 * What this covers that the unit tests cannot: the unit suite drives
 * {@link buildSourceIntoStorage} through a hand-rolled session that returns
 * exactly the shape the code expects, so it proves the logic and nothing about
 * the surface. If `bigquery_jobs()`'s columns move, or `bigquery_execute` stops
 * returning `job_id`, or the anonymous result table stops being reachable, every
 * one of those tests still passes and every real build breaks.
 *
 * Reuses the public-data service account and the credential-gating shape of
 * `connection.spec.ts`. The destination is a plain local DuckDB file, so this
 * needs no DuckLake catalog, no Postgres and no object storage — only a BigQuery
 * credential.
 *
 * <b>Not yet wired into `connection-integration-tests.yml`, for one reason:</b>
 * the repository's public-data service account lacks
 * `bigquery.readsessions.create`, and every path here needs it. That is not a
 * property of the split — the untagged case below uses `bigquery_query()`, the
 * shape `storage=` builds have always used, and it fails the same way:
 *
 *     the user does not have 'bigquery.readsessions.create' permission
 *
 * The passthrough reads its results over the Storage Read API, so
 * `roles/bigquery.readSessionUser` (or an equivalent grant) is a standing
 * requirement for materializing ANY BigQuery source into a storage destination,
 * independent of tagging. Granting it to that service account is all this needs;
 * adding the file to the workflow's path filter and a step that runs it is then a
 * two-line change.
 *
 * Until then it runs wherever the credentials do exist — set
 * `BIGQUERY_PUBLIC_DATA_CREDENTIALS` to a key file and
 * `BIGQUERY_PUBLIC_DATA_PROJECT_ID` to its project. Absent those it skips, so it
 * costs a developer without credentials nothing.
 */
const hasPublicDataBigQueryCredentials = () =>
   !!(
      process.env.BIGQUERY_PUBLIC_DATA_CREDENTIALS &&
      process.env.BIGQUERY_PUBLIC_DATA_PROJECT_ID
   );

const SKIP_MESSAGE =
   "Skipping: BIGQUERY_PUBLIC_DATA_CREDENTIALS / BIGQUERY_PUBLIC_DATA_PROJECT_ID not configured";

/** A read of a real table, with a unique predicate so BigQuery cannot serve it from cache. */
const buildSQLFor = (nonce: string) =>
   `SELECT word AS who, word_count AS n ` +
   `FROM \`bigquery-public-data.samples.shakespeare\` ` +
   `WHERE word_count > 3 AND corpus != 'nonce_${nonce}' LIMIT 12`;

async function runBuild(
   nonce: string,
   queryMetadata: Record<string, string> | undefined,
) {
   const serviceAccountKeyJson = await fs.readFile(
      process.env.BIGQUERY_PUBLIC_DATA_CREDENTIALS!,
      "utf-8",
   );
   const environmentPath = mkdtempSync(
      path.join(os.tmpdir(), `storage-build-${nonce}-`),
   );
   const physicalTableName = `orders_${nonce}`;
   const result = await buildSourceIntoStorage({
      destinationName: "lake",
      destinationConnection: { name: "lake", type: "duckdb" } as never,
      sourceConnection: {
         name: "bq_src",
         type: "bigquery",
         bigqueryConnection: {
            serviceAccountKeyJson,
            defaultProjectId: process.env.BIGQUERY_PUBLIC_DATA_PROJECT_ID!,
         },
      } as never,
      buildSQL: buildSQLFor(nonce),
      physicalTableName,
      environmentPath,
      queryMetadata,
   });
   return { result, environmentPath, physicalTableName };
}

/** Rows the build actually landed in the destination file. */
async function rowsInDestination(
   environmentPath: string,
   physicalTableName: string,
): Promise<number> {
   const session = new DuckDBConnection("verify", ":memory:");
   try {
      await session.runSQL(
         `ATTACH '${path.join(storageDestinationRoot(environmentPath), "lake.duckdb")}' AS lake (READ_ONLY)`,
      );
      const raw = await session.runSQL(
         `SELECT count(*) AS n FROM lake."${physicalTableName}"`,
      );
      const rows = (
         Array.isArray(raw) ? raw : ((raw as { rows?: unknown[] }).rows ?? [])
      ) as Record<string, unknown>[];
      return Number(rows[0]?.n);
   } finally {
      await session.close();
   }
}

describe("storage= build against live BigQuery", () => {
   it("labels the read, captures its rows, and reports what it cost", async () => {
      if (!hasPublicDataBigQueryCredentials()) {
         console.log(SKIP_MESSAGE);
         return;
      }
      const nonce = `lbl${Date.now()}`;
      const { result, environmentPath, physicalTableName } = await runBuild(
         nonce,
         { cred_run: nonce, cred_org: "acme_corp", cred_class: "ops" },
      );

      // The rows are the point: a cost figure from a build that captured
      // nothing would be worse than no figure.
      expect(await rowsInDestination(environmentPath, physicalTableName)).toBe(
         12,
      );
      expect(result.schema.map((c) => c.name)).toEqual(["who", "n"]);

      // The split ran, which is the only way a label reaches the job.
      const cost = result.readCost;
      expect(cost).not.toBeNull();
      expect(cost!.engine).toBe("bigquery");
      expect(cost!.jobId).toBeTruthy();
      // The child job is not in the default listing, so the parent is what an
      // operator needs to reach it.
      expect(cost!.parentJobId).toBeTruthy();

      // Real figures off the real job record — this is the surface a fake
      // session cannot exercise.
      expect(cost!.bytesScanned).toBeGreaterThan(0);
      expect(cost!.bytesBilled).toBeGreaterThan(0);
      // BigQuery's 10MB-per-query floor: a small read bills above what it
      // scanned, which is why these are separate fields.
      expect(cost!.bytesBilled).toBeGreaterThanOrEqual(cost!.bytesScanned!);
      expect(cost!.cacheHit).toBe(false);
   }, 120_000);

   it("puts SCANNED bytes on the manifest entry, never billed", async () => {
      // The one field the manifest carries, pinned against the one that would
      // look plausible in its place. Worth doing here rather than over a fake:
      // these two numbers only diverge against a real warehouse — the 10MB floor
      // is what makes billed exceed scanned — so a stub would have to assert the
      // difference it invented.
      if (!hasPublicDataBigQueryCredentials()) {
         console.log(SKIP_MESSAGE);
         return;
      }
      const nonce = `mft${Date.now()}`;
      const serviceAccountKeyJson = await fs.readFile(
         process.env.BIGQUERY_PUBLIC_DATA_CREDENTIALS!,
         "utf-8",
      );
      const environmentPath = mkdtempSync(
         path.join(os.tmpdir(), `storage-manifest-${nonce}-`),
      );
      const sourceConnection = {
         name: "bq_src",
         type: "bigquery",
         bigqueryConnection: {
            serviceAccountKeyJson,
            defaultProjectId: process.env.BIGQUERY_PUBLIC_DATA_PROJECT_ID!,
         },
      };
      const service = Object.create(MaterializationService.prototype) as Record<
         string,
         (...a: unknown[]) => Promise<Record<string, unknown>>
      >;
      const entry = await service.buildOneSourceIntoStorage(
         { name: "orders", connectionName: "bq_src" },
         {
            sourceEntityId: `sid-${nonce}`,
            physicalTableName: `orders_${nonce}`,
            destination: "lake",
         },
         { strict: false, update: () => {} },
         {
            getApiConnection: () => sourceConnection,
            getStorageDestination: () => ({ name: "lake", type: "duckdb" }),
            getEnvironmentPath: () => environmentPath,
         },
         buildSQLFor(nonce),
         {},
         false,
         { cred_run: nonce, cred_class: "ops" },
      );

      const billed = 10_485_760;
      expect(entry.queryCostBytes).toBeGreaterThan(0);
      // Scanned and billed genuinely differ on this read, so this distinguishes.
      expect(entry.queryCostBytes).toBeLessThan(billed);
      expect(entry.queryCostBytes).not.toBe(billed);
   }, 120_000);

   it("leaves an untagged read in the shape it had before, and reports no cost", async () => {
      if (!hasPublicDataBigQueryCredentials()) {
         console.log(SKIP_MESSAGE);
         return;
      }
      const nonce = `pln${Date.now()}`;
      const { result, environmentPath, physicalTableName } = await runBuild(
         nonce,
         undefined,
      );

      // Same rows by the older path — the split is not load-bearing for data.
      expect(await rowsInDestination(environmentPath, physicalTableName)).toBe(
         12,
      );
      // No label to carry means no split, and a rows-returning passthrough
      // call hands back no job to account for.
      expect(result.readCost).toBeNull();
   }, 120_000);
});
