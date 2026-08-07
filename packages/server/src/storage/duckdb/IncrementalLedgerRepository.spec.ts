import { afterEach, describe, expect, it } from "bun:test";
import type { IncrementalLedgerEntry } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";
import { IncrementalLedgerRepository } from "./IncrementalLedgerRepository";
import { initializeSchema } from "./schema";

/**
 * Against a real in-memory DuckDB rather than a stubbed connection: the parts of
 * this store that can actually be wrong are the upsert's conflict target and the
 * JSON round-trip of the merge keys, and neither is exercised by asserting the
 * SQL text.
 */
describe("IncrementalLedgerRepository", () => {
   const ENV = "env-1";
   const PKG = "pkg-a";
   const SOURCE = "sha-daily-revenue";
   const dbs: DuckDBConnection[] = [];

   afterEach(async () => {
      while (dbs.length) await dbs.pop()!.close();
   });

   async function freshRepo(): Promise<IncrementalLedgerRepository> {
      const db = new DuckDBConnection(":memory:");
      dbs.push(db);
      await db.initialize();
      await initializeSchema(db);
      return new IncrementalLedgerRepository(db);
   }

   function entry(
      overrides: Partial<IncrementalLedgerEntry> = {},
   ): Omit<IncrementalLedgerEntry, "createdAt" | "advancedAt"> {
      return {
         environmentId: ENV,
         packageName: PKG,
         sourceEntityId: SOURCE,
         coveredThroughValue: "2024-06-01",
         coveredThroughType: "date",
         watermarkDimension: "order_date",
         mergeKeyDimensions: [],
         derivedStrategy: "range_replace",
         physicalTableName: "analytics.daily_revenue",
         connectionName: "warehouse",
         advancedByMaterializationId: "run-1",
         ...overrides,
      };
   }

   it("returns null for a lineage with no boundary yet (which means SEED)", async () => {
      const repo = await freshRepo();
      expect(await repo.get(ENV, PKG, SOURCE)).toBeNull();
   });

   it("round-trips a boundary, its declarations and its lineage identity", async () => {
      const repo = await freshRepo();
      const written = await repo.upsert(
         entry({
            mergeKeyDimensions: ["order_id", "region"],
            derivedStrategy: "merge",
         }),
      );
      expect(written.coveredThroughValue).toBe("2024-06-01");
      expect(written.mergeKeyDimensions).toEqual(["order_id", "region"]);
      expect(written.derivedStrategy).toBe("merge");

      const read = await repo.get(ENV, PKG, SOURCE);
      expect(read).not.toBeNull();
      expect(read!.watermarkDimension).toBe("order_date");
      expect(read!.mergeKeyDimensions).toEqual(["order_id", "region"]);
      expect(read!.physicalTableName).toBe("analytics.daily_revenue");
      expect(read!.connectionName).toBe("warehouse");
      expect(read!.advancedByMaterializationId).toBe("run-1");
      expect(read!.advancedAt).toBeInstanceOf(Date);
      expect(read!.createdAt).toBeInstanceOf(Date);
   });

   it("advances in place: one row per lineage, never a second boundary", async () => {
      // The property that makes a retried advance a no-op instead of ambiguity.
      const repo = await freshRepo();
      await repo.upsert(entry());
      await repo.upsert(
         entry({
            coveredThroughValue: "2024-07-01",
            advancedByMaterializationId: "run-2",
         }),
      );

      const rows = await repo.list(ENV, PKG);
      expect(rows).toHaveLength(1);
      expect(rows[0].coveredThroughValue).toBe("2024-07-01");
      expect(rows[0].advancedByMaterializationId).toBe("run-2");
   });

   it("keeps created_at across an advance while advanced_at moves", async () => {
      const repo = await freshRepo();
      const seeded = await repo.upsert(entry());
      await Bun.sleep(5);
      const advanced = await repo.upsert(
         entry({ coveredThroughValue: "2024-07-01" }),
      );
      expect(advanced.createdAt.getTime()).toBe(seeded.createdAt.getTime());
      expect(advanced.advancedAt.getTime()).toBeGreaterThanOrEqual(
         seeded.advancedAt.getTime(),
      );
   });

   it("keys by (environment, package, source): neighbours never collide", async () => {
      // A BuildID carries no environment input, so two environments can address
      // the same source identically. Their boundaries must not be the same row.
      const repo = await freshRepo();
      await repo.upsert(entry({ coveredThroughValue: "2024-06-01" }));
      await repo.upsert(
         entry({ environmentId: "env-2", coveredThroughValue: "2024-01-01" }),
      );
      await repo.upsert(
         entry({ packageName: "pkg-b", coveredThroughValue: "2024-02-01" }),
      );
      await repo.upsert(
         entry({
            sourceEntityId: "sha-other",
            coveredThroughValue: "2024-03-01",
         }),
      );

      expect((await repo.get(ENV, PKG, SOURCE))!.coveredThroughValue).toBe(
         "2024-06-01",
      );
      expect((await repo.get("env-2", PKG, SOURCE))!.coveredThroughValue).toBe(
         "2024-01-01",
      );
      expect((await repo.list(ENV, PKG)).map((r) => r.sourceEntityId)).toEqual([
         "sha-daily-revenue",
         "sha-other",
      ]);
   });

   it("preserves a numeric and a string boundary as text, unrounded", async () => {
      const repo = await freshRepo();
      await repo.upsert(
         entry({
            coveredThroughType: "number",
            coveredThroughValue: "9007199254740993",
         }),
      );
      expect((await repo.get(ENV, PKG, SOURCE))!.coveredThroughValue).toBe(
         "9007199254740993",
      );

      await repo.upsert(
         entry({
            coveredThroughType: "string",
            coveredThroughValue: "us-east-1'; DROP TABLE x",
         }),
      );
      expect((await repo.get(ENV, PKG, SOURCE))!.coveredThroughValue).toBe(
         "us-east-1'; DROP TABLE x",
      );
   });

   it("deletes one lineage's boundary, so the next run re-seeds it", async () => {
      const repo = await freshRepo();
      await repo.upsert(entry());
      await repo.upsert(entry({ sourceEntityId: "sha-other" }));

      await repo.deleteEntry(ENV, PKG, SOURCE);

      expect(await repo.get(ENV, PKG, SOURCE)).toBeNull();
      expect(await repo.get(ENV, PKG, "sha-other")).not.toBeNull();
   });

   it("cascades by package and by environment", async () => {
      const repo = await freshRepo();
      await repo.upsert(entry());
      await repo.upsert(entry({ packageName: "pkg-b" }));
      await repo.upsert(entry({ environmentId: "env-2" }));

      await repo.deleteByPackage(ENV, PKG);
      expect(await repo.list(ENV, PKG)).toEqual([]);
      expect(await repo.list(ENV, "pkg-b")).toHaveLength(1);

      await repo.deleteByEnvironmentId(ENV);
      expect(await repo.list(ENV, "pkg-b")).toEqual([]);
      expect(await repo.list("env-2", PKG)).toHaveLength(1);
   });

   it("reads an unparseable merge-key list as empty, which forces a re-seed", async () => {
      // Degrading to [] reads as "range replace", which MISMATCHES any merge
      // declaration — the direction that re-seeds rather than merging under keys
      // we could not confirm.
      const db = new DuckDBConnection(":memory:");
      dbs.push(db);
      await db.initialize();
      await initializeSchema(db);
      const repo = new IncrementalLedgerRepository(db);
      await repo.upsert(entry({ mergeKeyDimensions: ["order_id"] }));
      await db.run(
         `UPDATE incremental_ledger SET merge_key_dimensions = '"not-a-list"'
           WHERE source_entity_id = ?`,
         [SOURCE],
      );
      expect((await repo.get(ENV, PKG, SOURCE))!.mergeKeyDimensions).toEqual(
         [],
      );
   });
});
