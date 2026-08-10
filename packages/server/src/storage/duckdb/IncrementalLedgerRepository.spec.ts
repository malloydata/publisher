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
   const CONN = "warehouse";
   const TABLE = "analytics.daily_revenue";
   const dbs: DuckDBConnection[] = [];

   afterEach(async () => {
      while (dbs.length) await dbs.pop()!.close();
   });

   async function freshRepo(): Promise<{
      repo: IncrementalLedgerRepository;
      db: DuckDBConnection;
   }> {
      const db = new DuckDBConnection(":memory:");
      dbs.push(db);
      await db.initialize();
      await initializeSchema(db);
      return { repo: new IncrementalLedgerRepository(db), db };
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
         physicalTableName: TABLE,
         connectionName: CONN,
         advancedByMaterializationId: "run-1",
         ...overrides,
      };
   }

   it("returns null for a table with no boundary yet (which means SEED)", async () => {
      const { repo } = await freshRepo();
      expect(await repo.get(ENV, CONN, TABLE)).toBeNull();
   });

   it("round-trips a boundary, its declarations and its lineage identity", async () => {
      const { repo } = await freshRepo();
      const written = await repo.upsert(
         entry({
            mergeKeyDimensions: ["order_id", "region"],
            derivedStrategy: "merge",
         }),
      );
      expect(written.coveredThroughValue).toBe("2024-06-01");
      expect(written.mergeKeyDimensions).toEqual(["order_id", "region"]);
      expect(written.derivedStrategy).toBe("merge");

      const read = await repo.get(ENV, CONN, TABLE);
      expect(read).not.toBeNull();
      expect(read!.watermarkDimension).toBe("order_date");
      expect(read!.mergeKeyDimensions).toEqual(["order_id", "region"]);
      expect(read!.sourceEntityId).toBe(SOURCE);
      expect(read!.packageName).toBe(PKG);
      expect(read!.advancedByMaterializationId).toBe("run-1");
      expect(read!.advancedAt).toBeInstanceOf(Date);
      expect(read!.createdAt).toBeInstanceOf(Date);
   });

   it("advances in place: one row per table, never a second boundary", async () => {
      // The property that makes a retried advance a no-op instead of ambiguity.
      const { repo, db } = await freshRepo();
      await repo.upsert(entry());
      await repo.upsert(
         entry({
            coveredThroughValue: "2024-07-01",
            advancedByMaterializationId: "run-2",
         }),
      );

      const count = await db.all<{ n: unknown }>(
         "SELECT count(*) AS n FROM incremental_ledger",
      );
      expect(Number(count[0].n)).toBe(1);
      const read = await repo.get(ENV, CONN, TABLE);
      expect(read!.coveredThroughValue).toBe("2024-07-01");
      expect(read!.advancedByMaterializationId).toBe("run-2");
   });

   it("keeps ONE boundary for a table refreshed under a second package name", async () => {
      // The reason the key is the table. An orchestrated host that serves several
      // versions of one package presents a version-qualified package name
      // (Credible sends `<package>|<versionId>`), and fires a shared table's
      // refresh through whichever version materialized most recently — so the
      // name changes on the first refresh after every publish while the table
      // stays exactly the same. Keyed on the package, that read missed and the
      // source re-seeded in full every publish; keyed on the table it advances.
      const { repo, db } = await freshRepo();
      await repo.upsert(entry({ packageName: "orders|v1" }));
      await repo.upsert(
         entry({ packageName: "orders|v2", coveredThroughValue: "2024-07-01" }),
      );

      const count = await db.all<{ n: unknown }>(
         "SELECT count(*) AS n FROM incremental_ledger",
      );
      expect(Number(count[0].n)).toBe(1);
      const read = await repo.get(ENV, CONN, TABLE);
      expect(read!.coveredThroughValue).toBe("2024-07-01");
      // The tag names the last writer, which is the honest answer for a table
      // several versions refresh in turn.
      expect(read!.packageName).toBe("orders|v2");
   });

   it("keeps created_at across an advance while advanced_at moves", async () => {
      const { repo } = await freshRepo();
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

   it("keys by (environment, connection, table): neighbours never collide", async () => {
      // A physical name is only unique WITHIN a connection, and nothing about a
      // table's identity carries an environment — so all three are needed for two
      // boundaries that describe different tables to be different rows.
      const { repo } = await freshRepo();
      await repo.upsert(entry({ coveredThroughValue: "2024-06-01" }));
      await repo.upsert(
         entry({ environmentId: "env-2", coveredThroughValue: "2024-01-01" }),
      );
      await repo.upsert(
         entry({
            connectionName: "other-wh",
            coveredThroughValue: "2024-02-01",
         }),
      );
      await repo.upsert(
         entry({
            physicalTableName: "analytics.other",
            coveredThroughValue: "2024-03-01",
         }),
      );

      expect((await repo.get(ENV, CONN, TABLE))!.coveredThroughValue).toBe(
         "2024-06-01",
      );
      expect((await repo.get("env-2", CONN, TABLE))!.coveredThroughValue).toBe(
         "2024-01-01",
      );
      expect(
         (await repo.get(ENV, "other-wh", TABLE))!.coveredThroughValue,
      ).toBe("2024-02-01");
      expect(
         (await repo.get(ENV, CONN, "analytics.other"))!.coveredThroughValue,
      ).toBe("2024-03-01");
   });

   it("preserves a numeric and a string boundary as text, unrounded", async () => {
      const { repo } = await freshRepo();
      await repo.upsert(
         entry({
            coveredThroughType: "number",
            coveredThroughValue: "9007199254740993",
         }),
      );
      expect((await repo.get(ENV, CONN, TABLE))!.coveredThroughValue).toBe(
         "9007199254740993",
      );

      await repo.upsert(
         entry({
            coveredThroughType: "string",
            coveredThroughValue: "us-east-1'; DROP TABLE x",
         }),
      );
      expect((await repo.get(ENV, CONN, TABLE))!.coveredThroughValue).toBe(
         "us-east-1'; DROP TABLE x",
      );
   });

   it("deletes one table's boundary, so the next run re-seeds it", async () => {
      const { repo } = await freshRepo();
      await repo.upsert(entry());
      await repo.upsert(entry({ physicalTableName: "analytics.other" }));

      await repo.deleteEntry(ENV, CONN, TABLE);

      expect(await repo.get(ENV, CONN, TABLE)).toBeNull();
      expect(await repo.get(ENV, CONN, "analytics.other")).not.toBeNull();
   });

   it("cascades by package and by environment", async () => {
      // Distinct tables per row, since the package name no longer separates them.
      const { repo } = await freshRepo();
      await repo.upsert(entry());
      await repo.upsert(
         entry({ packageName: "pkg-b", physicalTableName: "analytics.b" }),
      );
      await repo.upsert(entry({ environmentId: "env-2" }));

      await repo.deleteByPackage(ENV, PKG);
      expect(await repo.get(ENV, CONN, TABLE)).toBeNull();
      expect(await repo.get(ENV, CONN, "analytics.b")).not.toBeNull();

      await repo.deleteByEnvironmentId(ENV);
      expect(await repo.get(ENV, CONN, "analytics.b")).toBeNull();
      expect(await repo.get("env-2", CONN, TABLE)).not.toBeNull();
   });

   describe("re-keying an existing database", () => {
      // The schema pass runs on every boot, so both halves matter: it has to
      // replace a table still keyed on the package, and it must not touch one
      // already keyed on the table — otherwise every restart would silently
      // discard every boundary and re-seed every incremental source.
      const LEGACY_DDL = `
         CREATE TABLE incremental_ledger (
           environment_id VARCHAR NOT NULL,
           package_name VARCHAR NOT NULL,
           source_entity_id VARCHAR NOT NULL,
           covered_through_value VARCHAR NOT NULL,
           covered_through_type VARCHAR NOT NULL,
           watermark_dimension VARCHAR NOT NULL,
           merge_key_dimensions JSON NOT NULL,
           derived_strategy VARCHAR NOT NULL,
           physical_table_name VARCHAR NOT NULL,
           connection_name VARCHAR NOT NULL,
           advanced_by_materialization_id VARCHAR,
           advanced_at TIMESTAMP NOT NULL,
           created_at TIMESTAMP NOT NULL,
           PRIMARY KEY (environment_id, package_name, source_entity_id)
         )`;

      /**
       * Seeded with a raw INSERT because `upsert` CANNOT write here: its
       * ON CONFLICT names the new key, and DuckDB rejects a conflict target that
       * no constraint covers. Which is the point — the drop is REQUIRED, not
       * housekeeping. Left in place, a legacy table would fail every advance (a
       * swallowed warning), and every incremental source would seed forever.
       */
      async function insertLegacyRow(
         db: DuckDBConnection,
         packageName: string,
      ): Promise<void> {
         const now = new Date().toISOString();
         await db.run(
            `INSERT INTO incremental_ledger VALUES
              (?, ?, ?, '2024-06-01', 'date', 'order_date', '[]',
               'range_replace', ?, ?, 'run-1', ?, ?)`,
            [ENV, packageName, SOURCE, TABLE, CONN, now, now],
         );
      }

      it("replaces a package-keyed ledger, and the new key then holds", async () => {
         const { repo, db } = await freshRepo();
         await db.run("DROP TABLE incremental_ledger");
         await db.run(LEGACY_DDL);
         // Two rows for ONE table, which is exactly what the old key permitted
         // and what made a shared table re-seed after every publish.
         await insertLegacyRow(db, "orders|v1");
         await insertLegacyRow(db, "orders|v2");
         expect(
            Number(
               (
                  await db.all<{ n: unknown }>(
                     "SELECT count(*) AS n FROM incremental_ledger",
                  )
               )[0].n,
            ),
         ).toBe(2);

         await initializeSchema(db);

         // Dropped, not migrated: the boundary is a cache whose miss path is a
         // seed, so each source rebuilds once and then advances by delta.
         expect(await repo.get(ENV, CONN, TABLE)).toBeNull();
         await repo.upsert(entry({ packageName: "orders|v1" }));
         await repo.upsert(entry({ packageName: "orders|v2" }));
         expect(
            Number(
               (
                  await db.all<{ n: unknown }>(
                     "SELECT count(*) AS n FROM incremental_ledger",
                  )
               )[0].n,
            ),
         ).toBe(1);
      });

      it("leaves a table-keyed ledger and its boundaries alone", async () => {
         const { repo, db } = await freshRepo();
         await repo.upsert(entry({ coveredThroughValue: "2024-07-01" }));

         await initializeSchema(db);

         expect((await repo.get(ENV, CONN, TABLE))?.coveredThroughValue).toBe(
            "2024-07-01",
         );
      });
   });

   it("reads an unparseable merge-key list as empty, which forces a re-seed", async () => {
      // Degrading to [] reads as "range replace", which MISMATCHES any merge
      // declaration — the direction that re-seeds rather than merging under keys
      // we could not confirm.
      const { repo, db } = await freshRepo();
      await repo.upsert(entry({ mergeKeyDimensions: ["order_id"] }));
      await db.run(
         `UPDATE incremental_ledger SET merge_key_dimensions = '"not-a-list"'
           WHERE physical_table_name = ?`,
         [TABLE],
      );
      expect((await repo.get(ENV, CONN, TABLE))!.mergeKeyDimensions).toEqual(
         [],
      );
   });
});
