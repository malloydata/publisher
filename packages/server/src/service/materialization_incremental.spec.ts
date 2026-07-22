import { Manifest } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { BadRequestError } from "../errors";
import { BuildInstruction } from "../storage/DatabaseInterface";
import { EnvironmentStore } from "./environment_store";
import { fakeSource } from "./materialization_test_fixtures";
import {
   buildMergeSQL,
   hydrateIncrementalKeysCte,
   MaterializationService,
   refreshMode,
} from "./materialization_service";

/**
 * Unit coverage for the incremental persist path (`#@ persist refresh=incremental`,
 * persistence.md §3 / Phase E1). Three surfaces:
 *
 *  - `refreshMode`: the annotation-to-mode mapping, defaulting to "full".
 *  - `hydrateIncrementalKeysCte` / `buildMergeSQL`: the pure SQL builders —
 *    the compile-safe self-reference primitive and the upsert MERGE.
 *  - `buildOneSource` dispatch: full-rebuild stays byte-for-byte unchanged
 *    (back-compat), incremental seeds on first run (CTAS) and MERGEs after.
 *
 * The SQL builders are dialect-quoted, so the assertions pin the exact emitted
 * text — a change to quoting or clause order is a deliberate, reviewed edit.
 */

// ── refreshMode ──────────────────────────────────────────────────────────────

describe("refreshMode", () => {
   it("defaults to full when refresh is unset", () => {
      const s = fakeSource({ name: "s", sourceEntityId: "e" });
      expect(refreshMode(s)).toBe("full");
   });

   it("resolves refresh=full to full", () => {
      const s = fakeSource({
         name: "s",
         sourceEntityId: "e",
         annotationFields: { refresh: "full" },
      });
      expect(refreshMode(s)).toBe("full");
   });

   it("resolves refresh=incremental to incremental", () => {
      const s = fakeSource({
         name: "s",
         sourceEntityId: "e",
         annotationFields: { refresh: "incremental" },
      });
      expect(refreshMode(s)).toBe("incremental");
   });

   it("degrades an unrecognized refresh value to full (policy rejects it separately)", () => {
      const s = fakeSource({
         name: "s",
         sourceEntityId: "e",
         annotationFields: { refresh: "merge" },
      });
      expect(refreshMode(s)).toBe("full");
   });
});

// ── hydrateIncrementalKeysCte (the compile-safe self-reference primitive) ─────

describe("hydrateIncrementalKeysCte", () => {
   it("returns the SQL unchanged when the reserved CTE is absent", () => {
      const sql = "SELECT item_name FROM raw";
      expect(
         hydrateIncrementalKeysCte(sql, '"ds"."t"', "item_name", "postgres"),
      ).toBe(sql);
   });

   it("rewrites the empty CTE body to select the primary key from the target", () => {
      const sql =
         "WITH __malloy_incremental_keys AS (SELECT NULL AS item_name WHERE false) " +
         "SELECT item_name FROM raw " +
         "WHERE item_name NOT IN (SELECT item_name FROM __malloy_incremental_keys)";
      const out = hydrateIncrementalKeysCte(
         sql,
         '"ds"."t"',
         "item_name",
         "postgres",
      );
      expect(out).toContain(
         'WITH __malloy_incremental_keys AS ( SELECT "item_name" FROM "ds"."t" )',
      );
      // The downstream reference to the CTE is untouched — only the body changed.
      expect(out).toContain(
         "WHERE item_name NOT IN (SELECT item_name FROM __malloy_incremental_keys)",
      );
   });

   it("balances nested parens: matches the CTE's closing paren, not the first", () => {
      // The empty body itself contains a parenthesized subquery. A naive
      // first-close-paren rewrite would corrupt the SQL; the balanced scan must
      // consume the whole body.
      const sql =
         "WITH __malloy_incremental_keys AS (SELECT x FROM (SELECT 1 AS x) z WHERE false) " +
         "SELECT * FROM src";
      const out = hydrateIncrementalKeysCte(sql, '"t"', "id", "postgres");
      expect(out).toBe(
         'WITH __malloy_incremental_keys AS ( SELECT "id" FROM "t" ) SELECT * FROM src',
      );
   });

   it("quotes the primary key for a backtick dialect (BigQuery)", () => {
      const sql =
         "WITH __malloy_incremental_keys AS (SELECT 1 WHERE false) SELECT * FROM src";
      const out = hydrateIncrementalKeysCte(
         sql,
         "`ds`.`t`",
         "order_id",
         "standardsql",
      );
      expect(out).toContain(
         "WITH __malloy_incremental_keys AS ( SELECT `order_id` FROM `ds`.`t` )",
      );
   });

   it("matches the AS keyword case-insensitively (a lowercase `as` still hydrates)", () => {
      // `as` is a valid SQL spelling; a case-sensitive scan would silently skip
      // hydration, leaving the filter inert and reprocessing every row.
      const sql =
         "WITH __malloy_incremental_keys as (SELECT 1 WHERE false) SELECT * FROM src";
      const out = hydrateIncrementalKeysCte(sql, '"t"', "id", "postgres");
      expect(out).toBe(
         'WITH __malloy_incremental_keys as ( SELECT "id" FROM "t" ) SELECT * FROM src',
      );
   });

   it("does not anchor on an `AS` substring in an unrelated upstream identifier", () => {
      // A column alias containing "AS" before the reserved CTE must not be
      // mistaken for the CTE's AS keyword.
      const sql =
         "WITH __malloy_incremental_keys AS (SELECT 1 WHERE false) " +
         "SELECT gross_sales AS revenue FROM src";
      const out = hydrateIncrementalKeysCte(sql, '"t"', "id", "postgres");
      expect(out).toBe(
         'WITH __malloy_incremental_keys AS ( SELECT "id" FROM "t" ) ' +
            "SELECT gross_sales AS revenue FROM src",
      );
   });
});

// ── buildMergeSQL (upsert on the primary key) ────────────────────────────────

describe("buildMergeSQL", () => {
   it("upserts: WHEN MATCHED updates non-key columns, WHEN NOT MATCHED inserts all", () => {
      const sql = buildMergeSQL(
         '"ds"."t"',
         "SELECT * FROM staged",
         "order_id",
         ["order_id", "venue", "amount"],
         "postgres",
      );
      expect(sql).toBe(
         'MERGE INTO "ds"."t" T USING (SELECT * FROM staged) S ON T."order_id" = S."order_id" ' +
            'WHEN MATCHED THEN UPDATE SET "venue" = S."venue", "amount" = S."amount" ' +
            'WHEN NOT MATCHED THEN INSERT ("order_id", "venue", "amount") ' +
            'VALUES (S."order_id", S."venue", S."amount")',
      );
   });

   it("omits the UPDATE clause when the key is the only column (insert-only append)", () => {
      const sql = buildMergeSQL(
         '"t"',
         "SELECT id FROM staged",
         "id",
         ["id"],
         "postgres",
      );
      expect(sql).not.toContain("WHEN MATCHED");
      expect(sql).toContain(
         'WHEN NOT MATCHED THEN INSERT ("id") VALUES (S."id")',
      );
   });

   it("quotes identifiers for a backtick dialect", () => {
      const sql = buildMergeSQL(
         "`t`",
         "SELECT * FROM s",
         "id",
         ["id", "name"],
         "standardsql",
      );
      expect(sql).toContain("ON T.`id` = S.`id`");
      expect(sql).toContain("UPDATE SET `name` = S.`name`");
      expect(sql).toContain("INSERT (`id`, `name`) VALUES (S.`id`, S.`name`)");
   });
});

// ── buildOneSource dispatch (full unchanged; incremental seed + MERGE) ────────

describe("buildOneSource incremental dispatch", () => {
   function service(): MaterializationService {
      // buildOneSource and its helpers never touch the environment store, so a
      // bare stand-in is enough to reach the private build path.
      return new MaterializationService({} as unknown as EnvironmentStore);
   }

   // A connection stub. `targetColumns`: null => the target table is absent, so
   // fetchSchemaForTables reports a keyed error (first-run seed); a string[] =>
   // the table exists with exactly those columns (MERGE). The incremental path
   // reads the MERGE column set straight from this schema, so these ARE the
   // columns the MERGE uses. The reserved lookup key is "__persist_target".
   function conn(targetColumns: string[] | null): {
      runSQL: sinon.SinonStub;
      fetchSchemaForTables: sinon.SinonStub;
   } {
      const fetchSchemaForTables = sinon.stub().resolves(
         targetColumns === null
            ? { schemas: {}, errors: { __persist_target: "not found" } }
            : {
                 schemas: {
                    __persist_target: {
                       fields: targetColumns.map((name) => ({ name })),
                    },
                 },
                 errors: {},
              },
      );
      return { runSQL: sinon.stub().resolves(), fetchSchemaForTables };
   }

   function callBuild(
      svc: MaterializationService,
      source: ReturnType<typeof fakeSource>,
      connection: {
         runSQL: sinon.SinonStub;
         fetchSchemaForTables?: sinon.SinonStub;
      },
      physicalTableName: string,
      manifest: Manifest = new Manifest(),
      forceFullRebuild = false,
   ) {
      const instruction: BuildInstruction = {
         sourceEntityId: "abcdef1234567890",
         materializedTableId: "mt-1",
         physicalTableName,
         realization: "COPY",
      };
      return (
         svc as unknown as {
            buildOneSource: (
               s: unknown,
               i: BuildInstruction,
               c: unknown,
               d: Record<string, string>,
               m: Manifest,
               forceFullRebuild: boolean,
            ) => Promise<{ sourceEntityId: string; physicalTableName: string }>;
         }
      ).buildOneSource(
         source,
         instruction,
         connection,
         { postgres: "dig" },
         manifest,
         forceFullRebuild,
      );
   }

   it("full (refresh unset) keeps the staging + atomic-rename path unchanged", async () => {
      const connection = conn(null);
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT * FROM t",
         dialectName: "postgres",
      });
      await callBuild(service(), source, connection, "orders_v1");
      const sql = connection.runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
         'CREATE TABLE "orders_v1_abcdef123456" AS (SELECT * FROM t)',
         'DROP TABLE IF EXISTS "orders_v1"',
         'ALTER TABLE "orders_v1_abcdef123456" RENAME TO "orders_v1"',
      ]);
      // The full path never introspects the target schema.
      expect(connection.fetchSchemaForTables.called).toBe(false);
   });

   it("incremental first run (target absent) seeds with a plain CTAS, no staging or rename", async () => {
      // fetchSchemaForTables reports the target absent -> the seed is a single
      // CTAS of the source SQL directly into the target (no staging suffix, no
      // rename), so the warehouse infers the schema.
      const connection = conn(null);
      const source = fakeSource({
         name: "classifications",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT item_name FROM raw",
         dialectName: "postgres",
         annotationFields: { refresh: "incremental" },
         explore: { primaryKey: "item_name" },
      });
      const entry = await callBuild(service(), source, connection, "class_v1");

      const sql = connection.runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         'CREATE TABLE "class_v1" AS (SELECT item_name FROM raw)',
      ]);
      expect(sql.some((s) => s.includes("RENAME"))).toBe(false);
      expect(sql.some((s) => s.includes("_abcdef123456"))).toBe(false);
      expect(entry.physicalTableName).toBe("class_v1");
   });

   it("incremental subsequent run (target present) MERGEs on the primary key", async () => {
      // Table exists -> MERGE, not CTAS. The MERGE column set comes from the
      // target table's schema (via fetchSchemaForTables), and it upserts on the
      // source's primary_key. MERGE (never raw INSERT) is idempotent under retry.
      const connection = conn(["order_id", "venue", "amount"]);
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT order_id, venue, amount FROM raw",
         dialectName: "postgres",
         annotationFields: { refresh: "incremental" },
         explore: { primaryKey: "order_id" },
      });
      await callBuild(service(), source, connection, "orders_v1");

      const statements = connection.runSQL
         .getCalls()
         .map((c) => c.args[0] as string);
      const merge = statements.find((s) => s.startsWith("MERGE INTO"));
      expect(merge).toBe(
         'MERGE INTO "orders_v1" T USING (SELECT order_id, venue, amount FROM raw) S ' +
            'ON T."order_id" = S."order_id" ' +
            'WHEN MATCHED THEN UPDATE SET "venue" = S."venue", "amount" = S."amount" ' +
            'WHEN NOT MATCHED THEN INSERT ("order_id", "venue", "amount") ' +
            'VALUES (S."order_id", S."venue", S."amount")',
      );
      // Never a CTAS or a raw INSERT on the incremental path once seeded.
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(false);
      expect(statements.some((s) => /^INSERT\b/.test(s))).toBe(false);
   });

   it("carries NON-ATOMIC target columns (struct/array) into the MERGE", async () => {
      // Regression guard for the deriveColumns bug: the MERGE column set is the
      // target table's real columns, so a struct/array/record column present in
      // the seeded table is upserted too — not silently dropped (which would
      // diverge or fail a NOT NULL insert). Here `attrs` stands in for a
      // non-atomic column the warehouse materialized on the first run.
      const connection = conn(["item_name", "category", "attrs"]);
      const source = fakeSource({
         name: "classifications",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT item_name, category, attrs FROM raw",
         dialectName: "postgres",
         annotationFields: { refresh: "incremental" },
         explore: { primaryKey: "item_name" },
      });
      await callBuild(service(), source, connection, "class_v1");

      const merge = connection.runSQL
         .getCalls()
         .map((c) => c.args[0] as string)
         .find((s) => s.startsWith("MERGE INTO"))!;
      expect(merge).toContain(
         'UPDATE SET "category" = S."category", "attrs" = S."attrs"',
      );
      expect(merge).toContain(
         'INSERT ("item_name", "category", "attrs") VALUES (S."item_name", S."category", S."attrs")',
      );
   });

   it("forceFullRebuild routes an incremental source through the full staging + rename path", async () => {
      // The escape hatch: forceFullRebuild bypasses the MERGE/seed path entirely
      // and rebuilds via staging + atomic rename. No schema fetch, no MERGE — the
      // empty self-reference CTE means the full SQL reprocesses every row.
      const connection = conn(["item_name"]);
      const source = fakeSource({
         name: "classifications",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT item_name FROM raw",
         dialectName: "postgres",
         annotationFields: { refresh: "incremental" },
         explore: { primaryKey: "item_name" },
      });
      await callBuild(
         service(),
         source,
         connection,
         "class_v1",
         new Manifest(),
         true,
      );
      const sql = connection.runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         'DROP TABLE IF EXISTS "class_v1_abcdef123456"',
         'CREATE TABLE "class_v1_abcdef123456" AS (SELECT item_name FROM raw)',
         'DROP TABLE IF EXISTS "class_v1"',
         'ALTER TABLE "class_v1_abcdef123456" RENAME TO "class_v1"',
      ]);
      expect(sql.some((s) => s.startsWith("MERGE"))).toBe(false);
      expect(connection.fetchSchemaForTables.called).toBe(false);
   });

   it("incremental MERGE without a primary_key is a BadRequestError", async () => {
      const connection = conn(["order_id"]); // table exists
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT order_id FROM raw",
         dialectName: "postgres",
         annotationFields: { refresh: "incremental" },
         explore: {}, // no primaryKey
      });
      await expect(
         callBuild(service(), source, connection, "orders_v1"),
      ).rejects.toThrow(BadRequestError);
   });

   it("hydrates the self-reference CTE against the live target before MERGE", async () => {
      // End-to-end of the primitive: on an incremental run the source's empty
      // __malloy_incremental_keys CTE is rewritten to read the target, so the
      // MERGE's USING subquery only surfaces rows not already present.
      const connection = conn(["item_name", "category"]);
      const source = fakeSource({
         name: "classifications",
         sourceEntityId: "abcdef1234567890",
         sql:
            "WITH __malloy_incremental_keys AS (SELECT NULL AS item_name WHERE false) " +
            "SELECT item_name, category FROM raw " +
            "WHERE item_name NOT IN (SELECT item_name FROM __malloy_incremental_keys)",
         dialectName: "postgres",
         annotationFields: { refresh: "incremental" },
         explore: { primaryKey: "item_name" },
      });
      await callBuild(service(), source, connection, "class_v1");

      const merge = connection.runSQL
         .getCalls()
         .map((c) => c.args[0] as string)
         .find((s) => s.startsWith("MERGE INTO"))!;
      expect(merge).toContain(
         'WITH __malloy_incremental_keys AS ( SELECT "item_name" FROM "class_v1" )',
      );
   });
});
