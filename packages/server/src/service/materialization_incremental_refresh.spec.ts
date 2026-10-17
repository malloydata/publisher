// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Manifest } from "@malloydata/malloy";
import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { BuildInstruction, LedgerEntry } from "../storage/DatabaseInterface";
import { indexCallerLedger } from "./incremental_build";
import { fakeSource } from "../test_helpers/materialization_test_fixtures";
import { createMocks } from "../test_helpers/materialization_service_harness";

// ── Incremental dispatch inside buildOneSource ────────────────────────────
//
// The planner's decisions are unit-tested against fakes in
// incremental_apply.spec.ts, and the DML's semantics are executed in
// incremental_dml_semantics.spec.ts. What is left, and what these cover, is the
// WIRING: that a delta replaces the CTAS rather than joining it, that a seed is
// still byte-for-byte the old build, and that the boundary is written and cleared
// at the right moments.
describe("buildOneSource: incremental refresh", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   const DECLARATION = {
      refresh: "incremental",
      incremental: true,
      declaredWatermark: true,
      declaredMergeKey: false,
      watermark: {
         name: "order_date",
         kind: "dimension" as const,
         malloyType: "date",
      },
      watermarkOrderable: true,
      mergeKeys: [],
      watermarkInMergeKeys: false,
      strategy: "range_replace" as const,
      malformed: [],
      unknownKeys: [],
      calculateFields: [],
      outputColumns: ["order_date", "revenue"],
   };

   const LEDGER_ROW = {
      environmentId: "env-1",
      packageName: "pkg",
      sourceEntityId: "abcdef1234567890",
      coveredThroughValue: "2024-06-01",
      coveredThroughType: "date",
      watermarkDimension: "order_date",
      mergeKeyDimensions: [] as string[],
      derivedStrategy: "range_replace" as const,
      physicalTableName: "orders_v1",
      connectionName: "wh",
      advancedByMaterializationId: "run-0",
      advancedAt: new Date("2024-06-01T00:00:00Z"),
      createdAt: new Date("2024-05-01T00:00:00Z"),
   };

   /** A runSQL stub that answers the probes and records every statement. */
   function probeAwareRunSQL(nonEmpty = true): sinon.SinonStub {
      const stub = sinon.stub();
      stub.callsFake(async (sql: string) => {
         if (sql.includes("information_schema")) {
            return {
               rows: [
                  { column_name: "order_date" },
                  { column_name: "revenue" },
               ],
            };
         }
         if (sql.includes("LIMIT 1")) {
            return { rows: nonEmpty ? [{ present: 1 }] : [] };
         }
         if (sql.includes("watermark_max")) {
            return { rows: [{ watermark_max: "2024-06-20" }] };
         }
         return { rows: [] };
      });
      return stub;
   }

   function incrementalContext(overrides: {
      ledgerEntry?: typeof LEDGER_ROW | null;
      forceRefresh?: boolean;
      /**
       * The caller-supplied ledger. Present (even `[]`) puts the run in
       * caller-ledger mode, replacing the local store; absent reads the store.
       */
      ledger?: LedgerEntry[];
      now?: Date;
   }) {
      const upsert = sinon.stub().resolves();
      const remove = sinon.stub().resolves();
      const read = sinon
         .stub()
         .resolves(
            overrides.ledgerEntry === undefined
               ? LEDGER_ROW
               : overrides.ledgerEntry,
         );
      const context = {
         environmentId: "env-1",
         packageName: "pkg",
         materializationId: "run-1",
         forceRefresh: overrides.forceRefresh ?? false,
         now: overrides.now ?? new Date("2024-07-01T00:00:00Z"),
         declarations: { orders: DECLARATION },
         ...(overrides.ledger !== undefined
            ? { callerLedger: indexCallerLedger(overrides.ledger) }
            : {}),
         ledger: {
            getIncrementalLedgerEntry: read,
            upsertIncrementalLedgerEntry: upsert,
            deleteIncrementalLedgerEntry: remove,
         },
      };
      return { context, upsert, remove, read };
   }

   function callBuildOneSource(params: {
      runSQL: sinon.SinonStub;
      context?: unknown;
      annotationFields?: Record<string, string>;
      manifest?: Manifest;
      dialectName?: string;
      reseed?: boolean;
   }): Promise<{
      sourceEntityId: string;
      physicalTableName: string;
      refresh?: string;
      ledger?: LedgerEntry;
   }> {
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT * FROM t",
         connectionName: "wh",
         dialectName: params.dialectName ?? "postgres",
         annotationFields: params.annotationFields,
         // What the delta's DML names, and what the seed's CTAS would have given
         // the table its columns from.
         columns: ["order_date", "revenue"],
      });
      const instruction = {
         sourceEntityId: "abcdef1234567890",
         materializedTableId: "mt-1",
         physicalTableName: "orders_v1",
         realization: "COPY",
         ...(params.reseed === undefined ? {} : { reseed: params.reseed }),
      } as BuildInstruction;
      return (
         ctx.service as unknown as {
            buildOneSource: (...args: unknown[]) => Promise<{
               sourceEntityId: string;
               physicalTableName: string;
               refresh?: string;
               ledger?: LedgerEntry;
            }>;
         }
      ).buildOneSource(
         source,
         instruction,
         { runSQL: params.runSQL },
         { wh: "dig" },
         params.manifest ?? new Manifest(),
         {
            getApiConnection: () => {
               throw new Error("no connection config in this test");
            },
            getEnvironmentPath: () => "/tmp/env",
         },
         {},
         undefined,
         params.context,
         // The content address, which is the ledger's key.
         "abcdef1234567890",
      );
   }

   it("applies the delta INSTEAD of the CTAS, and advances the boundary", async () => {
      const runSQL = probeAwareRunSQL();
      const { context, upsert } = incrementalContext({});
      const entry = await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      // The delta went out as ONE call: the DELETE and the INSERT have to commit
      // together.
      const script = statements.find((s) => s.startsWith("BEGIN;"));
      expect(script).toBeDefined();
      expect(script).toContain(`DELETE FROM "orders_v1" WHERE "order_date" >=`);
      expect(script).toContain(
         `INSERT INTO "orders_v1" ("order_date", "revenue")`,
      );
      // The INSERT reads the SEED's own SQL, filtered. Not a compiled query's
      // SQL, which on Postgres would arrive wrapped in row_to_json and make this
      // INSERT fail on the first column it names — see deltaSelect.
      expect(script).toContain(`FROM (SELECT * FROM (SELECT * FROM t) AS __d`);
      expect(script).toContain("COMMIT;");
      // And nothing rebuilt the table.
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(false);
      expect(statements.some((s) => s.includes("RENAME TO"))).toBe(false);

      expect(upsert.calledOnce).toBe(true);
      expect(upsert.firstCall.args[0]).toMatchObject({
         sourceEntityId: "abcdef1234567890",
         // The run's snapshot, for a date watermark.
         coveredThroughValue: "2024-07-01",
         coveredThroughType: "date",
         watermarkDimension: "order_date",
         derivedStrategy: "range_replace",
         physicalTableName: "orders_v1",
         advancedByMaterializationId: "run-1",
      });
      // The entry still names the same table: a delta advances in place.
      expect(entry.physicalTableName).toBe("orders_v1");
      // And it reports the coverage it reached, so an orchestrating caller can
      // verify the delta advanced instead of inferring it from the run's
      // outcome. The boundary is reported ONLY here, alongside the watermark and
      // address it was measured under, which is what makes it comparable.
      expect(entry.ledger).toEqual({
         connectionName: "wh",
         physicalTableName: "orders_v1",
         coveredThrough: "2024-07-01",
         coveredThroughType: "date",
         watermark: "order_date",
         strategy: "range_replace",
         sourceEntityId: "abcdef1234567890",
      });
   });

   it("re-seeds the ONE source whose instruction says so", async () => {
      // Per-source, so a host can rebuild one while the rest advance by delta.
      // forceRefresh is never how this is asked for, in any mode.
      const runSQL = probeAwareRunSQL();
      const { context, upsert } = incrementalContext({});
      const entry = await callBuildOneSource({ runSQL, context, reseed: true });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(true);
      expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
      // A rebuild still leaves a boundary behind — probed from the table it just
      // wrote — so the NEXT refresh is a delta again rather than another rebuild.
      expect(upsert.calledOnce).toBe(true);
      expect(entry.ledger?.coveredThrough).toBe("2024-07-01");
   });

   it("reports the boundary a SEED reached on the manifest entry", async () => {
      // Same field as the delta case, so a caller reads coverage one way whatever
      // the step did.
      const runSQL = probeAwareRunSQL();
      const { context } = incrementalContext({ ledgerEntry: null });
      const entry = await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(true);
      expect(entry.ledger?.coveredThrough).toBe("2024-07-01");
      expect(entry.ledger?.coveredThroughType).toBe("date");
   });

   it("carries the Snowflake delta as ONE scripting block, not a script", async () => {
      // Snowflake's driver executes exactly one statement per call, each on a
      // possibly different pooled session — a BEGIN;…;COMMIT; script is refused
      // and a statement-per-call loop would autocommit the DELETE alone. The
      // whole transaction has to arrive inside a single Snowflake Scripting
      // statement.
      const runSQL = probeAwareRunSQL();
      const { context, upsert } = incrementalContext({});
      await callBuildOneSource({ runSQL, context, dialectName: "snowflake" });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      const block = statements.find((s) =>
         s.startsWith("EXECUTE IMMEDIATE $$"),
      );
      expect(block).toBeDefined();
      expect(block).toContain(`DELETE FROM "orders_v1" WHERE "order_date" >=`);
      expect(block).toContain(
         `INSERT INTO "orders_v1" ("order_date", "revenue")`,
      );
      expect(statements.some((s) => s.startsWith("BEGIN"))).toBe(false);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(false);
      expect(upsert.calledOnce).toBe(true);
   });

   it("skips when nothing advanced: no DML, no ledger write, entry still emitted", async () => {
      // The boundary already sits at the run's snapshot, so there is no range to
      // compute. The table keeps serving as-is — but the manifest entry must
      // still appear, or a downstream source built later in this run would
      // recompute this upstream from raw.
      const runSQL = probeAwareRunSQL();
      const { context, upsert, remove } = incrementalContext({
         ledgerEntry: { ...LEDGER_ROW, coveredThroughValue: "2024-07-01" },
      });
      const manifest = new Manifest();
      const entry = await callBuildOneSource({ runSQL, context, manifest });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(false);
      expect(upsert.called).toBe(false);
      expect(remove.called).toBe(false);
      expect(entry.physicalTableName).toBe("orders_v1");
      // Downstream resolution: the QUOTED serving table is in the build manifest.
      expect(
         manifest.buildManifest.entries["abcdef1234567890"]?.tableName,
      ).toBe('"orders_v1"');
      // A skip reports the boundary that STAYS in force, so a caller does not
      // read a gap where coverage is simply unchanged.
      expect(entry.ledger?.coveredThrough).toBe("2024-07-01");
   });

   it("fails the build and leaves the boundary alone when the delta's DML fails", async () => {
      // The crash-safety property: an unadvanced boundary makes the next run
      // recompute the same (idempotent) range. Advancing it past a failed write
      // would skip those rows forever.
      const runSQL = probeAwareRunSQL();
      runSQL
         .withArgs(sinon.match((sql: string) => sql.startsWith("BEGIN;")))
         .rejects(new Error("deadlock detected"));
      const { context, upsert } = incrementalContext({});

      await expect(callBuildOneSource({ runSQL, context })).rejects.toThrow(
         "deadlock detected",
      );
      expect(upsert.called).toBe(false);
      // The session was rolled back so a pooled connection is not returned
      // inside an aborted transaction.
      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements).toContain("ROLLBACK");
   });

   it("rebuilds and re-records the boundary when none is recorded", async () => {
      const runSQL = probeAwareRunSQL();
      const { context, upsert, remove } = incrementalContext({
         ledgerEntry: null,
      });
      await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      // The full build, unchanged.
      expect(statements).toContain(
         'CREATE TABLE "orders_v1_abcdef123456" AS (SELECT * FROM t)',
      );
      expect(statements).toContain(
         'ALTER TABLE "orders_v1_abcdef123456" RENAME TO "orders_v1"',
      );
      // Cleared before the rebuild starts, then set from the table it wrote.
      expect(remove.calledOnce).toBe(true);
      expect(upsert.calledOnce).toBe(true);
      expect(upsert.firstCall.args[0]).toMatchObject({
         coveredThroughValue: "2024-07-01",
      });
   });

   it("routes forceRefresh to a full rebuild and resets the boundary", async () => {
      const runSQL = probeAwareRunSQL();
      const { context, remove } = incrementalContext({ forceRefresh: true });
      await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(true);
      expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
      expect(remove.calledOnce).toBe(true);
   });

   it("records nothing when the rebuilt table has no watermark values", async () => {
      // An empty seed sets no boundary on purpose: one taken from an empty table
      // would exclude every historical row that arrives later.
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         if (sql.includes("watermark_max")) {
            return { rows: [{ watermark_max: null }] };
         }
         return { rows: [] };
      });
      const { context, upsert } = incrementalContext({ ledgerEntry: null });
      await callBuildOneSource({ runSQL, context });
      expect(upsert.called).toBe(false);
   });

   it("leaves a non-incremental source's build exactly as it was", async () => {
      // The context is present (another source in the package declared it), so
      // this proves the branch is keyed on the SOURCE, not on the run.
      const runSQL = sinon.stub().resolves({ rows: [] });
      const { context, upsert, remove } = incrementalContext({});
      context.declarations = {} as typeof context.declarations;
      await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements).toEqual([
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
         'CREATE TABLE "orders_v1_abcdef123456" AS (SELECT * FROM t)',
         'DROP TABLE IF EXISTS "orders_v1"',
         'ALTER TABLE "orders_v1_abcdef123456" RENAME TO "orders_v1"',
      ]);
      expect(upsert.called).toBe(false);
      expect(remove.called).toBe(false);
   });

   it("leaves the build alone on a dialect the delta path does not support", async () => {
      // Same declaration, DuckDB instead of Postgres: no probe, no delta, no
      // ledger write — the gate rejects this at publish, and a package that
      // predates the gate simply builds full.
      const runSQL = sinon.stub().resolves({ rows: [] });
      const { context, upsert } = incrementalContext({});
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT * FROM t",
         dialectName: "duckdb",
      });
      await (
         ctx.service as unknown as {
            buildOneSource: (...args: unknown[]) => Promise<unknown>;
         }
      ).buildOneSource(
         source,
         {
            sourceEntityId: "abcdef1234567890",
            materializedTableId: "mt-1",
            physicalTableName: "orders_v1",
            realization: "COPY",
         } as BuildInstruction,
         { runSQL },
         { duckdb: "dig" },
         new Manifest(),
         {
            getApiConnection: () => {
               throw new Error("no connection config in this test");
            },
            getEnvironmentPath: () => "/tmp/env",
         },
         {},
         undefined,
         context,
         "abcdef1234567890",
      );
      expect(runSQL.getCalls().map((c) => c.args[0])).toHaveLength(4);
      expect(upsert.called).toBe(false);
   });

   it("reports what an incremental refresh DID: delta, full or none", async () => {
      // Otherwise indistinguishable from the entry, and a caller cannot infer it
      // from what it instructed: every fallback answers success and rebuilds.
      const delta = await callBuildOneSource({
         runSQL: probeAwareRunSQL(),
         context: incrementalContext({}).context,
      });
      expect(delta.refresh).toBe("delta");

      const rebuilt = await callBuildOneSource({
         runSQL: probeAwareRunSQL(),
         context: incrementalContext({ ledgerEntry: null }).context,
      });
      expect(rebuilt.refresh).toBe("full");

      // A skip applied nothing and SAYS so, rather than leaving it to an absent
      // field: its unchanged boundary already says the table stands where it did.
      const skipped = await callBuildOneSource({
         runSQL: probeAwareRunSQL(),
         context: incrementalContext({
            ledgerEntry: { ...LEDGER_ROW, coveredThroughValue: "2024-07-01" },
         }).context,
      });
      expect(skipped.refresh).toBe("none");

      // Which leaves absence with exactly one meaning: this source is not
      // refreshed incrementally at all.
      const { context } = incrementalContext({});
      context.declarations = {} as typeof context.declarations;
      const ordinary = await callBuildOneSource({
         runSQL: probeAwareRunSQL(),
         context,
      });
      expect(ordinary.refresh).toBeUndefined();
   });

   // ── caller-supplied ledger ──────────────────────────────────────────────
   //
   // The same dispatch, reading the boundary off `buildInstructions.ledger`
   // instead of the local store. What these pin: the boundary reaches the same
   // planner through the same checks, the local store is neither read nor
   // written on any path, a missing entry seeds exactly like a missing row —
   // and, unlike a stored row, an entry that no longer describes its source
   // FAILS the run, per the API contract that an invalid input is an error.
   describe("with a caller-supplied ledger", () => {
      /** The publisher's own last report (`ManifestEntry.ledger`), echoed back. */
      const ENTRY: LedgerEntry = {
         connectionName: "wh",
         physicalTableName: "orders_v1",
         coveredThrough: "2024-06-20",
         coveredThroughType: "date",
         watermark: "order_date",
         strategy: "range_replace",
         sourceEntityId: "abcdef1234567890",
      };

      it("advances from the caller's entry while a DIFFERENT local row sits unread", async () => {
         // The local row says 2024-06-01 and is deliberately wrong: this is the
         // whole point of the mode, since the row a given worker holds may be a
         // generation stale or absent entirely.
         const runSQL = probeAwareRunSQL();
         const { context, upsert, remove, read } = incrementalContext({
            ledger: [ENTRY],
         });
         const entry = await callBuildOneSource({ runSQL, context });

         const script = runSQL
            .getCalls()
            .map((c) => c.args[0] as string)
            .find((s) => s.startsWith("BEGIN;"));
         expect(script).toContain(`>= DATE '2024-06-20'`);
         expect(script).not.toContain("2024-06-01");
         expect(read.called).toBe(false);
         // Not written either, on either path: a row left behind here is a stale
         // landmine for a later run in the other mode.
         expect(upsert.called).toBe(false);
         expect(remove.called).toBe(false);
         // The advance travels back on the entry's `ledger`, which is the
         // caller's own next input — verbatim.
         expect(entry.refresh).toBe("delta");
         expect(entry.ledger).toEqual({
            ...ENTRY,
            coveredThrough: "2024-07-01",
         });
      });

      it("round-trips: store run 1's `ledger`, return it, run 2 deltas from it", async () => {
         const first = await callBuildOneSource({
            runSQL: probeAwareRunSQL(),
            // An empty ledger, which is what an orchestrator sends the first
            // time: it owns the ledger and has nothing recorded yet.
            context: incrementalContext({ ledger: [] }).context,
         });
         expect(first.refresh).toBe("full");
         // The seed reports the whole entry to store — the caller's entire
         // protocol is `entries[*].ledger` back as `buildInstructions.ledger`.
         expect(first.ledger).toBeDefined();
         expect(first.ledger!.coveredThrough).toBe("2024-07-01");

         const runSQL = probeAwareRunSQL();
         const second = await callBuildOneSource({
            runSQL,
            context: incrementalContext({
               ledger: [first.ledger!],
               now: new Date("2024-07-02T00:00:00Z"),
            }).context,
         });
         expect(second.refresh).toBe("delta");
         const script = runSQL
            .getCalls()
            .map((c) => c.args[0] as string)
            .find((s) => s.startsWith("BEGIN;"));
         expect(script).toContain(`>= DATE '2024-07-01'`);
         expect(script).toContain(`< DATE '2024-07-02'`);
         expect(second.ledger!.coveredThrough).toBe("2024-07-02");
      });

      it("recomputes an idempotent overlap when the entry lags the table", async () => {
         // A stale-but-lagging entry (the caller echoed an older report) is
         // SAFE, not an error and not a rebuild: the half-open range is
         // idempotent, so the delta recomputes rows the table already holds and
         // replaces them with themselves. This is the same tolerance a lagging
         // local row has always had.
         const runSQL = probeAwareRunSQL();
         const { context } = incrementalContext({
            ledger: [{ ...ENTRY, coveredThrough: "2024-06-10" }],
         });
         const entry = await callBuildOneSource({ runSQL, context });
         expect(entry.refresh).toBe("delta");
         const script = runSQL
            .getCalls()
            .map((c) => c.args[0] as string)
            .find((s) => s.startsWith("BEGIN;"));
         expect(script).toContain(`>= DATE '2024-06-10'`);
      });

      // These still rebuild rather than erroring: none of them is a defect in
      // the caller's INPUT. A missing entry means "nothing recorded" (a first
      // build), and `reseed` is the documented escape hatch that ignores the
      // entry. Both report the path they took.
      const fallbacks: [
         string,
         {
            ledger: LedgerEntry[];
            reseed?: boolean;
         },
      ][] = [
         ["no entry for this table (an empty ledger)", { ledger: [] }],
         ["reseed, which ignores the entry", { ledger: [ENTRY], reseed: true }],
      ];

      for (const [what, params] of fallbacks) {
         it(`rebuilds in full, truthfully, given ${what}`, async () => {
            const runSQL = probeAwareRunSQL();
            const { context, upsert, remove } = incrementalContext({
               ledger: params.ledger,
            });
            const entry = await callBuildOneSource({
               runSQL,
               context,
               ...(params.reseed === undefined
                  ? {}
                  : { reseed: params.reseed }),
            });

            const statements = runSQL
               .getCalls()
               .map((c) => c.args[0] as string);
            expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(
               true,
            );
            expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
            expect(entry.refresh).toBe("full");
            // A rebuild still reports the entry to store, so the caller's next
            // run has a boundary to send.
            expect(entry.ledger?.coveredThrough).toBe("2024-07-01");
            expect(upsert.called).toBe(false);
            expect(remove.called).toBe(false);
         });
      }

      // An entry that no longer describes the source it names is the caller's
      // error, and the API contract makes it one. Only a mismatch create-time
      // validation cannot see reaches this throw — one the compiled declaration
      // alone reveals — but the unit pins the whole family.
      const invalid: [string, LedgerEntry][] = [
         ["measures another watermark", { ...ENTRY, watermark: "shipped_at" }],
         [
            "was measured under another watermark type",
            { ...ENTRY, coveredThroughType: "timestamp" },
         ],
         ["was advanced by another strategy", { ...ENTRY, strategy: "merge" }],
         [
            "carries a different row identity",
            { ...ENTRY, mergeKeys: ["order_id"] },
         ],
         [
            "was measured under different SQL",
            { ...ENTRY, sourceEntityId: "some-older-address" },
         ],
      ];

      for (const [what, entry] of invalid) {
         it(`fails the run when the supplied entry ${what}`, async () => {
            const runSQL = probeAwareRunSQL();
            const { context, upsert, remove } = incrementalContext({
               ledger: [entry],
            });
            await expect(
               callBuildOneSource({ runSQL, context }),
            ).rejects.toThrow(/different definition of this source/);
            // Nothing was built and nothing was written: the run fails before
            // any DML, so the caller can fix the input and re-send.
            const statements = runSQL
               .getCalls()
               .map((c) => c.args[0] as string);
            expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(
               false,
            );
            expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
            expect(upsert.called).toBe(false);
            expect(remove.called).toBe(false);
         });
      }

      it("still seeds on a fact about the TABLE, exactly as local mode does", async () => {
         // The entry is fine; the table is empty. That is not the caller's
         // input being wrong, so it degrades the same way a local row does.
         const runSQL = probeAwareRunSQL(false);
         const { context, upsert } = incrementalContext({ ledger: [ENTRY] });
         const entry = await callBuildOneSource({ runSQL, context });
         expect(entry.refresh).toBe("full");
         expect(upsert.called).toBe(false);
      });
   });
});
