// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { contractFor } from "../../../test_helpers/contract_fixture";
import { deriveSelfInstructions } from "./run_instructions";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { MaterializationEligibilityError } from "../../../errors";
import { BuildInstruction } from "../domain/types";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import { installSettings } from "../../../test_helpers/settings_env";

/**
 * Auto-run self-instructions: which sources an auto run instructs itself to
 * build, and which it refuses (eligibility, the database #(authorize) gate,
 * a refused source beside admitted ones).
 */

describe("deriveSelfInstructions", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("rebuilds a source the prior run failed on, and reuses the one it built", () => {
      // Reuse reads a prior manifest's `entries`, and a source that failed is not
      // in it -- so the source has nothing to carry and is instructed to build
      // again. That is what keeps a warehouse error that clears on its own (an
      // expired grant, a dataset created a minute later) from retiring the source
      // from every later run. Asserted through the shape a real prior manifest
      // has, so it holds for the reason the code holds rather than by construction
      // of the fixture: `s2` is absent from `entries` because it failed.
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            // At the name this run writes: carried only there.
            physicalTableName: "s1",
            contract: contractFor(),
            connectionName: "duckdb",
         },
      };

      const { instructions, carried } = (
         ctx.service.collaborators as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, undefined, priorEntries);

      const carriedMap = carried as Record<string, unknown>;
      expect(
         carriedMap["b2bbbbbbbbbbbbbb"],
         "a source the prior run failed on must not be carried forward",
      ).toBeUndefined();
      expect(
         instructions.map((i) => i.sourceEntityId),
         "a source the prior run failed on must be instructed to build again",
      ).toContain("b2bbbbbbbbbbbbbb");
      // The healthy one is still reused, so this is not simply refusing to carry
      // anything.
      expect(carriedMap["b1aaaaaaaaaaaaaa"]).toBeDefined();
   });

   it("carries forward unchanged sourceEntityIds and builds the rest (deduping repeats)", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"], ["s2"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            // At the name this run writes: carried only there.
            physicalTableName: "s1",
            contract: contractFor(),
            connectionName: "duckdb",
         },
      };

      const { instructions, carried } = (
         ctx.service.collaborators as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, undefined, priorEntries);

      expect(instructions).toHaveLength(1);
      expect(instructions[0].sourceEntityId).toBe("b2bbbbbbbbbbbbbb");
      expect(instructions[0].physicalTableName).toBe("s2");
      expect(instructions[0].realization).toBe("COPY");
      expect(instructions[0].materializedTableId).toBe("local-b2bbbbbbbbbb");
      expect(
         (carried as Record<string, unknown>)["b1aaaaaaaaaaaaaa"],
      ).toBeDefined();
   });

   it("instructs a storage rollup over a storage= base beside the base, each to its own table", () => {
      const { restore } = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      });
      try {
         const compiled = compiledWith(
            {
               orders: fakeSource({
                  name: "orders",
                  sourceEntityId: "aa11aa11aa11aa11aa11",
                  annotationFields: { storage: "lake" },
               }),
               rollup: fakeSource({
                  name: "rollup",
                  sourceEntityId: "cc33cc33cc33cc33cc33",
                  annotationFields: { storage: "lake" },
               }),
            },
            [["orders", "rollup"]],
         );
         compiled.tables.rollup = {
            kind: "rollup",
            source: compiled.tables.rollup.source,
            rollup: {
               variant: true,
               baseSourceName: "orders",
               baseSourceID: "orders",
               grainDimensions: ["category"],
               measures: [],
            } as never,
         };
         const { instructions, refused } = (
            ctx.service.collaborators as unknown as {
               deriveSelfInstructions: (
                  c: unknown,
                  n: string[] | undefined,
                  p: unknown,
               ) => {
                  instructions: BuildInstruction[];
                  refused: Record<string, unknown>;
               };
            }
         ).deriveSelfInstructions(compiled, undefined, {});
         expect(instructions.map((i) => i.sourceEntityId)).toEqual([
            "aa11aa11aa11aa11aa11",
            "cc33cc33cc33cc33cc33",
         ]);
         const [base, rolled] = instructions;
         expect(rolled.physicalTableName).not.toBe(base.physicalTableName);
         expect(refused).toEqual({});
      } finally {
         restore();
      }
   });

   it("self-assigns the name= verbatim for a storage= source, exactly as a database build", () => {
      const { restore } = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      });
      try {
         const compiled = compiledWith(
            {
               lake_src: fakeSource({
                  name: "lake_src",
                  sourceEntityId: "aa11aa11aa11aa11aa11",
                  annotationFields: { storage: "lake", name: "daily" },
               }),
               wh_src: fakeSource({
                  name: "wh_src",
                  sourceEntityId: "bb22bb22bb22bb22bb22",
               }),
            },
            [["lake_src", "wh_src"]],
         );
         const { instructions } = (
            ctx.service.collaborators as unknown as {
               deriveSelfInstructions: (
                  c: unknown,
                  n: string[] | undefined,
                  p: unknown,
               ) => { instructions: BuildInstruction[] };
            }
         ).deriveSelfInstructions(compiled, undefined, {});
         const byId = Object.fromEntries(
            instructions.map((i) => [i.sourceEntityId, i]),
         );
         const lake = byId["aa11aa11aa11aa11aa11"];
         const wh = byId["bb22bb22bb22bb22bb22"];
         // storage= → the `name=` value verbatim in the destination (no
         // content-hash decoration); only the destination differs from a database build.
         expect(lake.destination).toBe("lake");
         expect(lake.physicalTableName).toBe("daily");
         // database → the plain logical (source) name, no destination.
         expect(wh.destination).toBeUndefined();
         expect(wh.physicalTableName).toBe("wh_src");
      } finally {
         restore();
      }
   });

   /** deriveSelfInstructions with defaults, for the row-level-authorize tests below. */
   function derive(compiled: unknown): { instructions: BuildInstruction[] } {
      return (
         ctx.service.collaborators as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[] };
         }
      ).deriveSelfInstructions(compiled, undefined, {});
   }

   /** A database (no `storage=`) fakeSource carrying an #(access_filter) gate. */
   const authorizeGatedDatabase = fakeSource({
      name: "s1",
      sourceEntityId: "c1c1c1c1c1c1c1c1",
      sourceDef: { blockNotes: ["#(authorize) true"] },
   });

   describe("database #(authorize) gate", () => {
      let restoreSettings: (() => void) | undefined;
      afterEach(() => {
         restoreSettings?.();
         restoreSettings = undefined;
      });

      it("refuses a database authorize-gated source with no compile-time gate outcome (fail-closed default)", () => {
         // No `sourceGateOutcomes` supplied — the relaxation below requires a
         // PROVEN row_level+attributed outcome, so an unclassified gate keeps
         // refusing exactly as it always has.
         const compiled = compiledWith({ s1: authorizeGatedDatabase }, [
            ["s1"],
         ]);
         expect(() => derive(compiled)).toThrow(
            MaterializationEligibilityError,
         );
         expect(() => derive(compiled)).toThrow(/authorize/i);
      });

      // A `row_level` + `attributed` compile-time gate outcome proves the
      // entry point's own gate is a row filter and nothing else is reachable
      // beneath it, so database serving grafts exactly what a live query
      // would — see `assertDatabasePersistNotAuthorizeGated`'s doc.
      it("admits a database authorize-gated source whose compile-time outcome is row_level and attributed", () => {
         const compiled = compiledWith(
            { s1: authorizeGatedDatabase },
            [["s1"]],
            new Map(),
            { s1: { classification: "row_level", attributed: true } },
         );
         expect(() => derive(compiled)).not.toThrow();
      });

      it("still refuses when the compile-time outcome is row_level but NOT attributed (a join-only gate outside identity reach)", () => {
         const compiled = compiledWith(
            { s1: authorizeGatedDatabase },
            [["s1"]],
            new Map(),
            { s1: { classification: "row_level", attributed: false } },
         );
         expect(() => derive(compiled)).toThrow(
            MaterializationEligibilityError,
         );
      });

      it("still refuses when the compile-time outcome is rejected", () => {
         const compiled = compiledWith(
            { s1: authorizeGatedDatabase },
            [["s1"]],
            new Map(),
            { s1: { classification: "rejected", attributed: true } },
         );
         expect(() => derive(compiled)).toThrow(
            MaterializationEligibilityError,
         );
      });

      it("leaves an ungated database source unaffected", () => {
         const compiled = compiledWith(
            {
               s1: fakeSource({
                  name: "s1",
                  sourceEntityId: "d1d1d1d1d1d1d1d1",
               }),
            },
            [["s1"]],
         );
         expect(() => derive(compiled)).not.toThrow();
      });

      it("still refuses a storage= authorize-gated source via the existing path, unchanged", () => {
         restoreSettings = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         }).restore;
         const storageGated = fakeSource({
            name: "s1",
            sourceEntityId: "f1f1f1f1f1f1f1f1",
            annotationFields: { storage: "lake" },
            sourceDef: { blockNotes: ["#(authorize) true"] },
         });
         const compiled = compiledWith({ s1: storageGated }, [["s1"]]);
         // Same message as assertMaterializationEligible's storage-destination
         // refusal (no double-refusal, no changed message from the database path).
         expect(() => derive(compiled)).toThrow(
            /cannot be materialized into a storage destination/,
         );
      });
   });

   describe("a refused source beside admitted ones", () => {
      type Derived = {
         instructions: BuildInstruction[];
         carried: Record<string, unknown>;
         refused: Record<
            string,
            { name: string; tier: string; reason: string; message: string }
         >;
      };
      function deriveWith(
         compiled: unknown,
         sourceNames?: string[],
         priorEntries: Record<string, unknown> = {},
      ): Derived {
         return (
            ctx.service.collaborators as unknown as {
               deriveSelfInstructions: (
                  c: unknown,
                  n: string[] | undefined,
                  p: unknown,
               ) => Derived;
            }
         ).deriveSelfInstructions(compiled, sourceNames, priorEntries);
      }
      const ok = fakeSource({ name: "ok", sourceEntityId: "a0a0a0a0a0a0a0a0" });
      const gated = (name: string) =>
         fakeSource({
            name,
            sourceEntityId: `${name}-c1c1c1c1c1c1`,
            sourceDef: { blockNotes: ["#(authorize) true"] },
         });

      it("instructs the admitted source and records the refused one instead of throwing", () => {
         const compiled = compiledWith({ ok, refused: gated("refused") }, [
            ["refused", "ok"],
         ]);

         const { instructions, refused } = deriveWith(compiled);

         expect(instructions.map((i) => i.sourceEntityId)).toEqual([
            "a0a0a0a0a0a0a0a0",
         ]);
         expect(Object.keys(refused)).toEqual(["refused"]);
         expect(refused.refused).toMatchObject({
            name: "refused",
            tier: "database",
         });
         expect(refused.refused.message).toMatch(/authorize/i);
      });

      it("records a refused storage= source under the storage tier", () => {
         const { restore } = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         });
         try {
            const compiled = compiledWith(
               {
                  ok,
                  lake: fakeSource({
                     name: "lake",
                     sourceEntityId: "f1f1f1f1f1f1f1f1",
                     annotationFields: { storage: "lake" },
                     sourceDef: { blockNotes: ["#(authorize) true"] },
                  }),
               },
               [["lake", "ok"]],
            );

            const { instructions, refused } = deriveWith(compiled);

            expect(instructions).toHaveLength(1);
            expect(refused.lake.tier).toBe("storage");
            expect(refused.lake.message).toMatch(
               /cannot be materialized into a storage destination/,
            );
         } finally {
            restore();
         }
      });

      it("does not throw when every admitted source is reused rather than rebuilt", () => {
         // Nothing is instructed, but the run still has a table to serve for
         // `ok`, so the refusal must not turn an all-reused run into a failure.
         const compiled = compiledWith({ ok, refused: gated("refused") }, [
            ["refused", "ok"],
         ]);

         const { instructions, carried, refused } = deriveWith(
            compiled,
            undefined,
            {
               a0a0a0a0a0a0a0a0: {
                  sourceEntityId: "a0a0a0a0a0a0a0a0",
                  physicalTableName: "ok",
                  contract: contractFor(),
                  connectionName: "duckdb",
               },
            },
         );

         expect(instructions).toHaveLength(0);
         expect(Object.keys(carried)).toEqual(["a0a0a0a0a0a0a0a0"]);
         expect(Object.keys(refused)).toEqual(["refused"]);
      });

      it("records a refused rollup without failing a run that has nothing else to build", () => {
         // A rollup asked for nothing an author can see: a package whose only
         // refused work is one must not fail every run over it.
         const rollup = gated("rollup");
         const compiled = compiledWith({ rollup }, [["rollup"]]);
         compiled.tables.rollup = {
            kind: "rollup",
            source: compiled.tables.rollup.source,
            rollup: {
               baseSourceName: "orders",
               grainDimensions: ["category"],
               measures: [],
            } as never,
         };

         const { instructions, refused } = deriveWith(compiled);

         expect(instructions).toHaveLength(0);
         expect(refused.rollup).toMatchObject({
            origin: "rollup",
            tier: "database",
         });
      });

      it("still throws when sourceNames names the refused source", () => {
         const compiled = compiledWith({ ok, refused: gated("refused") }, [
            ["refused", "ok"],
         ]);

         expect(() => deriveWith(compiled, ["refused", "ok"])).toThrow(
            MaterializationEligibilityError,
         );
      });

      it("throws naming every refused source when all of them are refused", () => {
         const compiled = compiledWith(
            { first: gated("first"), second: gated("second") },
            [["first", "second"]],
         );

         let thrown: unknown;
         try {
            deriveWith(compiled);
         } catch (err) {
            thrown = err;
         }
         expect(thrown).toBeInstanceOf(MaterializationEligibilityError);
         expect(String((thrown as Error).message)).toContain("'first'");
         expect(String((thrown as Error).message)).toContain("'second'");
      });
   });

   // An incremental source is the one case where an unchanged content address
   // does NOT mean there is nothing to do: its data moves while its SQL stays
   // put. Carrying it forward here would strand the delta path behind a check it
   // can never pass, so it is exempt — but only when the delta path could
   // actually act on the declaration.
   const INCREMENTAL_DECLARATION = {
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

   /** deriveSelfInstructions with an incremental context for source `s1`. */
   function selfInstructions(
      compiled: unknown,
      priorEntries: unknown,
   ): { instructions: BuildInstruction[]; carried: Record<string, unknown> } {
      return (
         ctx.service.collaborators as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
               i: unknown,
            ) => {
               instructions: BuildInstruction[];
               carried: Record<string, unknown>;
            };
         }
      ).deriveSelfInstructions(compiled, undefined, priorEntries, {
         environmentId: "env-1",
         packageName: "pkg",
         materializationId: "run-1",
         forceRefresh: false,
         now: new Date("2024-07-01T00:00:00Z"),
         declarations: { s1: INCREMENTAL_DECLARATION },
         materializers: {},
         ledger: {},
      });
   }

   it("does NOT carry an incremental source forward, even at an unchanged address", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({
               name: "s1",
               sourceEntityId: "b1aaaaaaaaaaaaaa",
               connectionName: "wh",
               dialectName: "postgres",
               annotationFields: {
                  refresh: "incremental",
                  watermark: "order_date",
               },
            }),
         },
         [["s1"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            physicalTableName: "s1",
            contract: contractFor(),
            connectionName: "wh",
         },
      };

      const { instructions, carried } = selfInstructions(
         compiled,
         priorEntries,
      );

      // Instructed, so the build reaches the dispatch and the ledger decides.
      expect(instructions).toHaveLength(1);
      expect(instructions[0].sourceEntityId).toBe("b1aaaaaaaaaaaaaa");
      expect(Object.keys(carried)).toHaveLength(0);
   });

   it("still carries forward an incremental source the delta path cannot act on", () => {
      // Same declaration on DuckDB, which is off the incremental dialect
      // allowlist. The delta will not run, so exempting it would rebuild the
      // table in full on every run in the name of an incremental refresh that
      // never happens.
      const compiled = compiledWith(
         {
            s1: fakeSource({
               name: "s1",
               sourceEntityId: "b1aaaaaaaaaaaaaa",
               annotationFields: {
                  refresh: "incremental",
                  watermark: "order_date",
               },
            }),
         },
         [["s1"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            physicalTableName: "s1",
            contract: contractFor(),
            connectionName: "duckdb",
         },
      };

      const { instructions, carried } = selfInstructions(
         compiled,
         priorEntries,
      );

      expect(instructions).toHaveLength(0);
      expect(carried["b1aaaaaaaaaaaaaa"]).toBeDefined();
   });

   it("honors the sourceNames filter (excluded sources are neither built nor carried)", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"]],
      );
      const { instructions, carried } = (
         ctx.service.collaborators as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, ["s2"], {});

      expect(instructions).toHaveLength(1);
      expect(instructions[0].sourceEntityId).toBe("b2bbbbbbbbbbbbbb");
      expect(Object.keys(carried as Record<string, unknown>)).toHaveLength(0);
   });
});

describe("a prior table its bind cannot serve is rebuilt in full", () => {
   const compiled = () =>
      compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
         },
         [["s1"]],
      );
   const prior = (contract?: unknown) => ({
      b1aaaaaaaaaaaaaa: {
         sourceEntityId: "b1aaaaaaaaaaaaaa",
         physicalTableName: "s1",
         connectionName: "duckdb",
         ...(contract !== undefined ? { contract } : {}),
      },
   });

   it("never carries a table whose entry has no contract (built before contracts were recorded)", () => {
      const { instructions, carried } = deriveSelfInstructions(
         compiled(),
         undefined,
         prior() as never,
      );
      expect(instructions.map((i) => i.sourceEntityId)).toEqual([
         "b1aaaaaaaaaaaaaa",
      ]);
      expect(carried).toEqual({});
   });

   it("never carries a table a bind found incompatible with its contract", () => {
      const { instructions, carried } = deriveSelfInstructions(
         compiled(),
         undefined,
         prior(contractFor()) as never,
         undefined,
         undefined,
         { incompatible: new Set(["b1aaaaaaaaaaaaaa"]) },
      );
      expect(instructions.map((i) => i.sourceEntityId)).toEqual([
         "b1aaaaaaaaaaaaaa",
      ]);
      expect(carried).toEqual({});
   });

   it("still carries a table whose contract it holds", () => {
      const { instructions, carried } = deriveSelfInstructions(
         compiled(),
         undefined,
         prior(contractFor()) as never,
      );
      expect(instructions).toEqual([]);
      expect(Object.keys(carried)).toEqual(["b1aaaaaaaaaaaaaa"]);
   });

   describe("a pair table", () => {
      // `s1` stores the inlined join_many `items`, whose key the model
      // declares now as `childKey`.
      const pairCompiled = (childKey: string) => {
         const c = compiled();
         for (const table of Object.values(c.tables)) {
            (table as { materialize?: unknown }).materialize = {
               pair: {
                  join: "items",
                  parentKey: "id",
                  childKey,
                  childSourceID: "items@m.malloy",
                  parentRowColumn: "__parent_row",
               },
            };
         }
         return c;
      };
      const checked = (key: string) => ({
         ...contractFor(),
         keys: { [key]: { unique: true, level: "leaf" } },
      });

      it("is carried while its contract records a check of the leaf key the model declares", () => {
         const { instructions, carried } = deriveSelfInstructions(
            pairCompiled("item_id"),
            undefined,
            prior(checked("leaf:items.item_id")) as never,
         );
         expect(instructions).toEqual([]);
         expect(Object.keys(carried)).toEqual(["b1aaaaaaaaaaaaaa"]);
      });

      it("is rebuilt once a reload moves the leaf key to one its contract records no check of", () => {
         const { instructions, carried } = deriveSelfInstructions(
            pairCompiled("sku"),
            undefined,
            prior(checked("leaf:items.item_id")) as never,
         );
         expect(instructions.map((i) => i.sourceEntityId)).toEqual([
            "b1aaaaaaaaaaaaaa",
         ]);
         expect(carried).toEqual({});
      });

      it("is carried when its check of the declared key could not run, recorded unverified, so a check that keeps failing to run does not rebuild it every run", () => {
         const { instructions, carried } = deriveSelfInstructions(
            pairCompiled("item_id"),
            undefined,
            prior({
               ...contractFor(),
               keys: {
                  "leaf:items.item_id": {
                     unique: false,
                     level: "leaf",
                     unverified: true,
                  },
               },
            }) as never,
         );
         expect(instructions).toEqual([]);
         expect(Object.keys(carried)).toEqual(["b1aaaaaaaaaaaaaa"]);
      });

      it("is carried when its check of the declared key recorded a failure, which a rebuild would not change", () => {
         const { carried } = deriveSelfInstructions(
            pairCompiled("item_id"),
            undefined,
            prior({
               ...contractFor(),
               keys: {
                  "leaf:items.item_id": { unique: false, level: "leaf" },
               },
            }) as never,
         );
         expect(Object.keys(carried)).toEqual(["b1aaaaaaaaaaaaaa"]);
      });
   });
});

describe("a run of named sources pulls in their upstreams", () => {
   // `s2` reads `s1`'s table (the plan's edge).
   const compiled = () => {
      const c = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1"], ["s2"]],
      );
      c.builds[0].dependsOn = { s1: [], s2: ["s1"] };
      return c;
   };
   const upstream = {
      b1aaaaaaaaaaaaaa: {
         sourceEntityId: "b1aaaaaaaaaaaaaa",
         physicalTableName: "s1",
         connectionName: "duckdb",
         contract: contractFor(),
      },
   };

   it("builds an upstream with no table beside the named source", () => {
      const { instructions } = deriveSelfInstructions(compiled(), ["s2"], {});
      expect(instructions.map((i) => i.sourceEntityId).sort()).toEqual([
         "b1aaaaaaaaaaaaaa",
         "b2bbbbbbbbbbbbbb",
      ]);
   });

   it("carries an upstream whose table it can serve, even on a forced run", () => {
      for (const forceRefresh of [false, true]) {
         const { instructions, carried } = deriveSelfInstructions(
            compiled(),
            ["s2"],
            upstream as never,
            undefined,
            undefined,
            { forceRefresh },
         );
         expect(instructions.map((i) => i.sourceEntityId)).toEqual([
            "b2bbbbbbbbbbbbbb",
         ]);
         expect(Object.keys(carried)).toEqual(["b1aaaaaaaaaaaaaa"]);
      }
   });

   it("rebuilds an upstream a bind found incompatible", () => {
      const { instructions, carried } = deriveSelfInstructions(
         compiled(),
         ["s2"],
         upstream as never,
         undefined,
         undefined,
         { incompatible: new Set(["b1aaaaaaaaaaaaaa"]) },
      );
      expect(instructions.map((i) => i.sourceEntityId).sort()).toEqual([
         "b1aaaaaaaaaaaaaa",
         "b2bbbbbbbbbbbbbb",
      ]);
      expect(carried).toEqual({});
   });
});

describe("a carried reader of a table not the one it was built from is rebuilt", () => {
   // `s2` reads `s1`'s table (the plan's edge), and was built from it.
   const compiled = () => {
      const c = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "c1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "c2bbbbbbbbbbbbbb" }),
         },
         [["s1"], ["s2"]],
      );
      c.builds[0].dependsOn = { s1: [], s2: ["s1"] };
      return c;
   };
   const prior = (s1BuiltAt: string, readBuiltAt: string) => ({
      c1aaaaaaaaaaaaaa: {
         sourceEntityId: "c1aaaaaaaaaaaaaa",
         physicalTableName: "s1",
         connectionName: "duckdb",
         builtAt: s1BuiltAt,
         contract: contractFor(),
      },
      c2bbbbbbbbbbbbbb: {
         sourceEntityId: "c2bbbbbbbbbbbbbb",
         physicalTableName: "s2",
         connectionName: "duckdb",
         builtAt: "2026-11-02T00:00:00.000Z",
         contract: contractFor(),
         builtFrom: [
            {
               sourceEntityId: "c1aaaaaaaaaaaaaa",
               physicalTableName: "s1",
               builtAt: readBuiltAt,
            },
         ],
      },
   });
   const T1 = "2026-11-01T00:00:00.000Z";
   const T2 = "2026-11-03T00:00:00.000Z";

   it("carries a reader built from the upstream table it would read now", () => {
      const { instructions, carried } = deriveSelfInstructions(
         compiled(),
         undefined,
         prior(T1, T1) as never,
      );
      expect(instructions).toEqual([]);
      expect(Object.keys(carried).sort()).toEqual([
         "c1aaaaaaaaaaaaaa",
         "c2bbbbbbbbbbbbbb",
      ]);
   });

   it("rebuilds a reader whose upstream this run rebuilds", () => {
      const { instructions, carried } = deriveSelfInstructions(
         compiled(),
         undefined,
         prior(T1, T1) as never,
         undefined,
         undefined,
         { incompatible: new Set(["c1aaaaaaaaaaaaaa"]) },
      );
      expect(instructions.map((i) => i.sourceEntityId).sort()).toEqual([
         "c1aaaaaaaaaaaaaa",
         "c2bbbbbbbbbbbbbb",
      ]);
      expect(carried).toEqual({});
   });

   it("rebuilds a reader of an older build of a carried upstream", () => {
      const { instructions, carried } = deriveSelfInstructions(
         compiled(),
         undefined,
         prior(T2, T1) as never,
      );
      expect(instructions.map((i) => i.sourceEntityId)).toEqual([
         "c2bbbbbbbbbbbbbb",
      ]);
      expect(Object.keys(carried)).toEqual(["c1aaaaaaaaaaaaaa"]);
   });
});
