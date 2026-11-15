// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import {
   packageTableSuffix,
   versionTableSuffix,
} from "../../versions/materialization_scope";
import type { CompiledBuildPlan } from "../plan/build_plan";
import type { ManifestEntry } from "../domain/types";
import { deriveSelfInstructions } from "./run_instructions";

/**
 * Auto-run's table names, which must never let two packages, or two
 * environments, on one schema build one table: every name the author did not
 * choose takes the package's suffix (a `scope: version` version's takes the
 * version's), an explicit `name=` keeps its name and is checked for ownership
 * at build (table_ownership.spec.ts), and a table a run before package-owned
 * names built under the bare name is rebuilt, never carried.
 */

const PKG = packageTableSuffix({ environmentName: "e1", packageName: "pa" });
const OTHER = packageTableSuffix({ environmentName: "e2", packageName: "pa" });

const compiled = (): CompiledBuildPlan =>
   compiledWith(
      {
         summary: fakeSource({ name: "summary", sourceEntityId: "a1" }),
         named: fakeSource({
            name: "named",
            sourceEntityId: "a2",
            annotationFields: { name: "named_t" },
         }),
         qualified: fakeSource({
            name: "qualified",
            sourceEntityId: "a3",
            annotationFields: { name: "analytics.q_t" },
         }),
         // Keyed by its sourceID, as a plan keys every table.
         rollup__g1a2b3c4: fakeSource({
            name: "rollup__g1a2b3c4",
            sourceEntityId: "a4",
            annotationFields: { name: "ns.rollup__g1a2b3c4" },
         }),
      },
      [["summary", "named", "qualified", "rollup__g1a2b3c4"]],
   );

const withRollup = (c: CompiledBuildPlan): CompiledBuildPlan => ({
   ...c,
   tables: {
      ...c.tables,
      rollup__g1a2b3c4: {
         ...c.tables.rollup__g1a2b3c4,
         kind: "rollup",
         rollup: {} as never,
         materialize: undefined,
      },
   },
});

const names = (r: ReturnType<typeof deriveSelfInstructions>) =>
   Object.fromEntries(
      r.instructions.map((i) => [i.sourceEntityId, i.physicalTableName]),
   );

describe("auto-run's table names", () => {
   it("suffix every name the author did not choose with the package's", () => {
      const r = deriveSelfInstructions(
         withRollup(compiled()),
         undefined,
         {},
         undefined,
         { packageSuffix: PKG },
      );
      expect(names(r)).toEqual({
         a1: `summary${PKG}`,
         a2: "named_t",
         a3: "analytics.q_t",
         // A rollup's namespace is a container; its table name is Publisher's.
         a4: `ns.rollup__g1a2b3c4${PKG}`,
      });
      // Another environment's package of the same name builds other tables.
      const other = names(
         deriveSelfInstructions(
            withRollup(compiled()),
            undefined,
            {},
            undefined,
            { packageSuffix: OTHER },
         ),
      );
      expect(other.a1).not.toBe(`summary${PKG}`);
      expect(other.a2).toBe("named_t");
   });

   it("keep a scope: version version's suffix on every name, name= included", () => {
      const suffix = versionTableSuffix({
         environmentName: "e1",
         packageName: "pa",
         version: "1.2.0",
      });
      const r = deriveSelfInstructions(compiled(), undefined, {}, undefined, {
         versionSuffix: suffix,
      });
      expect(names(r).a1).toBe(`summary${suffix}`);
      expect(names(r).a2).toBe(`named_t${suffix}`);
   });

   it("cut a long name to fit, so a dialect that truncates identifiers never folds two", () => {
      const long = "x".repeat(70);
      const c = compiledWith(
         { s: fakeSource({ name: long, sourceEntityId: "a1" }) },
         [["s"]],
      );
      const [instruction] = deriveSelfInstructions(
         c,
         undefined,
         {},
         undefined,
         {
            packageSuffix: PKG,
         },
      ).instructions;
      expect(instruction.physicalTableName.length).toBeLessThanOrEqual(50);
      expect(instruction.physicalTableName.endsWith(PKG)).toBe(true);
   });

   it("rebuild, never carry, a table built under the bare name before", () => {
      const prior = (table: string, sourceEntityId: string): ManifestEntry =>
         ({
            sourceEntityId,
            physicalTableName: table,
            connectionName: "duckdb",
         }) as ManifestEntry;
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            a1: prior("summary", "a1"),
            a2: prior("named_t", "a2"),
            a3: prior("analytics.q_t", "a3"),
         },
         undefined,
         { packageSuffix: PKG },
      );
      // The bare `summary` another package may have written since: rebuilt.
      expect(names(r).a1).toBe(`summary${PKG}`);
      expect(r.carried.a1).toBeUndefined();
      // An author's name= is carried, but only while its mark says it is
      // this package's (the service asks, uncarryUnownedTables).
      expect(Object.keys(r.carried).sort()).toEqual(["a2", "a3"]);
      expect(Object.keys(r.recheck).sort()).toEqual(["a2", "a3"]);
      expect(r.recheck.a2.physicalTableName).toBe("named_t");
      // Its own suffixed table is carried as before, with nothing to recheck.
      const again = deriveSelfInstructions(
         compiled(),
         undefined,
         { a1: prior(`summary${PKG}`, "a1") },
         undefined,
         { packageSuffix: PKG },
      );
      expect(again.carried.a1).toBeDefined();
      expect(again.recheck.a1).toBeUndefined();
   });

   it("leave names as written without a suffix (a test seam)", () => {
      const r = deriveSelfInstructions(compiled(), undefined, {});
      expect(names(r).a1).toBe("summary");
      expect(r.recheck).toEqual({});
   });
});

describe("a #(materialize) table's name", () => {
   // The generator writes the declared `name=` on the build source's persist
   // line, or the source's own name when there is none (`nameDefaulted`).
   const withMaterialize = (
      c: CompiledBuildPlan,
      defaulted: boolean,
   ): CompiledBuildPlan => ({
      ...c,
      tables: {
         ...c.tables,
         named: {
            ...c.tables.named,
            kind: "materialize",
            materialize: {
               persist: { name: "named_t" },
               ...(defaulted ? { nameDefaulted: true } : {}),
            } as never,
            rollup: undefined,
         },
      },
   });

   it("keeps a declared name= as written, and suffixes a defaulted one", () => {
      const named = deriveSelfInstructions(
         withMaterialize(compiled(), false),
         undefined,
         {},
         undefined,
         { packageSuffix: PKG },
      );
      expect(names(named).a2).toBe("named_t");
      const defaulted = deriveSelfInstructions(
         withMaterialize(compiled(), true),
         undefined,
         {},
         undefined,
         { packageSuffix: PKG },
      );
      expect(names(defaulted).a2).toBe(`named_t${PKG}`);
      // A scope: version run keeps name='s container and suffixes its leaf,
      // as for a v1 name=.
      const suffix = versionTableSuffix({
         environmentName: "e1",
         packageName: "pa",
         version: "1.2.0",
      });
      const versioned = deriveSelfInstructions(
         withMaterialize(compiled(), false),
         undefined,
         {},
         undefined,
         { versionSuffix: suffix },
      );
      expect(names(versioned).a2).toBe(`named_t${suffix}`);
      expect(names(versioned).a3).toBe(`analytics.q_t${suffix}`);
   });
});

describe("a table whose name changed", () => {
   const prior = (table: string, sourceEntityId: string): ManifestEntry =>
      ({
         sourceEntityId,
         physicalTableName: table,
         connectionName: "duckdb",
      }) as ManifestEntry;

   it("is rebuilt under its name now, never carried at the old one", () => {
      // The content address does not encode the name: a name= added,
      // removed or changed leaves the address as it was.
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            // name= removed: the table was at the author's name.
            a1: prior("summary_old", "a1"),
            // name= changed.
            a2: prior("named_old", "a2"),
            // name= added: the table was the package's default.
            a3: prior(`qualified${PKG}`, "a3"),
         },
         undefined,
         { packageSuffix: PKG },
      );
      expect(r.carried).toEqual({});
      expect(names(r)).toMatchObject({
         a1: `summary${PKG}`,
         a2: "named_t",
         a3: "analytics.q_t",
      });
      // The same name, spelled in another case on a case-folding schema, is
      // the same table, and carried.
      const again = deriveSelfInstructions(
         compiled(),
         undefined,
         { a2: prior("NAMED_T", "a2") },
         undefined,
         { packageSuffix: PKG },
      );
      expect(again.carried.a2).toBeDefined();
      // On a warehouse that keeps case (Postgres), a case-only rename is
      // another table: rebuilt, and checked again under its new name.
      const kept = deriveSelfInstructions(
         compiledWith(
            {
               named: fakeSource({
                  name: "named",
                  sourceEntityId: "a2",
                  annotationFields: { name: "named_t" },
                  dialectName: "postgres",
               }),
            },
            [["named"]],
         ),
         undefined,
         { a2: prior("NAMED_T", "a2") },
         undefined,
         { packageSuffix: PKG },
      );
      expect(kept.carried).toEqual({});
      expect(names(kept).a2).toBe("named_t");
      // A name= moved to another schema is another table.
      const moved = deriveSelfInstructions(
         compiled(),
         undefined,
         { a3: prior("staging.q_t", "a3") },
         undefined,
         { packageSuffix: PKG },
      );
      expect(moved.carried).toEqual({});
      expect(names(moved).a3).toBe("analytics.q_t");
   });
});
