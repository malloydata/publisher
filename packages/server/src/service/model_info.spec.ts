// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { describe, expect, it } from "bun:test";
import { modelInfoOf } from "./model_info";

const SOURCE = `source: s is duckdb.sql("select 'a' v, 1 n, true b, date '2020-01-01' d, timestamp '2020-01-01 00:00:00' ts, timestamptz '2020-01-01 00:00:00+00' tstz")`;

async function compile(text: string): Promise<ModelDef> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([["file:///r/m.malloy", text]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const model = await runtime
      .loadModel(new URL("file:///r/m.malloy"))
      .getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   return (model as any)._modelDef as ModelDef;
}

function model(givenType: string, def: string | undefined, query: string) {
   return (
      `##! experimental.givens\n` +
      `given: G :: ${givenType}${def === undefined ? "" : ` is ${def}`}\n` +
      `${SOURCE}\n${query}\n`
   );
}

/** Output field names and types of the model's queries, minus the random reference_ids. */
function shape(info: Malloy.ModelInfo): string[] {
   const queries = [
      ...info.entries.filter((e) => e.name === "q"),
      ...info.anonymous_queries,
   ];
   return queries.map((e) =>
      e.schema.fields
         .map((f) =>
            f.kind === "dimension" ? `${f.name}:${f.type.kind}` : f.name,
         )
         .join(","),
   );
}

const SCALARS: [string, string | undefined][] = [
   ["string", "'a'"],
   ["number", "1"],
   ["boolean", "true"],
   ["date", "@2020-01-01"],
   ["timestamp", "@2020-01-01 00:00:00"],
   ["timestamptz", undefined],
];

const FILTERS: [string, string][] = [
   ["filter<string>", "v ~ $G"],
   ["filter<number>", "n ~ $G"],
   ["filter<date>", "d ~ $G"],
   ["filter<timestamp>", "ts ~ $G"],
   ["filter<boolean>", "b ~ $G"],
];

describe("modelInfoOf", () => {
   for (const [type, def] of SCALARS) {
      it(`reads a query that bakes an unbound ${type} given`, async () => {
         const query = "query: q is s -> { select: v, g is $G }";
         const info = modelInfoOf(await compile(model(type, undefined, query)));
         expect(shape(info)).toEqual([`v:string_type,g:${type}_type`]);
         if (def !== undefined) {
            const bound = await compile(model(type, def, query));
            expect(shape(info)).toEqual(shape(modelInfoOf(bound)));
         }
      });
   }

   for (const [type, where] of FILTERS) {
      it(`reads a query that filters on an unbound ${type} given`, async () => {
         const query = `query: q is s -> { where: ${where}; select: v }`;
         const info = modelInfoOf(await compile(model(type, undefined, query)));
         expect(shape(info)).toEqual(["v:string_type"]);
      });
   }

   it("reads an anonymous run: that bakes an unbound given", async () => {
      const info = modelInfoOf(
         await compile(
            model("string", undefined, "run: s -> { select: v, g is $G }"),
         ),
      );
      expect(info.anonymous_queries).toHaveLength(1);
   });

   it("never mutates the model it is given", async () => {
      const modelDef = await compile(
         model("string", undefined, "query: q is s -> { select: g is $G }"),
      );
      const before = structuredClone(modelDef);
      modelInfoOf(modelDef);
      expect(modelDef).toEqual(before);
      expect(Object.values(modelDef.givens ?? {})[0]?.default).toBeUndefined();
   });

   it("compiles a model with no unbound given exactly once", () => {
      let reads = 0;
      const modelDef = {
         contents: {},
         exports: [],
         givens: {},
         get queryList() {
            reads++;
            return [];
         },
      } as unknown as ModelDef;
      modelInfoOf(modelDef);
      expect(reads).toBe(1);
   });

   it("rethrows an error that is not an unbound given, unchanged", () => {
      const boom = Object.assign(new Error("boom"), { code: "other" });
      const bad = {
         get contents(): never {
            throw boom;
         },
      } as unknown as ModelDef;
      expect(() => modelInfoOf(bad)).toThrow(boom);
   });

   // Malloy compiles a source view's later nest stage without a given scope, so
   // even a declared default does not help; the original error must surface.
   it("still throws for a multi-stage nest that reads a given", async () => {
      const nest = `source: s2 is duckdb.sql("select 'a' v") extend {
   view: vw is { group_by: v; nest: nn is { group_by: v } -> { where: v = $G; select: v } }
}`;
      for (const def of [undefined, "'a'"]) {
         const modelDef = await compile(
            `##! experimental.givens\ngiven: G :: string${def ? ` is ${def}` : ""}\n${nest}\n`,
         );
         expect(() => modelInfoOf(modelDef)).toThrow(/Given 'G' has no value/);
      }
   });
});
