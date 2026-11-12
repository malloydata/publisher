// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { BadRequestError } from "../../errors";
import {
   diffPlans,
   MAX_WHAT_IF_QUERIES,
   parseWhatIfQueries,
   storedSchemaOf,
} from "./what_if";

type Plan = Parameters<typeof diffPlans>[1];

function plan(
   sources: Record<string, { entity: string; storage?: string }>,
): Plan {
   return {
      graphs: [],
      sources: Object.fromEntries(
         Object.entries(sources).map(([id, s]) => [
            id,
            {
               name: id.split("@")[0],
               sourceID: id,
               connectionName: "duckdb",
               dialect: "duckdb",
               origin: "persist",
               sourceEntityId: s.entity,
               sql: "SELECT 1",
               columns: [],
               annotationFields: s.storage ? { storage: s.storage } : {},
            },
         ]),
      ),
   } as unknown as Plan;
}

describe("parseWhatIfQueries", () => {
   it("keeps ad-hoc and named queries", () => {
      expect(
         parseWhatIfQueries([
            {
               modelPath: "m.malloy",
               query: "run: a -> { aggregate: c is count() }",
            },
            { modelPath: "m.malloy", sourceName: "a", queryName: "v" },
         ]),
      ).toEqual([
         {
            modelPath: "m.malloy",
            query: "run: a -> { aggregate: c is count() }",
         },
         { modelPath: "m.malloy", sourceName: "a", queryName: "v" },
      ]);
   });

   it("refuses a missing, empty or oversized list, and a malformed entry", () => {
      for (const bad of [
         undefined,
         [],
         "q",
         [{ query: "x" }],
         [{ modelPath: "m.malloy", query: 3 }],
         [null],
         Array.from({ length: MAX_WHAT_IF_QUERIES + 1 }, () => ({
            modelPath: "m.malloy",
            query: "x",
         })),
      ]) {
         expect(() => parseWhatIfQueries(bad)).toThrow(BadRequestError);
      }
   });
});

describe("storedSchemaOf", () => {
   it("declares each Malloy type as the DuckDB type a build stores, and leaves the rest off", () => {
      expect(
         storedSchemaOf([
            { name: "s", type: "string" },
            { name: "n", type: "number" },
            { name: "b", type: "boolean" },
            { name: "d", type: "date" },
            { name: "t", type: "timestamp" },
            { name: "tz", type: "timestamptz" },
            { name: "j", type: "json" },
            { name: "x", type: "sql native" },
            { type: "string" },
         ]),
      ).toEqual([
         { name: "s", type: "VARCHAR" },
         { name: "n", type: "DOUBLE" },
         { name: "b", type: "BOOLEAN" },
         { name: "d", type: "DATE" },
         { name: "t", type: "TIMESTAMP" },
         { name: "tz", type: "TIMESTAMP WITH TIME ZONE" },
      ]);
   });
});

describe("diffPlans", () => {
   it("sorts the proposal's sources into added, removed, changed and unchanged", () => {
      const current = plan({
         "kept@m": { entity: "e1" },
         "moved@m": { entity: "e2" },
         "restored@m": { entity: "e3" },
         "dropped@m": { entity: "e4" },
      });
      const proposed = plan({
         "kept@m": { entity: "e1" },
         "moved@m": { entity: "e2x" },
         "restored@m": { entity: "e3", storage: "lake" },
         "new@m": { entity: "e5" },
      });
      const diff = diffPlans(current, proposed);
      expect(diff.added.map((s) => s.name)).toEqual(["new"]);
      expect(diff.removed.map((s) => s.name)).toEqual(["dropped"]);
      expect(diff.changed.map((s) => s.name)).toEqual(["moved", "restored"]);
      expect(diff.changed[1]).toMatchObject({ storage: "lake" });
      expect(diff.unchanged.map((s) => s.name)).toEqual(["kept"]);
   });

   it("reads every proposed source as added when nothing is planned now", () => {
      const diff = diffPlans(null, plan({ "a@m": { entity: "e" } }));
      expect(diff.added.map((s) => s.name)).toEqual(["a"]);
      expect(diff.removed).toEqual([]);
   });
});
