// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { FieldClassification } from "../../classification";
import type { Binding } from "../domain/binding";
import {
   materializedReadPlan,
   staleOnChain,
   storedColumnNotes,
} from "./served_copy_serve";

const classified = (c: Partial<FieldClassification>): FieldClassification =>
   ({ declared: [], classifications: [], ...c }) as FieldClassification;
const field = (...notes: string[]) => ({
   annotations: { notes: notes.map((text) => ({ text })) },
});

describe("storedColumnNotes", () => {
   it("keeps a field's own notes, a declared class among them", () => {
      expect(
         storedColumnNotes(
            field('#(classification) "pii"', "# currency"),
            classified({ declared: ["pii"], classifications: ["pii"] }),
         ),
      ).toEqual(['#(classification) "pii"', "# currency"]);
   });

   it("declares an inferred class, since the column has no expression to infer it from", () => {
      expect(
         storedColumnNotes(
            field('#(doc) "shouted"'),
            classified({ classifications: ["pii", 'a"b'] }),
         ),
      ).toEqual(['#(doc) "shouted"', '#(classification) ["pii", "a\\"b"]']);
   });

   it("adds nothing for an unclassified or malformed field", () => {
      expect(storedColumnNotes(field(), classified({}))).toEqual([]);
      expect(storedColumnNotes(undefined, undefined)).toEqual([]);
      expect(
         storedColumnNotes(
            field("#(classification) bad"),
            classified({ malformed: "is malformed", classifications: ["x"] }),
         ),
      ).toEqual(["#(classification) bad"]);
   });
});

describe("staleOnChain", () => {
   const stale = (sourceIDs: string[]) => ({ sourceIDs }) as unknown as Binding;
   it("is a stale table storing a source on the queried source's chain", () => {
      expect(staleOnChain([stale(["base"])], ["daily", "base"])).toBe(true);
   });
   it("is not a stale table off the chain, or none at all", () => {
      expect(staleOnChain([stale(["other"])], ["daily", "base"])).toBe(false);
      expect(staleOnChain([], ["daily"])).toBe(false);
   });
});

describe("materializedReadPlan", () => {
   const def = {
      queryTimezone: undefined,
      fields: [
         { name: "id", type: "number" },
         { name: "amt", type: "number" },
         { name: "flag", type: "boolean" },
         { name: "raw_d", as: "d", type: "date" },
         { name: "ts", type: "timestamp" },
         { name: "total", type: "number", expressionType: "aggregate" },
         { name: "c", type: "record", join: "one" },
      ],
   };

   it("reads each drifted column as live types its field: a faithful cast, else left out", () => {
      const plan = materializedReadPlan(
         [
            { name: "id", type: "BIGINT" },
            { name: "amt", type: "VARCHAR" },
            { name: "flag", type: "INTEGER" },
            { name: "d", type: "TIMESTAMP" },
            { name: "ts", type: "TIMESTAMP WITH TIME ZONE" },
         ],
         def,
      );
      expect(plan.casts).toEqual([
         { column: "flag", castTo: "BOOLEAN" },
         { column: "d", castTo: "DATE" },
      ]);
      expect(plan.leftOut.map((m) => [m.column, m.storedType])).toEqual([
         ["amt", "VARCHAR"],
      ]);
   });

   it("checks no column that names a measure or a join, or no field at all", () => {
      const plan = materializedReadPlan(
         [
            { name: "total", type: "VARCHAR" },
            { name: "c", type: "VARCHAR" },
            { name: "c__fee", type: "VARCHAR" },
         ],
         def,
      );
      expect(plan.mismatches).toEqual([]);
   });
});
