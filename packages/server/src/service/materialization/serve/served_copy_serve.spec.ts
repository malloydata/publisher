// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { FieldClassification } from "../../classification";
import { storedColumnNotes } from "./served_copy_serve";

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
