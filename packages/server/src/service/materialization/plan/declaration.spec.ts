// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Annotations } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { persistAnnotationFields, placementOf, readNotes } from "./declaration";

function notes(line: string) {
   return [
      {
         text: `${line}\n`,
         at: {
            url: "m.malloy",
            range: {
               start: { line: 0, character: 0 },
               end: { line: 0, character: line.length },
            },
         },
      },
   ];
}

const placed = (line: string) =>
   placementOf(readNotes(notes(line), "@").tag, "persist");

const fields = (line: string) =>
   persistAnnotationFields(
      new Annotations({ notes: notes(line) } as ConstructorParameters<
         typeof Annotations
      >[0]),
   );

describe("placementOf: the one placement rule", () => {
   it("places a table beside or inside the key, the nested form winning", () => {
      expect(placed('#@ persist storage="lake"')).toEqual({
         placement: { tier: "storage", destination: "lake" },
      });
      expect(placed('#@ persist { storage="lake" name="t" }')).toEqual({
         placement: { tier: "storage", destination: "lake", name: "t" },
      });
      expect(placed('#@ persist { storage="in" } storage="out"')).toEqual({
         placement: { tier: "storage", destination: "in" },
      });
      expect(placed('#@ persist name="t"')).toEqual({
         placement: { tier: "colocated", name: "t" },
      });
   });

   for (const [label, line] of [
      ["empty", '#@ persist storage=""'],
      ["whitespace", '#@ persist storage="  "'],
      ["a list", '#@ persist storage=["lake"]'],
      ["a block", '#@ persist storage { at="lake" }'],
      ["nested and empty", '#@ persist { storage="" }'],
      ["a nested list", '#@ persist { storage=["lake"] }'],
      ["a nested block", '#@ persist { storage { at="lake" } }'],
      // The nested form wins even when it names nothing: never the flat one.
      [
         "a nested list over a flat name",
         '#@ persist { storage=["x"] } storage="lake"',
      ],
      [
         "a nested block over a flat name",
         '#@ persist { storage { at="x" } } storage="lake"',
      ],
      [
         "nested whitespace over a flat name",
         '#@ persist { storage=" " } storage="lake"',
      ],
   ] as const) {
      it(`refuses a storage= that is ${label}, with no destination in its fields`, () => {
         expect(placed(line)).toEqual({ refused: "empty_storage" });
         expect(fields(line).storage).toBeUndefined();
      });
   }

   it("passes any other text through as a name, trimmed, for the resolver to look up", () => {
      expect(placed('#@ persist storage="  lake  "')).toEqual({
         placement: { tier: "storage", destination: "lake" },
      });
      expect(placed('#@ persist storage="no.such"')).toEqual({
         placement: { tier: "storage", destination: "no.such" },
      });
      expect(placed("#@ persist storage=5")).toEqual({
         placement: { tier: "storage", destination: "5" },
      });
   });
});
