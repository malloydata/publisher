// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Annotations } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import type { PersistSource } from "@malloydata/malloy";
import {
   notesDeclarePersist,
   persistAnnotationFields,
   placementOf,
   readNotes,
} from "./declaration";
import { declaresOwnPersist } from "./addressing";

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

const at = {
   url: "file:///m.malloy",
   range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } },
};

describe("notesDeclarePersist: a source's own `#@ persist`", () => {
   const notes = (...texts: string[]) => texts.map((text) => ({ text, at }));

   it("reads `#@ persist`, flat or with a block", () => {
      expect(notesDeclarePersist(notes("#@ persist"))).toBe(true);
      expect(notesDeclarePersist(notes('#@ persist name="t"'))).toBe(true);
      expect(notesDeclarePersist(notes("#@ persist { storage=lake }"))).toBe(
         true,
      );
   });

   it("does not read a `#@` line that writes another key as a declaration", () => {
      expect(notesDeclarePersist(notes("#@ preaggregate"))).toBe(false);
      expect(notesDeclarePersist(notes("#@ preaggregate", "# label=x"))).toBe(
         false,
      );
   });

   it("reads no other route", () => {
      expect(notesDeclarePersist([])).toBe(false);
      expect(notesDeclarePersist(notes("# persist"))).toBe(false);
      expect(notesDeclarePersist(notes("#(materialize)"))).toBe(false);
   });
});

describe("declaresOwnPersist: the fold's reading of one compiled source", () => {
   const sourceWith = (annotation: () => unknown) =>
      ({
         get annotation() {
            return annotation();
         },
      }) as unknown as PersistSource;

   it("counts an unreadable or missing annotation as declaring", () => {
      expect(
         declaresOwnPersist(
            sourceWith(() => {
               throw new Error("unreadable");
            }),
         ),
      ).toBe(true);
      expect(declaresOwnPersist(sourceWith(() => undefined))).toBe(true);
      // A note the reader cannot place throws inside it: declaring.
      expect(
         declaresOwnPersist(
            sourceWith(() => ({ notes: [{ text: "#@ preaggregate" }] })),
         ),
      ).toBe(true);
   });

   it("reads its own notes and block notes with the declaration reader", () => {
      expect(
         declaresOwnPersist(
            sourceWith(() => ({ notes: [{ text: "#@ persist", at }] })),
         ),
      ).toBe(true);
      expect(
         declaresOwnPersist(
            sourceWith(() => ({ blockNotes: [{ text: "#@ persist", at }] })),
         ),
      ).toBe(true);
      expect(
         declaresOwnPersist(
            sourceWith(() => ({ notes: [{ text: "#@ preaggregate", at }] })),
         ),
      ).toBe(false);
      expect(declaresOwnPersist(sourceWith(() => ({ notes: [] })))).toBe(false);
   });
});
