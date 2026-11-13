// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { declaredFieldNotes, declaredNotes } from "./declared_notes";

const M = "file:///pkg/m.malloy";
const A = "file:///pkg/a.malloy";

const note = (url: string, line: number) => ({
   text: "#(materialize)\n",
   at: {
      url,
      range: {
         start: { line, character: 0 },
         end: { line, character: 14 },
      },
   },
});
const at = (url: string, line: number) => ({
   url,
   range: { start: { line, character: 8 } },
});

describe("declaredNotes", () => {
   it("reads a shared annotations object as the earliest source's alone", () => {
      const annotations = { blockNotes: [note(M, 0)] };
      const base = { location: at(M, 1), annotations };
      const ext = { location: at(M, 2), annotations };
      const contents = { base, ext };
      expect(declaredNotes(base, contents)).toHaveLength(1);
      expect(declaredNotes(ext, contents)).toEqual([]);
   });

   it("reads each definition of a block as declaring the block's notes", () => {
      // Malloy spreads the block's annotation into a fresh object per
      // definition, sharing only the notes array.
      const blockNotes = [note(M, 0)];
      const a = { location: at(M, 1), annotations: { blockNotes } };
      const b = { location: at(M, 2), annotations: { blockNotes } };
      expect(declaredNotes(b, { a, b })).toHaveLength(1);
   });

   it("drops a note parsed in another file", () => {
      const annotations = { blockNotes: [note(A, 0)] };
      const ext = { location: at(M, 2), annotations };
      expect(declaredNotes(ext, { ext })).toEqual([]);
   });

   it("keeps every note of a source with no location", () => {
      const annotations = { blockNotes: [note(A, 0)] };
      expect(declaredNotes({ annotations }, {})).toHaveLength(1);
   });
});

describe("declaredFieldNotes", () => {
   it("reads a field's shared annotations object as the earliest source's alone", () => {
      const annotations = { blockNotes: [note(M, 0)] };
      const base = { location: at(M, 1), fields: [{ annotations }] };
      // An extension copies the field (a new object) but keeps its notes.
      const ext = { location: at(M, 2), fields: [{ annotations }] };
      const contents = { base, ext };
      expect(declaredFieldNotes(base, base.fields[0], contents)).toHaveLength(
         1,
      );
      expect(declaredFieldNotes(ext, ext.fields[0], contents)).toEqual([]);
   });

   it("reads a fresh annotations object as the source's own", () => {
      const base = {
         location: at(M, 1),
         fields: [{ annotations: { blockNotes: [note(M, 0)] } }],
      };
      const ext = {
         location: at(M, 2),
         fields: [{ annotations: { blockNotes: [note(M, 2)] } }],
      };
      expect(declaredFieldNotes(ext, ext.fields[0], [base, ext])).toHaveLength(
         1,
      );
   });

   it("drops a field's note parsed in another file", () => {
      const annotations = { blockNotes: [note(A, 0)] };
      const ext = { location: at(M, 2), fields: [{ annotations }] };
      expect(declaredFieldNotes(ext, ext.fields[0], { ext })).toEqual([]);
   });

   it("does not read a later holder as the declarer", () => {
      const annotations = { blockNotes: [note(M, 0)] };
      const base = { location: at(M, 1), fields: [{ annotations }] };
      const later = { location: at(M, 5), fields: [{ annotations }] };
      expect(
         declaredFieldNotes(base, base.fields[0], [later, base]),
      ).toHaveLength(1);
   });
});
