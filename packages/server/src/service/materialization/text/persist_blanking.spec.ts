// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { blankNoteSpans, type NoteSpan } from "./persist_blanking";

const span = (
   line: number,
   from: number,
   to: number,
   text = '#@ persist name="t"\n',
): NoteSpan => ({
   text,
   range: {
      start: { line, character: from },
      end: { line, character: to },
   },
});

const FILE = `source: a is duckdb.table('t') extend {
  #@ persist name="t"
  view: v is { group_by: x }
}
`;

describe("blankNoteSpans", () => {
   it("blanks a note's span, keeping every position", () => {
      const out = blankNoteSpans(FILE, [span(1, 2, 22)]);
      expect(out).toBeDefined();
      expect(out?.length).toBe(FILE.length);
      expect(out).not.toContain("#@");
      expect(out?.split("\n")[1].trim()).toBe("");
   });

   it("refuses a span that does not hold its note", () => {
      expect(blankNoteSpans(FILE, [span(2, 2, 22)])).toBeUndefined();
      expect(
         blankNoteSpans(FILE, [span(1, 2, 22, "#@ persist name=other\n")]),
      ).toBeUndefined();
   });

   it("refuses a span over more than one line", () => {
      const s = span(1, 2, 22);
      s.range.end.line = 2;
      expect(blankNoteSpans(FILE, [s])).toBeUndefined();
   });

   it("refuses a span the lexer reads as code (inside a string)", () => {
      const text = `source: a is duckdb.sql("""
#@ persist name="t"
select 1""")
`;
      expect(blankNoteSpans(text, [span(1, 0, 20)])).toBeUndefined();
   });

   it("refuses a span past its line", () => {
      expect(blankNoteSpans(FILE, [span(1, 80, 90)])).toBeUndefined();
      expect(blankNoteSpans(FILE, [span(40, 0, 3)])).toBeUndefined();
   });
});
