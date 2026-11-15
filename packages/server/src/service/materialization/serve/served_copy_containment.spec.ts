// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { MalloyError } from "@malloydata/malloy";
import {
   buildServedCopy,
   rerootDeclaration,
   termsKeptAsWritten,
   type Reroot,
} from "./served_copy";
import { failingReroots } from "./served_copy_containment";
import { keepsTermsAsWritten } from "./served_copy_serve";
import { JOIN_CONNECTION_MISMATCH } from "../plan/cross_connection_joins";

const URL = "file:///m.malloy";

function rangeOf(text: string, name: string): Reroot["range"] {
   const lines = text.split("\n");
   const startLine = lines.findIndex((l) => l.includes(`source: ${name} is`));
   const startChar = lines[startLine].indexOf(`${name} is`);
   let endLine = startLine;
   while (endLine + 1 < lines.length && lines[endLine + 1].trim() !== "") {
      endLine++;
   }
   return {
      start: { line: startLine, character: startChar },
      end: { line: endLine, character: lines[endLine].length },
   };
}

function reroot(
   text: string,
   name: string,
   extra: Partial<Reroot> = {},
): Reroot {
   return {
      url: URL,
      range: rangeOf(text, name),
      sourceName: name,
      connectionAlias: "__publisher_storage_t_0",
      virtualHandle: `h_${name}`,
      typeName: `${name}__served`,
      columns: [
         { name: "ts", type: "TIMESTAMP" },
         { name: "amount", type: "DOUBLE" },
      ],
      storedDimensions: [],
      ...extra,
   };
}

const window = (field: string, code: string) => ({
   kind: "current_time" as const,
   code,
   columns: [field],
   window: {
      field,
      op: ">=" as const,
      offset: { sign: -1 as const, amount: 60, unit: "day" as const },
   },
});

const text = `##! experimental { access_modifiers }
// a comment

source: events is duckdb.table('events')

source: a is events extend {
  measure: n is count()
}

source: w is events extend {
  internal dimension: t2 is ts
  where: t2 >= now - 60 days
  measure: n is count()
}

source: c is events extend {
  measure: total is amount.sum()
}
`;

/** A Malloy error with one problem at `needle`'s first place in `served`. */
function errorAt(
   served: string,
   needle: string,
   code = "field-not-found",
): MalloyError {
   const offset = served.indexOf(needle);
   const before = served.slice(0, offset).split("\n");
   const start = {
      line: before.length - 1,
      character: before[before.length - 1].length,
   };
   return new MalloyError("Error(s) compiling model", [
      {
         message: `problem at ${needle}`,
         severity: "error",
         code,
         at: { url: URL, range: { start, end: start } },
      } as never,
   ]);
}

describe("buildServedCopy's spans", () => {
   const copy = buildServedCopy(
      [reroot(text, "a"), reroot(text, "w"), reroot(text, "c")],
      () => text,
   );
   const served = copy.files.get(URL) as string;
   const spans = copy.spans.get(URL) ?? [];

   it("places each re-root's declaration and type exactly in the served text", () => {
      expect(spans.map((s) => s.sourceName).sort()).toEqual(["a", "c", "w"]);
      for (const s of spans) {
         expect(
            served.slice(s.declaration.start, s.declaration.end),
         ).toStartWith(
            `${s.sourceName} is __publisher_storage_t_0.virtual('h_${s.sourceName}')`,
         );
         expect(served.slice(s.declaration.end - 1, s.declaration.end)).toBe(
            "}",
         );
         expect(served.slice(s.type.start, s.type.end)).toStartWith(
            `type: ${s.sourceName}__served is {`,
         );
         expect(served.slice(s.type.end - 1, s.type.end)).toBe("}");
      }
   });

   it("traces a problem in one re-root's text to that re-root alone", () => {
      const inW = errorAt(served, "where: t2");
      expect(
         failingReroots(inW, copy.files, copy.spans, "precise").map(
            (f) => f.sourceName,
         ),
      ).toEqual(["w"]);
      const inType = errorAt(served, "type: c__served");
      expect(
         failingReroots(inType, copy.files, copy.spans, "precise").map(
            (f) => f.sourceName,
         ),
      ).toEqual(["c"]);
   });

   it("traces nothing for a problem in the author's own text, outside every re-root", () => {
      const outside = errorAt(served, "duckdb.table('events')");
      expect(
         failingReroots(outside, copy.files, copy.spans, "precise"),
      ).toEqual([]);
      expect(
         failingReroots(outside, copy.files, copy.spans, "last_resort"),
      ).toEqual([]);
      expect(
         failingReroots(
            new Error("not Malloy's"),
            copy.files,
            copy.spans,
            "precise",
         ),
      ).toEqual([]);
   });

   it("leaves a join refused across connections, and what may follow from it, to the leave-out pass", () => {
      const refusal = errorAt(
         served,
         "measure: total",
         JOIN_CONNECTION_MISMATCH,
      );
      expect(
         failingReroots(refusal, copy.files, copy.spans, "precise"),
      ).toEqual([]);
      // A missing field in the same re-root may follow from the refusal:
      // precise leaves it; the last resort traces it.
      const followOn = new MalloyError("x", [
         ...refusal.problems,
         ...errorAt(served, "total is amount").problems,
      ]);
      expect(
         failingReroots(followOn, copy.files, copy.spans, "precise"),
      ).toEqual([]);
      expect(
         failingReroots(followOn, copy.files, copy.spans, "last_resort").map(
            (f) => f.sourceName,
         ),
      ).toEqual(["c"]);
      // A failure that is nothing but a refusal and missing fields is the
      // leave-out pass's first, wherever the fields are; once that cannot
      // clear it, the last resort traces the one in another re-root.
      const elsewhere = new MalloyError("x", [
         ...refusal.problems,
         ...errorAt(served, "where: t2").problems,
      ]);
      expect(
         failingReroots(elsewhere, copy.files, copy.spans, "precise"),
      ).toEqual([]);
      expect(
         failingReroots(elsewhere, copy.files, copy.spans, "last_resort").map(
            (f) => f.sourceName,
         ),
      ).toEqual(["w"]);
      // Beside a problem no leave-out can cause, a missing field in the
      // refused join's own re-root is still left to the leave-out pass.
      const mixed = new MalloyError("x", [
         ...followOn.problems,
         ...errorAt(served, "where: t2", "unexpected-type").problems,
      ]);
      expect(
         failingReroots(mixed, copy.files, copy.spans, "precise").map(
            (f) => f.sourceName,
         ),
      ).toEqual(["w"]);
      // A problem no leave-out can cause, beside a refusal, is traced at once.
      const unrelated = new MalloyError("x", [
         ...refusal.problems,
         ...errorAt(served, "where: t2", "unexpected-type").problems,
      ]);
      expect(
         failingReroots(unrelated, copy.files, copy.spans, "precise").map(
            (f) => f.sourceName,
         ),
      ).toEqual(["w"]);
   });

   it("keeps offsets right after a pass blanks a definition in place", () => {
      const blanked = new Map([
         [URL, served.replace("measure: n is count()", " ".repeat(21))],
      ]);
      expect(
         failingReroots(
            errorAt(served, "where: t2"),
            blanked,
            copy.spans,
            "precise",
         ).map((f) => f.sourceName),
      ).toEqual(["w"]);
   });
});

describe("a rolling window over a hidden dimension the kept text computes", () => {
   const term = window("t2", "t2 >= now - 60 days");

   it("is left to the kept text, not written on the stored table ahead of the definition", () => {
      const copy = buildServedCopy(
         [
            reroot(text, "w", {
               columns: [
                  { name: "ts", type: "TIMESTAMP" },
                  { name: "t2", type: "TIMESTAMP", access: "internal" },
               ],
               storedDimensions: ["t2"],
               readTimeTerms: [term],
            }),
         ],
         () => text,
      );
      const served = copy.files.get(URL) as string;
      expect(served).toContain(
         "w is __publisher_storage_t_0.virtual('h_w')::w__served extend {\n  internal dimension: t2 is ts\n  where: t2 >= now - 60 days",
      );
      expect(served).not.toContain("extend { where: t2");
      expect(copy.spans.get(URL)?.[0].termsKeptAsWritten).toEqual([
         "t2 >= now - 60 days",
      ]);
   });

   it("still writes a window over a stored column on the stored table", () => {
      const r = reroot(text, "w", {
         columns: [
            { name: "ts", type: "TIMESTAMP" },
            { name: "t2", type: "TIMESTAMP", access: "internal" },
         ],
         storedDimensions: ["t2"],
         readTimeTerms: [term, window("ts", "ts >= now - 60 days")],
      });
      const copy = buildServedCopy([r], () => text);
      expect(copy.files.get(URL)).toContain(
         "::w__served extend { where: ts >= now - 60 days }",
      );
      expect(termsKeptAsWritten({ ...r, computedAtRead: ["t2"] })).toEqual([
         term,
      ]);
      expect(termsKeptAsWritten(r)).toEqual([]);
   });
});

describe("a pair table beside a column kept hidden under its own name (known limitation 30)", () => {
   const pairText = `wide is base extend {
  primary_key: id
  join_many: legs on legs.fid = id
  measure: n is count()
}`;
   const pair = {
      join: "legs",
      parentRowColumn: { name: "__parent_row", type: "BOOLEAN" },
      parentRowField: "__parent_row__pair",
      childColumns: [
         {
            name: "miles",
            column: "legs__miles",
            internalName: "legs__miles__pair",
            type: "DOUBLE",
         },
      ],
      parentMeasures: [{ name: "n", internalName: "n__pair" }],
      childStoredDimensions: [],
   };

   it("renames the pair's stored columns ahead of the include, never just after it", () => {
      const out = rerootDeclaration(pairText, {
         sourceName: "wide",
         connectionAlias: "a",
         virtualHandle: "h",
         typeName: "wide__served",
         storedDimensions: [],
         columns: [
            { name: "id", type: "BIGINT" },
            { name: "secret", type: "VARCHAR", access: "private" },
         ],
         pair,
      });
      expect(out).toBe(`wide is a.virtual('h')::wide__served extend {
  rename:
    __parent_row__pair is __parent_row
    legs__miles__pair is legs__miles
} include { public: *; private: secret; internal: __parent_row__pair, legs__miles__pair } extend {
  primary_key: id
  dimension: legs is {
    miles is legs__miles__pair
  }
  measure: n is count()
} extend {
  internal rename:
    n__pair is n
  measure:
    n is n__pair { where: __parent_row__pair }
}`);
   });

   it("keeps a multi-path block's own rows behind the include", () => {
      const out = rerootDeclaration(pairText, {
         sourceName: "wide",
         connectionAlias: "a",
         virtualHandle: "h",
         typeName: "wide__served",
         storedDimensions: [],
         columns: [{ name: "secret", type: "VARCHAR", access: "internal" }],
         pair: {
            ...pair,
            block: {
               lane: 2,
               laneColumn: { name: "__lane", type: "INTEGER" },
               laneField: "__lane__pair",
               others: [],
            },
         },
      });
      expect(out).toContain(
         "} include { public: *; internal: secret, __parent_row__pair, __lane__pair, legs__miles__pair } extend {\n  where: __lane__pair = 2\n} extend {",
      );
   });

   it("labels a pair re-root refused for an author rename behind the include definition_not_parsed, not pair_join_not_found", () => {
      const source = `source: wide is base extend {
  rename: dist is distance
  join_many: legs on legs.fid = id
}
`;
      const copy = buildServedCopy(
         [
            reroot(source, "wide", {
               columns: [
                  { name: "dist", type: "DOUBLE" },
                  { name: "secret", type: "VARCHAR", access: "internal" },
               ],
               pair,
            }),
         ],
         () => source,
      );
      expect(copy.refused.map((r) => r.reason)).toEqual([
         "definition_not_parsed",
      ]);
      const elsewhere = `source: wide is base extend {
  measure: n is count()
}
`;
      const missing = buildServedCopy(
         [reroot(elsewhere, "wide", { pair })],
         () => elsewhere,
      );
      expect(missing.refused.map((r) => r.reason)).toEqual([
         "pair_join_not_found",
      ]);
   });
});

describe("keepsTermsAsWritten", () => {
   it("holds a term only when the served filters carry it as written, whitespace aside", () => {
      const served = { filterList: [{ code: "t2  >= now -\n 60 days" }] };
      expect(keepsTermsAsWritten(served, [])).toBe(true);
      expect(keepsTermsAsWritten(served, ["t2 >= now - 60 days"])).toBe(true);
      expect(keepsTermsAsWritten(served, ["t2 >= now - 30 days"])).toBe(false);
      expect(keepsTermsAsWritten({}, ["t2 >= now - 60 days"])).toBe(false);
      expect(keepsTermsAsWritten(undefined, ["t2 >= now - 60 days"])).toBe(
         false,
      );
   });
});
