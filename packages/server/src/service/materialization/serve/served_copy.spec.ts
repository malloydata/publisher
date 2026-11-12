// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   buildServedCopy,
   declarationSpans,
   declaredBaseName,
   dropDimensions,
   rerootDeclaration,
   SERVED_COPY_FLAG,
   typeDeclaration,
   type Reroot,
} from "./served_copy";

/** The zero-based range of `needle`'s declaration (after `source: `) in `text`. */
function rangeOf(text: string, name: string): Reroot["range"] {
   const lines = text.split("\n");
   const startLine = lines.findIndex((l) => l.includes(`source: ${name} is`));
   const startChar = lines[startLine].indexOf(`${name} is`);
   // The declaration runs to the line before the next blank line or EOF.
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
      url: "file:///m.malloy",
      range: rangeOf(text, name),
      sourceName: name,
      connectionAlias: "__publisher_storage_t_0",
      virtualHandle: "h1",
      typeName: `${name}__served`,
      columns: [
         { name: "r", type: "VARCHAR" },
         { name: "total", type: "DOUBLE" },
      ],
      ...extra,
   };
}

describe("rerootDeclaration", () => {
   it("replaces the base and keeps the author's extend block verbatim", () => {
      const out = rerootDeclaration(
         `daily is ev -> {\n  group_by: r\n  aggregate: total is amt.sum()\n} extend {\n  dimension: big is total > 1\n  primary_key: r\n}`,
         {
            sourceName: "daily",
            connectionAlias: "__publisher_storage_t_0",
            virtualHandle: "h1",
            typeName: "daily__served",
         },
      );
      expect(out).toBe(
         `daily is __publisher_storage_t_0.virtual('h1')::daily__served extend {\n  dimension: big is total > 1\n  primary_key: r\n}`,
      );
   });

   it("keeps every trailing extend block, and nothing from the pipeline", () => {
      const out = rerootDeclaration(
         "d is ev -> { group_by: r } extend { dimension: a is 1 } extend { dimension: b is 2 }",
         {
            sourceName: "d",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "d__served",
         },
      );
      expect(out).toBe(
         "d is x.virtual('h')::d__served extend { dimension: a is 1 } extend { dimension: b is 2 }",
      );
   });

   it("re-roots a source with no extend block", () => {
      expect(
         rerootDeclaration("d is ev -> { group_by: r }", {
            sourceName: "d",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "d__served",
         }),
      ).toBe("d is x.virtual('h')::d__served");
   });

   it("ignores braces, `is` and `extend` inside strings and comments", () => {
      const spans = declarationSpans(
         `d is ev -> { where: r = 'extend { is }' -- extend {\n group_by: r } extend { dimension: s is "}" }`,
      );
      expect(spans?.nameEnd).toBe(2);
      expect(spans?.extendStart).toBeDefined();
   });

   it("quotes a name and handle that need it", () => {
      expect(
         rerootDeclaration("`order facts` is ev -> { group_by: r }", {
            sourceName: "order facts",
            connectionAlias: "x",
            virtualHandle: "it's",
            typeName: "order facts__served",
         }),
      ).toBe("`order facts` is x.virtual('it\\'s')::`order facts__served`");
   });

   it("reads a base as query-shaped only from a top-level `->` before the extends", () => {
      expect(declarationSpans("d is ev -> { group_by: r }")?.queryShaped).toBe(
         true,
      );
      expect(
         declarationSpans(
            "d is (ev extend { where: x }) -> by_r extend { dimension: a is 1 }",
         )?.queryShaped,
      ).toBe(true);
      expect(declarationSpans("d is ev extend { where: x }")?.queryShaped).toBe(
         false,
      );
      expect(
         declarationSpans(
            "d is ev extend { view: v is { group_by: r } -> { select: * } }",
         )?.queryShaped,
      ).toBe(false);
      expect(
         declarationSpans(
            "d is duckdb.sql('select 1 -> 2') extend { where: x }",
         )?.queryShaped,
      ).toBe(false);
      expect(declarationSpans("d is ev -- -> {\n")?.queryShaped).toBe(false);
   });

   it("refuses a declaration with no top-level `is`", () => {
      expect(declarationSpans("not a declaration")).toBeUndefined();
      expect(declarationSpans("d is ev -> { group_by: r")).toBeUndefined();
   });
});

describe("typeDeclaration", () => {
   it("declares each public column with its Malloy type", () => {
      expect(
         typeDeclaration({
            typeName: "daily__served",
            columns: [
               { name: "r", type: "VARCHAR" },
               { name: "date", type: "DATE" },
               { name: "total", type: "DECIMAL(18,2)" },
            ],
         }),
      ).toBe(
         "type: daily__served is {\n   r::string,\n   `date`::date,\n   total::number\n}",
      );
   });
});

describe("buildServedCopy", () => {
   const file = `##! experimental { persistence }
source: ev is duckdb.table('ev')

#@ persist name="daily" storage="lake"
#(doc) "daily things"
source: daily is ev -> {
  group_by: r
  aggregate: total is amt.sum()
} extend {
  dimension: big is total > 1
}

source: daily_ext is daily extend { measure: t2 is total.sum() }
`;

   it("re-roots the declaration, keeps everything else, and adds the flag and type after the model annotations", () => {
      const copy = buildServedCopy([reroot(file, "daily")], () => file);
      expect(copy.refused).toEqual([]);
      expect(copy.files.get("file:///m.malloy"))
         .toBe(`##! experimental { persistence }
${SERVED_COPY_FLAG}
type: daily__served is {
   r::string,
   total::number
}

source: ev is duckdb.table('ev')

#@ persist name="daily" storage="lake"
#(doc) "daily things"
source: daily is __publisher_storage_t_0.virtual('h1')::daily__served extend {
  dimension: big is total > 1
}

source: daily_ext is daily extend { measure: t2 is total.sum() }
`);
   });

   it("re-roots several declarations in one file from the last back", () => {
      const two = `${file}
#@ persist name="weekly" storage="lake"
source: weekly is ev -> { group_by: r }
`;
      const copy = buildServedCopy(
         [reroot(two, "daily"), reroot(two, "weekly", { virtualHandle: "h2" })],
         () => two,
      );
      const text = copy.files.get("file:///m.malloy") ?? "";
      expect(text).toContain(
         "source: weekly is __publisher_storage_t_0.virtual('h2')::weekly__served",
      );
      expect(text).toContain(
         "source: daily is __publisher_storage_t_0.virtual('h1')::daily__served extend {",
      );
      expect(text).toContain("type: weekly__served is {");
   });

   it("leaves a file with nothing re-rooted out, and refuses a range past the text", () => {
      const bad = reroot(file, "daily", {
         range: {
            start: { line: 99, character: 0 },
            end: { line: 99, character: 4 },
         },
      });
      const copy = buildServedCopy([bad], () => file);
      expect(copy.files.size).toBe(0);
      expect(copy.refused).toEqual([
         {
            sourceName: "daily",
            url: "file:///m.malloy",
            reason: "range_not_found",
         },
      ]);
   });

   it("leaves a source declared by refinement as written, since its table holds its root's rows", () => {
      const refined = `source: all_deals is duckdb.table('d')

#@ persist name="deals" storage="lake"
source: deals is all_deals extend {
  where: not is_deleted
}
`;
      const copy = buildServedCopy([reroot(refined, "deals")], () => refined);
      expect(copy.files.size).toBe(0);
      expect(copy.refused).toEqual([
         {
            sourceName: "deals",
            url: "file:///m.malloy",
            reason: "not_query_shaped",
         },
      ]);
   });

   it("refuses every re-root of a file it cannot read", () => {
      const copy = buildServedCopy([reroot(file, "daily")], () => undefined);
      expect(copy.refused.map((r) => r.reason)).toEqual(["range_not_found"]);
   });
});

describe("the #(materialize) re-root", () => {
   const target = {
      sourceName: "east",
      connectionAlias: "x",
      virtualHandle: "h",
      typeName: "east__served",
   };

   it("drops a stored dimension's definition with its annotations, keeping its neighbours", () => {
      const out = dropDimensions(
         `extend {
  where: region = 'east'
  dimension: big is amt > 10, small is (amt < 2 and region = 'is'), keep1 is 1
  dimension:
    keep2 is amt * 2
    # label="Month"
    m is d.month
  # doc for the whole statement
  dimension: only is 3
  measure: big_total is amt.sum()
}`,
         new Set(["big", "small", "m", "only"]),
      );
      expect(out?.text).toBe(`extend {
  where: region = 'east'
  dimension: keep1 is 1
  dimension:
    keep2 is amt * 2
  measure: big_total is amt.sum()
}`);
      expect([...(out?.dropped ?? [])].sort()).toEqual([
         "big",
         "m",
         "only",
         "small",
      ]);
   });

   it("drops a middle and a last definition after a kept one", () => {
      expect(
         dropDimensions(
            "extend { dimension: a is 1, b is 2, c is 3 }",
            new Set(["b", "c"]),
         )?.text,
      ).toBe("extend { dimension: a is 1 }");
      expect(
         dropDimensions(
            "extend { dimension: a is 1, b is 2, c is 3 }",
            new Set(["b"]),
         )?.text,
      ).toBe("extend { dimension: a is 1, c is 3 }");
   });

   it("drops a quoted name, and leaves views and nested queries alone", () => {
      const out = dropDimensions(
         "extend { dimension: `order month` is d.month, k is 1\n view: v is { group_by: big is 1 } }",
         new Set(["order month", "big"]),
      );
      expect(out?.text).toBe(
         "extend { dimension: k is 1\n view: v is { group_by: big is 1 } }",
      );
      expect([...(out?.dropped ?? [])]).toEqual(["order month"]);
   });

   it("re-roots a table-shaped source; an inherited dimension is just its stored column", () => {
      expect(
         rerootDeclaration(
            "east is orders extend {\n  where: region = 'east'\n  dimension: big is amt > 10\n  measure: n is count()\n}",
            { ...target, storedDimensions: ["tier", "big"] },
         ),
      ).toBe(
         "east is x.virtual('h')::east__served extend {\n  where: region = 'east'\n  measure: n is count()\n}",
      );
   });

   it("writes each field's annotations on it in the type", () => {
      expect(
         typeDeclaration({
            typeName: "east__served",
            columns: [
               {
                  name: "order_month",
                  type: "DATE",
                  notes: [
                     '#(doc) "The month"\n',
                     "#(classification) internal\n",
                  ],
               },
               { name: "amt", type: "DOUBLE" },
            ],
         }),
      ).toBe(
         'type: east__served is {\n   #(doc) "The month"\n   #(classification) internal\n   order_month::date,\n   amt::number\n}',
      );
   });

   it("is not refused for a refinement-shaped declaration, unlike a #@ persist re-root", () => {
      const text = `source: orders is duckdb.table('ev')

#(materialize)
source: east is orders extend {
  where: region = 'east'
}
`;
      const copy = buildServedCopy(
         [{ ...reroot(text, "east"), storedDimensions: [] }],
         () => text,
      );
      expect(copy.refused).toEqual([]);
      expect(copy.files.get("file:///m.malloy")).toContain(
         "source: east is __publisher_storage_t_0.virtual('h1')::east__served extend {\n  where: region = 'east'\n}",
      );
   });

   it("names a declaration's base when it is a bare source name", () => {
      const text =
         "source: a is b extend { measure: n is count() }\nsource: c is b -> { group_by: r }\n";
      expect(
         declaredBaseName(text, {
            start: { line: 0, character: 8 },
            end: { line: 0, character: 47 },
         }),
      ).toBe("b");
      expect(
         declaredBaseName(text, {
            start: { line: 1, character: 8 },
            end: { line: 1, character: 34 },
         }),
      ).toBeUndefined();
   });
});
