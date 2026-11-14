// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   buildServedCopy,
   hiddenInclude,
   SERVED_COPY_ACCESS_FLAG,
   declarationSpans,
   declaredBaseName,
   dropDimensions,
   dropJoinDefinition,
   rerootDeclaration,
   rewriteDimensions,
   SERVED_COPY_FLAG,
   storedMalloyType,
   storedTypeText,
   typeDeclaration,
   type Reroot,
   replacePairJoin,
} from "./served_copy";
import { inverseRenameBlocks } from "../text/declaration_text";

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
   it("keeps an extension table's ancestors' blocks ahead of its own, less the dimensions it stores (P10)", () => {
      const out = rerootDeclaration(
         "east is stored extend {\n  where: region = 'east'\n  dimension: twice is amt * 2\n}",
         {
            sourceName: "east",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "east__served",
            storedDimensions: ["twice", "m"],
            columns: [
               { name: "region", type: "VARCHAR" },
               { name: "amt", type: "DOUBLE" },
               { name: "twice", type: "DOUBLE" },
               { name: "m", type: "DATE" },
            ],
            keptBlocks:
               "extend {\n  dimension: m is d.month\n  measure: total is amt.sum()\n}",
         },
      );
      expect(out).toBe(
         "east is x.virtual('h')::east__served extend {\n  measure: total is amt.sum()\n} extend {\n  where: region = 'east'\n}",
      );
   });

   it("gives a stored renamed column back the name its kept blocks rename it from", () => {
      const out = rerootDeclaration(
         "renamed is stored extend {\n  rename: order_id is id\n  measure: s is order_id.sum()\n}",
         {
            sourceName: "renamed",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "renamed__served",
            storedDimensions: [],
            columns: [
               { name: "order_id", type: "INTEGER" },
               { name: "amt", type: "DOUBLE" },
            ],
            keptBlocks: "extend { measure: n is count() }",
         },
      );
      expect(out).toBe(
         "renamed is x.virtual('h')::renamed__served extend { rename: id is order_id } extend { measure: n is count() } extend {\n  rename: order_id is id\n  measure: s is order_id.sum()\n}",
      );
   });

   it("refuses a rename it cannot read, rather than re-root around it", () => {
      expect(
         rerootDeclaration("r is s extend {\n  internal rename: a is b\n}", {
            sourceName: "r",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "r__served",
            columns: [{ name: "a", type: "INTEGER" }],
         }),
      ).toBeUndefined();
   });

   it("re-applies a rolling window on the stored table, before the author's blocks", () => {
      const out = rerootDeclaration(
         "recent is last30 extend {\n  measure: n is count()\n}",
         {
            sourceName: "recent",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "recent__served",
            storedDimensions: [],
            readTimeTerms: [
               {
                  kind: "current_time",
                  code: "now - 30 days <= ts",
                  columns: ["ts"],
                  window: {
                     field: "ts",
                     op: ">=",
                     offset: { sign: -1, amount: 30, unit: "day" },
                  },
               },
            ],
         },
      );
      expect(out).toBe(
         "recent is x.virtual('h')::recent__served extend { where: ts >= now - 30 days } extend {\n  measure: n is count()\n}",
      );
   });

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

describe("storedTypeText", () => {
   it("writes a persisted nest's list of structs as Malloy's record array", () => {
      expect(storedTypeText("STRUCT(status VARCHAR, n BIGINT)[]")).toBe(
         "{ status::string, n::number }[]",
      );
      expect(
         storedTypeText('STRUCT("line total" DOUBLE, "select" VARCHAR)[]'),
      ).toBe("{ `line total`::number, `select`::string }[]");
      expect(storedTypeText("STRUCT(a STRUCT(b INTEGER)[])[]")).toBe(
         "{ a::{ b::number }[] }[]",
      );
      expect(storedTypeText("STRUCT(d DECIMAL(10,2))")).toBe("{ d::number }");
      expect(storedTypeText("VARCHAR[]")).toBe("string[]");
   });

   it("keeps the serve shape's scalar mapping, and its fallback for what it cannot read", () => {
      expect(storedTypeText("DECIMAL(18,2)")).toBe("number");
      expect(storedTypeText("TIMESTAMP WITH TIME ZONE")).toBe("timestamp");
      expect(storedTypeText("MAP(VARCHAR, INTEGER)")).toBe("json");
      expect(storedTypeText("STRUCT(a VARCHAR")).toBe("json");
   });

   it("storedMalloyType: undefined for what the type: could only call json", () => {
      // A nest built from Postgres or Snowflake lands as JSON: the source is
      // declined on its own rather than failing the whole model's copy.
      expect(storedMalloyType("JSON")).toBeUndefined();
      expect(storedMalloyType("MAP(VARCHAR, INTEGER)")).toBeUndefined();
      expect(storedMalloyType("STRUCT(a VARCHAR")).toBeUndefined();
      expect(storedMalloyType("BLOB")).toBeUndefined();
      expect(storedMalloyType("STRUCT(s VARCHAR)[]")).toBe("{ s::string }[]");
      expect(storedMalloyType("BIGINT")).toBe("number");
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

   it("re-roots a connection's SQL or table under an extend run, which its table holds as written", () => {
      const raw = `#@ persist name="raw" storage="lake"
source: raw is duckdb.sql("""SELECT region, amount FROM t""") extend {
  where: amount > 0
  measure: total is amount.sum()
}
`;
      const copy = buildServedCopy([reroot(raw, "raw")], () => raw);
      expect(copy.refused).toEqual([]);
      expect(copy.files.get("file:///m.malloy")).toContain(
         "source: raw is __publisher_storage_t_0.virtual('h1')::raw__served extend {\n  where: amount > 0\n  measure: total is amount.sum()\n}",
      );
      expect(
         declarationSpans("t is duckdb.table('t') extend { }")
            ?.connectionRooted,
      ).toBe(true);
      // A named base, or a method call that is not a root, is not one.
      expect(declarationSpans("t is base extend { }")?.connectionRooted).toBe(
         false,
      );
      expect(
         declarationSpans("t is duckdb.table('t') -> { select: * }")
            ?.connectionRooted,
      ).toBe(false);
      expect(
         declarationSpans("t is duckdb.virtual('t')::x extend { }")
            ?.connectionRooted,
      ).toBe(false);
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

   it("undoes a rename whose new name the table stores, since the table holds the field under it", () => {
      expect(
         rerootDeclaration(
            "daily_r is orders -> {\n  group_by: area is region, order_date\n} extend {\n  rename: order_day is order_date\n  measure: grand is total.sum()\n}",
            {
               ...target,
               storedDimensions: [],
               columns: [
                  { name: "area", type: "VARCHAR" },
                  { name: "order_day", type: "DATE" },
               ],
            },
         ),
      ).toBe(
         "daily_r is x.virtual('h')::east__served extend { rename: order_date is order_day } extend {\n  rename: order_day is order_date\n  measure: grand is total.sum()\n}",
      );
      // Only the renames to a stored name are undone; a #@ persist re-root,
      // whose table holds its query's output under the old name, keeps all.
      expect(
         inverseRenameBlocks(
            "extend {\n  rename: a is x, b is y\n  dimension: c is 1\n}",
            new Set(["a"]),
         ),
      ).toBe(" extend { rename: x is a }");
      expect(
         rerootDeclaration(
            "d is q -> { group_by: x } extend {\n  rename: y is x\n}",
            { ...target, columns: [{ name: "x", type: "VARCHAR" }] },
         ),
      ).toBe("d is x.virtual('h')::east__served extend {\n  rename: y is x\n}");
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

describe("a stored timeframe truncation (decision 3)", () => {
   const target = {
      sourceName: "east",
      connectionAlias: "x",
      virtualHandle: "h",
      typeName: "east__served",
   };

   it("renames the stored column internally and rewrites the definition in place, keeping its annotations", () => {
      expect(
         rerootDeclaration(
            "east is orders extend {\n  where: region = 'east'\n  dimension:\n    big is amt > 10\n    #(doc) \"The month\"\n    order_month is d.month\n  measure: n is count()\n}",
            {
               ...target,
               storedDimensions: ["big", "order_month"],
               columns: [
                  { name: "big", type: "BOOLEAN" },
                  { name: "order_month", type: "DATE", timeframe: "month" },
               ],
            },
         ),
      ).toBe(
         "east is x.virtual('h')::east__served extend {\n  internal rename:\n    order_month__stored is order_month\n  where: region = 'east'\n  dimension:\n    #(doc) \"The month\"\n    order_month is order_month__stored.month\n  measure: n is count()\n}",
      );
   });

   it("rewrites a definition in a later extend, and defines an inherited one in the first with its annotations", () => {
      expect(
         rerootDeclaration(
            "east is orders extend { where: r = 1 } extend { dimension: wk is d.week, k is 1 }",
            {
               ...target,
               storedDimensions: ["wk", "k", "inh"],
               columns: [
                  { name: "wk", type: "DATE", timeframe: "week" },
                  { name: "k", type: "INTEGER" },
                  {
                     name: "inh",
                     type: "TIMESTAMP",
                     timeframe: "hour",
                     notes: ['#(doc) "Inherited"\n'],
                  },
               ],
            },
         ),
      ).toBe(
         "east is x.virtual('h')::east__served extend {\n  internal rename:\n    wk__stored is wk\n    inh__stored is inh\n  dimension:\n    #(doc) \"Inherited\"\n    inh is inh__stored.hour\n where: r = 1 } extend { dimension: wk is wk__stored.week }",
      );
   });

   it("opens an extend for a #@ persist query's timeframe output column", () => {
      expect(
         rerootDeclaration(
            "daily is ev -> { group_by: m is d.month; aggregate: t is amt.sum() }",
            {
               ...target,
               sourceName: "daily",
               typeName: "daily__served",
               columns: [
                  { name: "m", type: "DATE", timeframe: "month" },
                  { name: "t", type: "DOUBLE" },
               ],
            },
         ),
      ).toBe(
         "daily is x.virtual('h')::daily__served extend {\n  internal rename:\n    m__stored is m\n  dimension:\n    m is m__stored.month\n}",
      );
   });

   it("picks an internal name no field of the model uses", () => {
      const out = rerootDeclaration(
         "east is orders extend { dimension: m is d.month }",
         {
            ...target,
            storedDimensions: ["m"],
            reservedNames: ["m__stored", "m__stored_2"],
            columns: [{ name: "m", type: "DATE", timeframe: "month" }],
         },
      );
      expect(out).toContain("m__stored_3 is m");
      expect(out).toContain("m is m__stored_3.month");
   });

   it("quotes a name that needs it, and ignores a timeframe outside the closed set", () => {
      const out = rerootDeclaration(
         "east is orders extend { dimension: `order month` is d.month }",
         {
            ...target,
            storedDimensions: ["order month", "odd"],
            columns: [
               { name: "order month", type: "DATE", timeframe: "month" },
               { name: "odd", type: "DATE", timeframe: "fortnight" },
            ],
         },
      );
      expect(out).toBe(
         "east is x.virtual('h')::east__served extend {\n  internal rename:\n    `order month__stored` is `order month`\n dimension: `order month` is `order month__stored`.month }",
      );
   });

   it("leaves a timeframe column's annotations off the type, since its field carries them", () => {
      expect(
         typeDeclaration({
            typeName: "t",
            columns: [
               {
                  name: "m",
                  type: "DATE",
                  timeframe: "month",
                  notes: ['#(doc) "x"\n'],
               },
            ],
         }),
      ).toBe("type: t is {\n   m::date\n}");
   });

   it("rewrites one definition among dropped neighbours", () => {
      const out = rewriteDimensions(
         "extend { dimension: a is 1, m is d.month, b is 2 }",
         new Set(["a", "b"]),
         new Map([["m", "m__stored.month"]]),
      );
      expect(out?.text).toBe("extend { dimension: m is m__stored.month }");
      expect([...(out?.rewritten ?? [])]).toEqual(["m"]);
      expect([...(out?.dropped ?? [])].sort()).toEqual(["a", "b"]);
   });
});

describe("hidden stored columns (P3)", () => {
   const text = `source: orders is base include { private: org_id; public: * } extend {
  private dimension: secret is amount * 2
  measure: total is secret.sum()
}
`;
   const columns: Reroot["columns"] = [
      { name: "amount", type: "DOUBLE" },
      { name: "org_id", type: "VARCHAR", access: "private" },
      { name: "region_code", type: "VARCHAR", access: "internal" },
      { name: "secret", type: "DOUBLE", access: "private" },
   ];

   it("writes the author's modifiers again on the stored table, before the author's extend", () => {
      const copy = buildServedCopy(
         [
            reroot(text, "orders", {
               columns,
               storedDimensions: ["region_code", "secret"],
            }),
         ],
         () => text,
      );
      const out = copy.files.get("file:///m.malloy") as string;
      expect(out.startsWith(SERVED_COPY_ACCESS_FLAG)).toBe(true);
      expect(out).toContain(
         "orders is __publisher_storage_t_0.virtual('h1')::orders__served include { public: *; private: org_id; internal: region_code } extend {",
      );
      // `secret` is defined in the source's own extend: that definition is
      // kept as written, modifier and all, and its column left out of the
      // type, so the stored column never stands in for it publicly.
      expect(out).toContain("  private dimension: secret is amount * 2");
      expect(out).not.toMatch(/secret::/);
      expect(out).toContain("org_id::string");
      expect(out).toContain("region_code::string");
   });

   it("writes nothing, and the plain flag, when no stored column is hidden", () => {
      expect(hiddenInclude([{ name: "a", type: "DOUBLE" }])).toBe("");
      const copy = buildServedCopy(
         [reroot(text, "orders", { storedDimensions: [] })],
         () => text,
      );
      const out = copy.files.get("file:///m.malloy") as string;
      expect(out.startsWith(SERVED_COPY_FLAG)).toBe(true);
      expect(out).not.toContain("include { public: *");
   });

   it("restores a hidden timeframe column under the author's modifier, its stored column renamed as hidden (row 8)", () => {
      // Inherited, so the re-root defines the field under its own modifier.
      // A hidden field's stored column is `private` (an `internal` one a
      // caller's own `extend` could read), hidden by a labelled rename in the
      // first block: no include, so an author's `rename:` still compiles.
      const out = rerootDeclaration(
         "orders is base extend {\n  measure: n is count()\n}",
         {
            sourceName: "orders",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "orders__served",
            storedDimensions: [],
            columns: [
               { name: "amount", type: "DOUBLE" },
               {
                  name: "order_month",
                  type: "DATE",
                  timeframe: "month",
                  access: "private",
               },
               {
                  name: "order_week",
                  type: "DATE",
                  timeframe: "week",
                  access: "internal",
               },
               { name: "order_day", type: "DATE", timeframe: "day" },
            ],
         },
      );
      expect(out).toBe(
         "orders is x.virtual('h')::orders__served extend {\n" +
            "  internal rename:\n" +
            "    order_day__stored is order_day\n" +
            "  private rename:\n" +
            "    order_month__stored is order_month\n" +
            "    order_week__stored is order_week\n" +
            "  dimension:\n" +
            "    order_day is order_day__stored.day\n" +
            "  internal dimension:\n" +
            "    order_week is order_week__stored.week\n" +
            "  private dimension:\n" +
            "    order_month is order_month__stored.month\n" +
            "  measure: n is count()\n" +
            "}",
      );
   });

   it("renames a hidden timeframe column ahead of the include another hidden column needs", () => {
      const out = rerootDeclaration(
         "orders is base extend {\n  measure: n is count()\n}",
         {
            sourceName: "orders",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "orders__served",
            storedDimensions: [],
            columns: [
               { name: "org_id", type: "VARCHAR", access: "internal" },
               {
                  name: "order_month",
                  type: "DATE",
                  timeframe: "month",
                  access: "private",
               },
               { name: "order_day", type: "DATE", timeframe: "day" },
            ],
         },
      );
      expect(out).toBe(
         "orders is x.virtual('h')::orders__served extend {\n" +
            "  rename:\n" +
            "    order_month__stored is order_month\n" +
            "    order_day__stored is order_day\n" +
            "} include { public: *; private: order_month__stored; internal: org_id, order_day__stored } extend {\n" +
            "  dimension:\n" +
            "    order_day is order_day__stored.day\n" +
            "  private dimension:\n" +
            "    order_month is order_month__stored.month\n" +
            "  measure: n is count()\n" +
            "}",
      );
   });

   it("writes no include for a hidden timeframe column alone, so an author's rename still compiles", () => {
      // An include would put the inverse rename block, and the author's own
      // `rename:`, just after it, which Malloy refuses: the whole served copy
      // would not compile.
      const out = rerootDeclaration(
         "orders is base extend {\n  rename: amt is amount\n  measure: n is amt.sum()\n}",
         {
            sourceName: "orders",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "orders__served",
            storedDimensions: [],
            columns: [
               { name: "amt", type: "DOUBLE" },
               {
                  name: "order_month",
                  type: "DATE",
                  timeframe: "month",
                  access: "internal",
               },
            ],
         },
      );
      expect(out).not.toContain("include");
      expect(out).toBe(
         "orders is x.virtual('h')::orders__served extend { rename: amount is amt } extend {\n" +
            "  private rename:\n" +
            "    order_month__stored is order_month\n" +
            "  internal dimension:\n" +
            "    order_month is order_month__stored.month\n" +
            "  rename: amt is amount\n" +
            "  measure: n is amt.sum()\n" +
            "}",
      );
   });

   it("refuses a re-root whose include would come just before the inverse renames (known limitation 30)", () => {
      // Malloy refuses a rename just after an include: written, the whole
      // served copy would not compile. Refused, only this source is live.
      expect(
         rerootDeclaration(
            "orders is base extend {\n  rename: amt is amount\n  measure: n is amt.sum()\n}",
            {
               sourceName: "orders",
               connectionAlias: "x",
               virtualHandle: "h",
               typeName: "orders__served",
               storedDimensions: [],
               columns: [
                  { name: "amt", type: "DOUBLE" },
                  { name: "org_id", type: "VARCHAR", access: "internal" },
               ],
            },
         ),
      ).toBeUndefined();
   });

   it("keeps a hidden timeframe field its own extend defines, computed at read, with no stored stand-in", () => {
      const text = `source: orders is base extend {
  private dimension: order_month is d.month
}
`;
      const copy = buildServedCopy(
         [
            reroot(text, "orders", {
               columns: [
                  { name: "d", type: "DATE" },
                  {
                     name: "order_month",
                     type: "DATE",
                     timeframe: "month",
                     access: "private",
                  },
               ],
               storedDimensions: ["order_month"],
            }),
         ],
         () => text,
      );
      const out = copy.files.get("file:///m.malloy") as string;
      expect(out).toContain("  private dimension: order_month is d.month");
      expect(out).not.toContain("order_month__stored");
      expect(out).not.toMatch(/order_month::/);
   });

   it("quotes a hidden name that needs it", () => {
      expect(
         hiddenInclude([
            { name: "source", type: "VARCHAR", access: "internal" },
         ]),
      ).toBe(" include { public: *; internal: `source` }");
   });
});

describe("an inlined join_one, answered from its stored columns", () => {
   const inlinedJoins = [
      {
         join: "carriers",
         columns: [
            { name: "nickname", column: "carriers__nickname", type: "VARCHAR" },
            {
               name: "since_month",
               column: "carriers__since_month",
               type: "DATE",
               timeframe: "month",
            },
         ],
      },
   ];
   const base = {
      sourceName: "flights",
      connectionAlias: "x",
      virtualHandle: "h",
      typeName: "flights__served",
      columns: [{ name: "carrier_region", type: "VARCHAR" }],
      storedDimensions: ["carrier_region"],
      inlinedJoins,
   };

   it("replaces the join with a record of the stored columns under its name", () => {
      const out = rerootDeclaration(
         "flights is duckdb.table('flights') extend {\n" +
            "  primary_key: id\n" +
            "  join_one: carriers with carrier\n" +
            "  dimension: carrier_region is carriers.region\n" +
            "  measure: n is count()\n" +
            "}",
         base,
      );
      expect(out).toBe(
         "flights is x.virtual('h')::flights__served extend {\n" +
            "  internal rename:\n" +
            "    carriers__nickname__stored is carriers__nickname\n" +
            "    carriers__since_month__stored is carriers__since_month\n" +
            "  dimension:\n" +
            "    carriers is {\n" +
            "      nickname is carriers__nickname__stored,\n" +
            "      since_month is carriers__since_month__stored.month\n" +
            "    }\n" +
            "  primary_key: id\n" +
            "\n" +
            "  measure: n is count()\n" +
            "}",
      );
   });

   it("declares a hidden join's record and renames under the join's modifier (row 24)", () => {
      for (const access of ["internal", "private"] as const) {
         const out = rerootDeclaration(
            "flights is duckdb.table('flights') extend {\n" +
               "  primary_key: id\n" +
               `  ${access} join_one: carriers with carrier\n` +
               "  dimension: carrier_region is carriers.region\n" +
               "  measure: n is count()\n" +
               "}",
            {
               ...base,
               inlinedJoins: [{ ...inlinedJoins[0], access }],
            },
         );
         expect(out).toBe(
            "flights is x.virtual('h')::flights__served extend {\n" +
               "  private rename:\n" +
               "    carriers__nickname__stored is carriers__nickname\n" +
               "    carriers__since_month__stored is carriers__since_month\n" +
               `  ${access} dimension:\n` +
               "    carriers is {\n" +
               "      nickname is carriers__nickname__stored,\n" +
               "      since_month is carriers__since_month__stored.month\n" +
               "    }\n" +
               "  primary_key: id\n" +
               "\n" +
               "  measure: n is count()\n" +
               "}",
         );
      }
   });

   it("adds the record when the join is inherited from the base it replaces", () => {
      const out = rerootDeclaration("flights is flights_base", base);
      expect(out).toContain("carriers is {");
      expect(
         out?.startsWith("flights is x.virtual('h')::flights__served extend {"),
      ).toBe(true);
   });

   it("declares the stored columns on the type", () => {
      expect(typeDeclaration({ ...base, columns: base.columns })).toBe(
         "type: flights__served is {\n" +
            "   carrier_region::string,\n" +
            "   carriers__nickname::string,\n" +
            "   carriers__since_month::date\n" +
            "}",
      );
   });

   it("drops one join_one of several, and leaves other kinds and mentions alone", () => {
      expect(
         dropJoinDefinition(
            "extend {\n  join_one:\n    a with x\n    carriers is c on carriers.code = carrier\n}",
            "carriers",
         ),
      ).toBe("extend {\n  join_one:\n    a with x\n}");
      expect(
         dropJoinDefinition(
            "extend { dimension: k is carriers.code }",
            "carriers",
         ),
      ).toBe("extend { dimension: k is carriers.code }");
      expect(
         dropJoinDefinition(
            "extend { join_many: carriers with c }",
            "carriers",
         ),
      ).toBeUndefined();
      // A hidden join's label goes with its statement, never onto the next
      // one; the record carries the join's modifier (InlinedJoin.access).
      expect(
         dropJoinDefinition(
            "extend {\n  internal join_one: carriers with carrier\n  measure: n is count()\n}",
            "carriers",
         ),
      ).toBe("extend {\n\n  measure: n is count()\n}");
      expect(
         dropJoinDefinition(
            "extend {\n  # doc\n  private join_one: carriers with carrier\n  dimension: k is 1\n}",
            "carriers",
         ),
      ).toBe("extend {\n\n  dimension: k is 1\n}");
   });
});

describe("replacePairJoin (pair tables, P5)", () => {
   const to = "dimension: segments is { miles is m }";
   it("replaces the join_many statement from its keyword, keeping its annotations and access", () => {
      expect(
         replacePairJoin(
            "extend {\n  # doc\n  internal join_many: segments on segments.flight_id = id\n  measure: c is count()\n}",
            "segments",
            to,
         ),
      ).toBe(
         "extend {\n  # doc\n  internal dimension: segments is { miles is m }\n  measure: c is count()\n}",
      );
   });
   it("replaces `<join> is <source> on …`, a quoted name, and a statement at the block's end", () => {
      expect(
         replacePairJoin(
            "extend { join_many: `segments` is segs extend { where: miles > 0 } on segments.flight_id = id }",
            "segments",
            to,
         ),
      ).toBe(`extend { ${to} }`);
   });
   it("refuses a statement that declares other joins too, a join_one, and a missing join", () => {
      expect(
         replacePairJoin(
            "extend {\n  join_many:\n    segments on segments.k = id\n    b on b.k = id\n}",
            "segments",
            to,
         ),
      ).toBeUndefined();
      expect(
         replacePairJoin(
            "extend { join_many: a on a.k = id, segments on segments.k = id }",
            "segments",
            to,
         ),
      ).toBeUndefined();
      expect(
         replacePairJoin(
            "extend { join_one: segments with flight_id; join_many: s on s.k = segments.k }",
            "segments",
            to,
         ),
      ).toBeUndefined();
      expect(replacePairJoin("extend { }", "segments", to)).toBeUndefined();
   });
});

describe("rerootDeclaration (pair tables, P5)", () => {
   it("reads the pair table as the source: a record for the join, internal columns, measures over the parent rows", () => {
      const text = `flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure:
    flight_count is count()
    leg_miles is segments.miles.sum()
}`;
      const out = rerootDeclaration(text, {
         sourceName: "flights",
         connectionAlias: "a",
         virtualHandle: "h",
         typeName: "flights__served",
         columns: [{ name: "id", type: "BIGINT" }],
         pair: {
            join: "segments",
            parentRowColumn: { name: "__parent_row", type: "BOOLEAN" },
            parentRowField: "__parent_row__pair",
            childColumns: [
               {
                  name: "miles",
                  column: "segments__miles",
                  internalName: "segments__miles__pair",
                  type: "DOUBLE",
               },
               {
                  name: "seg_day",
                  column: "segments__day",
                  internalName: "segments__day__pair",
                  type: "DATE",
                  timeframe: "day",
               },
            ],
            parentMeasures: [
               {
                  name: "flight_count",
                  internalName: "flight_count__pair",
                  notes: ['#(doc) "Flights"'],
               },
            ],
            childStoredDimensions: [],
         },
      });
      expect(out).toBe(`flights is a.virtual('h')::flights__served extend {
  internal rename:
    __parent_row__pair is __parent_row
    segments__miles__pair is segments__miles
    segments__day__pair is segments__day
} extend {
  primary_key: id
  dimension: segments is {
    miles is segments__miles__pair,
    seg_day is segments__day__pair.day
  }
  measure:
    flight_count is count()
    leg_miles is segments.miles.sum()
} extend {
  internal rename:
    flight_count__pair is flight_count
  measure:
    #(doc) "Flights"
    flight_count is flight_count__pair { where: __parent_row__pair }
}`);
   });
});

describe("rerootDeclaration (a hidden join_many, known limitation 24)", () => {
   const child = (name: string, column: string) => ({
      name,
      column,
      internalName: `${column}__pair`,
      type: "VARCHAR",
   });
   const reroot = (text: string, access: "private" | "internal") =>
      rerootDeclaration(text, {
         sourceName: "flights",
         connectionAlias: "a",
         virtualHandle: "h",
         typeName: "flights__served",
         columns: [{ name: "id", type: "BIGINT" }],
         pair: {
            join: "segments",
            parentRowColumn: { name: "__parent_row", type: "BOOLEAN" },
            parentRowField: "__parent_row__pair",
            childColumns: [child("tag", "segments__tag")],
            parentMeasures: [],
            childStoredDimensions: [],
            access,
            block: {
               lane: 0,
               laneColumn: { name: "__pair_lane", type: "INTEGER" },
               laneField: "__pair_lane__pair",
               others: [
                  {
                     join: "crew",
                     childColumns: [child("crew_name", "crew__crew_name")],
                  },
               ],
            },
         },
      });

   it("declares the record under the join's compiled modifier, its stored columns renamed private in the record's block", () => {
      const text = `flights is duckdb.table('flights') extend {
  primary_key: id
  # doc
  public join_many: segments on segments.flight_id = id
  join_many: crew on crew.flight_id = id
  measure: tags is count(segments.tag)
}`;
      expect(reroot(text, "private"))
         .toBe(`flights is a.virtual('h')::flights__served extend {
  internal rename:
    __parent_row__pair is __parent_row
    __pair_lane__pair is __pair_lane
    crew__crew_name__pair is crew__crew_name
  where: __pair_lane__pair = 0
} extend {
  primary_key: id
  # doc
  private dimension: segments is {
    tag is segments__tag__pair
  }
  private rename:
    segments__tag__pair is segments__tag
  dimension: crew is {
    crew_name is crew__crew_name__pair
  }
  measure: tags is count(segments.tag)
}`);
   });

   it("writes the modifier the compiled source gives, not the label in the text", () => {
      const out = reroot(
         `flights is duckdb.table('flights') extend {
  private join_many: segments on segments.flight_id = id
  join_many: crew on crew.flight_id = id
}`,
         "internal",
      );
      expect(out).toContain(
         "  internal dimension: segments is {\n    tag is segments__tag__pair\n  }\n  private rename:\n    segments__tag__pair is segments__tag\n",
      );
      expect(out).not.toContain("private internal");
      expect(out).not.toContain("private dimension");
   });
});

describe("a statement's access label stays on its own statement (P3 with P10)", () => {
   it("drops a labelled dimension statement whole, label included", () => {
      expect(
         dropDimensions(
            "extend {\n  internal dimension: big is amt > 10\n  private dimension: secret is amt * 2\n  measure: total is amt.sum()\n}",
            new Set(["big", "secret"]),
         )?.text,
      ).toBe("extend {\n  measure: total is amt.sum()\n}");
   });

   it("never takes the next statement's label with a dropped one", () => {
      const out = dropDimensions(
         "extend {\n  dimension: big is amt > 10\n  internal dimension: secret is amt * 2\n}",
         new Set(["big"]),
      );
      expect(out?.text).toBe(
         "extend {\n  internal dimension: secret is amt * 2\n}",
      );
   });

   it("reads a rename followed by a labelled statement, and undoes it", () => {
      expect(
         inverseRenameBlocks(
            "extend {\n  rename: amount is amt\n  private dimension: secret is amount * 2\n}",
            new Set(["amount"]),
         ),
      ).toBe(" extend { rename: amt is amount }");
   });

   it("still refuses an access-labelled rename (it would rename a hidden column)", () => {
      expect(
         inverseRenameBlocks(
            "extend {\n  internal rename: amount is amt\n}",
            new Set(["amount"]),
         ),
      ).toBeUndefined();
   });

   it("drops one definition of an access-labelled join_one statement, the label staying on the other", () => {
      for (const join of ["a", "b"]) {
         const out = dropJoinDefinition(
            "extend {\n  internal join_one: a with x, b with y\n  measure: n is count()\n}",
            join,
         );
         const other = join === "a" ? "b" : "a";
         expect(out).toMatch(
            new RegExp(
               `internal join_one:\\s*${other} with [xy]\\s*\\n  measure`,
            ),
         );
         expect(out).not.toMatch(new RegExp(`\\b${join} with`));
      }
   });
});
