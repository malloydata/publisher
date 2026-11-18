// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shapes the warehouse conformance variant runs, and how each renders on
 * a real warehouse.
 *
 * Every conformance shape whose model is plain Malloy over the fixture's raw
 * sources runs unchanged ({@link portableShapes}); the few that write DuckDB
 * SQL (`duckdb.sql(…)`) cannot, and their warehouse counterparts are
 * {@link WAREHOUSE_SHAPES}, over tables of the types only a warehouse has.
 *
 * Rendering differs from the DuckDB fixture's in one way: a table built in
 * the warehouse (the `database` and `materialize_database` variants) is
 * named into the run's scratch schema or dataset, since BigQuery refuses a
 * table outside a dataset and the connection's default schema is shared. A
 * `@PERSIST()` with no name gets one too (`<case>_p<n>`), so these variants
 * do not exercise the readable default name.
 *
 * A finding on a warehouse is a gap like any other, keyed by dialect:
 * `bigquery:<variant>`, `snowflake:*` and so on, on the shape or the query,
 * in shapes.ts and coverage_shapes.ts for the shared shapes and here for the
 * warehouse's own. The DuckDB keys apply too: a gap main has on every
 * engine is a gap here.
 */

import {
   type ConformanceQuery,
   type Expect,
   type Gap,
   type GapMap,
   isDatabase,
   MATERIALIZE_VARIANTS,
   type Shape,
   SHAPES,
   type Variant,
   VARIANTS,
} from "../shapes";
import { sqlTable, warehouseRawModel } from "./fixture_data";
import type { Dialect, Scratch } from "./targets";

const HEADER = "##! experimental { persistence }";

/**
 * Postgres (the local stand-in, not a CI target) returns a big integer live
 * as a string through its JSON path, and a storage= table reads it back as a
 * number: known limitation 12 (docs/materialization.md, "Integers past 2^53").
 */
const POSTGRES_BIGINT: Gap = {
   kind: "wrong",
   reason:
      "Postgres answers a big integer live as a string, which a storage= table reads back as a number (known limitation 12)",
};

/** Postgres stores the query-shaped source's sums in types the model does not declare. */
const POSTGRES_DATABASE: Gap = {
   kind: "live",
   reason:
      "on Postgres the database table's stored column types differ from the model's (served-copy-not-served), so it answers live",
};

export const WAREHOUSE_SHAPES: Shape[] = [
   {
      name: "warehouse types: big integers, NUMERIC, zoned timestamps, a mixed-case name",
      about: "A #(materialize) source over a warehouse table carrying INT64/NUMBER(38,0) past 2^53 and negative, a NUMERIC with nine decimal places, a zoned timestamp (BigQuery TIMESTAMP, Snowflake TIMESTAMP_TZ) across offsets, a quoted mixed-case column, and NULLs in each. v2 only: v1 #@ persist refuses a table extension (persist-not-materializable); the next shape is its query-shaped counterpart",
      variants: ["materialize", "materialize_database"],
      gaps: {
         "postgres:materialize": POSTGRES_BIGINT,
      },
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(typed_p)
source: typed is typed_raw extend {
  measure: big_sum is big.sum()
  measure: big_max is big.max()
  measure: dec_sum is dec.sum()
  measure: n is count()
}
`,
      },
      queries: [
         {
            name: "every column, row by row",
            malloy:
               "run: typed -> { select: id, big, dec, ts, `MixedCase`, g; order_by: id }",
         },
         {
            name: "a sum and a max past 2^53, by group",
            malloy:
               "run: typed -> { group_by: g; aggregate: big_sum, big_max; order_by: g }",
         },
         {
            name: "a NUMERIC sum",
            malloy:
               "run: typed -> { group_by: g; aggregate: dec_sum; order_by: g }",
         },
         {
            name: "grouped by a quoted mixed-case column",
            malloy:
               "run: typed -> { group_by: `MixedCase`; aggregate: n, big_sum; order_by: `MixedCase` }",
         },
         {
            name: "a zoned timestamp's day and hour",
            malloy:
               "run: typed -> { group_by: d is ts.day, h is ts.hour; aggregate: n; order_by: d, h }",
         },
         {
            name: "a zoned timestamp under a query time zone",
            malloy:
               "run: typed -> { timezone: 'Asia/Kolkata'; group_by: d is ts.day; aggregate: n; order_by: d }",
         },
         {
            name: "filtered on a big integer",
            malloy:
               "run: typed -> { where: big > 9007199254740993; aggregate: n, big_sum }",
         },
         {
            name: "NULLs grouped",
            malloy:
               "run: typed -> { group_by: no_big is big is null; aggregate: n, dec_sum; order_by: no_big }",
         },
      ],
   },
   {
      name: "warehouse types, query-shaped",
      about: "A query-shaped persisted source over the warehouse-typed table, as v1 #@ persist and v2 both build it: big-integer and NUMERIC aggregates, a zoned timestamp's minimum and a quoted mixed-case column in its grain",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(typed_q)
source: typed_q is typed_raw -> {
  group_by: g, \`MixedCase\`, d is ts.day
  aggregate:
    big_sum is big.sum()
    big_max is big.max()
    dec_sum is dec.sum()
    first_ts is ts.min()
    n is count()
} extend {
  measure: total_big is big_sum.sum()
  measure: total_dec is dec_sum.sum()
}
`,
      },
      queries: [
         {
            name: "the stored rows",
            malloy:
               "run: typed_q -> { select: *; order_by: g, `MixedCase`, d }",
         },
         {
            name: "a big-integer and a NUMERIC total, by group",
            malloy:
               "run: typed_q -> { group_by: g; aggregate: total_big, total_dec; order_by: g }",
         },
         {
            name: "by the quoted mixed-case column",
            malloy:
               "run: typed_q -> { group_by: `MixedCase`; aggregate: total_big; order_by: `MixedCase` }",
         },
         {
            name: "a stored zoned timestamp's minimum",
            malloy:
               "run: typed_q -> { group_by: g; aggregate: m is first_ts.min(); order_by: g }",
         },
      ],
      gaps: {
         "postgres:storage": POSTGRES_BIGINT,
         "postgres:materialize": POSTGRES_BIGINT,
         "postgres:database": POSTGRES_DATABASE,
         "postgres:materialize_database": POSTGRES_DATABASE,
      },
   },
   {
      name: "persist on a warehouse SQL source",
      about: "The persist annotation directly on a wh.sql(...) source, with no -> query: the warehouse port of the DuckDB fixture's 'persist on a raw SQL source'",
      files: {
         "m.malloy": `${HEADER}

@PERSIST(raw_direct)
source: raw_direct is wh.sql("""
  SELECT @COL(region), @COL(amount) FROM @TABLE(orders)
""") extend {
  measure: total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "the source itself",
            malloy:
               "run: raw_direct -> { group_by: region; aggregate: total; order_by: region }",
         },
      ],
   },
   {
      name: "warehouse types under a rollup",
      about: "A rollup over the warehouse-typed table, summing a big integer past 2^53 and a NUMERIC by a string grain",
      gaps: {
         "postgres:storage": POSTGRES_BIGINT,
         "postgres:materialize": POSTGRES_BIGINT,
      },
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: typed_r is typed_raw extend {
  @PREAGG(g)
  measure: big_sum is big.sum()
  @PREAGG(g)
  measure: dec_sum is dec.sum()
  @PREAGG(g)
  measure: n is count()
}
`,
      },
      queries: [
         {
            name: "a big-integer sum by the grain",
            malloy:
               "run: typed_r -> { group_by: g; aggregate: big_sum; order_by: g }",
            expect: "rollup",
         },
         {
            name: "a NUMERIC sum by the grain",
            malloy:
               "run: typed_r -> { group_by: g; aggregate: dec_sum; order_by: g }",
            expect: "rollup",
         },
         {
            name: "a count, rolled up past the grain",
            malloy: "run: typed_r -> { aggregate: n, big_sum }",
            expect: "rollup",
         },
      ],
   },
   {
      name: "warehouse nested columns: an array and a struct",
      about: "A #(materialize) source over a table with an ARRAY<STRING> and a STRUCT (Snowflake ARRAY and OBJECT), empty and NULL among them. A struct column sends a #(materialize) source live as a whole (docs/materialization.md, 'A column the served copy cannot carry'), so every answer is live's, from live. v2 only: v1 #@ persist refuses a table extension",
      variants: ["materialize", "materialize_database"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(nested_p)
source: nested is nested_raw extend {
  measure: n is count()
}
`,
      },
      queries: [
         {
            name: "a struct's field",
            gaps: {
               "postgres:*": {
                  kind: "error",
                  reason:
                     "the Postgres stand-in stores the struct as JSONB, which Malloy reads as no struct",
               },
            },
            malloy:
               "run: nested -> { group_by: label is attrs.label; aggregate: n; order_by: label }",
            expect: "live",
         },
         {
            name: "an array's elements",
            malloy:
               "run: nested -> { group_by: tag is tags.each; aggregate: n; order_by: tag }",
            expect: "live",
         },
         {
            name: "a count over the source",
            malloy: "run: nested -> { aggregate: n }",
            expect: "live",
         },
      ],
   },
];

/** A shape that writes DuckDB SQL cannot run on another engine. */
function isPortable(shape: Shape): boolean {
   const text = [
      ...Object.values(shape.files),
      ...shape.queries.map((q) => q.malloy),
   ].join("\n");
   return !/\bduckdb\b|\.sql\(/.test(text);
}

/** The shared conformance shapes (coverage_shapes.ts's among them) that run on a warehouse unchanged. */
export function portableShapes(): Shape[] {
   return SHAPES.filter(isPortable);
}

/**
 * Every shape the warehouse variant runs, narrowed by
 * `WAREHOUSE_CONFORMANCE_SHAPES` (a regular expression over shape names).
 */
export function warehouseShapes(): Shape[] {
   const all = [...portableShapes(), ...WAREHOUSE_SHAPES];
   const only = process.env.WAREHOUSE_CONFORMANCE_SHAPES;
   return only ? all.filter((s) => new RegExp(only, "i").test(s.name)) : all;
}

/** The variants a shape runs, narrowed by `WAREHOUSE_CONFORMANCE_VARIANTS`. */
export function warehouseVariants(shape: Shape): Variant[] {
   // An empty setting (the workflow passes "" when no input names one)
   // narrows nothing.
   const only = process.env.WAREHOUSE_CONFORMANCE_VARIANTS?.split(",")
      .map((v) => v.trim())
      .filter(Boolean);
   return (shape.variants ?? VARIANTS).filter(
      (v) => !only?.length || only.includes(v),
   );
}

/** The tier a variant builds in. */
export function tierOf(variant: Variant): "database" | "storage" {
   return isDatabase(variant) ? "database" : "storage";
}

/** A grain or name as a table-name leaf: letters, digits, single underscores. */
function leaf(text: string): string {
   return (
      text
         .toLowerCase()
         .replace(/[^a-z0-9]+/g, "_")
         .replace(/^_+|_+$/g, "") || "t"
   );
}

/**
 * Render a shape's files for one variant on one warehouse. `tag` names the
 * case, so two cases' tables in the one scratch area never meet.
 */
export function renderWarehouseFiles(
   shape: Shape,
   variant: Variant,
   scratch: Scratch,
   tag: string,
): Record<string, string> {
   const named = (name: string) => scratch.name(`${tag}_${leaf(name)}`);
   let unnamed = 0;
   const out: Record<string, string> = {
      "raw.malloy": warehouseRawModel(scratch),
   };
   for (const [file, text] of Object.entries(shape.files)) {
      out[file] = text
         .replace(/@TABLE\(([^)]*)\)/g, (_m, table: string) =>
            sqlTable(scratch.dialect, scratch, table),
         )
         // A fixture column, quoted: Snowflake's are lower-case, created quoted.
         .replace(/@COL\(([^)]*)\)/g, (_m, column: string) =>
            scratch.dialect === "bigquery" ? `\`${column}\`` : `"${column}"`,
         )
         .replace(/@PREAGG\(([^)]*)\)/g, (_m, grain: string) => {
            const name = `name="${named(`r_${grain}`)}"`;
            switch (variant) {
               case "storage":
                  return `#@ preaggregate grain="${grain}" storage="lake"`;
               case "database":
                  return `#@ preaggregate grain="${grain}" ${name}`;
               case "materialize_database":
                  return `#(rollup) grain=["${grain}"] ${name}`;
               case "materialize":
                  return `#(rollup) grain=["${grain}"] storage="lake"`;
            }
         })
         .replace(/@PVIEW\(([^)]*)\)/g, (_m, name: string) =>
            variant === "storage"
               ? `#@ persist name="${name}" storage="lake"`
               : `#@ persist name="${named(name)}"`,
         )
         .replace(/@PERSIST_SPLIT\(([^)]*)\)/g, (_m, name: string) =>
            variant === "storage"
               ? `#@ persist name="${name}"\n#@ storage="lake"`
               : `#@ persist\n#@ name="${named(name)}"`,
         )
         .replace(/@PERSIST\(([^)]*)\)/g, (_m, name: string) => {
            const qualified = named(name || `p${++unnamed}`);
            switch (variant) {
               case "storage":
                  return `#@ persist${name ? ` name="${name}"` : ""} storage="lake"`;
               case "database":
                  return `#@ persist name="${qualified}"`;
               case "materialize":
                  return `#(materialize) storage="lake"`;
               case "materialize_database":
                  return `#(materialize) name="${qualified}"`;
            }
         });
   }
   return out;
}

/**
 * A case a warehouse answers at another tier than the DuckDB fixture does,
 * by design: `expect` replaces the shared shape's for the dialects and
 * variants in `on`. Not a gap: the answer is still compared with live, and
 * a case that starts to be served fails until this is updated.
 */
interface WarehouseExpect {
   shape: string;
   /** The queries it applies to; every query of the shape when absent. */
   queries?: string[];
   on: `${Dialect}:${Variant}`[];
   expect: Expect;
   because: string;
}

/**
 * The stored-comparison rules (#1880, #1884): a `storage=` copy compares
 * strings byte-exact, so a gate or a `where:` scope over a string column is
 * kept only where the source's collation is read as binary. Snowflake's is
 * not read (a column can carry its own COLLATE), so it fails closed.
 */
const SNOWFLAKE_STRING_SCOPE =
   "#1880/#1884: Snowflake's collation is not read, so a storage= copy past a gate or where: scope over a string column fails closed and answers live";

const WAREHOUSE_EXPECT: WarehouseExpect[] = [
   {
      shape: "givens and access filters",
      on: ["snowflake:storage", "snowflake:materialize"],
      expect: "live",
      because: SNOWFLAKE_STRING_SCOPE,
   },
   {
      shape: "extensions of persisted sources",
      queries: [
         "an extension with a where:",
         "an extension of an extension",
         "a query-shaped source over an extension",
         "an extension in another file",
      ],
      on: ["snowflake:storage", "snowflake:materialize"],
      expect: "live",
      because: SNOWFLAKE_STRING_SCOPE,
   },
   {
      shape: "materialize: a refined source",
      on: ["snowflake:materialize"],
      expect: "live",
      because: SNOWFLAKE_STRING_SCOPE,
   },
   {
      shape: "extensions of persisted sources",
      queries: ["a query-shaped source over an extension"],
      on: ["postgres:storage", "postgres:materialize"],
      expect: "live",
      because:
         "#1880/#1884: the Postgres collation probe does not reach a where: read through a query stage's base, so the storage= copy fails closed and answers live",
   },
   {
      shape: "persist with a rolling window",
      queries: ["a window in the extend block"],
      on: ["snowflake:storage", "postgres:storage"],
      expect: "live",
      because:
         "#1880/#1884: the scope's `now - 3650 days` term is not one the rules read as comparing no string, so off a binary-collation warehouse the storage= copy fails closed and answers live",
   },
];

/**
 * The tier a served query must report: as the DuckDB fixture expects it,
 * unless {@link WAREHOUSE_EXPECT} names another for this warehouse, and why.
 */
export function expectedTier(
   shape: Shape,
   query: ConformanceQuery,
   variant: Variant,
   dialect: Dialect,
): { want: Expect; because?: string } {
   const own = WAREHOUSE_EXPECT.find(
      (e) =>
         e.shape === shape.name &&
         (!e.queries || e.queries.includes(query.name)) &&
         e.on.includes(`${dialect}:${variant}`),
   );
   if (own) return { want: own.expect, because: own.because };
   return {
      want:
         typeof query.expect === "string"
            ? query.expect
            : (query.expect?.[variant] ?? "served"),
   };
}

/** Each {@link WAREHOUSE_EXPECT} entry whose shape or query does not exist. */
export function staleWarehouseExpects(): string[] {
   const all = [...SHAPES, ...WAREHOUSE_SHAPES];
   return WAREHOUSE_EXPECT.flatMap((e) => {
      const shape = all.find((s) => s.name === e.shape);
      if (!shape) return [`no shape "${e.shape}"`];
      return (e.queries ?? [])
         .filter((q) => !shape.queries.some((sq) => sq.name === q))
         .map((q) => `no query "${q}" in "${e.shape}"`);
   });
}

/**
 * The gap a case is declared with: the dialect's own first (`bigquery:storage`,
 * `bigquery:*`), then the DuckDB fixture's under the shipped switch (the
 * `cutover` column, with `#(materialize)`'s `on` gaps standing for it).
 */
export function warehouseGapFor(
   shape: Shape,
   query: { gaps?: GapMap },
   variant: Variant,
   dialect: Dialect,
): Gap | undefined {
   for (const map of [query.gaps, shape.gaps]) {
      const gap =
         map?.[`${dialect}:${variant}`] ??
         map?.[`${dialect}:*`] ??
         map?.[`${variant}/cutover`] ??
         (MATERIALIZE_VARIANTS.includes(variant)
            ? map?.[`${variant}/on`]
            : undefined) ??
         map?.[variant] ??
         map?.["*"];
      if (gap) return gap;
   }
   return undefined;
}
