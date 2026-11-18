// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of the collation rule as pure unit tables, with no
 * warehouse: the Postgres collation judge (`postgresJudged`, read through the
 * exported probes over a fake connection), the DuckDB probes, and the
 * allow-lists that let an answer skip the probe (`NON_STRING_OPERATORS`,
 * `EQUALITY_NODES`, read through `gateReads` and `equalityOnlyExpr`). The
 * live Postgres cases stay in gate_collation.spec.ts as integration tests.
 *
 * Every row is today's answer. The rows marked CURRENT are questionable
 * ones, pinned so a change shows as a diff.
 */

import { describe, expect, it } from "bun:test";
import type { Connection } from "@malloydata/malloy";
import {
   duckdbCollation,
   duckdbTableCollation,
   postgresCollation,
   postgresTableCollation,
   unfinalizedPostgresSQL,
} from "./gate_collation";
import { equalityOnlyExpr, gateReads } from "./gate_reads";

/** A connection answering every query with `rows`, recording the SQL. */
function connection(answer: (sql: string) => Record<string, unknown>[]): {
   conn: Connection;
   sql: string[];
} {
   const sql: string[] = [];
   return {
      sql,
      conn: {
         runSQL: async (text: string) => {
            sql.push(text);
            return { rows: answer(text) };
         },
      } as unknown as Connection,
   };
}

const answering = (rows: Record<string, unknown>[]) =>
   connection(() => rows).conn;

describe("the Postgres collation judge (postgresJudged)", () => {
   // name, det (collisdeterministic), cc (collcollate), prov (collprovider),
   // dbc (datcollate), dbp (datlocprovider; absent before Postgres 15).
   const JUDGED: [
      string,
      Record<string, unknown> | undefined,
      "binary" | "deterministic" | "unknown",
   ][] = [
      ["no row (an unknown collation OID)", undefined, "unknown"],
      [
         "the database default under libc C",
         { name: "default", dbp: "c", dbc: "C" },
         "binary",
      ],
      [
         "the database default under libc POSIX",
         { name: "default", dbp: "c", dbc: "POSIX" },
         "binary",
      ],
      [
         "the database default under C, before datlocprovider",
         { name: "default", dbc: "C" },
         "binary",
      ],
      [
         "the database default under ICU, whatever datcollate says",
         { name: "default", dbp: "i", dbc: "C" },
         "deterministic",
      ],
      [
         "the database default under en_US.UTF-8",
         { name: "default", dbp: "c", dbc: "en_US.UTF-8" },
         "deterministic",
      ],
      [
         "the database default under lower-case c (names compare exactly)",
         { name: "default", dbp: "c", dbc: "c" },
         "deterministic",
      ],
      // CURRENT: the default row is judged without reading `det`.
      [
         "the database default marked nondeterministic",
         { name: "default", det: false, dbp: "c", dbc: "C" },
         "binary",
      ],
      [
         "a named libc C collation",
         { name: "C", det: true, prov: "c", cc: "C" },
         "binary",
      ],
      [
         "a named libc POSIX collation",
         { name: "POSIX", det: true, prov: "c", cc: "POSIX" },
         "binary",
      ],
      [
         "a named ICU collation over C",
         { name: "und-x-icu", det: true, prov: "i", cc: "C" },
         "deterministic",
      ],
      [
         "a named libc en_US collation",
         { name: "en_US", det: true, prov: "c", cc: "en_US.utf8" },
         "deterministic",
      ],
      [
         "a named collation of the database's provider (d)",
         { name: "x", det: true, prov: "d", cc: "C" },
         "deterministic",
      ],
      [
         "a nondeterministic ICU collation",
         { name: "ci", det: false, prov: "i", cc: "und" },
         "unknown",
      ],
      [
         "a determinism that is not the boolean true",
         { name: "x", det: "true", prov: "c", cc: "C" },
         "unknown",
      ],
      [
         "a named row with no determinism",
         { name: "x", prov: "c", cc: "C" },
         "unknown",
      ],
   ];

   for (const [label, row, judged] of JUDGED) {
      it(`${label}: ${judged}`, async () => {
         expect(
            await postgresCollation(answering(row ? [row] : []), "SELECT 1"),
         ).toBe(judged);
      });
   }

   it("judges a table's collatable columns together: unknown beats deterministic beats binary", async () => {
      const C = { name: "C", det: true, prov: "c", cc: "C" };
      const EN = { name: "en", det: true, prov: "c", cc: "en_US" };
      const CI = { name: "ci", det: false, prov: "i", cc: "und" };
      const table = (rows: Record<string, unknown>[]) =>
         postgresTableCollation(answering(rows), "public.t");
      expect(await table([C, C])).toBe("binary");
      expect(await table([C, EN])).toBe("deterministic");
      expect(await table([C, EN, CI])).toBe("unknown");
      // CURRENT: no collatable column at all (or a table the catalog does
      // not list) reads binary: nothing compares under a collation.
      expect(await table([])).toBe("binary");
   });

   it("probes a column through the source's SQL, and a table by its path, quoted as a literal", async () => {
      const probe = connection(() => []);
      await postgresCollation(probe.conn, "SELECT x AS probe_value FROM t");
      expect(probe.sql[0]).toContain(
         "pg_collation_for((SELECT q.probe_value FROM (SELECT x AS probe_value FROM t) q LIMIT 1))",
      );
      const table = connection(() => []);
      await postgresTableCollation(table.conn, `public."o'brien"`);
      expect(table.sql[0]).toContain(`'public."o''brien"'::regclass`);
   });

   it("takes the JSON final stage off Malloy's Postgres SQL, and leaves other SQL as it is", () => {
      expect(
         unfinalizedPostgresSQL(
            "WITH __stage0 AS (SELECT 1) SELECT row_to_json(finalStage) as row FROM __stage0 AS finalStage",
         ),
      ).toBe("WITH __stage0 AS (SELECT 1) SELECT * FROM __stage0");
      expect(unfinalizedPostgresSQL("SELECT 1")).toBe("SELECT 1");
   });
});

describe("the DuckDB collation probes", () => {
   /** A DuckDB instance: its default collation, and its definitions. */
   const instance = (opts: {
      defaultCollation?: string;
      collatedDefinitions?: number;
      tables?: Record<string, unknown>[];
   }) =>
      connection((sql) =>
         /default_collation/.test(sql)
            ? [{ c: opts.defaultCollation ?? "" }]
            : /duckdb_tables\(\) WHERE table_name/.test(sql)
              ? (opts.tables ?? [])
              : opts.collatedDefinitions === undefined
                ? []
                : [{ n: opts.collatedDefinitions }],
      );

   it("reads a source's SQL as binary only with no COLLATE anywhere and no default collation", async () => {
      expect(
         await duckdbCollation(
            instance({ collatedDefinitions: 0 }).conn,
            "SELECT 1",
         ),
      ).toBe("binary");
      expect(
         await duckdbCollation(
            instance({ collatedDefinitions: 1 }).conn,
            "SELECT 1",
         ),
      ).toBe("unknown");
      expect(
         await duckdbCollation(
            instance({ defaultCollation: "nocase", collatedDefinitions: 0 })
               .conn,
            "SELECT 1",
         ),
      ).toBe("unknown");
      // An unreadable definition count fails closed.
      expect(await duckdbCollation(instance({}).conn, "SELECT 1")).toBe(
         "unknown",
      );
      // A COLLATE in the SQL itself decides before anything is read.
      const probe = instance({ collatedDefinitions: 0 });
      expect(await duckdbCollation(probe.conn, "SELECT a COLLATE nocase")).toBe(
         "unknown",
      );
      expect(probe.sql).toEqual([]);
   });

   it("reads a table as binary only when the catalog lists it and no definition of that name names COLLATE", async () => {
      const table = (opts: Parameters<typeof instance>[0], path = "t") =>
         duckdbTableCollation(instance(opts).conn, path);
      expect(
         await table({ tables: [{ sql: "CREATE TABLE t(a VARCHAR);" }] }),
      ).toBe("binary");
      expect(await table({ tables: [] })).toBe("unknown");
      expect(
         await table({
            tables: [{ sql: "CREATE TABLE t(a VARCHAR COLLATE nocase);" }],
         }),
      ).toBe("unknown");
      expect(await table({ tables: [{ sql: null }] })).toBe("unknown");
      // Every same-named table in the instance counts, in any schema.
      expect(
         await table({
            tables: [
               { sql: "CREATE TABLE t(a VARCHAR);" },
               { sql: "CREATE TABLE t(a VARCHAR COLLATE nocase);" },
            ],
         }),
      ).toBe("unknown");
      expect(
         await table({
            defaultCollation: "nocase",
            tables: [{ sql: "CREATE TABLE t(a VARCHAR);" }],
         }),
      ).toBe("unknown");
   });

   it("looks a table up by its last name, unquoted, with a quote in it escaped", async () => {
      for (const [path, name] of [
         ["t", "t"],
         ["main.t", "t"],
         ["lake.main.t", "t"],
         [`"Main"."My""T"`, `My"T`],
         ["`s`.`T`", "T"],
         ["o'brien", "o''brien"],
      ]) {
         const probe = instance({ tables: [] });
         await duckdbTableCollation(probe.conn, path);
         expect(probe.sql[1]).toBe(
            `SELECT sql FROM duckdb_tables() WHERE table_name = '${name}'`,
         );
      }
   });
});

// ---------------------------------------------------------------------------
// The allow-lists that let an answer skip the probe.
// ---------------------------------------------------------------------------

const field = (...path: string[]) => ({ node: "field", path });
const num = { node: "numberLiteral", literal: "1" };
const str = { node: "stringLiteral", literal: "M" };
const op = (node: string, left: unknown, right: unknown = num) => ({
   node,
   kids: { left, right },
});

describe("equalityOnlyExpr (EQUALITY_NODES)", () => {
   // Every comparison and logical node Malloy's filter IR uses, with the
   // answer: true only for the equality allow-list; any other node, known
   // or not, is false.
   const NODES: Record<string, boolean> = {
      "=": true,
      "!=": true,
      in: true,
      not: true,
      and: true,
      or: true,
      "()": true,
      "is-null": true,
      "is-not-null": true,
      "<": false,
      "<=": false,
      ">": false,
      ">=": false,
      like: false,
      "!like": false,
      regexpMatch: false,
      "+": false,
      "-": false,
      "*": false,
      "/": false,
      "%": false,
      cast: false,
      pick: false,
      case: false,
      coalesce: false,
      function_call: false,
      filterCondition: false,
      dateLiteral: false,
      timestampLiteral: false,
      a_node_this_does_not_know: false,
   };
   for (const [node, expected] of Object.entries(NODES)) {
      it(`${node}: ${expected}`, () => {
         expect(equalityOnlyExpr(op(node, field("a")))).toBe(expected);
      });
   }

   it("holds for the leaves it allows, nested, and fails past its depth bound", () => {
      expect(
         equalityOnlyExpr({
            node: "and",
            kids: {
               left: op("=", field("a"), { node: "given", id: "g" }),
               right: op("is-null", field("b"), { node: "null" }),
            },
         }),
      ).toBe(true);
      let deep: unknown = op("=", field("a"));
      for (let i = 0; i < 70; i++) deep = { node: "()", e: deep };
      expect(equalityOnlyExpr(deep)).toBe(false);
   });
});

/** A source the adversarial cases read: one field of each kind. */
const ORDERS = {
   type: "table",
   sourceID: "orders@m",
   fields: [
      { name: "region", type: "string" },
      { name: "amount", type: "number" },
      { name: "flag", type: "boolean" },
      { name: "at", type: "timestamp" },
      // `dimension: late is region > 'M'`: a boolean over a string.
      { name: "late", type: "boolean", e: op(">", field("region"), str) },
      // `dimension: big is amount > 1`: a boolean over a number.
      { name: "big", type: "boolean", e: op(">", field("amount")) },
      // `dimension: r is region`: a computed rename of a string.
      { name: "r", type: "string", e: field("region") },
      // `rename: area is region`.
      { name: "region", as: "area", type: "string" },
      {
         name: "u",
         type: "table",
         join: "one",
         fields: [
            { name: "name", type: "string" },
            { name: "id", type: "number" },
         ],
      },
   ],
};

const GIVENS = {
   givens: {
      "given/FLAG": { name: "FLAG", type: { type: "boolean" } },
      "given/N": { name: "N", type: { type: "number" } },
      "given/S": { name: "S", type: { type: "string" } },
   },
};

/** `gateReads` of ORDERS scoped by a `where:` term. */
const scoped = (e: unknown) =>
   gateReads({ ...ORDERS, filterList: [{ e }] }, [], GIVENS);
/** `gateReads` of ORDERS under a row filter, as gate text. */
const gated = (text: string) => gateReads(ORDERS, [text], GIVENS);

/** The two flags a probe is skipped (comparesNoString) or relaxed (equalityOnly) on. */
const flags = (r: ReturnType<typeof gateReads>) => ({
   equalityOnly: r.equalityOnly,
   comparesNoString: r.comparesNoString,
});

describe("gateReads' probe allow-lists (NON_STRING_OPERATORS, comparesNoString)", () => {
   const SCOPES: [string, unknown, ReturnType<typeof flags>][] = [
      [
         "an equality on a number",
         op("=", field("amount")),
         { equalityOnly: true, comparesNoString: true },
      ],
      [
         "an ordering on a number",
         op(">", field("amount")),
         { equalityOnly: true, comparesNoString: true },
      ],
      [
         "an equality on a string",
         op("=", field("region"), str),
         { equalityOnly: true, comparesNoString: false },
      ],
      [
         "an ordering on a string",
         op(">", field("region"), str),
         { equalityOnly: false, comparesNoString: false },
      ],
      [
         "a boolean field",
         field("flag"),
         { equalityOnly: true, comparesNoString: true },
      ],
      [
         "a boolean over a number, read through a field",
         field("big"),
         { equalityOnly: true, comparesNoString: true },
      ],
      // A boolean dimension computed over a string compares that string:
      // the walk below the field turns both flags off, so the probe runs.
      [
         "a boolean over a string, read through a field",
         field("late"),
         { equalityOnly: false, comparesNoString: false },
      ],
      [
         "a computed rename of a string",
         op("=", field("r"), str),
         { equalityOnly: true, comparesNoString: false },
      ],
      [
         "a rename of a string, by its new name",
         op("=", field("area"), str),
         { equalityOnly: true, comparesNoString: false },
      ],
      [
         "a join's string field",
         op("=", field("u", "name"), str),
         { equalityOnly: true, comparesNoString: false },
      ],
      [
         "a join's number field",
         op(">", field("u", "id")),
         { equalityOnly: true, comparesNoString: true },
      ],
      [
         "a field the source does not have",
         op("=", field("nope")),
         { equalityOnly: true, comparesNoString: false },
      ],
      [
         "a function of a number",
         op(">", {
            node: "function_call",
            name: "abs",
            kids: { args: [field("amount")] },
         }),
         { equalityOnly: false, comparesNoString: false },
      ],
      [
         "a cast of a number to a string",
         op(
            "=",
            { node: "cast", e: field("amount"), dstType: { type: "string" } },
            str,
         ),
         { equalityOnly: false, comparesNoString: false },
      ],
      [
         "a pick",
         op("=", { node: "pick", kids: { pickWhen: [], pickThen: [] } }, num),
         { equalityOnly: false, comparesNoString: false },
      ],
      [
         "a case",
         op("=", { node: "case", kids: { caseWhen: [], caseThen: [] } }, num),
         { equalityOnly: false, comparesNoString: false },
      ],
      [
         "a like pattern",
         op("like", field("region"), str),
         { equalityOnly: false, comparesNoString: false },
      ],
      [
         "a timestamp literal ordering",
         op(">", field("at"), { node: "timestampLiteral" }),
         { equalityOnly: true, comparesNoString: true },
      ],
      [
         "a number given",
         op("=", field("amount"), { node: "given", id: "given/N" }),
         { equalityOnly: true, comparesNoString: true },
      ],
      [
         "a string given",
         op("=", field("region"), { node: "given", id: "given/S" }),
         { equalityOnly: true, comparesNoString: false },
      ],
      [
         "an undeclared given",
         op("=", field("amount"), { node: "given", id: "given/X" }),
         { equalityOnly: true, comparesNoString: false },
      ],
   ];
   for (const [label, e, expected] of SCOPES) {
      it(`a where: of ${label}`, () => {
         expect(flags(scoped(e))).toEqual(expected);
      });
   }

   const GATES: [string, ReturnType<typeof flags>][] = [
      ["amount = $N", { equalityOnly: true, comparesNoString: true }],
      ["region = $S", { equalityOnly: true, comparesNoString: false }],
      ["region = $N", { equalityOnly: true, comparesNoString: false }],
      ["amount = $S", { equalityOnly: true, comparesNoString: false }],
      ["u.id = $N", { equalityOnly: true, comparesNoString: true }],
      ["u.name = $S", { equalityOnly: true, comparesNoString: false }],
      // A gate on a boolean computed by ordering a string: the probe runs.
      ["late = $FLAG", { equalityOnly: false, comparesNoString: false }],
      ["big = $FLAG", { equalityOnly: true, comparesNoString: true }],
      ["r = $S", { equalityOnly: true, comparesNoString: false }],
      // A quoted name is not followed: unresolved, which turns the skip off.
      ["`amount` = $N", { equalityOnly: true, comparesNoString: false }],
      // A function call names no field; its argument is not typed.
      ["abs(amount) = $N", { equalityOnly: true, comparesNoString: true }],
   ];
   for (const [text, expected] of GATES) {
      it(`a row filter \`${text}\``, () => {
         expect(flags(gated(text))).toEqual(expected);
      });
   }
});
