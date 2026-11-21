import { DuckDBConnection } from "@malloydata/db-duckdb";
import { type Connection, SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import {
   gateColumnUpstreamTypesOf,
   gateReads,
   gateReadsOfQuery,
} from "./gate_reads";

/** `where: <path> > 0` as Malloy's IR holds it. */
const termOn = (...path: string[]) => ({
   e: { node: ">", kids: { left: { node: "field", path }, right: 0 } },
});

const ACCOUNTS = {
   type: "table",
   sourceID: "accounts@m",
   fields: [
      { name: "org_id", type: "number" },
      { name: "acct", type: "number" },
      { name: "amount", type: "number" },
   ],
};

// A stored query-shaped source: its stage scopes by `amount`, which its
// table (`org_id`, `acct`) does not hold. Its build applied that stage.
const stored = {
   type: "query_result",
   sourceID: "stored@m",
   query: {
      structRef: ACCOUNTS,
      pipeline: [
         {
            type: "project",
            filterList: [termOn("amount")],
            queryFields: [
               { type: "fieldref", path: ["org_id"] },
               { type: "fieldref", path: ["acct"] },
            ],
         },
      ],
   },
   fields: [
      { name: "org_id", type: "number" },
      { name: "acct", type: "number" },
   ],
};
const STORED = new Set(["stored@m"]);

describe("gateReads: the row scope", () => {
   it("reads an extension of a stored query-shaped source, its query unchanged, as that table", () => {
      // `source: q is accounts -> { group_by: tenant is <computed> }`, stored;
      // `q extend { where: tenant = … }` carries its query as it is.
      const query = {
         structRef: ACCOUNTS,
         pipeline: [
            {
               type: "reduce",
               queryFields: [
                  {
                     name: "tenant",
                     type: "string",
                     e: {
                        node: "concat",
                        kids: [{ node: "field", path: ["acct"] }],
                     },
                  },
               ],
            },
         ],
      };
      const q = {
         type: "query_result",
         sourceID: "q@m",
         query,
         fields: [{ name: "tenant", type: "string" }],
      };
      const ext = {
         ...q,
         sourceID: "ext@m",
         filterList: [
            {
               e: {
                  node: "=",
                  kids: {
                     left: { node: "field", path: ["tenant"] },
                     right: { node: "stringLiteral" },
                  },
               },
            },
         ],
      };
      const model = { contents: { q } };
      const asTable = gateReads(ext, [], model, new Set(["q@m"]));
      expect(asTable.entryPathsComplete).toBe(true);
      expect(asTable.columns).toEqual([["tenant"]]);
      // Not stored: the stage is followed back to what it computes from.
      const followed = gateReads(ext, [], model, new Set());
      expect(followed.entryPathsComplete).toBe(false);
   });

   it("judges a stage's where: when no answer reads the source's table", () => {
      const reads = gateReads(stored, []);
      expect(reads.scoped).toBe(true);
      expect(reads.columns).toEqual([["amount"]]);
   });

   it("leaves out the query a stored source's build applied", () => {
      const reads = gateReads(stored, [], undefined, STORED);
      expect(reads.scoped).toBe(false);
      expect(reads.columns).toEqual([]);
   });

   it("still judges a stored source's own where:, applied at read", () => {
      const withWhere = { ...stored, filterList: [termOn("acct")] };
      const reads = gateReads(withWhere, [], undefined, STORED);
      expect(reads.scoped).toBe(true);
      expect(reads.columns).toEqual([["acct"]]);
   });

   it("still judges a stored source's join's own where:, applied at read", () => {
      const withJoin = {
         ...stored,
         fields: [
            ...stored.fields,
            {
               ...ACCOUNTS,
               name: "u",
               join: "one",
               filterList: [termOn("acct")],
            },
         ],
      };
      const reads = gateReads(withJoin, [], undefined, STORED);
      expect(reads.scoped).toBe(true);
      expect(reads.columns.flat()).toContain("u__acct");
   });

   it("leaves out a joined stored source's query, and judges its own where:", () => {
      const root = {
         ...ACCOUNTS,
         sourceID: "root@m",
         fields: [...ACCOUNTS.fields, { ...stored, name: "s", join: "one" }],
      };
      expect(gateReads(root, []).scoped).toBe(true);
      expect(gateReads(root, [], undefined, STORED).scoped).toBe(false);
      const scopedJoin = {
         ...root,
         fields: [
            ...ACCOUNTS.fields,
            { ...stored, name: "s", join: "one", filterList: [termOn("acct")] },
         ],
      };
      expect(gateReads(scopedJoin, [], undefined, STORED).scoped).toBe(true);
   });

   it("leaves out the query of a joined source the answer's compile re-rooted, read or not", () => {
      const root = {
         ...ACCOUNTS,
         sourceID: "root@m",
         fields: [...ACCOUNTS.fields, { ...stored, name: "s", join: "one" }],
      };
      const ON_TABLES = new Set(["stored@m"]);
      // Unread and not re-rooted: its query is walked, to `amount`.
      expect(gateReads(root, []).columns).toEqual([["amount"]]);
      const reads = gateReads(root, [], undefined, new Set(), [], ON_TABLES);
      expect(reads.scoped).toBe(false);
      expect(reads.columns).toEqual([]);
      // Its own `where:`, applied on read, is still judged.
      const scopedJoin = {
         ...root,
         fields: [
            ...ACCOUNTS.fields,
            { ...stored, name: "s", join: "one", filterList: [termOn("acct")] },
         ],
      };
      const own = gateReads(
         scopedJoin,
         [],
         undefined,
         new Set(),
         [],
         ON_TABLES,
      );
      expect(own.scoped).toBe(true);
      expect(own.columns.flat()).toContain("s__acct");
      expect(own.columns.flat()).not.toContain("amount");
      // The entry point itself is not a join: re-rooted but unread, its own
      // query is still walked.
      expect(
         gateReads(stored, [], undefined, new Set(), [], ON_TABLES).columns,
      ).toEqual([["amount"]]);
   });

   it("walks the whole query of a source extending a stored one", () => {
      const extended = { ...stored, sourceID: "extended@m" };
      const reads = gateReads(extended, [], undefined, STORED);
      expect(reads.scoped).toBe(true);
      expect(reads.columns).toEqual([["amount"]]);
   });
});

describe("gateReads: equality-only", () => {
   const str = (path: string[]) => ({ node: "field", path });
   const US = { node: "stringLiteral", literal: "US" };
   const ORDERS = {
      type: "table",
      sourceID: "orders@m",
      fields: [
         { name: "region", type: "string" },
         { name: "amount", type: "number" },
         // `dimension: big is amount > 100`
         {
            name: "big",
            type: "boolean",
            e: {
               node: ">",
               kids: {
                  left: str(["amount"]),
                  right: { node: "numberLiteral" },
               },
            },
         },
         // `dimension: late is region > 'M'`
         {
            name: "late",
            type: "boolean",
            e: { node: ">", kids: { left: str(["region"]), right: US } },
         },
      ],
   };
   const scopedBy = (...terms: unknown[]) => ({
      ...ORDERS,
      filterList: terms.map((e) => ({ e })),
   });
   const regionIsUS = { node: "=", kids: { left: str(["region"]), right: US } };

   it("holds beside an ordering that compares no string, written or read through a field", () => {
      const ordering = {
         node: ">",
         kids: { left: str(["amount"]), right: { node: "numberLiteral" } },
      };
      expect(gateReads(scopedBy(regionIsUS, ordering), []).equalityOnly).toBe(
         true,
      );
      expect(
         gateReads(scopedBy(regionIsUS, str(["big"])), []).equalityOnly,
      ).toBe(true);
   });

   it("does not hold for an ordering over a string, written or read through a field", () => {
      const ordering = {
         node: ">",
         kids: { left: str(["region"]), right: US },
      };
      expect(gateReads(scopedBy(ordering), []).equalityOnly).toBe(false);
      expect(gateReads(scopedBy(str(["late"])), []).equalityOnly).toBe(false);
   });
});

describe("gateReads: compares no string", () => {
   const field = (path: string[]) => ({ node: "field", path });
   const M = { node: "stringLiteral", literal: "M" };
   const ORDERS = {
      type: "table",
      sourceID: "orders@m",
      fields: [
         { name: "region", type: "string" },
         { name: "amount", type: "number" },
         // `dimension: big is amount > 100`
         {
            name: "big",
            type: "boolean",
            e: {
               node: ">",
               kids: {
                  left: field(["amount"]),
                  right: { node: "numberLiteral" },
               },
            },
         },
         // `dimension: late is region > 'M'`
         {
            name: "late",
            type: "boolean",
            e: { node: ">", kids: { left: field(["region"]), right: M } },
         },
         // `dimension: is_m is region = 'M'`
         {
            name: "is_m",
            type: "boolean",
            e: { node: "=", kids: { left: field(["region"]), right: M } },
         },
         // `dimension: late_n is pick 1 when region > 'M' else 0`
         {
            name: "late_n",
            type: "number",
            e: {
               node: "pick",
               kids: {
                  pickWhen: [
                     { node: ">", kids: { left: field(["region"]), right: M } },
                  ],
                  pickThen: [{ node: "numberLiteral" }],
                  pickElse: { node: "numberLiteral" },
               },
            },
         },
      ],
   };
   const MODEL = {
      givens: { "flag@m": { name: "FLAG", type: { type: "boolean" } } },
   };

   it("holds for a gate over a boolean computed from no string", () => {
      expect(gateReads(ORDERS, ["big = $FLAG"], MODEL).comparesNoString).toBe(
         true,
      );
   });

   it("holds for a where: that tests a string column for null, and only that", () => {
      const scoped = (e: unknown) =>
         gateReads({ ...ORDERS, filterList: [{ e }] }, [], MODEL);
      for (const node of ["is-null", "is-not-null"]) {
         const reads = scoped({ node, e: field(["region"]) });
         expect(reads.scoped, node).toBe(true);
         expect(reads.comparesNoString, node).toBe(true);
         expect(reads.columns, node).toEqual([["region"]]);
      }
      const empty = { node: "stringLiteral", literal: "" };
      for (const [what, e] of [
         [
            "an equality with ''",
            { node: "=", kids: { left: field(["region"]), right: empty } },
         ],
         [
            "a null test of a function of the column",
            {
               node: "is-null",
               e: {
                  node: "function_call",
                  name: "nullif",
                  kids: { args: [field(["region"]), M] },
               },
            },
         ],
         // `late is region > 'M'`: the null test reads `late`, which orders `region`.
         [
            "a null test of a dimension computed from a string",
            { node: "is-null", e: field(["late"]) },
         ],
      ] as const) {
         expect(scoped(e).comparesNoString, what).toBe(false);
      }
   });

   it("does not hold for a where: on a field read through a join whose on: compares strings", () => {
      const joined = (on: unknown) => ({
         ...ORDERS,
         fields: [
            ...ORDERS.fields,
            { ...ORDERS, name: "o", join: "one", onExpression: on },
         ],
      });
      const byRegion = {
         node: "=",
         kids: { left: field(["region"]), right: field(["o", "region"]) },
      };
      const byAmount = {
         node: "=",
         kids: { left: field(["amount"]), right: field(["o", "amount"]) },
      };
      for (const [what, e] of [
         ["a null test", { node: "is-null", e: field(["o", "amount"]) }],
         [
            "a number compared",
            {
               node: ">",
               kids: {
                  left: field(["o", "amount"]),
                  right: { node: "numberLiteral" },
               },
            },
         ],
      ] as const) {
         const where = (on: unknown) =>
            gateReads({ ...joined(on), filterList: [{ e }] }, [], MODEL);
         expect(where(byRegion).comparesNoString, what).toBe(false);
         expect(where(byAmount).comparesNoString, what).toBe(true);
      }
      // A row filter on a joined field: whether the row is joined follows
      // the on:, as for a where:.
      const gate = (on: unknown) =>
         gateReads(joined(on), ["o.big = $FLAG"], MODEL).comparesNoString;
      expect(gate(byRegion)).toBe(false);
      expect(gate(byAmount)).toBe(true);
   });

   it("does not hold for a gate over a dimension computed from a string", () => {
      for (const name of ["late", "is_m", "late_n"]) {
         expect(
            gateReads(ORDERS, [`${name} = $FLAG`], MODEL).comparesNoString,
            name,
         ).toBe(false);
      }
   });

   it("lists the columns a computed field reads as entry paths, through a join too", () => {
      expect(gateReads(ORDERS, ["late = $FLAG"], MODEL).entryPaths).toEqual([
         ["late"],
         ["region"],
      ]);
      const joined = {
         type: "table",
         sourceID: "j@m",
         fields: [{ ...ORDERS, name: "o", join: "one" }],
      };
      const reads = gateReads(joined, ["o.is_m = $FLAG"], MODEL);
      expect(reads.entryPaths).toEqual([
         ["o", "is_m"],
         ["o", "region"],
      ]);
      expect(reads.entryPathsComplete).toBe(true);
   });

   it("leaves the entry paths incomplete for a field whose expression names nothing it reads", () => {
      // A composite's field, resolved per member at compile.
      const composite = {
         ...ORDERS,
         fields: [
            ...ORDERS.fields,
            { name: "c_late", type: "boolean", e: { node: "compositeField" } },
         ],
      };
      const reads = gateReads(composite, ["c_late = $FLAG"], MODEL);
      expect(reads.entryPathsComplete).toBe(false);
      expect(reads.comparesNoString).toBe(false);
   });

   it("leaves the entry paths incomplete for a composite's string field too", () => {
      // Which member a query resolves to depends on the whole query, so
      // the field's own path names no one column.
      const composite = {
         ...ORDERS,
         fields: [
            ...ORDERS.fields,
            { name: "c_reg", type: "string", e: { node: "compositeField" } },
         ],
      };
      const reads = gateReads(
         { ...composite, filterList: [{ e: field(["c_reg"]) }] },
         [],
      );
      expect(reads.entryPathsComplete).toBe(false);
      expect(reads.comparesNoString).toBe(false);
   });

   it("does not hold for a where: over a dimension computed from a string", () => {
      const scoped = { ...ORDERS, filterList: [{ e: field(["is_m"]) }] };
      expect(gateReads(scoped, []).comparesNoString).toBe(false);
   });

   it("does not hold for a computed stage output of a query-shaped source", () => {
      const q = {
         type: "query_result",
         sourceID: "q@m",
         query: {
            structRef: ORDERS,
            pipeline: [
               {
                  type: "reduce",
                  queryFields: [
                     {
                        name: "late",
                        type: "boolean",
                        e: {
                           node: ">",
                           kids: { left: field(["region"]), right: M },
                        },
                     },
                  ],
               },
            ],
         },
         fields: [{ name: "late", type: "boolean" }],
      };
      const reads = gateReads(q, ["late = $FLAG"], MODEL);
      expect(reads.comparesNoString).toBe(false);
      // Its reads have no path from the entry point.
      expect(reads.entryPathsComplete).toBe(false);
   });
});

describe("gateReads: a stage output of a stored source is a stored column", () => {
   const field = (path: string[]) => ({ node: "field", path });
   const RAW = {
      type: "table",
      sourceID: "raw@m",
      fields: [{ name: "category", type: "string" }],
   };
   // `source: gated is raw -> { group_by: category, tenant is concat(category, '-x') }`
   const GATED = {
      type: "query_result",
      sourceID: "gated@m",
      query: {
         structRef: RAW,
         pipeline: [
            {
               type: "reduce",
               queryFields: [
                  { type: "fieldref", path: ["category"] },
                  {
                     name: "tenant",
                     type: "string",
                     e: {
                        node: "function_call",
                        name: "concat",
                        kids: {
                           args: [
                              field(["category"]),
                              { node: "stringLiteral", literal: "-x" },
                           ],
                        },
                     },
                  },
               ],
            },
         ],
      },
      fields: [
         { name: "category", type: "string" },
         { name: "tenant", type: "string" },
      ],
   };
   const MODEL = {
      givens: { "groups@m": { name: "GROUPS", type: { type: "string" } } },
   };

   it("compares only the stored column when the source's own table is read", () => {
      const reads = gateReads(
         GATED,
         ["tenant in $GROUPS"],
         MODEL,
         new Set(["gated@m"]),
      );
      // The read compares `tenant IN (...)`: equality, over a string.
      expect(reads.equalityOnly).toBe(true);
      expect(reads.comparesNoString).toBe(false);
      expect(reads.unresolved).toBe(false);
      expect(reads.entryPaths).toEqual([["tenant"]]);
      expect(reads.entryPathsComplete).toBe(true);
      expect(reads.columns).toEqual([["tenant"]]);
   });

   it("follows the stage back, failing closed, when the stage is computed at read", () => {
      // Read through another table (or none): the stage runs when read.
      const reads = gateReads(GATED, ["tenant in $GROUPS"], MODEL, new Set());
      expect(reads.equalityOnly).toBe(false);
      expect(reads.entryPathsComplete).toBe(false);
      expect(reads.columns).toEqual([["tenant", "category"]]);
   });
});

describe("R-10: what a stored gate column copies", () => {
   const SCORES = {
      type: "table",
      sourceID: "scores@m",
      connection: "sf",
      tablePath: "analytics.scores",
      fields: [
         { name: "id", type: "number" },
         { name: "score", type: "number" },
         { name: "amount", type: "number" },
         {
            name: "doubled",
            type: "number",
            e: { node: "field", path: ["score"] },
         },
      ],
   };
   const upstream = [
      {
         connectionName: "sf",
         table: "analytics.scores",
         dialect: "snowflake",
         columns: [
            { name: "ID", nativeType: "NUMBER(38,0)" },
            { name: "SCORE", nativeType: "FLOAT" },
            { name: "AMOUNT", nativeType: "NUMBER(12,2)" },
         ],
      },
   ];
   const schema = [
      { name: "id", type: "BIGINT" },
      { name: "score", type: "DOUBLE" },
      { name: "amount", type: "DOUBLE" },
      { name: "doubled", type: "DOUBLE" },
   ];
   it("reads a plain column's upstream type, its name matched case-blind", () => {
      const r = gateReads(
         { ...SCORES, filterList: [termOn("score")] },
         [],
         {},
         new Set(),
      );
      expect(r.columnOrigins).toEqual([
         { connectionName: "sf", table: "analytics.scores", column: "score" },
      ]);
      expect(gateColumnUpstreamTypesOf(r, [schema], [upstream])).toEqual([
         [{ dialect: "snowflake", nativeType: "FLOAT" }],
      ]);
   });

   it("reads an exact number stored as a DOUBLE as the exact number it is", () => {
      const r = gateReads(
         { ...SCORES, filterList: [termOn("amount")] },
         [],
         {},
         new Set(),
      );
      expect(gateColumnUpstreamTypesOf(r, [schema], [upstream])).toEqual([
         [{ dialect: "snowflake", nativeType: "NUMBER(12,2)" }],
      ]);
   });

   it("reads no upstream type for a computed field's column beside it, nor without a reading", () => {
      const r = gateReads(
         { ...SCORES, filterList: [termOn("doubled")] },
         [],
         {},
         new Set(),
      );
      // `doubled` is computed from `score`: the gate compares `score`.
      expect(r.columnOrigins).toEqual([
         { connectionName: "sf", table: "analytics.scores", column: "score" },
      ]);
      expect(gateColumnUpstreamTypesOf(r, [schema], [undefined])).toEqual([
         [undefined],
      ]);
      expect(
         gateColumnUpstreamTypesOf(
            r,
            [schema],
            [[{ ...upstream[0], table: "analytics.other" }]],
         ),
      ).toEqual([[undefined]]);
   });

   it("reads no origin for a stage output of a stored query-shaped source", () => {
      const r = gateReads(stored, [], {}, STORED);
      expect(r.columnOrigins?.every((o) => o === undefined)).toBe(true);
   });
});

/**
 * Time arithmetic makes a time or a number of its operands alone, so a scope
 * or gate built of `now`, time literals, `± N <unit>` offsets, truncations
 * and extractions compares no string, while one whose operand reads a string
 * (a cast, a `pick`, a string given, a function over a string column) still
 * does. Compiled by Malloy, so the walk reads the IR the server reads.
 *
 * Only a `where:` is read from its IR. A gate is read from its text, which
 * names `now` and a unit as fields it cannot find, so a gate written with
 * time arithmetic still fails closed.
 */
describe("gateReads: time arithmetic", () => {
   const db = new DuckDBConnection("duckdb", ":memory:");
   afterAll(async () => {
      await db.close();
   });
   const TABLE = `duckdb.sql("""SELECT 1 AS id, TIMESTAMP '2021-01-01' AS created_at, DATE '2021-01-01' AS d, 'x' AS region""")`;

   /** The reads of `run: s -> …` where `s` is scoped by `where`, or gated by `gate`. */
   const readsOf = async (scope: { where?: string; gate?: string }) => {
      const model = `##! experimental { givens access_modifiers }
given: S :: string
given: N :: number
${scope.gate ? `#(access_filter) ${scope.gate}\n` : ""}source: s is ${TABLE} extend {
  dimension: cut is pick @2020-01-01 00:00 when region = 'a' else @2021-01-01 00:00
  ${scope.where ? `where: ${scope.where}` : ""}
}`;
      const q = new SingleConnectionRuntime({
         connection: db as unknown as Connection,
      })
         .loadModel(model)
         .loadQuery("run: s -> { group_by: id }");
      const reads = await gateReadsOfQuery(q as never);
      expect(reads, model).toBeDefined();
      return reads!;
   };

   const NOT_STRING = [
      "created_at >= now - 3650 days",
      "d >= @2020-01-01 + 1 month",
      "created_at >= @2020-01-01 00:00 + 1 month",
      "created_at.month > @2020-01",
      "created_at >= now.day",
      "created_at.day = now.day",
      "day(now) > 0",
      "created_at < now",
      "created_at >= now - $N days",
      "created_at > @2020-01-01 10:00:00[America/New_York]",
   ];
   for (const where of NOT_STRING) {
      it(`compares no string: ${where}`, async () => {
         const reads = await readsOf({ where });
         expect(reads.unresolved).toBe(false);
         expect(reads.comparesNoString).toBe(true);
      });
   }

   const READS_STRING = [
      "created_at > region::timestamp",
      "created_at > (region::timestamp + 1 day)",
      "created_at.month > (region::timestamp).month",
      "day(region::timestamp) > 3",
      "created_at > cut",
      "(pick @2020-01-01 00:00 when region = 'a' else @2021-01-01 00:00) < created_at",
      "created_at > $S::timestamp",
      "created_at >= now - $S::number days",
      "created_at >= now - length(region) days",
   ];
   for (const where of READS_STRING) {
      it(`still reads a string: ${where}`, async () => {
         const reads = await readsOf({ where });
         expect(reads.comparesNoString).toBe(false);
         expect(reads.equalityOnly).toBe(false);
      });
   }

   // A gate is read from its text, not its IR: one that reads a string given
   // through a cast still compares a string.
   it("still reads a string through a gate over a string given cast to a timestamp", async () => {
      const reads = await readsOf({ gate: "created_at > $S::timestamp" });
      expect(reads.comparesNoString).toBe(false);
   });
});
