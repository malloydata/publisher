import { describe, expect, it } from "bun:test";
import { gateReads } from "./gate_reads";

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

   it("reads a composite's string field as the column each member holds", () => {
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
      expect(reads.entryPathsComplete).toBe(true);
      expect(reads.comparesNoString).toBe(false);
      expect(reads.columns).toContainEqual(["c_reg"]);
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
