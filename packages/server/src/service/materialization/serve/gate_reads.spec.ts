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
