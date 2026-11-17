import { describe, expect, it } from "bun:test";
import { gateReads } from "./gate_reads";

// A stored source scoped by `where: amount > 0`, holding only `org_id` and
// `total`: the build applied its scope, so a read of its table does not.
const term = {
   e: {
      node: ">",
      kids: { left: { node: "field", path: ["amount"] }, right: 0 },
   },
};
const stored = {
   type: "table",
   sourceID: "stored@m",
   fields: [
      { name: "org_id", type: "number" },
      { name: "total", type: "number" },
      { name: "amount", type: "number" },
   ],
   filterList: [term],
};

describe("gateReads: the row scope", () => {
   it("judges a where: term when no answer reads the source's table", () => {
      const reads = gateReads(stored, []);
      expect(reads.scoped).toBe(true);
      expect(reads.columns).toEqual([["amount"]]);
   });

   it("stops at a source whose table the answer reads", () => {
      const reads = gateReads(stored, [], undefined, new Set(["stored@m"]));
      expect(reads.scoped).toBe(false);
      expect(reads.columns).toEqual([]);
   });

   it("stops at a joined source whose table the answer reads", () => {
      const root = {
         type: "table",
         sourceID: "root@m",
         fields: [
            { name: "id", type: "number" },
            { ...stored, name: "s", join: "one" },
         ],
      };
      expect(gateReads(root, []).scoped).toBe(true);
      const reads = gateReads(root, [], undefined, new Set(["stored@m"]));
      expect(reads.scoped).toBe(false);
   });

   it("still judges the terms a source extending a stored one inherits", () => {
      const extended = { ...stored, sourceID: "extended@m" };
      const reads = gateReads(extended, [], undefined, new Set(["stored@m"]));
      expect(reads.scoped).toBe(true);
      expect(reads.columns).toEqual([["amount"]]);
   });
});
