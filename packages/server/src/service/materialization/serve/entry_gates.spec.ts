// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Serve's one gate walk (`entryGates`) and its one fail-closed result: every
 * way the walk cannot read the gates is `unreadable`, and each reader maps it
 * to a deny.
 */

import { afterEach, describe, expect, it, spyOn } from "bun:test";
import * as gateClassification from "../../gate_classification";
import { entryGates, entryPointGateExprs } from "./gate_reads";

const struct = { type: "table", name: "t", fields: [] };
const modelDef = { contents: {}, exports: [] };

afterEach(() => {
   spyOn(gateClassification, "collectEntryPointGates").mockRestore();
});

describe("entryGates", () => {
   it("is unreadable with no struct or no model to walk", () => {
      expect(entryGates(undefined, modelDef)).toBe("unreadable");
      expect(entryGates(struct, undefined)).toBe("unreadable");
   });

   it("is unreadable when the walk throws", () => {
      spyOn(gateClassification, "collectEntryPointGates").mockImplementation(
         () => {
            throw new Error("unreadable base");
         },
      );
      expect(entryGates(struct, modelDef)).toBe("unreadable");
      expect(entryPointGateExprs(struct, modelDef)).toBeUndefined();
   });

   it("is unreadable on the walk's own deny (an entry with nowhere to graft)", () => {
      spyOn(gateClassification, "collectEntryPointGates").mockReturnValue([
         {
            label: "x",
            exprs: ["region = $S"],
            route: "access_filter",
            selfContained: false,
            struct: struct as never,
         },
         {
            label: "deny",
            exprs: ["false"],
            route: "authorize",
            selfContained: true,
         },
      ]);
      expect(entryGates(struct, modelDef)).toBe("unreadable");
      expect(entryPointGateExprs(struct, modelDef)).toBeUndefined();
   });

   it("lists the entries, filtered by route for the gate texts", () => {
      spyOn(gateClassification, "collectEntryPointGates").mockReturnValue([
         {
            label: "rows",
            exprs: ["region = $S"],
            route: "access_filter",
            selfContained: false,
            struct: struct as never,
         },
         {
            label: "lock",
            exprs: ["'a' = $ROLE"],
            route: "authorize",
            selfContained: false,
            struct: struct as never,
         },
      ]);
      expect(entryGates(struct, modelDef)).toHaveLength(2);
      expect(entryPointGateExprs(struct, modelDef)).toEqual([
         "region = $S",
         "'a' = $ROLE",
      ]);
      expect(entryPointGateExprs(struct, modelDef, "access_filter")).toEqual([
         "region = $S",
      ]);
   });

   it("is an empty list for an ungated source", () => {
      expect(entryGates(struct, modelDef)).toEqual([]);
   });
});
