// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Serve's one gate walk (`entryGates`) and its one fail-closed result: every
 * way the walk cannot read the gates is `unreadable`, and each reader maps it
 * to a deny.
 */

import { afterEach, describe, expect, it, spyOn } from "bun:test";
import * as gateClassification from "../../gate_classification";
import { isDeclineOf } from "../domain/leg_decline";
import { namedEntryOf } from "./admission_lookup";
import { entryGates, entryPointGateExprs } from "./gate_reads";
import {
   liveEntryPointGates,
   proveGates,
   writtenGatesOf,
} from "./rollup_variants";

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
            baseHops: 0,
            struct: struct as never,
         },
         {
            label: "deny",
            exprs: ["false"],
            route: "authorize",
            selfContained: true,
            baseHops: 0,
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
            baseHops: 0,
            struct: struct as never,
         },
         {
            label: "lock",
            exprs: ["'a' = $ROLE"],
            route: "authorize",
            selfContained: false,
            baseHops: 0,
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

/** The walk cannot be read: it throws. */
function walkThrows(): void {
   spyOn(gateClassification, "collectEntryPointGates").mockImplementation(
      () => {
         throw new Error("unreadable base");
      },
   );
}

/** What a call throws, or undefined. */
function thrown(fn: () => unknown): unknown {
   try {
      fn();
      return undefined;
   } catch (error) {
      return error;
   }
}

describe("each reader maps an unreadable walk to a deny", () => {
   it("the rollup leg's live gates: a gate no rollup proves", () => {
      walkThrows();
      expect(liveEntryPointGates(struct, modelDef)).toEqual({
         authorize: ["false"],
         accessFilter: [],
      });
      expect(liveEntryPointGates(struct, undefined)).toEqual({
         authorize: ["false"],
         accessFilter: [],
      });
   });

   it("the rollup leg's written gates (its proof and the join branch's graft): ROW_LEVEL_GATE", () => {
      walkThrows();
      const error = thrown(() => writtenGatesOf(struct, modelDef));
      expect(isDeclineOf(error, "rollup")).toBe(true);
      expect((error as { code?: string }).code).toBe("rollup_row_level_gate");
   });

   it("the rollup leg's gate proof: ROW_LEVEL_GATE", () => {
      const source = { ...struct, sourceID: "s@m.malloy" };
      const model = { contents: { s: source }, exports: [] };
      walkThrows();
      const error = thrown(() =>
         proveGates(
            { grainDimensions: [] } as never,
            {
               gates: { authorize: ["'a' = $ROLE"], accessFilter: [] },
               sourceID: "s@m.malloy",
               struct: source,
            } as never,
            { structRef: "s", modelDef: model } as never,
            undefined,
         ),
      );
      expect((error as { code?: string }).code).toBe("rollup_row_level_gate");
   });

   it("the collation rule's named entry: no named entry, so the probe refuses", () => {
      const base = {
         type: "table",
         name: "t",
         connection: "duckdb",
         fields: [],
      };
      const model = { contents: { base }, exports: [] } as never;
      const extension = { ...base, as: "base" };
      // A readable walk names the source the inline extend extends...
      spyOn(gateClassification, "collectEntryPointGates").mockReturnValue([]);
      expect(namedEntryOf({}, extension, model)).toBe("base");
      // ...an unreadable one names none.
      walkThrows();
      expect(namedEntryOf({}, extension, model)).toBeUndefined();
   });
});
