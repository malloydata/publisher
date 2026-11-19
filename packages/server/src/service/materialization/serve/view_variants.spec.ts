// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { RunViewCall } from "../../query_text";
import {
   callerTextDecline,
   callTargetDecline,
   exactCallDecline,
   readStoredVariant,
   refinedCallDecline,
} from "./view_variants";

const SOURCE_ID = "flights@file:///p/flights.malloy";

function call(over: Partial<RunViewCall> = {}): RunViewCall {
   return {
      source: "flights",
      view: "by_carrier",
      refined: false,
      laterStages: 0,
      otherNames: new Set(["run"]),
      ...over,
   };
}

describe("refinedCallDecline", () => {
   it("passes a call that does not refine the view", () => {
      expect(refinedCallDecline(call())).toBeUndefined();
   });

   it("declines a refined call", () => {
      const decline = refinedCallDecline(call({ refined: true }));
      expect(decline?.code).toBe("view_shape_mismatch");
      expect(decline?.callerDetail).toContain("refines `by_carrier`");
   });
});

describe("callTargetDecline", () => {
   const plan = { sourceID: SOURCE_ID };

   it("passes a call on the source that declares the view", () => {
      expect(
         callTargetDecline(call(), plan, "flights", SOURCE_ID),
      ).toBeUndefined();
   });

   it("declines a run target that is not named", () => {
      expect(callTargetDecline(call(), plan, undefined, SOURCE_ID)?.code).toBe(
         "view_shape_mismatch",
      );
      expect(callTargetDecline(call(), plan, "", SOURCE_ID)?.code).toBe(
         "view_shape_mismatch",
      );
   });

   it("declines another source of the same name", () => {
      const decline = callTargetDecline(
         call(),
         plan,
         "flights",
         "flights@file:///p/other.malloy",
      );
      expect(decline?.callerDetail).toContain(
         "does not call `by_carrier` on the source that declares it",
      );
   });
});

describe("exactCallDecline", () => {
   const plan = { viewName: "by_carrier" };

   it("recognizes a result named for the view as an exact call", () => {
      expect(exactCallDecline(call(), plan, "by_carrier")).toBeUndefined();
   });

   it("declines a result named for anything else", () => {
      const decline = exactCallDecline(call(), plan, "flights");
      expect(decline?.code).toBe("view_shape_mismatch");
      expect(decline?.callerDetail).toContain("not a call of `by_carrier`");
   });
});

describe("callerTextDecline", () => {
   const M = "file:///p/m.malloy";
   const model = {
      declaringURL: M,
      contents: {
         fl: { type: "source", sourceID: `fl@${M}` },
         counts: { type: "source", sourceID: `counts@${M}` },
         fl_q: { type: "query", location: { url: M } },
         alias: { type: "source", sourceID: "alias@file:///p/viewer.malloy" },
      },
   };

   it("passes a caller's own names and the declaring file's other sources", () => {
      expect(
         callerTextDecline(
            new Set(["run", "mine", "counts"]),
            ["fl", "q"],
            "q",
            model,
         ),
      ).toBeUndefined();
   });

   it("declines a text that names a redeclared name, saying which", () => {
      const decline = callerTextDecline(
         new Set(["run", "join_many", "fl"]),
         ["q", undefined, "fl"],
         "q",
         model,
      );
      expect(decline?.code).toBe("view_shape_mismatch");
      expect(decline?.reason).toBe("SHAPE_MISMATCH");
      expect(decline?.callerDetail).toContain("`fl`");
   });

   it("declines a named query of the model, and a name from another file", () => {
      expect(
         callerTextDecline(new Set(["fl_q"]), ["fl"], "q", model)?.callerDetail,
      ).toContain("named query `fl_q`");
      expect(
         callerTextDecline(new Set(["alias"]), ["fl"], "q", model)
            ?.callerDetail,
      ).toContain("`alias`, declared in another file");
   });

   it("attributes its decline to the leg that asks", () => {
      for (const others of [["q"], ["fl_q"], ["alias"]]) {
         expect(
            callerTextDecline(new Set(others), ["q"], "q", model, "query")
               ?.code,
         ).toBe("query_shape_mismatch");
         expect(
            callerTextDecline(new Set(others), ["q"], "q", model)?.code,
         ).toBe("view_shape_mismatch");
      }
   });
});

describe("the view leg's decisions at their edges", () => {
   it("declines a run target whose compiled source has no sourceID", () => {
      expect(
         callTargetDecline(
            call(),
            { sourceID: SOURCE_ID },
            "flights",
            undefined,
         )?.code,
      ).toBe("view_shape_mismatch");
   });

   it("reads the plan's sourceID, never the run target's name", () => {
      expect(
         callTargetDecline(
            call({ source: "renamed" }),
            { sourceID: SOURCE_ID },
            "renamed",
            SOURCE_ID,
         ),
      ).toBeUndefined();
   });

   it("names the called view in an exact-call decline, not the plan's", () => {
      const decline = exactCallDecline(
         call({ view: "called" }),
         { viewName: "by_carrier" },
         "called",
      );
      expect(decline?.callerDetail).toBe("the query is not a call of `called`");
   });

   it("compares the result's name exactly, case and all", () => {
      expect(
         exactCallDecline(call(), { viewName: "by_carrier" }, "By_Carrier")
            ?.code,
      ).toBe("view_shape_mismatch");
   });

   it("declines a refined call with later stages as refined", () => {
      expect(
         refinedCallDecline(call({ refined: true, laterStages: 2 }))?.code,
      ).toBe("view_shape_mismatch");
      expect(refinedCallDecline(call({ laterStages: 2 }))).toBeUndefined();
   });
});

describe("readStoredVariant", () => {
   const read = (variant: object, cacheScope: string | undefined) => {
      let loaded = false;
      const run = readStoredVariant({
         queryString: "run: flights -> by_carrier",
         givens: undefined,
         buildManifest: undefined,
         variant: {
            ...variant,
            materializer: {
               loadRestrictedQuery: () => {
                  loaded = true;
                  throw new Error("not reached in this spec");
               },
            },
         } as never,
         table: {} as never,
         modelPath: "m.malloy",
         subject: "flights -> by_carrier",
         cacheScope,
      });
      return { run, loaded: () => loaded };
   };

   it("refuses a compiled variant with no graft cache scope of its own, before reading it", async () => {
      for (const scope of [undefined, ""]) {
         const { run, loaded } = read({ modelDef: {} }, scope);
         await expect(run).rejects.toThrow("no graft cache scope");
         expect(loaded()).toBe(false);
      }
   });

   it("declines a variant that did not compile as before, whatever its scope", async () => {
      await expect(
         readStoredVariant({
            queryString: "run: flights -> by_carrier",
            givens: undefined,
            buildManifest: undefined,
            variant: { error: "does not compile" },
            table: {} as never,
            modelPath: "m.malloy",
            subject: "flights -> by_carrier",
            cacheScope: undefined,
         }),
      ).rejects.toMatchObject({ code: "view_missing_field" });
   });
});
