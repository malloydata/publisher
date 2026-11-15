// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { RunViewCall } from "../../query_text";
import {
   callTargetDecline,
   exactCallDecline,
   refinedCallDecline,
} from "./view_variants";

const SOURCE_ID = "flights@file:///p/flights.malloy";

function call(over: Partial<RunViewCall> = {}): RunViewCall {
   return {
      source: "flights",
      view: "by_carrier",
      refined: false,
      laterStages: 0,
      ...over,
   };
}

describe("refinedCallDecline", () => {
   it("passes a call that does not refine the view", () => {
      expect(refinedCallDecline(call())).toBeUndefined();
   });

   it("declines a refined call", () => {
      const decline = refinedCallDecline(call({ refined: true }));
      expect(decline?.code).toBe("view_missing_field");
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
         "view_missing_field",
      );
      expect(callTargetDecline(call(), plan, "", SOURCE_ID)?.code).toBe(
         "view_missing_field",
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
      expect(decline?.code).toBe("view_missing_field");
      expect(decline?.callerDetail).toContain("not a call of `by_carrier`");
   });
});
