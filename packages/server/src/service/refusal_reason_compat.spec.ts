// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";

import {
   compatBuildPlan,
   REFUSAL_REASON_0_12_FALLBACK,
   REFUSAL_REASONS_0_12,
} from "./refusal_reason_compat";
import type { components } from "../api";
import type { BuildPlan } from "./materialization/domain/types";

type RefusedSource = components["schemas"]["RefusedSource"];

function refused(
   sourceID: string,
   reason: RefusedSource["reason"],
): RefusedSource {
   return {
      name: sourceID.split("@")[0],
      sourceID,
      tier: "storage",
      reason,
      message: `refused for ${reason}`,
   };
}

function plan(...entries: RefusedSource[]): BuildPlan {
   return {
      graphs: [],
      sources: {},
      refusedSources: Object.fromEntries(entries.map((e) => [e.sourceID, e])),
      omittedSources: [],
      omittedFiles: [],
   };
}

/** `RefusedSource.reason`'s enum in this tree's `api-doc.yaml`, read as text. */
function currentReasons(): string[] {
   const doc = fs.readFileSync(
      path.join(__dirname, "../../../../api-doc.yaml"),
      "utf8",
   );
   const schema = doc.slice(doc.indexOf("\n    RefusedSource:\n"));
   const reason = schema.slice(schema.indexOf("\n        reason:\n"));
   const list = reason.slice(reason.indexOf("[") + 1, reason.indexOf("]"));
   return list
      .split(",")
      .map((r) => r.trim())
      .filter((r) => r.length > 0);
}

describe("compatBuildPlan", () => {
   it("returns the plan itself under off, whatever its reasons", () => {
      const p = plan(refused("orders@m.malloy", "current_time"));
      expect(compatBuildPlan(p, "off")).toBe(p);
   });

   it("returns the plan itself under 0.12 when 0.12 declared every reason", () => {
      const p = plan(
         refused("orders@m.malloy", "authorize"),
         refused("items@m.malloy", "given"),
      );
      expect(compatBuildPlan(p, "0.12")).toBe(p);
   });

   it("writes a reason 0.12 did not declare as the fallback, keeping it in the message", () => {
      const p = plan(
         refused("orders@m.malloy", "current_time"),
         refused("items@m.malloy", "authorize"),
      );
      const written = compatBuildPlan(p, "0.12");
      expect(written.refusedSources!["orders@m.malloy"]).toEqual({
         ...p.refusedSources!["orders@m.malloy"],
         reason: REFUSAL_REASON_0_12_FALLBACK,
         message: "[current_time] refused for current_time",
      });
      expect(written.refusedSources!["items@m.malloy"]).toBe(
         p.refusedSources!["items@m.malloy"],
      );
   });

   it("never changes the plan the server holds, and keeps every refused source", () => {
      const p = plan(refused("orders@m.malloy", "table_name_collision"));
      const before = structuredClone(p);
      const written = compatBuildPlan(p, "0.12");
      expect(p).toEqual(before);
      expect(Object.keys(written.refusedSources!)).toEqual(
         Object.keys(p.refusedSources!),
      );
   });

   it("computes a plan's 0.12 form once", () => {
      const p = plan(refused("orders@m.malloy", "empty_storage"));
      expect(compatBuildPlan(p, "0.12")).toBe(compatBuildPlan(p, "0.12"));
   });

   it("passes no plan through", () => {
      expect(compatBuildPlan(null, "0.12")).toBeNull();
      expect(compatBuildPlan(undefined, "0.12")).toBeUndefined();
   });

   it("writes every current reason as one 0.12 declared", () => {
      const reasons = currentReasons() as RefusedSource["reason"][];
      const written = compatBuildPlan(
         plan(...reasons.map((r, i) => refused(`s${i}@m.malloy`, r))),
         "0.12",
      );
      for (const r of Object.values(written.refusedSources!)) {
         expect(REFUSAL_REASONS_0_12.has(r.reason)).toBe(true);
      }
   });

   it("falls back to a reason 0.12 declared and the current API still declares", () => {
      expect(REFUSAL_REASONS_0_12.has(REFUSAL_REASON_0_12_FALLBACK)).toBe(true);
      expect(currentReasons()).toContain(REFUSAL_REASON_0_12_FALLBACK);
      for (const r of REFUSAL_REASONS_0_12)
         expect(currentReasons()).toContain(r);
   });
});
