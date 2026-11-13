// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   SHADOW_BUDGET_MS,
   SHADOW_MAX_IN_FLIGHT,
   shadowBudgetMs,
   shadowOutcome,
   ShadowScheduler,
} from "./v1_shadow";

const field = (
   name: string,
   type: unknown,
   annotations: string[] = [],
): unknown => ({
   name,
   type,
   annotations: annotations.map((value) => ({ value })),
});

const TODAY = {
   rows: [
      { region: "east", total: 3 },
      { region: "west", total: 1.5 },
   ],
   fields: [
      field("region", { kind: "string_type" }),
      field("total", { kind: "number_type", subtype: "integer" }, [
         "# currency",
      ]),
   ],
};

describe("shadowOutcome", () => {
   it("is equal for the same rows in another order, a number's subtype and drill annotations aside", () => {
      expect(
         shadowOutcome(TODAY, {
            servedBy: "materialized",
            rows: [
               { region: "west", total: 1.5 + 1e-12 },
               { region: "east", total: 3 },
            ],
            fields: [
               field("region", { kind: "string_type" }, [
                  '#(malloy) drill_expression = region reference_id = "x"',
               ]),
               field("total", { kind: "number_type" }, ["# currency"]),
            ],
         }),
      ).toEqual({ outcome: "equal" });
   });

   it("is live when the cutover answered live, whatever the rows", () => {
      expect(shadowOutcome(TODAY, { ...TODAY, servedBy: "live" }).outcome).toBe(
         "live",
      );
      expect(
         shadowOutcome(TODAY, { ...TODAY, servedBy: undefined }).outcome,
      ).toBe("live");
   });

   it("names where rows differ, never a value", () => {
      const out = shadowOutcome(TODAY, {
         servedBy: "materialized",
         rows: [
            { region: "east", total: 4 },
            { region: "west", total: 1.5 },
         ],
         fields: TODAY.fields,
      });
      expect(out).toEqual({
         outcome: "rows_differ",
         detail: "row 0 column total",
      });
      expect(
         shadowOutcome(TODAY, {
            servedBy: "materialized",
            rows: TODAY.rows.slice(1),
            fields: TODAY.fields,
         }).detail,
      ).toBe("row counts differ: 2 against 1");
   });

   it("names a metadata difference: a dropped annotation or timeframe", () => {
      expect(
         shadowOutcome(TODAY, {
            servedBy: "rollup",
            rows: TODAY.rows,
            fields: [TODAY.fields[0], field("total", { kind: "number_type" })],
         }),
      ).toEqual({ outcome: "metadata_differ", detail: "total.annotations" });
      expect(
         shadowOutcome(
            {
               rows: [],
               fields: [field("m", { kind: "date_type", timeframe: "month" })],
            },
            {
               servedBy: "materialized",
               rows: [],
               fields: [field("m", { kind: "date_type" })],
            },
         ),
      ).toEqual({ outcome: "metadata_differ", detail: "m.type" });
   });
});

describe("ShadowScheduler", () => {
   it("compares a text once, and skips a new one past the server's cap", async () => {
      const releases: (() => void)[] = [];
      const run = () =>
         new Promise<void>((resolve) => {
            releases.push(resolve);
         });
      const a = new ShadowScheduler();
      const b = new ShadowScheduler();
      const started = [];
      for (let i = 0; i < SHADOW_MAX_IN_FLIGHT; i++) {
         started.push(a.schedule(`q${i}`, run));
      }
      expect(started.every((s) => s === "started")).toBe(true);
      expect(a.schedule("q0", run)).toBe("seen");
      // The cap is the server's: another model's new text waits its turn.
      expect(b.schedule("other", run)).toBe("skipped");
      expect(ShadowScheduler.inFlightForTests()).toBe(SHADOW_MAX_IN_FLIGHT);
      for (const release of releases.splice(0)) release();
      await new Promise((r) => setTimeout(r, 0));
      expect(ShadowScheduler.inFlightForTests()).toBe(0);
      // Skipped, so not remembered: compared on a later run.
      expect(b.schedule("other", async () => {})).toBe("started");
      await new Promise((r) => setTimeout(r, 0));
   });

   it("frees its place when a comparison fails", async () => {
      const s = new ShadowScheduler();
      expect(s.schedule("x", () => Promise.reject(new Error("boom")))).toBe(
         "started",
      );
      await new Promise((r) => setTimeout(r, 0));
      expect(ShadowScheduler.inFlightForTests()).toBe(0);
   });
});

describe("shadowBudgetMs", () => {
   it("is the shadow budget, or the query timeout when that is shorter", () => {
      for (const [timeout, want] of [
         ["0", SHADOW_BUDGET_MS],
         ["5000", 5000],
         [String(SHADOW_BUDGET_MS * 4), SHADOW_BUDGET_MS],
      ] as const) {
         const { restore } = installSettings({
            env: { PUBLISHER_QUERY_TIMEOUT_MS: timeout },
         });
         try {
            expect(shadowBudgetMs()).toBe(want);
         } finally {
            restore();
         }
      }
   });
});
