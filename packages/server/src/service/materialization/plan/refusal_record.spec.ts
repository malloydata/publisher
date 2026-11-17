// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one refusal record: a planner's refusal finding moves into the plan's
 * `refusedSources` once, keeps its warning's code and subject as a notice,
 * and the package's not-built warnings come from the records alone, with the
 * `persist-source-refused` alias beside `persist-not-built` only.
 */

import { describe, expect, it } from "bun:test";
import * as path from "path";
import { pathToFileURL } from "url";
import type { PackageAnnotationFinding } from "../../annotation_reader";
import type { BuildPlan } from "../domain/types";
import { persistRefusalWarnings } from "../package_warnings";
import { projectRefusals } from "./build_plan";
import { assertPersistNamesQuoted } from "./persist_annotation_validation";
import { measureRefusalKey } from "./rollup_plan";

/** The model file's URL as a package load makes it, on this platform. */
const MODEL_FILE = path.resolve("/pkg/m.malloy");
const URL = pathToFileURL(MODEL_FILE).href;
/** Its rollup companion's, as the rollup planner compiles it. */
const COMPANION = pathToFileURL(`${MODEL_FILE}.preaggregate.malloy`).href;

function finding(
   o: Partial<PackageAnnotationFinding> = {},
): PackageAnnotationFinding {
   return {
      route: "preaggregate",
      tier: "unservable",
      code: "preaggregate-not-built",
      message: '#(preaggregate) on measure "total" is not built',
      subject: "orders.total",
      model: "m.malloy",
      refused: measureRefusalKey(
         `orders@${URL}`,
         "orders",
         "total",
         "hidden_field",
      ),
      ...o,
   };
}

describe("measureRefusalKey", () => {
   it("keys a measure by its path on its base, in the base file's rollup companion", () => {
      expect(
         measureRefusalKey(`orders@${URL}`, "orders", "total", "time_zone"),
      ).toEqual({
         sourceID: `orders.total@${COMPANION}`,
         name: "orders.total",
         tier: "preaggregate",
         reason: "time_zone",
      });
   });

   it("names the companion of a Windows-style URL, with no drive needed to read it", () => {
      expect(
         measureRefusalKey(
            "orders@file:///D:/pkg/m.malloy",
            "orders",
            "total",
            "time_zone",
         ).sourceID,
      ).toBe("orders.total@file:///D:/pkg/m.malloy.preaggregate.malloy");
      expect(
         measureRefusalKey(
            "orders@file:///pkg/m.malloy",
            "orders",
            "total",
            "time_zone",
         ).sourceID,
      ).toBe("orders.total@file:///pkg/m.malloy.preaggregate.malloy");
   });

   it("names the companion of a Windows path as the planner does", () => {
      const win = (p: string) => pathToFileURL(p, { windows: true }).href;
      expect(
         measureRefusalKey(
            `orders@${win("D:\\pkg\\m.malloy")}`,
            "orders",
            "total",
            "time_zone",
         ).sourceID,
      ).toBe(`orders.total@${win("D:\\pkg\\m.malloy.preaggregate.malloy")}`);
   });

   it("keys two declarations on one measure apart, by grain and destination", () => {
      const key = (grain: string[], storage?: string) =>
         measureRefusalKey(`orders@${URL}`, "orders", "total", "time_zone", {
            grain,
            ...(storage !== undefined ? { storage } : {}),
         }).sourceID;
      expect(key(["region", "day"])).toBe(
         `orders.total[day,region]@${COMPANION}`,
      );
      expect(key(["day"], "lake")).toBe(
         `orders.total[day in lake]@${COMPANION}`,
      );
   });

   it("keys a refusal of every rollup on a base by the base alone", () => {
      expect(
         measureRefusalKey(`orders@${URL}`, "orders", undefined, "join_level")
            .name,
      ).toBe("orders");
   });
});

describe("projectRefusals", () => {
   it("moves each refusal finding into refusedSources once, and keeps the rest", () => {
      const kept = finding({
         tier: "malformed",
         code: "preaggregate-invalid",
         refused: undefined,
      });
      const refusal = finding({ rewrite: "expose it" });
      const { plan, annotationFindings, refusalNotices } = projectRefusals(
         null,
         [kept, refusal, finding()],
      );
      expect(annotationFindings).toEqual([kept]);
      const id = refusal.refused!.sourceID;
      expect(plan).toEqual({
         graphs: [],
         sources: {},
         refusedSources: {
            [id]: {
               name: "orders.total",
               sourceID: id,
               modelPath: "m.malloy",
               tier: "preaggregate",
               reason: "hidden_field",
               message: `${refusal.message}; expose it`,
            },
         },
      });
      expect(refusalNotices).toEqual({
         [id]: { code: "preaggregate-not-built", subject: "orders.total" },
      });
   });

   it("leaves a plan alone when no finding refuses anything", () => {
      const plan: BuildPlan = { graphs: [], sources: {} };
      const out = projectRefusals(plan, []);
      expect(out.plan).toBe(plan);
      expect(out.refusalNotices).toEqual({});
   });

   it("keeps a malformed finding that carries a key until it is relaxed", () => {
      const collision = finding({
         tier: "malformed",
         code: "materialization-table-collision",
      });
      const out = projectRefusals(null, [collision]);
      expect(out.plan).toBeNull();
      expect(out.annotationFindings).toEqual([collision]);
   });
});

describe("persistRefusalWarnings", () => {
   const record = (
      sourceID: string,
      o: Partial<NonNullable<BuildPlan["refusedSources"]>[string]> = {},
   ) => ({
      [sourceID]: {
         name: sourceID.split("@")[0],
         sourceID,
         modelPath: "m.malloy",
         tier: "database" as const,
         reason: "authorize" as const,
         message: "refused",
         ...o,
      },
   });

   it("names a finding's refusal by its notice, with no alias", () => {
      const id = `orders.total@${URL}.preaggregate.malloy`;
      const warnings = persistRefusalWarnings(
         {
            graphs: [],
            sources: {},
            refusedSources: record(id, { tier: "preaggregate" }),
         },
         {},
         {},
         { [id]: { code: "preaggregate-not-built", subject: "orders.total" } },
      );
      expect(warnings).toEqual([
         {
            model: "m.malloy",
            subject: "orders.total",
            message: "refused",
            code: "preaggregate-not-built",
            severity: "warn",
         },
      ]);
   });

   it("repeats a #@ persist gate refusal under the alias, and no other mechanism's", () => {
      const persist = `orders@${URL}`;
      const view = `orders__by_x__view@${URL}.persist-view.malloy`;
      const materialized = `mat@${URL}`;
      const unsupported = `lake@${URL}`;
      const codes = persistRefusalWarnings(
         {
            graphs: [],
            sources: {},
            refusedSources: {
               ...record(persist),
               ...record(view),
               ...record(materialized),
               ...record(unsupported, {
                  reason: "storage_source_unsupported",
               }),
            },
         },
         {},
         { [materialized]: {} as never },
      ).map((w) => [w.subject, w.code]);
      expect(codes).toEqual([
         ["orders", "persist-not-built"],
         ["orders", "persist-source-refused"],
         ["orders__by_x__view", "persist-view-not-built"],
         ["mat", "materialize-not-built"],
         ["lake", "persist-not-built"],
      ]);
   });
});

describe("assertPersistNamesQuoted", () => {
   it("fails with one coded line per offending annotation", () => {
      let message = "";
      try {
         assertPersistNamesQuoted(
            [
               '#@ persist name="a b"',
               "source: x is y",
               "#@ persist name=bare",
               "source: z is y",
            ].join("\n"),
            "m.malloy",
         );
      } catch (error) {
         message = (error as Error).message;
      }
      const lines = message.split("\n");
      expect(lines).toHaveLength(2);
      // In the order the file writes them.
      expect(lines[0]).toContain("plain identifier path");
      expect(lines[1]).toContain("must be quoted");
      for (const line of lines) expect(line).toEndWith(". [persist-invalid]");
   });
});
