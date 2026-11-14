// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The rollup leg's plans are memoised per model, and since 6f
 * they depend on `PUBLISHER_MATERIALIZE` (`readsV2`: `#(preaggregate)` is read
 * only with it on) as well as on the v1 cutover (`readsV1`). A memo keyed on
 * the model alone kept the plans read under whichever switch asked first, so
 * a model queried with the materialize switch off and then on never routed a
 * `#(preaggregate)` rollup: the gated-sources golden recorded both of its
 * gated rollups as live under the switch on, built and never read.
 */

import { describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import { FEATURE_SHAPES } from "../materialization_goldens/feature_shapes";
import { runCase } from "../materialization_goldens/package_case";

describe("the rollup leg's plans follow the switches they read", () => {
   it("routes a gated #(preaggregate) rollup under the switch on after a query under the switch off", async () => {
      const shape = FEATURE_SHAPES.find(
         (s) => s.name === "gated sources, gated switch on",
      )!;
      const rollupQueries = shape.queries.filter((q) =>
         q.name.endsWith("over a rollup"),
      );
      expect(rollupQueries.map((q) => q.name)).toEqual([
         "a row filter over a rollup",
         "a lock admitting over a rollup",
      ]);
      const goldens = await runCase({
         stem: "rollup-plan-memo-switches",
         title: "rollup plan memo",
         files: { ...DATA_FILES, ...shape.files },
         entry: "m.malloy",
         queries: rollupQueries,
         env: shape.env,
      });
      const served = JSON.parse(goldens["serve.json"]!) as {
         queries: {
            query: string;
            switch: string;
            servedBy: string;
            equalToLive?: boolean;
         }[];
      };
      const on = served.queries.filter((q) => q.switch === "on");
      expect(on.map((q) => [q.query, q.servedBy, q.equalToLive])).toEqual([
         ["a row filter over a rollup", "rollup", true],
         ["a lock admitting over a rollup", "rollup", true],
      ]);
   }, 200_000);
});
