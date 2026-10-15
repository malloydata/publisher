// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { DEFAULT_COLUMNS } from "../Dashboard/DashboardGrid";
import { TILE_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { layoutRules } from "./layoutRules";

describe("layoutRules", () => {
   it("lays a linear report in one column of full-width tiles that grow to fit, added between others", () => {
      expect(layoutRules("linear", 12)).toEqual({
         layout: "linear",
         columns: 1,
         widths: false,
         insertBetween: true,
         tileMaxHeight: undefined,
      });
   });

   it("lays a grid across its declared columns, with widths, added at the end", () => {
      expect(layoutRules("grid", 12)).toEqual({
         layout: "grid",
         columns: 12,
         widths: true,
         insertBetween: false,
         tileMaxHeight: TILE_MAX_HEIGHT,
      });
   });

   it("reads no layout as a grid, at the default width", () => {
      expect(layoutRules(undefined).columns).toBe(DEFAULT_COLUMNS);
      expect(layoutRules(undefined).layout).toBe("grid");
   });

   it("gives a one-column grid no widths to set", () => {
      expect(layoutRules("grid", 1).widths).toBe(false);
   });
});
