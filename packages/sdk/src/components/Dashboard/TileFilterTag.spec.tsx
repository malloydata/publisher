// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { render, screen } from "@testing-library/react";
import { TileFilterTag, tileFilterLabels } from "./TileFilterTag";

const declared = [
   { name: "CATEGORY", label: "Category" },
   { name: "REGION" },
   { name: "SINCE", label: "Ordered since" },
];

describe("tileFilterLabels", () => {
   it("names the controls the tile references, by label", () => {
      expect(tileFilterLabels(["CATEGORY", "REGION"], declared)).toEqual([
         "Category",
         "REGION",
      ]);
   });

   it("lists every control when discovery could not resolve the tile", () => {
      expect(tileFilterLabels(undefined, declared)).toEqual([
         "Category",
         "REGION",
         "Ordered since",
      ]);
   });

   it("is empty for a tile no control reaches", () => {
      expect(tileFilterLabels([], declared)).toEqual([]);
   });
});

describe("TileFilterTag", () => {
   it("shows the labels", () => {
      render(<TileFilterTag labels={["Category", "Region"]} />);
      expect(screen.getByTestId("tile-filter-tag").textContent).toContain(
         "Category, Region",
      );
   });

   it("renders nothing without filters", () => {
      render(<TileFilterTag labels={[]} />);
      expect(screen.queryByTestId("tile-filter-tag")).toBeNull();
   });
});
