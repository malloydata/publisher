// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { render, screen } from "@testing-library/react";
import { TileFilterTag, tileIgnoredFilterLabels } from "./TileFilterTag";

const declared = [
   { name: "CATEGORY", label: "Category" },
   { name: "REGION" },
   { name: "SINCE", label: "Ordered since" },
];

describe("tileIgnoredFilterLabels", () => {
   it("is empty when the tile reads every control", () => {
      expect(
         tileIgnoredFilterLabels(["SINCE", "CATEGORY", "REGION"], declared),
      ).toEqual([]);
   });

   it("names the controls the tile does not read, by label, in row order", () => {
      expect(tileIgnoredFilterLabels(["REGION"], declared)).toEqual([
         "Category",
         "Ordered since",
      ]);
   });

   it("is empty when discovery could not resolve the tile, since the whole row applies", () => {
      expect(tileIgnoredFilterLabels(undefined, declared)).toEqual([]);
   });

   it("names every control for a tile that reads none", () => {
      expect(tileIgnoredFilterLabels([], declared)).toEqual([
         "Category",
         "REGION",
         "Ordered since",
      ]);
   });
});

describe("TileFilterTag", () => {
   it("names one ignored filter, and says why in its tooltip", () => {
      render(<TileFilterTag ignored={["Brand"]} />);
      const chip = screen.getByTestId("tile-filter-tag");
      expect(chip.textContent).toBe("Doesn't respond to Brand");
      expect(chip.getAttribute("title")).toBe(
         "This tile's query never reads Brand, so changing it won't change this tile",
      );
   });

   it("names up to three", () => {
      render(<TileFilterTag ignored={["Brand", "Region", "Year"]} />);
      const chip = screen.getByTestId("tile-filter-tag");
      expect(chip.textContent).toBe("Doesn't respond to Brand, Region, Year");
      expect(chip.getAttribute("title")).toBe(
         "This tile's query never reads Brand, Region or Year, so changing them won't change this tile",
      );
   });

   it("counts past three, and keeps the names in the tooltip", () => {
      render(<TileFilterTag ignored={["Brand", "Region", "Year", "Store"]} />);
      const chip = screen.getByTestId("tile-filter-tag");
      expect(chip.textContent).toBe("Doesn't respond to 4 filters");
      expect(chip.getAttribute("title")).toContain(
         "Brand, Region, Year or Store",
      );
   });

   it("renders nothing when the tile reads every filter", () => {
      render(<TileFilterTag ignored={[]} />);
      expect(screen.queryByTestId("tile-filter-tag")).toBeNull();
   });
});
