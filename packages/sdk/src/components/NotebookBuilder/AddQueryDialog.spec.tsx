// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import type { CatalogSource } from "../DashboardBuilder/catalog";
import { AddQueryDialog } from "./AddQueryDialog";

const SOURCES: CatalogSource[] = [
   {
      name: "orders",
      modelPath: "m.malloy",
      description: "Order lines",
      views: [
         { name: "by_category", description: "Revenue", chart: "bar_chart" },
         { name: "detail" },
      ],
      givens: [],
      fields: [],
   },
   {
      name: "empty_source",
      modelPath: "m.malloy",
      views: [],
      givens: [],
      fields: [],
   },
];

const open = (sources: CatalogSource[] | null = SOURCES) => {
   const onAdd = mock((_run: unknown) => {});
   const onClose = mock(() => {});
   render(
      <AddQueryDialog
         open
         sources={sources ?? undefined}
         onClose={onClose}
         onAdd={onAdd}
      />,
   );
   return { onAdd, onClose };
};

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

beforeEach(() => cleanup());

describe("AddQueryDialog", () => {
   it("starts on the first source with no view, so Add is off until a view is picked", () => {
      open();
      expect(button("Add query").hasAttribute("disabled")).toBe(true);
      expect(button("View by_category")).toBeDefined();
      fireEvent.click(button("View detail"));
      expect(button("Add query").hasAttribute("disabled")).toBe(false);
   });

   it("shows each view's chart as a chip", () => {
      open();
      expect(screen.getByText("bar")).toBeDefined();
   });

   it("adds the source, view and trimmed caption it was given", () => {
      const { onAdd } = open();
      fireEvent.click(button("View by_category"));
      fireEvent.change(screen.getByLabelText("Query caption"), {
         target: { value: "  Revenue  " },
      });
      fireEvent.click(button("Add query"));
      expect(onAdd).toHaveBeenCalledWith({
         source: "orders",
         view: "by_category",
         caption: "Revenue",
      });
   });

   it("leaves the caption out when it is blank", () => {
      const { onAdd } = open();
      fireEvent.click(button("View detail"));
      fireEvent.click(button("Add query"));
      expect(onAdd).toHaveBeenCalledWith({ source: "orders", view: "detail" });
   });

   it("says the chart will not follow the filters", () => {
      open();
      expect(
         screen.getByText(/This chart will not follow the filters/),
      ).toBeDefined();
   });

   it("says what is wrong when the model is loading or offers no source", () => {
      open(null);
      expect(screen.getByText(/still loading/)).toBeDefined();
      cleanup();
      open([]);
      expect(screen.getByText(/reads no source/)).toBeDefined();
   });
});
