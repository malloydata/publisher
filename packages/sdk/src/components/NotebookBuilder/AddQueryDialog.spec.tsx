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

   it("does not claim the query follows the filter controls", () => {
      open();
      expect(
         screen.getByText(/not connected to the filter controls/),
      ).toBeDefined();
      expect(screen.getByText(/reads a given as \$NAME/)).toBeDefined();
   });

   it("flags a caption the writer would refuse, before Add", () => {
      open();
      fireEvent.click(button("View detail"));
      fireEvent.change(screen.getByLabelText("Query caption"), {
         target: { value: "# authorize" },
      });
      expect(screen.getByText(/access-control tag/)).toBeDefined();
      expect(button("Add query").hasAttribute("disabled")).toBe(true);
      fireEvent.change(screen.getByLabelText("Query caption"), {
         target: { value: "Fine" },
      });
      expect(button("Add query").hasAttribute("disabled")).toBe(false);
   });

   it("names an import that could not be read instead of saying the notebook reads no source", () => {
      render(
         <AddQueryDialog
            open
            sources={[]}
            failedImports={["shop.malloy"]}
            onClose={() => {}}
            onAdd={() => {}}
         />,
      );
      expect(screen.getByText(/Could not read shop\.malloy/)).toBeDefined();
      expect(screen.queryByText(/reads no source/)).toBeNull();
   });

   it("says the sources could not be read, not that they are loading, after a failed read", () => {
      cleanup();
      render(
         <AddQueryDialog
            open
            sources={undefined}
            failed
            onClose={() => {}}
            onAdd={() => {}}
         />,
      );
      expect(screen.getByText(/could not be read/)).toBeDefined();
      expect(screen.queryByText(/still loading/)).toBeNull();
   });

   it("says what is wrong when the model is loading or offers no source", () => {
      open(null);
      expect(screen.getByText(/still loading/)).toBeDefined();
      cleanup();
      open([]);
      expect(screen.getByText(/reads no source/)).toBeDefined();
   });
});
