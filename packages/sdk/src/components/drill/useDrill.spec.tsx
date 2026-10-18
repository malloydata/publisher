// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
   cleanup,
   fireEvent,
   render,
   screen,
} from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import type { DrillField, DrillTagReader } from "./resolveDrill";
import {
   useDrill,
   type UseDrillOptions,
   type UseDrillResult,
} from "./useDrill";

afterEach(cleanup);

/**
 * The hook mounted with its menu, as a surface renders it: the menu is what
 * every click opens, so a test reads and picks its items the way a reader does.
 */
function mountDrill(options: UseDrillOptions): { current: UseDrillResult } {
   const ref = {} as { current: UseDrillResult };
   function Host() {
      ref.current = useDrill(options);
      return <>{ref.current.drillMenu}</>;
   }
   render(<Host />);
   return ref;
}

const menuItems = () =>
   screen.queryAllByRole("menuitem").map((item) => item.textContent);

const pick = (name: string) =>
   act(() => {
      fireEvent.click(screen.getByRole("menuitem", { name }));
   });

const tag = (drill?: { to: string[]; given?: string }): DrillTagReader => ({
   tag: (...at: string[]) =>
      drill && at[0] === "drill" ? tag(drill) : undefined,
   text: (...at: string[]) =>
      at[0] === "given"
         ? drill?.given
         : at[0] === "to"
           ? drill?.to[0]
           : undefined,
   textArray: (...at: string[]) => (at[0] === "to" ? drill?.to : undefined),
});

const dimension = (
   name: string,
   drill?: { to: string[]; given?: string },
): DrillField => ({ name, tag: tag(drill), wasDimension: () => true });
/**
 * A grouped field as the renderer's metadata reports it: its expression as
 * code, under a root that names its source. What a rows query is built from.
 */
const reported = (name: string, code: string): DrillField => {
   const root = {
      name: "root",
      isArray: () => true,
      metadataTag: {
         text: (...at: string[]) =>
            at.join(".") === "source.name"
               ? "overview"
               : at[0] === "query_name"
                 ? "by_cat"
                 : undefined,
         array: () => undefined,
      },
      fields: [] as unknown[],
   };
   const field = {
      name,
      tag: tag(),
      parent: root,
      isBasic: () => true,
      wasDimension: () => true,
      metadataTag: {
         text: (...at: string[]) =>
            at.join(".") === "drill_expression.code" ? code : undefined,
         array: () => undefined,
      },
   };
   root.fields.push(field);
   return field;
};
const measure = (name: string): DrillField => ({
   name,
   tag: tag(),
   wasDimension: () => false,
});

describe("useDrill: every drillable click opens a menu that says what it does", () => {
   it("offers Show rows alone for a grouped value with no drill, and opens the rows only when picked", () => {
      const onRows = mock(() => {});
      const result = mountDrill({ onRows });
      const cat = reported("cat", "products.category");
      expect(result.current.drill.canDrill(cat)).toBe(true);
      expect(result.current.drill.canDrill(measure("revenue"))).toBe(false);
      act(() =>
         result.current.drill.onClick({
            field: cat,
            value: "Jeans",
            context: "overview -> by_cat",
         }),
      );
      expect(onRows).not.toHaveBeenCalled();
      expect(menuItems()).toEqual(["Show rows for Jeans"]);
      pick("Show rows for Jeans");
      expect(onRows).toHaveBeenCalledWith({
         field: "cat",
         rawValue: "Jeans",
         label: "Jeans",
         target: { source: "overview", filters: [], queryName: "by_cat" },
         code: "products.category",
         context: "overview -> by_cat",
      });
      expect(menuItems()).toEqual([]);
   });

   it("marks no dimension the result names no rows for, and opens no menu for it", () => {
      const onRows = mock(() => {});
      const result = mountDrill({ onRows });
      // No source or code in its metadata: nothing to build a query from.
      expect(result.current.drill.canDrill(dimension("cat"))).toBe(false);
      act(() =>
         result.current.drill.onClick({
            field: dimension("cat"),
            value: "Jeans",
         }),
      );
      expect(menuItems()).toEqual([]);
      expect(onRows).not.toHaveBeenCalled();
   });

   it("does nothing for a header, a measure, or an empty cell", () => {
      const onRows = mock(() => {});
      const result = mountDrill({ onRows });
      act(() => {
         result.current.drill.onClick({
            field: reported("cat", "products.category"),
            value: "Jeans",
            isHeader: true,
         });
         result.current.drill.onClick({ field: measure("revenue"), value: 3 });
         result.current.drill.onClick({ field: dimension("cat"), value: null });
      });
      expect(menuItems()).toEqual([]);
      expect(onRows).not.toHaveBeenCalled();
   });

   it("offers a to=self drill and the rows side by side, filter first", () => {
      const onRows = mock(() => {});
      const onSelf = mock(() => {});
      const result = mountDrill({
         onRows,
         onSelf,
         selfLabel: "Filter this report",
      });
      const cat = {
         ...reported("cat", "products.category"),
         tag: tag({ to: ["self"], given: "CATEGORY" }),
      } as DrillField;
      act(() =>
         result.current.drill.onClick({
            field: cat,
            value: "Jeans",
            context: "overview -> by_cat",
         }),
      );
      expect(onSelf).not.toHaveBeenCalled();
      expect(menuItems()).toEqual([
         "Filter this report to Jeans",
         "Show rows for Jeans",
      ]);
      pick("Filter this report to Jeans");
      expect(onSelf).toHaveBeenCalledWith("CATEGORY", "Jeans");
      expect(onRows).not.toHaveBeenCalled();
   });

   it("lists every report a drill names, self first, and opens the one picked", () => {
      const onSelf = mock(() => {});
      const onNavigate = mock(() => {});
      const result = mountDrill({ onSelf, onNavigate });
      act(() =>
         result.current.drill.onClick({
            field: dimension("cat", {
               to: [
                  "category-detail",
                  "self",
                  "brand_review",
                  "category-detail",
               ],
               given: "CATEGORY",
            }),
            value: "Jeans",
         }),
      );
      expect(menuItems()).toEqual([
         "Filter this view to Jeans",
         "Open Category detail for Jeans",
         "Open Brand review for Jeans",
      ]);
      pick("Open Brand review for Jeans");
      expect(onNavigate).toHaveBeenCalledWith(
         {
            path: "brand_review",
            dashboard: "brand_review",
            givens: { CATEGORY: "Jeans" },
         },
         undefined,
      );
      expect(onSelf).not.toHaveBeenCalled();
   });

   it("still says what one tag action does when rows are not on offer", () => {
      const onSelf = mock(() => {});
      const result = mountDrill({ onSelf });
      expect(result.current.drill.canDrill(dimension("cat"))).toBe(false);
      act(() =>
         result.current.drill.onClick({
            field: dimension("cat", { to: ["self"], given: "CATEGORY" }),
            value: "Jeans",
         }),
      );
      expect(onSelf).not.toHaveBeenCalled();
      expect(menuItems()).toEqual(["Filter this view to Jeans"]);
      pick("Filter this view to Jeans");
      expect(onSelf).toHaveBeenCalledWith("CATEGORY", "Jeans");
   });

   it("moves focus into the menu, and Escape closes it without acting", async () => {
      const onRows = mock(() => {});
      const result = mountDrill({ onRows });
      act(() =>
         result.current.drill.onClick({
            field: reported("cat", "products.category"),
            value: "Jeans",
         }),
      );
      await act(async () => {
         await new Promise((resolve) => setTimeout(resolve, 0));
      });
      const first = screen.getByRole("menuitem", {
         name: "Show rows for Jeans",
      });
      expect(document.activeElement).toBe(first);
      act(() => {
         fireEvent.keyDown(first, { key: "Escape" });
      });
      await act(async () => {
         await new Promise((resolve) => setTimeout(resolve, 300));
      });
      expect(menuItems()).toEqual([]);
      expect(onRows).not.toHaveBeenCalled();
   });

   it("names every value of the row the rows are filtered on, and holds nothing but actions", () => {
      const onRows = mock(() => {});
      const onSelf = mock(() => {});
      const result = mountDrill({
         onRows,
         onSelf,
         selfLabel: "Filter this report",
      });
      // A two-dimension row: the click is on `region`, the row also has `month`.
      const root = {
         name: "root",
         isArray: () => true,
         metadataTag: {
            text: (...at: string[]) =>
               at.join(".") === "source.name" ? "orders" : undefined,
            array: () => undefined,
         },
         fields: [] as unknown[],
      };
      const rowField = (
         name: string,
         drill?: { to: string[]; given?: string },
      ) => {
         const field = {
            name,
            tag: tag(drill),
            parent: root,
            isBasic: () => true,
            wasDimension: () => true,
            drillStableExpression: { kind: "field_reference", name },
            metadataTag: {
               text: (...at: string[]) =>
                  at.join(".") === "drill_expression.code" ? name : undefined,
               array: () => undefined,
            },
         };
         root.fields.push(field);
         return field as unknown as DrillField;
      };
      const region = rowField("region", { to: ["self"], given: "REGION" });
      rowField("status");
      const clause = (name: string, value: string) => ({
         filter: {
            kind: "literal_equality" as const,
            expression: { kind: "field_reference" as const, name },
            value: { kind: "string_literal" as const, string_value: value },
         },
      });
      act(() =>
         result.current.drill.onClick({
            field: region,
            value: "West",
            drillClauses: [
               clause("region", "West"),
               clause("status", "Shipped"),
            ],
         }),
      );
      expect(menuItems()).toEqual([
         "Filter this report to West",
         "Show rows for West, Shipped",
      ]);
      // No header or divider: every child of the menu list is an action.
      const list = screen.getByRole("menu");
      expect(
         Array.from(list.children).every(
            (child) => child.getAttribute("role") === "menuitem",
         ),
      ).toBe(true);
      const rows = screen.getByRole("menuitem", {
         name: "Show rows for West, Shipped",
      });
      expect(rows.getAttribute("title")).toBe("Show rows for West, Shipped");
   });

   it("leaves out a destination this surface cannot honour", () => {
      const onSelf = mock(() => {});
      const result = mountDrill({ onSelf, canSelf: () => false });
      const field = dimension("cat", {
         to: ["self", "detail"],
         given: "CATEGORY",
      });
      expect(result.current.drill.canDrill(field)).toBe(false);
      act(() => result.current.drill.onClick({ field, value: "Jeans" }));
      expect(menuItems()).toEqual([]);
   });
});

describe("useDrill: a tile entry's drill is that tile's click actions", () => {
   const entries = {
      cat: { to: ["regions"], given: "CATEGORY" },
      brand: { to: [] },
   };

   it("replaces the model's tag for the dimension it names, on that tile only", () => {
      const onSelf = mock(() => {});
      const onNavigate = mock(() => {});
      const result = mountDrill({
         onSelf,
         onNavigate,
         selfLabel: "Filter this report",
      });
      const cat = dimension("cat", { to: ["self"], given: "CATEGORY" });
      act(() =>
         result.current
            .bindingFor("overview -> by_cat", false, entries)
            .onClick({ field: cat, value: "Jeans" }),
      );
      expect(menuItems()).toEqual(["Open Regions for Jeans"]);
      pick("Open Regions for Jeans");
      expect(onNavigate).toHaveBeenCalledTimes(1);
      expect(onSelf).not.toHaveBeenCalled();
      // The same field on a tile whose entry says nothing keeps the model's tag.
      act(() =>
         result.current
            .bindingFor("overview -> other", false)
            .onClick({ field: cat, value: "Jeans" }),
      );
      expect(menuItems()).toEqual(["Filter this report to Jeans"]);
   });

   it("keeps the model's tag on a dimension the entry does not name", () => {
      const result = mountDrill({
         onSelf: mock(() => {}),
         selfLabel: "Filter this report",
      });
      const binding = result.current.bindingFor("t", false, entries);
      const region = dimension("region", { to: ["self"] });
      expect(binding.canDrill(region)).toBe(true);
      act(() => binding.onClick({ field: region, value: "West" }));
      expect(menuItems()).toEqual(["Filter this report to West"]);
   });

   it("turns the model's tag off where the entry writes to=[]", () => {
      const result = mountDrill({ onSelf: mock(() => {}) });
      const binding = result.current.bindingFor("t", false, entries);
      expect(binding.canDrill(dimension("brand", { to: ["self"] }))).toBe(false);
   });
});
