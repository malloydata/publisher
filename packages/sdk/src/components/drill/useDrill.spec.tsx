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
      expect(screen.getByText("Jeans →")).toBeTruthy();
      expect(menuItems()).toEqual(["Show rows"]);
      pick("Show rows");
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
      expect(menuItems()).toEqual(["Filter this report", "Show rows"]);
      pick("Filter this report");
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
         "Filter this view",
         "Open Category detail",
         "Open Brand review",
      ]);
      pick("Open Brand review");
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
      expect(menuItems()).toEqual(["Filter this view"]);
      pick("Filter this view");
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
      const first = screen.getByRole("menuitem", { name: "Show rows" });
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
