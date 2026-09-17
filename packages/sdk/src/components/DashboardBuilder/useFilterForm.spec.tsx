// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { CatalogField } from "./catalog";
import { controlsOf, type BuilderControl, type MappingRow } from "./controls";
import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";
import { openDocument } from "./testing/fixtures";
import { useFilterForm } from "./useFilterForm";

const SOURCE = `##! experimental.givens
## artifact { title="T" tiles=["a -> by_cat", "a -> by_brand", "orders -> by_region"] }
import { orders } from "../m.malloy"

given: CATEGORY :: filter<string> is f''

source: a is orders extend {
  view: by_cat is by_category + { where: cat ~ $CATEGORY }
  view: by_brand is by_brand_view
}`;

const FIELDS: CatalogField[] = [
   { name: "cat", kind: "dimension", type: "string_type" },
   { name: "brand", kind: "dimension", type: "string_type" },
   { name: "amount", kind: "dimension", type: "number_type" },
   { name: "created_at", kind: "dimension", type: "date_type" },
];
const fieldsFor = (tile: DashboardTile) =>
   tile.source === "a" ? FIELDS : undefined;

const form = async (
   document: DashboardDocument,
   control?: BuilderControl,
   available: BuilderControl[] = [],
) => {
   const onApply = mock(
      (_given: string, _rows: MappingRow[], _declare?: LocalGiven) => {},
   );
   const view = renderHook(() =>
      useFilterForm({
         open: true,
         document,
         control,
         available,
         fieldsFor,
         onApply,
      }),
   );
   return { view, onApply };
};

describe("useFilterForm: a new control", () => {
   it("starts on every tile that can take a binding, and names itself after its field", async () => {
      const { view } = await form(await openDocument(SOURCE));
      const { current } = view.result;
      expect(current.editing).toBe(false);
      expect(current.rows.map((row) => row.include)).toEqual([
         true,
         true,
         false,
      ]);
      expect(current.bindableCount).toBe(2);
      expect(current.canApply).toBe(false);

      act(() => view.result.current.pickField("brand"));
      expect(view.result.current.newName).toBe("BRAND");
      expect(view.result.current.canApply).toBe(true);
   });

   it("follows the field's type to the kind of control, and the comparison it needs", async () => {
      const { view } = await form(await openDocument(SOURCE));
      // A number range is still a filter expression, compared with `~`.
      act(() => view.result.current.pickField("amount"));
      expect(view.result.current.kind).toBe("number");
      expect(view.result.current.valueTyped).toBe(false);
      // A date is a value, and a value needs a comparison.
      act(() => view.result.current.pickField("created_at"));
      expect(view.result.current.kind).toBe("date");
      expect(view.result.current.valueTyped).toBe(true);
      expect(view.result.current.commonOp).toBe(">=");
   });

   it("holds Apply while a ticked tile cannot take the field, and says which", async () => {
      const { view } = await form(await openDocument(SOURCE));
      act(() => view.result.current.pickField("nope"));
      expect(view.result.current.canApply).toBe(false);
      expect(view.result.current.commonProblem).toContain("Not a field of");
      act(() => view.result.current.setAll(false));
      act(() => view.result.current.setRow(1, { include: true }));
      expect(view.result.current.included).toBe(1);
   });

   it("declares the control it applies, suggesting over the ticked tile's source", async () => {
      const { view, onApply } = await form(await openDocument(SOURCE));
      act(() => view.result.current.pickField("brand"));
      act(() => view.result.current.setLabel("Brand name"));
      act(() => view.result.current.apply());
      expect(onApply).toHaveBeenCalledTimes(1);
      const [given, rows, declared] = onApply.mock.calls[0];
      expect(given).toBe("BRAND");
      expect(rows.filter((row) => row.include).map((row) => row.field)).toEqual(
         ["brand", "brand"],
      );
      expect(declared?.label).toBe("Brand name");
      expect(declared?.suggest?.source).toBe("orders");
   });
});

describe("useFilterForm: an existing control", () => {
   it("opens on what the control binds now, and unticking a row unbinds it", async () => {
      const document = await openDocument(SOURCE);
      const [category] = controlsOf(document);
      const { view, onApply } = await form(document, category);
      expect(view.result.current.editing).toBe(true);
      expect(view.result.current.rows[0]).toMatchObject({
         include: true,
         field: "cat",
      });
      expect(view.result.current.rows[1].include).toBe(false);
      act(() => view.result.current.setRow(0, { include: false }));
      act(() => view.result.current.apply());
      const [, rows, declared] = onApply.mock.calls[0];
      expect(rows.every((row) => !row.include)).toBe(true);
      // The declaration is left as it is when only the bindings changed.
      expect(declared).toBeUndefined();
   });

   it("opens per tile when the bound tiles disagree on the field", async () => {
      const source = SOURCE.replace(
         "view: by_brand is by_brand_view",
         "view: by_brand is by_brand_view + { where: brand ~ $CATEGORY }",
      );
      const document = await openDocument(source);
      const [category] = controlsOf(document);
      const { view } = await form(document, category);
      expect(view.result.current.perTile).toBe(true);
      expect(view.result.current.rows.map((row) => row.field)).toEqual([
         "cat",
         "brand",
         "category",
      ]);
   });
});
