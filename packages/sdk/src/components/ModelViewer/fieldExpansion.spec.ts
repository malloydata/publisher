// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   expandableFirst,
   hasDefinitionToShow,
   isFieldExpandable,
} from "./fieldExpansion";
import type { FieldCodeLookup } from "./FieldRow";
import type { ExplorerField } from "./schemaModel";

const dimension = (name: string, indexed = false): ExplorerField => ({
   name,
   kind: "dimension",
   indexed,
});

const lookup = (over: Partial<FieldCodeLookup> = {}): FieldCodeLookup => ({
   loading: false,
   ...over,
});

describe("hasDefinitionToShow", () => {
   it("is true for code, while loading, and on error", () => {
      expect(hasDefinitionToShow(lookup({ code: "sum(x)" }))).toBe(true);
      // Unknown yet — better a chevron that arrives late than one that vanishes
      // under the pointer.
      expect(hasDefinitionToShow(lookup({ loading: true }))).toBe(true);
      // A fact about the pane, not about the field.
      expect(hasDefinitionToShow(lookup({ error: true }))).toBe(true);
   });

   it("is false for an inherited column and for no lookup at all", () => {
      expect(hasDefinitionToShow(lookup({ inherited: true }))).toBe(false);
      expect(hasDefinitionToShow(undefined)).toBe(false);
   });
});

describe("isFieldExpandable", () => {
   it("keeps an indexed dimension expandable without a definition", () => {
      // Its sample values live in the expansion and nowhere else.
      expect(
         isFieldExpandable(
            dimension("state", true),
            lookup({ inherited: true }),
         ),
      ).toBe(true);
      expect(
         isFieldExpandable(dimension("state"), lookup({ inherited: true })),
      ).toBe(false);
   });
});

describe("expandableFirst", () => {
   const getFieldCode = (field: ExplorerField) =>
      field.name.startsWith("defined")
         ? lookup({ code: "1" })
         : lookup({ inherited: true });

   it("hoists expandable fields, each block keeping declaration order", () => {
      const items = [
         dimension("inherited_a"),
         dimension("defined_x"),
         dimension("inherited_b"),
         dimension("defined_y"),
      ];
      expect(expandableFirst(items, getFieldCode).map((f) => f.name)).toEqual([
         "defined_x",
         "defined_y",
         "inherited_a",
         "inherited_b",
      ]);
   });

   it("returns the SAME array when there is nothing to reorder", () => {
      // Identity matters: a fresh array on every render would remount the rows,
      // closing whatever the reader had expanded.
      const allDefined = [dimension("defined_x"), dimension("defined_y")];
      expect(expandableFirst(allDefined, getFieldCode)).toBe(allDefined);
      const noneDefined = [dimension("inherited_a"), dimension("inherited_b")];
      expect(expandableFirst(noneDefined, getFieldCode)).toBe(noneDefined);
   });

   it("holds declaration order while the definitions read is in flight", () => {
      // The churn this prevents: if `loading` counted as expandable, EVERY row
      // would hoist, the group would paint in declaration order anyway, and
      // then re-sort under the reader once the read landed and the inherited
      // columns dropped to the tail. Same order as the settled-empty case, but
      // reached deliberately.
      // One indexed row is what makes this bite: with `loading` counted as
      // expandable EVERY row hoists, `rest` is empty, and the identity
      // short-circuit hands back declaration order by accident. With it not
      // counted, the indexed row hoists alone — so if the read were allowed to
      // decide order, THIS is the order that would later be disturbed.
      const items = [
         dimension("inherited_a"),
         dimension("state", true),
         dimension("inherited_b"),
      ];
      const loadingCode = () => lookup({ loading: true });
      expect(expandableFirst(items, loadingCode).map((f) => f.name)).toEqual([
         "state",
         "inherited_a",
         "inherited_b",
      ]);
   });

   it("sorts once the read lands", () => {
      const items = [dimension("inherited_a"), dimension("defined_x")];
      expect(expandableFirst(items, getFieldCode).map((f) => f.name)).toEqual([
         "defined_x",
         "inherited_a",
      ]);
   });
});
