// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { DrillField, DrillTagReader } from "./resolveDrill";
import { useDrill } from "./useDrill";

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
const measure = (name: string): DrillField => ({
   name,
   tag: tag(),
   wasDimension: () => false,
});

describe("useDrill: the rows behind a value", () => {
   it("opens the rows for a grouped value that has no drill, and marks it clickable", () => {
      const onRows = mock(() => {});
      const { result } = renderHook(() => useDrill({ onRows }));
      expect(result.current.drill.canDrill(dimension("cat"))).toBe(true);
      expect(result.current.drill.canDrill(measure("revenue"))).toBe(false);
      act(() =>
         result.current.drill.onClick({
            field: dimension("cat"),
            value: "Jeans",
            context: "overview -> by_cat",
         }),
      );
      expect(onRows).toHaveBeenCalledWith({
         field: "cat",
         rawValue: "Jeans",
         label: "Jeans",
         context: "overview -> by_cat",
      });
   });

   it("does nothing for a header, a measure, or an empty cell", () => {
      const onRows = mock(() => {});
      const { result } = renderHook(() => useDrill({ onRows }));
      act(() => {
         result.current.drill.onClick({
            field: dimension("cat"),
            value: "Jeans",
            isHeader: true,
         });
         result.current.drill.onClick({ field: measure("revenue"), value: 3 });
         result.current.drill.onClick({ field: dimension("cat"), value: null });
      });
      expect(onRows).not.toHaveBeenCalled();
   });

   it("leaves a drilled value to its drill: one destination still navigates at once", () => {
      const onRows = mock(() => {});
      const onSelf = mock(() => {});
      const { result } = renderHook(() => useDrill({ onRows, onSelf }));
      act(() =>
         result.current.drill.onClick({
            field: dimension("cat", { to: ["self"], given: "CATEGORY" }),
            value: "Jeans",
            context: "overview -> by_cat",
         }),
      );
      expect(onSelf).toHaveBeenCalledWith("CATEGORY", "Jeans");
      expect(onRows).not.toHaveBeenCalled();
   });

   it("still navigates straight away when rows are not on offer", () => {
      const onSelf = mock(() => {});
      const { result } = renderHook(() => useDrill({ onSelf }));
      expect(result.current.drill.canDrill(dimension("cat"))).toBe(false);
      act(() =>
         result.current.drill.onClick({
            field: dimension("cat", { to: ["self"], given: "CATEGORY" }),
            value: "Jeans",
         }),
      );
      expect(onSelf).toHaveBeenCalledWith("CATEGORY", "Jeans");
   });
});
