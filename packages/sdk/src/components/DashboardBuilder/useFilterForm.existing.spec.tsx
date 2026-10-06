// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { controlsOf } from "./controls";
import { openDocument } from "./testing/fixtures";
import { useFilterForm } from "./useFilterForm";

const SOURCE = `##! experimental.givens
## artifact { title="T" tiles=["a -> by_cat"] }
import { orders } from "../m.malloy"

given: CATEGORY :: filter<string> is f''

source: a is orders extend {
  view: by_cat is by_category + { where: nope ~ $CATEGORY }
}`;

describe("useFilterForm: opening an existing control that no longer resolves", () => {
   it("shows its problem on open, without waiting for an edit", async () => {
      const document = await openDocument(SOURCE);
      const [category] = controlsOf(document);
      const view = renderHook(() =>
         useFilterForm({
            open: true,
            document,
            control: category,
            available: [],
            fieldsFor: () => [
               { name: "cat", kind: "dimension", type: "string_type" },
            ],
            onApply: mock(() => {}),
         }),
      );
      expect(view.result.current.rowProblems.some((p) => p !== undefined)).toBe(
         true,
      );
      expect(view.result.current.canApply).toBe(false);
   });
});
