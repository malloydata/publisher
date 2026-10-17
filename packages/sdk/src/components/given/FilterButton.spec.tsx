// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A filter's popover in the builder: where the report opens the filter, for
 * any filter (the model's included), and for one the model sets, a way to the
 * model in place of settings the builder cannot change.
 */
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { Given } from "../../client";
import { FilterButton } from "./FilterButton";
import {
   FilterEditingContext,
   type FilterEditing,
   type FilterEditState,
} from "./filterEditing";

const BRAND: Given = { name: "BRAND", label: "Brand", type: "filter<string>" };
const SINCE: Given = { name: "SINCE", label: "Since", type: "date" };

function mount(
   given: Given,
   value: unknown,
   {
      state = { origin: "dashboard" },
      start,
   }: { state?: FilterEditState; start?: string } = {},
) {
   const setStartingValue = mock((_name: string, _text?: string) => {});
   const onEdit = mock((_name: string) => {});
   const editing: FilterEditing = {
      stateOf: () => state,
      unrendered: [],
      onEdit,
      onRemove: () => {},
      onAdd: () => {},
      startingValueOf: () => start,
      setStartingValue,
   };
   render(
      <FilterEditingContext.Provider value={editing}>
         <FilterButton
            given={given}
            value={value as never}
            onChange={() => {}}
         />
      </FilterEditingContext.Provider>,
   );
   fireEvent.click(
      screen.getByRole("button", { name: new RegExp(`^${given.label}: `) }),
   );
   return { setStartingValue, onEdit };
}

describe("FilterButton's starting value, in the builder", () => {
   it("makes the current value the report's default, a filter as its text", () => {
      // Not an f'…' literal: the server unwraps one without un-escaping it.
      const { setStartingValue } = mount(BRAND, "O'Brien");
      fireEvent.click(
         screen.getByRole("button", {
            name: "Make this the report's default",
         }),
      );
      expect(setStartingValue).toHaveBeenCalledWith("BRAND", "O'Brien");
   });

   it("spells a non-filter value as the query endpoint does", () => {
      const { setStartingValue } = mount(SINCE, "2024-03-01");
      fireEvent.click(
         screen.getByRole("button", {
            name: "Make this the report's default",
         }),
      );
      expect(setStartingValue).toHaveBeenCalledWith("SINCE", "2024-03-01");
   });

   it("says Report default when it is, and offers the filter's own default for one this report declares", () => {
      const { setStartingValue } = mount(BRAND, "Ford", { start: "f'Ford'" });
      expect(screen.getByText("Report default")).toBeDefined();
      expect(
         screen.queryByRole("button", {
            name: "Make this the report's default",
         }),
      ).toBeNull();
      expect(
         screen.queryByRole("button", { name: "Use the model's default" }),
      ).toBeNull();
      fireEvent.click(
         screen.getByRole("button", { name: "Use the filter's default" }),
      );
      expect(setStartingValue).toHaveBeenCalledWith("BRAND", undefined);
   });

   it("offers the model's default for a filter the model declares", () => {
      const { setStartingValue } = mount(BRAND, "Ford", {
         start: "f'Ford'",
         state: { origin: "model" },
      });
      expect(screen.getByText("Report default")).toBeDefined();
      expect(
         screen.queryByRole("button", { name: "Use the filter's default" }),
      ).toBeNull();
      fireEvent.click(
         screen.getByRole("button", { name: "Use the model's default" }),
      );
      expect(setStartingValue).toHaveBeenCalledWith("BRAND", undefined);
   });

   it("offers nothing to start from while the filter has no value", () => {
      mount(BRAND, undefined);
      expect(
         screen.queryByRole("button", {
            name: "Make this the report's default",
         }),
      ).toBeNull();
   });
});

describe("FilterButton for a filter the model sets", () => {
   it("gives the reason and Open the model, not the filter's settings", () => {
      const openModel = mock(() => {});
      mount(BRAND, "Ford", {
         state: {
            origin: "model",
            fixed: "Set in the model storefront.malloy. Edit the model to change it.",
            openModel,
         },
      });
      expect(
         screen.getByText(
            "Set in the model storefront.malloy. Edit the model to change it.",
         ),
      ).toBeDefined();
      expect(
         screen.queryByRole("button", { name: "Edit filter BRAND" }),
      ).toBeNull();
      // Where the report opens is still the report's to say.
      expect(
         screen.getByRole("button", {
            name: "Make this the report's default",
         }),
      ).toBeDefined();
      fireEvent.click(screen.getByRole("button", { name: "Open the model" }));
      expect(openModel).toHaveBeenCalledTimes(1);
   });

   it("offers no Open the model when the host cannot open one", () => {
      mount(BRAND, "Ford", {
         state: {
            origin: "model",
            fixed: "Set in a model this report imports.",
         },
      });
      expect(
         screen.queryByRole("button", { name: "Open the model" }),
      ).toBeNull();
   });
});
