// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it } from "bun:test";
import { ChartPicker } from "./ChartPicker";

afterEach(cleanup);

describe("ChartPicker", () => {
   it("shows the chosen chart as plain text when closed, without the menu's reason line", () => {
      const { container } = render(
         <ChartPicker
            state="none"
            view={undefined}
            viewStatus="unlisted"
            cellLabel="cell 1"
            onChange={() => {}}
         />,
      );
      const shown = container.querySelector('[role="combobox"]');
      expect(shown?.textContent).toBe("No chart (table)");
      expect(shown?.querySelector(".MuiListItemText-root")).toBeNull();
   });
});
