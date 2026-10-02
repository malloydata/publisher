// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
   within,
} from "@testing-library/react";
import { afterEach, describe, expect, it } from "bun:test";
import { MAX_COLUMNS, nudgedSpan } from "../Dashboard/DashboardGrid";
import { SettingsPopover, type PageSettings } from "./SettingsPopover";

afterEach(cleanup);

function open(settings: PageSettings, onCommit = (_: PageSettings) => {}) {
   const anchor = document.body.appendChild(document.createElement("button"));
   render(
      <SettingsPopover
         anchor={anchor}
         settings={settings}
         inUse={new Set()}
         onClose={() => {}}
         onCommit={onCommit}
      />,
   );
}

const select = () => screen.getByLabelText("Grid width");
const options = async () => {
   fireEvent.mouseDown(select());
   const list = await screen.findByRole("listbox");
   return within(list)
      .getAllByRole("option")
      .map((o) => o.textContent);
};

describe("Grid width", () => {
   it("shows 2 for an unset file, with no separate Default item", async () => {
      open({ imports: [] });
      expect(select().textContent).toBe("2");
      const shown = await options();
      expect(shown.filter((t) => t === "2")).toHaveLength(1);
      expect(shown.some((t) => /default/i.test(t ?? ""))).toBe(false);
      expect(shown.at(-1)).toBe(String(MAX_COLUMNS));
   });

   it("still lists a width the file wrote that is not a usual one", async () => {
      open({ imports: [], columns: 10 });
      expect(select().textContent).toBe("10");
      expect(await options()).toContain("10");
   });

   it("writes the picked width", async () => {
      let next: PageSettings | undefined;
      open({ imports: [] }, (s) => (next = s));
      fireEvent.mouseDown(select());
      const list = await screen.findByRole("listbox");
      fireEvent.click(within(list).getByText("12"));
      fireEvent.keyDown(list, { key: "Escape" });
      await waitFor(() => expect(next?.columns).toBe(12));
   });
});

describe("nudgedSpan", () => {
   it("stays within the grid and never passes the builder maximum", () => {
      expect(nudgedSpan(1, -1, 12)).toBe(1);
      expect(nudgedSpan(12, 1, 12)).toBe(12);
      expect(nudgedSpan(5, 1, 12)).toBe(6);
      expect(nudgedSpan(MAX_COLUMNS, 1, 36)).toBe(MAX_COLUMNS);
   });
});
