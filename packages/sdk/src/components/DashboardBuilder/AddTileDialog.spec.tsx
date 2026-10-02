// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { AddTileDialog } from "./AddTileDialog";
import { openDocument } from "./testing/fixtures";

const SOURCE = `## artifact { title="T" tiles=["a -> x"] } dashboard { columns=12 }
import { empty, stocked } from "../m.malloy"

source: a is empty extend {
  view: x is vx
}`;

const catalog = {
   sources: [
      {
         name: "empty",
         modelPath: "m.malloy",
         views: [],
         givens: [],
         fields: [],
      },
      {
         name: "stocked",
         modelPath: "m.malloy",
         views: [{ name: "totals" }],
         givens: [],
         fields: [],
      },
   ],
};

describe("AddTileDialog", () => {
   it("opens on a source that has views and lists one without as disabled with its reason", async () => {
      const document = await openDocument(SOURCE);
      render(
         <AddTileDialog
            open
            document={document}
            catalog={catalog}
            columns={12}
            onClose={() => {}}
            onAdd={() => {}}
         />,
      );
      expect(screen.getByLabelText("View totals")).toBeDefined();
      fireEvent.mouseDown(
         screen.getByRole("combobox", { name: /Source/, hidden: true }),
      );
      const menu = screen.getAllByRole("listbox", { hidden: true }).at(-1)!;
      const options = within(menu).getAllByRole("option", { hidden: true });
      const empty = options.find((o) => o.textContent?.startsWith("empty"))!;
      expect(empty.getAttribute("aria-disabled")).toBe("true");
      expect(empty.textContent).toContain("declares no views");
      const stocked = options.find((o) =>
         o.textContent?.startsWith("stocked"),
      )!;
      expect(stocked.getAttribute("aria-disabled")).not.toBe("true");
   });
});
