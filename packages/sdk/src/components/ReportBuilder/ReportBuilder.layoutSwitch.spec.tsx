// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { ReportEvent } from "../Report/telemetry";
import { ReportBuilder } from "./ReportBuilder";
import { openDocument } from "./testing/fixtures";

/**
 * The layout switch: one editor for both layouts, and switching rewrites the
 * tag's `layout=` and nothing else (docs/reports-design.md §2, §10).
 */
const LINEAR = `## artifact { layout=linear title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] }
import "../data_app.malloy"

source: a is scoped_orders extend {
  # label="By category"
  view: by_cat is by_category

  view: by_brand is by_brand_view
}`;

const GRID = `## artifact { layout=grid title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category

  # colspan=6
  view: by_brand is by_brand_view
}`;

const button = (name: string | RegExp) =>
   screen.getByRole("button", { name, hidden: true });

const mount = async (
   source: string,
   onSave: (source: string) => Promise<void> | void,
   textHeld = false,
) =>
   render(
      <ReportBuilder
         source={source}
         document={await openDocument(source)}
         onSave={onSave}
         {...(textHeld ? { visibleSources: ["scoped_orders"] } : {})}
      />,
   );

describe("the layout switch", () => {
   it("shows Linear and Grid for a file, the file's own layout pressed", async () => {
      await mount(LINEAR, () => {});
      expect(button("Linear layout").getAttribute("aria-pressed")).toBe("true");
      expect(button("Grid layout").getAttribute("aria-pressed")).toBe("false");
   });

   it("is not offered when the host turns it off with allowLayoutChange={false}", async () => {
      render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={() => {}}
            allowLayoutChange={false}
         />,
      );
      expect(
         screen.queryByRole("button", { name: "Grid layout", hidden: true }),
      ).toBeNull();
      expect(
         screen.queryByRole("button", { name: "Linear layout", hidden: true }),
      ).toBeNull();
   });

   it("is not offered for a document held as text, whose layout is the host's record", async () => {
      await mount(LINEAR, () => {}, true);
      expect(
         screen.queryByRole("button", { name: "Grid layout", hidden: true }),
      ).toBeNull();
   });

   it("switches linear to grid by rewriting layout= and nothing else", async () => {
      const onSave = mock((_source: string) => Promise.resolve());
      await mount(LINEAR, onSave);
      fireEvent.click(button("Grid layout"));
      await waitFor(() =>
         expect(button("Grid layout").getAttribute("aria-pressed")).toBe(
            "true",
         ),
      );
      fireEvent.click(button("Save"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0]).toBe(
         LINEAR.replace("layout=linear", "layout=grid"),
      );
   });

   // KNOWN BUG, not yet fixed in source: the switch sets only the document's
   // kind, and the splice refuses a linear document whose `columns` is still
   // set ("one column"), so a grid report with `dashboard { columns=N }`
   // cannot be switched to linear. The design keeps the file's own
   // `dashboard { columns=N }` and changes only `layout=`.
   it("switches grid to linear by rewriting layout= and nothing else, keeping the file's width", async () => {
      const onSave = mock((_source: string) => Promise.resolve());
      await mount(GRID, onSave);
      fireEvent.click(button("Linear layout"));
      fireEvent.click(button("Save"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0]).toBe(
         GRID.replace("layout=grid", "layout=linear"),
      );
   });

   it("reports a switch once, and the save after it under the new layout", async () => {
      const onEvent = mock((_event: ReportEvent) => {});
      render(
         <ReportBuilder
            source={LINEAR}
            document={await openDocument(LINEAR)}
            onSave={() => Promise.resolve()}
            modelPath="store/front.malloy"
            onEvent={onEvent}
         />,
      );
      fireEvent.click(button("Grid layout"));
      // The layout already pressed is not a switch.
      fireEvent.click(button("Grid layout"));
      fireEvent.click(button("Save"));
      await waitFor(() => expect(onEvent).toHaveBeenCalledTimes(2));
      expect(onEvent.mock.calls[0][0]).toEqual({
         type: "report.layout_changed",
         path: "store/front.malloy",
         layout: "grid",
         from: "linear",
         to: "grid",
      });
      expect(onEvent.mock.calls[1][0]).toMatchObject({
         type: "report.saved",
         path: "store/front.malloy",
         layout: "grid",
      });
   });
});
