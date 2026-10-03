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
import { DashboardBuilder } from "./DashboardBuilder";
import { openDocument } from "./testing/fixtures";

const NOTEBOOK = `## artifact { kind=notebook title="Review" tiles=["a -> by_cat"] }
import { scoped_orders } from "../data_app.malloy"
import { spare } from "../spare.malloy"

source: a is scoped_orders extend {
  view: by_cat is by_category
}
`;

const source = (name: string, modelPath: string) => ({
   name,
   modelPath,
   views: [{ name: "by_x" }],
   givens: [],
   fields: [],
});
const catalog = {
   sources: [
      source("scoped_orders", "data_app.malloy"),
      source("spare", "spare.malloy"),
      source("regions", "data_app.malloy"),
      source("events", "events.malloy"),
      source("hidden", "dashboards/shared.malloy"),
   ],
};

const mount = async (onSave: (source: string) => void) =>
   render(
      <DashboardBuilder
         source={NOTEBOOK}
         document={await openDocument(NOTEBOOK)}
         catalog={catalog}
         onSave={onSave}
      />,
   );

const openSettings = () =>
   fireEvent.click(
      screen.getByRole("button", { name: "Settings", hidden: true }),
   );
const closeSettings = () =>
   fireEvent.keyDown(screen.getByLabelText("Show as"), { key: "Escape" });
const save = () =>
   fireEvent.click(
      screen.getByRole("button", { name: "Save changes", hidden: true }),
   );

afterEach(cleanup);

describe("the settings list a document's sources", () => {
   it("shows what is imported and only lets an unread source go", async () => {
      await mount(() => {});
      openSettings();
      const sources = screen.getByLabelText("Sources");
      expect(within(sources).getByText("scoped_orders")).toBeDefined();
      expect(screen.queryByLabelText("Remove source scoped_orders")).toBeNull();
      expect(screen.getByLabelText("Remove source spare")).toBeDefined();
   });

   it("writes a source taken off the picker as one import line, and adds nothing else", async () => {
      let written: string | undefined;
      await mount((text) => {
         written = text;
      });
      openSettings();
      fireEvent.click(screen.getByLabelText("Remove source spare"));
      closeSettings();
      save();
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toBe(
         NOTEBOOK.replace('import { spare } from "../spare.malloy"\n', ""),
      );
   });

   it("adds a source from the package into the file's import for its model, or a new one", async () => {
      let written: string | undefined;
      await mount((text) => {
         written = text;
      });
      openSettings();
      const add = () =>
         screen.getByRole("combobox", { name: /Add a source/, hidden: true });
      fireEvent.mouseDown(add());
      const options = within(
         screen.getAllByRole("listbox", { hidden: true }).at(-1)!,
      ).getAllByRole("option", { hidden: true });
      // Imported sources and the shared includes in dashboards/ are not on offer.
      expect(options.map((o) => o.textContent)).toEqual([
         "regionsdata_app.malloy",
         "eventsevents.malloy",
      ]);
      fireEvent.click(options[0]);
      fireEvent.mouseDown(add());
      fireEvent.click(
         within(
            screen.getAllByRole("listbox", { hidden: true }).at(-1)!,
         ).getByRole("option", { name: /events/, hidden: true }),
      );
      closeSettings();
      save();
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         'import { scoped_orders, regions } from "../data_app.malloy"',
      );
      expect(written).toContain(
         'import { spare } from "../spare.malloy"\nimport { events } from "../events.malloy"\n',
      );
   });
});
