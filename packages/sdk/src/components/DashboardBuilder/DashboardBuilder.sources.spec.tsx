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

const save = () =>
   fireEvent.click(screen.getByRole("button", { name: "Save", hidden: true }));

/** Add a tile on `name`'s one view through the add-tile picker. */
const addTileOn = (name: string) => {
   fireEvent.click(
      screen.getByRole("button", { name: "Add tile", hidden: true }),
   );
   fireEvent.mouseDown(
      screen.getByRole("combobox", { name: /Source/, hidden: true }),
   );
   fireEvent.click(
      within(
         screen.getAllByRole("listbox", { hidden: true }).at(-1)!,
      ).getByRole("option", { name: new RegExp(`^${name}`), hidden: true }),
   );
   fireEvent.click(screen.getByLabelText("View by_x"));
   fireEvent.click(
      screen.getByRole("button", { name: "Add tile", hidden: false }),
   );
};

const written = async (pick: string) => {
   let text: string | undefined;
   await mount((source) => {
      text = source;
   });
   addTileOn(pick);
   save();
   await waitFor(() => expect(text).toBeDefined());
   return text as string;
};

afterEach(cleanup);

describe("adding a tile brings its source in", () => {
   it("adds no import for a source the file already imports", async () => {
      const text = await written("spare");
      expect(text).toContain(
         'import { scoped_orders } from "../data_app.malloy"\nimport { spare } from "../spare.malloy"\n\n',
      );
      expect(text.match(/^import /gm)).toHaveLength(2);
   });

   it("joins a source to its model's existing import", async () => {
      const text = await written("regions");
      expect(text).toContain(
         'import { scoped_orders, regions } from "../data_app.malloy"',
      );
      expect(text.match(/^import /gm)).toHaveLength(2);
   });

   it("gives a source from a model the file does not import a line of its own", async () => {
      const text = await written("events");
      expect(text).toContain(
         'import { spare } from "../spare.malloy"\nimport { events } from "../events.malloy"\n',
      );
   });

   it("offers no Settings to edit sources by hand", async () => {
      await mount(() => {});
      expect(
         screen.queryByRole("button", { name: "Settings", hidden: true }),
      ).toBeNull();
   });
});
