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
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";
import { globalQueryClient } from "../../utils/queryClient";
import type { CatalogSource } from "../DashboardBuilder/catalog";
import { chartLineText } from "../DashboardBuilder/chartLine";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "./readNotebookSource";
import type { NotebookEvent } from "./telemetry";

const executeQueryModel = mock(
   async (
      _env: string,
      _pkg: string,
      _path: string,
      _request: { query?: string },
   ) => ({ data: { result: undefined } }),
);
mockServerProvider({ models: { executeQueryModel } });

const { NotebookBuilder } = await import("./NotebookBuilder");

const SOURCE = `## artifact { kind=notebook }
##(markdown) Intro.

source: a is duckdb.table('t')

run: a -> by_cat

run: a -> totals

# bar_chart { size=spark }
run: a -> by_cat

# line_chart
# bar_chart
run: a -> by_cat
`;

const SOURCES: CatalogSource[] = [
   {
      name: "a",
      modelPath: "notebooks/tour.malloy",
      views: [
         { name: "by_cat", chart: "bar_chart" },
         { name: "totals", aggregateOnly: true },
         { name: "geo", chart: "shape_map" },
      ],
      givens: [],
      fields: [],
   },
];

async function readOf(text: string): Promise<NotebookSource> {
   const read = await readNotebookSource(text);
   if (notebookSourceRefused(read)) throw new Error(read.refused);
   return read.source;
}

const mount = async (
   options: {
      source?: string;
      sources?: CatalogSource[] | null;
      sourcesFailed?: boolean;
      onSourcesWanted?: () => void;
      onSave?: (source: string) => Promise<void> | void;
      onEvent?: (event: NotebookEvent) => void;
   } = {},
) => {
   const source = options.source ?? SOURCE;
   const notebook = await readOf(source);
   const sources = options.sources === undefined ? SOURCES : options.sources;
   return render(
      <NotebookBuilder
         source={source}
         notebook={notebook}
         {...(sources ? { sources } : {})}
         {...(options.sourcesFailed ? { sourcesFailed: true } : {})}
         {...(options.onSourcesWanted
            ? { onSourcesWanted: options.onSourcesWanted }
            : {})}
         environmentName="env"
         packageName="pkg"
         modelPath="notebooks/tour.malloy"
         {...(options.onSave ? { onSave: options.onSave } : {})}
         {...(options.onEvent ? { onEvent: options.onEvent } : {})}
      />,
      { wrapper: serverWrapper },
   );
};

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });
const cell = (label: string) =>
   screen.getByRole("group", { name: label, hidden: true });
const inCell = (label: string, name: string) =>
   within(cell(label)).getByRole("button", { name, hidden: true });
const cells = () =>
   screen
      .queryAllByRole("group", { hidden: true })
      .map((c) => c.getAttribute("aria-label"))
      // A closing dialog's outlined fields are unlabelled groups too.
      .filter((label) => label !== null);
const settled = () =>
   waitFor(() => expect(globalQueryClient.isFetching()).toBe(0));

/** The picker of a cell: its combobox, which opens on mouse down. */
const picker = (label: string) =>
   within(cell(label)).getByRole("combobox", { hidden: true });
/** The menu just opened; a closed one may linger while its transition ends. */
const openMenu = (label: string) => {
   fireEvent.mouseDown(picker(label));
   return screen
      .getAllByRole("listbox", { hidden: true })
      .at(-1) as HTMLElement;
};
const optionNames = (label: string) => {
   const menu = openMenu(label);
   const options = within(menu).getAllByRole("option", { hidden: true });
   // Choosing what is already chosen closes the menu without a change.
   fireEvent.click(
      options.find((o) => o.getAttribute("aria-selected") === "true")!,
   );
   return options.map((o) => o.textContent);
};
const choose = (label: string, option: string) => {
   fireEvent.click(
      within(openMenu(label)).getByRole("option", {
         name: option,
         hidden: true,
      }),
   );
};
const diffDialog = () =>
   screen
      .getByLabelText("File changes")
      .closest('[role="dialog"]') as HTMLElement;
const ran = () =>
   executeQueryModel.mock.calls.map((call) => call[3].query as string);
const lastQuery = () =>
   executeQueryModel.mock.calls.at(-1)?.[3].query as string;

beforeEach(() => {
   cleanup();
   clearCache();
   executeQueryModel.mockClear();
});

describe("the chart picker", () => {
   it("offers the renderer's charts, with no sparkline and no map the view does not carry", async () => {
      await mount();
      expect(optionNames("Cell 3, query")).toEqual([
         "Default",
         "No chart (table)",
         "Line",
         "Bar",
         "Scatter",
      ]);
   });

   it("offers big value only for a view whose every column is an aggregate", async () => {
      await mount();
      expect(optionNames("Cell 4, query")).toContain("Big value");
      expect(optionNames("Cell 3, query")).not.toContain("Big value");
   });

   it("offers a map only when the view already carries that map tag", async () => {
      await mount({
         source: `${SOURCE}
run: a -> geo
`,
      });
      expect(optionNames("Cell 7, query")).toContain("Shape map");
      expect(optionNames("Cell 7, query")).not.toContain("Segment map");
   });

   it("names each control by its cell", async () => {
      await mount();
      expect(
         within(cell("Cell 3, query"))
            .getByRole("combobox", { hidden: true })
            .getAttribute("aria-label"),
      ).toBe("Chart, cell 3");
   });

   it("starts on the cell's own chart state", async () => {
      await mount({
         source: `## artifact { kind=notebook }
source: a is duckdb.table('t')

${chartLineText("bar_chart")}
run: a -> by_cat
`,
      });
      expect(picker("Cell 2, query").textContent).toBe("Bar");
   });

   it("is off, with its reason, on a cell whose chart line the editor does not model", async () => {
      await mount();
      const locked = cell("Cell 5, query");
      expect(
         within(locked)
            .getByRole("combobox", { hidden: true })
            .getAttribute("aria-disabled"),
      ).toBe("true");
      // On screen, and the control is described by it.
      const reason = within(locked).getByText(
         /does not model \(# bar_chart \{ size=spark \}\)/,
      );
      expect(
         within(locked)
            .getByRole("combobox", { hidden: true })
            .getAttribute("aria-describedby"),
      ).toBe(reason.id);
   });

   it("is off, with its reason, on a cell with two chart lines", async () => {
      await mount();
      expect(
         within(cell("Cell 6, query")).getByText(/more than one chart line/),
      ).toBeDefined();
   });

   it("runs and saves the cell with the chart line the picker holds, not the file's text", async () => {
      let written: string | undefined;
      const onSave = mock(async (text: string) => {
         written = text;
      });
      await mount({ onSave });
      await settled();
      executeQueryModel.mockClear();

      choose("Cell 3, query", "Line");
      await settled();
      expect(lastQuery()).toContain(chartLineText("line_chart"));
      expect(lastQuery()).toContain("run: a -> by_cat");

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         `${chartLineText("line_chart")}\nrun: a -> by_cat\n\nrun: a -> totals`,
      );
   });

   it("takes the chart line back off the cell on Default", async () => {
      await mount({
         source: `## artifact { kind=notebook }
source: a is duckdb.table('t')

${chartLineText("bar_chart")}
run: a -> by_cat
`,
      });
      await settled();
      expect(lastQuery()).toContain(chartLineText("bar_chart"));
      choose("Cell 2, query", "Default");
      await settled();
      expect(lastQuery()).not.toContain("bar_chart");
      expect(lastQuery()).toContain("run: a -> by_cat");
   });
});

describe("adding a query", () => {
   const addBelowLast = async () => {
      fireEvent.click(inCell("Cell 6, query", "Add query below"));
      fireEvent.click(button("View by_cat"));
   };

   it("adds a cell whose text, caption and chart come from the document", async () => {
      await mount();
      await settled();
      await addBelowLast();
      expect(
         screen.getByText(/not connected to the filter controls/),
      ).toBeDefined();
      fireEvent.change(screen.getByLabelText("Query caption"), {
         target: { value: "Revenue by category" },
      });
      fireEvent.click(button("Add query"));
      await settled();

      expect(cells().at(-1)).toBe("Cell 7, query");
      expect(
         within(cell("Cell 7, query")).getByText("Revenue by category"),
      ).toBeDefined();
      expect(ran()).toContain("run: a -> by_cat\n");

      choose("Cell 7, query", "Bar");
      await settled();
      expect(ran()).toContain(
         `${chartLineText("bar_chart")}\nrun: a -> by_cat\n`,
      );
   });

   it("offers the caption for an added cell until it is in the file", async () => {
      let written: string | undefined;
      await mount({
         onSave: async (text) => {
            written = text;
         },
      });
      await addBelowLast();
      fireEvent.click(button("Add query"));
      expect(
         within(cell("Cell 7, query")).getByLabelText("Query caption"),
      ).toBeDefined();
      expect(
         within(cell("Cell 3, query")).queryByLabelText("Query caption"),
      ).toBeNull();

      fireEvent.click(button("Save changes"));
      fireEvent.click(await screen.findByRole("button", { name: /Save/ }));
      await waitFor(() => expect(written).toBeDefined());
      await waitFor(() =>
         expect(
            within(cell("Cell 7, query")).queryByLabelText("Query caption"),
         ).toBeNull(),
      );
   });

   it("saves the added query through the diff, and says a text or query cell was added", async () => {
      let written: string | undefined;
      const onEvent = mock((_event: NotebookEvent) => {});
      await mount({
         onEvent,
         onSave: async (text) => {
            written = text;
         },
      });
      await addBelowLast();
      fireEvent.change(screen.getByLabelText("Query caption"), {
         target: { value: "Revenue" },
      });
      fireEvent.click(button("Add query"));
      choose("Cell 7, query", "Bar");

      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(screen.getByLabelText("File changes")).toBeDefined(),
      );
      expect(
         screen.getByText(/A text or query cell was added or removed/),
      ).toBeDefined();
      // Nothing that was read is removed, so undo survives this save.
      expect(screen.queryByText(/clears undo/)).toBeNull();
      fireEvent.click(
         within(diffDialog()).getByRole("button", {
            name: /Save/,
            hidden: true,
         }),
      );
      await waitFor(() => expect(written).toBeDefined());
      expect(
         written?.endsWith(
            `#" Revenue\n${chartLineText("bar_chart")}\nrun: a -> by_cat\n`,
         ),
      ).toBe(true);
      expect(onEvent.mock.calls.at(-1)?.[0]).toMatchObject({
         type: "notebook.saved",
         structural: true,
      });
   });

   it("reports a property-only save as not structural", async () => {
      const onEvent = mock((_event: NotebookEvent) => {});
      await mount({ onEvent, onSave: async () => {} });
      choose("Cell 3, query", "Line");
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(onEvent.mock.calls.at(-1)?.[0]).toMatchObject({
            type: "notebook.saved",
            structural: false,
         }),
      );
   });
});

describe("undo around saves", () => {
   it("keeps undo when the chart of a query added in this session is picked and saved", async () => {
      const onSave = mock(async (_text: string) => {});
      await mount({ onSave });
      fireEvent.click(inCell("Cell 6, query", "Add query below"));
      fireEvent.click(button("View by_cat"));
      fireEvent.click(button("Add query"));
      fireEvent.click(button("Save changes"));
      fireEvent.click(
         within(
            (await screen.findByLabelText("File changes")).closest(
               '[role="dialog"]',
            ) as HTMLElement,
         ).getByRole("button", { name: /Save/, hidden: true }),
      );
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      await waitFor(() => expect(button("Saved")).toBeDefined());

      choose("Cell 7, query", "Bar");
      await waitFor(() => expect(button("Save changes")).toBeDefined());
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(button("Undo").hasAttribute("disabled")).toBe(false);
   });

   it("shows the diff, and says undo will clear, before saving a chart over a bare chart line", async () => {
      const onSave = mock(async (_text: string) => {});
      await mount({
         onSave,
         source: `## artifact { kind=notebook }
source: a is duckdb.table('t')

# line_chart
run: a -> by_cat
`,
      });
      choose("Cell 2, query", "Bar");
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(screen.getByLabelText("File changes")).toBeDefined(),
      );
      expect(onSave).not.toHaveBeenCalled();
      expect(screen.getByText(/Saving this clears undo/)).toBeDefined();
      expect(screen.getByText(/A chart line was changed/)).toBeDefined();
      expect(screen.queryByText(/added or removed/)).toBeNull();
   });
});

describe("the Add menu", () => {
   it("turns query-above off where a definition would end up below it, and says why", async () => {
      await mount();
      expect(
         inCell("Cell 1, text", "Add query above").hasAttribute("disabled"),
      ).toBe(true);
      expect(
         within(cell("Cell 1, text")).getByLabelText(
            /Add query above: A query cannot go above a definition/,
         ),
      ).toBeDefined();
      // Below the definition itself is the first legal slot.
      expect(
         inCell("Cell 2, definition", "Add query below").hasAttribute(
            "disabled",
         ),
      ).toBe(false);
      expect(
         inCell("Cell 3, query", "Add query above").hasAttribute("disabled"),
      ).toBe(false);
   });

   it("is off while the notebook's sources load, and when it reads none", async () => {
      await mount({ sources: null });
      expect(
         inCell("Cell 3, query", "Add query below").hasAttribute("disabled"),
      ).toBe(true);
      expect(
         within(cell("Cell 3, query")).getAllByLabelText(/sources are loading/),
      ).not.toHaveLength(0);
      cleanup();
      await mount({ sources: [] });
      expect(
         within(cell("Cell 3, query")).getAllByLabelText(/reads no source/),
      ).not.toHaveLength(0);
   });

   it("says the sources could not be read, not that they load, after a failed read", async () => {
      await mount({ sources: null, sourcesFailed: true });
      expect(
         within(cell("Cell 3, query")).getAllByLabelText(
            /sources could not be read/,
         ),
      ).not.toHaveLength(0);
   });

   it("asks for the imported sources when the dialog or a picker opens, not before", async () => {
      const onSourcesWanted = mock(() => {});
      await mount({ onSourcesWanted });
      expect(onSourcesWanted).not.toHaveBeenCalled();
      optionNames("Cell 3, query");
      expect(onSourcesWanted).toHaveBeenCalledTimes(1);
      fireEvent.click(inCell("Cell 3, query", "Add query below"));
      expect(onSourcesWanted).toHaveBeenCalledTimes(2);
   });

   it("offers Add text and Add query on an empty notebook", async () => {
      await mount({ source: "## artifact { kind=notebook }\n" });
      expect(cells()).toEqual([]);
      expect(button("Add text")).toBeDefined();
      fireEvent.click(button("Add query"));
      expect(screen.getByText("Add a query")).toBeDefined();
   });
});

describe("removing a query", () => {
   it("says the save clears undo, and then undo is gone", async () => {
      const onSave = mock(async (_text: string) => {});
      await mount({ onSave });
      fireEvent.click(inCell("Cell 3, query", "Remove query"));
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(screen.getByLabelText("File changes")).toBeDefined(),
      );
      expect(
         screen.getByText(/A text or query cell was added or removed/),
      ).toBeDefined();
      expect(screen.getByText(/Saving this clears undo/)).toBeDefined();
      fireEvent.click(
         within(diffDialog()).getByRole("button", {
            name: /Save/,
            hidden: true,
         }),
      );
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      await waitFor(() =>
         expect(button("Undo").hasAttribute("disabled")).toBe(true),
      );
   });

   it("does not say so for a removed text cell", async () => {
      await mount({ onSave: async () => {} });
      fireEvent.click(inCell("Cell 1, text", "Remove text"));
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(screen.getByLabelText("File changes")).toBeDefined(),
      );
      expect(screen.queryByText(/clears undo/)).toBeNull();
   });
});
