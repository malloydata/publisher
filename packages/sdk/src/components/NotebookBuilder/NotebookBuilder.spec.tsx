// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
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
import type { Given } from "../../client";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "./readNotebookSource";
import type { NotebookEvent } from "./telemetry";

/** The route's real answer to a restricted construct: a 400 whose problems carry Malloy's code. */
const restrictedRefusal = {
   response: {
      status: 400,
      data: {
         code: 400,
         message:
            "line 1:6 `duckdb.sql(...)` cannot be used in a restricted query — raw SQL is not permitted.",
         problems: [
            {
               message:
                  "`duckdb.sql(...)` cannot be used in a restricted query — raw SQL is not permitted.",
               severity: "error",
               code: "restricted-construct-forbidden",
            },
         ],
      },
   },
   message: "Request failed with status code 400",
};

const executeQueryModel = mock(
   async (
      _env: string,
      _pkg: string,
      _path: string,
      request: { query?: string; givens?: Record<string, unknown> },
   ) => {
      const query = request.query ?? "";
      if (query.includes("duckdb.sql")) throw restrictedRefusal;
      if (query.includes("missing_field"))
         throw {
            response: {
               status: 400,
               data: {
                  code: 400,
                  message: "line 1:15 'missing_field' is not defined",
                  problems: [
                     {
                        message: "'missing_field' is not defined",
                        severity: "error",
                        code: "field-not-found",
                     },
                  ],
               },
            },
            message: "Request failed with status code 400",
         };
      if (query.includes("hidden"))
         throw {
            response: {
               status: 404,
               data: { code: 404, message: "No queryable source" },
            },
            message: "Request failed with status code 404",
         };
      return { data: { result: undefined } };
   },
);
mockServerProvider({ models: { executeQueryModel } });

const { NotebookBuilder } = await import("./NotebookBuilder");

const DEF = "source: a is duckdb.table('t')";
const RUN = "# bar_chart\nrun: a -> { select: x }";
const SOURCE = `## artifact { kind=notebook }
##(markdown) Intro.

// Documents the source.
${DEF}

// About the result.
##(markdown) Middle.

${RUN}
`;

async function readOf(text: string): Promise<NotebookSource> {
   const read = await readNotebookSource(text);
   if (notebookSourceRefused(read)) throw new Error(read.refused);
   return read.source;
}

const mount = async (
   options: {
      source?: string;
      onSave?: (source: string) => Promise<void> | void;
      onEvent?: (event: NotebookEvent) => void;
      givens?: Given[];
      startingGivens?: Record<string, string>;
      autorun?: boolean;
   } = {},
) => {
   const source = options.source ?? SOURCE;
   const notebook = await readOf(source);
   return render(
      <NotebookBuilder
         source={source}
         notebook={notebook}
         environmentName="env"
         packageName="pkg"
         modelPath="notebooks/tour.malloy"
         {...(options.onSave ? { onSave: options.onSave } : {})}
         {...(options.onEvent ? { onEvent: options.onEvent } : {})}
         {...(options.givens ? { givens: options.givens } : {})}
         {...(options.startingGivens
            ? { startingGivens: options.startingGivens }
            : {})}
         {...(options.autorun !== undefined
            ? { autorun: options.autorun }
            : {})}
      />,
      { wrapper: serverWrapper },
   );
};

/** A named button, hidden or not: a closed dialog's transition leaves the page aria-hidden under the runner. */
const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

/** Every query the cache holds has finished, so a run that was going to start has started and ended. */
const settled = () =>
   waitFor(() => expect(globalQueryClient.isFetching()).toBe(0));

const cells = () =>
   screen
      .getAllByRole("group", { hidden: true })
      .map((cell) => cell.getAttribute("aria-label"))
      // An open text field's outline is an unlabelled group too.
      .filter((label) => label !== null);

const cell = (label: string) =>
   screen.getByRole("group", { name: label, hidden: true });

const inCell = (label: string, name: string) =>
   within(cell(label)).getByRole("button", { name, hidden: true });

const editText = (label: string, next: string) => {
   fireEvent.click(inCell(label, "Edit text"));
   fireEvent.change(screen.getByLabelText("Markdown"), {
      target: { value: next },
   });
   fireEvent.click(button("Done"));
};

beforeEach(() => {
   clearCache();
   executeQueryModel.mockClear();
});

describe("NotebookBuilder", () => {
   it("shows every cell in file order, folding the definition", async () => {
      await mount();
      expect(cells()).toEqual([
         "Cell 1, text",
         "Cell 2, definition",
         "Cell 3, text",
         "Cell 4, query",
      ]);
      expect(within(cell("Cell 1, text")).getByText("Intro.")).toBeDefined();
      expect(
         within(cell("Cell 2, definition")).getByText("Setup: source: a"),
      ).toBeDefined();
      // Only prose can be removed here.
      expect(
         within(cell("Cell 4, query")).queryByRole("button", {
            name: "Remove text",
         }),
      ).toBeNull();
   });

   it("sets every cell in a card that holds its tools and its content", async () => {
      await mount();
      for (const label of cells()) {
         const card = cell(label as string).querySelector("[data-cell-card]");
         expect(card).not.toBeNull();
         expect(
            within(card as HTMLElement).getByRole("button", {
               name: "Move up",
               hidden: true,
            }),
         ).toBeDefined();
      }
      const intro = cell("Cell 1, text").querySelector("[data-cell-card]");
      expect(within(intro as HTMLElement).getByText("Intro.")).toBeDefined();
   });

   it("runs a query cell's exact text against the notebook", async () => {
      await mount();
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      const [env, pkg, path, request] = executeQueryModel.mock.calls[0];
      expect([env, pkg, path]).toEqual(["env", "pkg", "notebooks/tour.malloy"]);
      expect(request.query).toBe(`${RUN}\n`);
   });

   it("runs a query cell without its prose, which still shows", async () => {
      await mount({
         source: `## artifact { kind=notebook }\n${DEF}\n\n#(markdown) Names #(authorize) and gated_source.\n#" A caption.\n${RUN}\n`,
      });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      expect(executeQueryModel.mock.calls[0][3].query).toBe(`${RUN}\n`);
      expect(
         within(cell("Cell 2, query")).getByText(/Names #\(authorize\)/),
      ).toBeDefined();
      expect(
         within(cell("Cell 2, query")).getByText("A caption."),
      ).toBeDefined();
   });
});

describe("NotebookBuilder: the notebook's own control settings", () => {
   const REGION: Given[] = [{ name: "REGION", type: "string" }];

   it("runs the first preview with the notebook's starting givens", async () => {
      await mount({ givens: REGION, startingGivens: { REGION: "EU" } });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      expect(executeQueryModel.mock.calls[0][3].givens).toEqual({
         REGION: "EU",
      });
   });

   it("holds control changes behind Apply under autorun=false", async () => {
      await mount({ givens: REGION, autorun: false });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      fireEvent.change(screen.getByLabelText("REGION"), {
         target: { value: "W" },
      });
      fireEvent.change(screen.getByLabelText("REGION"), {
         target: { value: "West" },
      });
      await settled();
      expect(executeQueryModel).toHaveBeenCalledTimes(1);
      fireEvent.click(button("Apply"));
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
      expect(executeQueryModel.mock.calls[1][3].givens).toEqual({
         REGION: "West",
      });
   });
});

describe("NotebookBuilder: markdown", () => {
   it("edits a cell in place, as one history entry, and writes only that cell", async () => {
      let written: string | undefined;
      const onEvent = mock((_event: NotebookEvent) => {});
      await mount({ onSave: (s) => void (written = s), onEvent });
      fireEvent.click(inCell("Cell 1, text", "Edit text"));
      const field = screen.getByLabelText("Markdown");
      fireEvent.change(field, { target: { value: "Intro, e" } });
      fireEvent.change(field, { target: { value: "Intro, edited." } });
      // The live preview renders while typing.
      expect(
         within(screen.getByLabelText("Preview")).getByText("Intro, edited."),
      ).toBeDefined();
      fireEvent.click(button("Done"));
      expect(
         within(cell("Cell 1, text")).getByText("Intro, edited."),
      ).toBeDefined();

      fireEvent.click(button("Undo"));
      expect(within(cell("Cell 1, text")).getByText("Intro.")).toBeDefined();
      expect(button("Undo")).toHaveProperty("disabled", true);
      fireEvent.click(button("Redo"));

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toBe(SOURCE.replace("Intro.", "Intro, edited."));
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.saved",
         cells: 4,
         where: "package",
      });
      await waitFor(() => expect(button("Saved")).toBeDefined());
   });

   it("commits an edit when focus leaves the field, not only on Done", async () => {
      await mount();
      fireEvent.click(inCell("Cell 3, text", "Edit text"));
      const field = screen.getByLabelText("Markdown");
      fireEvent.change(field, { target: { value: "Middle, kept." } });
      fireEvent.keyDown(window, { key: "Escape" });
      fireEvent.blur(field);
      expect(
         within(cell("Cell 3, text")).getByText("Middle, kept."),
      ).toBeDefined();
      fireEvent.click(button("Undo"));
      expect(within(cell("Cell 3, text")).getByText("Middle.")).toBeDefined();
   });

   it("adds text above and below a cell, and saves at once", async () => {
      let written: string | undefined;
      await mount({ onSave: (s) => void (written = s) });
      fireEvent.click(inCell("Cell 2, definition", "Add text above"));
      fireEvent.change(screen.getByLabelText("Markdown"), {
         target: { value: "Above." },
      });
      fireEvent.click(button("Done"));
      fireEvent.click(inCell("Cell 5, query", "Add text below"));
      fireEvent.change(screen.getByLabelText("Markdown"), {
         target: { value: "Below." },
      });
      fireEvent.click(button("Done"));
      expect(cells()).toHaveLength(6);

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(await screen.findByText(/Added 2 cells/)).toBeDefined();
      expect(written).toContain(
         "##(markdown) Intro.\n\n##(markdown) Above.\n\n// Documents the source.\n",
      );
      expect(written).toContain(`${RUN}\n\n##(markdown) Below.\n`);
   });

   it("removes a text cell, and says the comment above it goes too", async () => {
      let written: string | undefined;
      await mount({ onSave: (s) => void (written = s) });
      fireEvent.click(inCell("Cell 3, text", "Remove text"));
      expect(cells()).toHaveLength(3);

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(
         (await screen.findByLabelText("Comments removed with their cell"))
            .textContent,
      ).toBe("// About the result.");
      expect(written).not.toContain("Middle.");
      expect(written).not.toContain("// About the result.");
      expect(written).toContain("// Documents the source.");
   });

   it("keeps an emptied cell on screen when the writer refuses it, and says why", async () => {
      let written: string | undefined;
      await mount({ onSave: (s) => void (written = s) });
      fireEvent.click(inCell("Cell 1, text", "Edit text"));
      const field = screen.getByLabelText("Markdown");
      fireEvent.change(field, { target: { value: "  " } });
      // Done holds while the text is invalid; leaving the field still commits it, and the writer refuses at Save.
      fireEvent.blur(field);
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(screen.getByRole("alert").textContent).toContain(
            "remove the cell instead",
         ),
      );
      expect(written).toBeUndefined();
   });
});

describe("NotebookBuilder: reorder", () => {
   it("moves a cell with Alt+Arrow from the Move handle as well as from the cell", async () => {
      await mount();
      const handle = screen.getByLabelText("Move Cell 4, query");
      fireEvent.keyDown(handle, { key: "ArrowUp", altKey: true });
      expect(cells()[2]).toBe("Cell 3, query");
      fireEvent.keyDown(screen.getByLabelText("Move Cell 3, query"), {
         key: "ArrowDown",
         altKey: true,
      });
      expect(cells()[3]).toBe("Cell 4, query");
   });

   it("moves a cell by keyboard, one history entry per move, and refuses a query above its definition", async () => {
      let written: string | undefined;
      await mount({ onSave: (s) => void (written = s) });
      const query = cell("Cell 4, query");
      query.focus();
      fireEvent.keyDown(query, { key: "ArrowUp", altKey: true });
      expect(cells()).toEqual([
         "Cell 1, text",
         "Cell 2, definition",
         "Cell 3, query",
         "Cell 4, text",
      ]);

      // Up again would put it above the definition it reads.
      expect(
         inCell("Cell 3, query", "Move up").getAttribute("aria-disabled"),
      ).toBe("true");
      fireEvent.keyDown(cell("Cell 3, query"), {
         key: "ArrowUp",
         altKey: true,
      });
      expect(cells()[2]).toBe("Cell 3, query");
      expect(screen.getByRole("status").textContent).toContain(
         "stay below the setup lines",
      );

      // Prose moves anywhere: the text below goes to the top in three moves.
      fireEvent.click(inCell("Cell 4, text", "Move up"));
      fireEvent.click(inCell("Cell 3, text", "Move up"));
      fireEvent.click(inCell("Cell 2, text", "Move up"));
      expect(cells()[0]).toBe("Cell 1, text");
      expect(within(cell("Cell 1, text")).getByText("Middle.")).toBeDefined();

      // Each move was its own entry.
      fireEvent.click(button("Undo"));
      expect(within(cell("Cell 2, text")).getByText("Middle.")).toBeDefined();
      fireEvent.keyDown(window, { key: "z", ctrlKey: true, metaKey: true });
      expect(within(cell("Cell 3, text")).getByText("Middle.")).toBeDefined();
      fireEvent.keyDown(window, {
         key: "z",
         ctrlKey: true,
         metaKey: true,
         shiftKey: true,
      });
      expect(within(cell("Cell 2, text")).getByText("Middle.")).toBeDefined();

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written?.indexOf("Middle.")).toBeLessThan(
         written?.indexOf("// Documents the source.") ?? -1,
      );
   });

   it("marks a cell the dragged query may not land on, and only that kind", async () => {
      // happy-dom has no Web Animations API, which dnd-kit reads when a drag starts.
      const doc = document as unknown as { getAnimations?: () => [] };
      const el = Element.prototype as unknown as { getAnimations?: () => [] };
      const had = [doc.getAnimations, el.getAnimations];
      doc.getAnimations ??= () => [];
      el.getAnimations ??= () => [];
      try {
         await dragQuery();
      } finally {
         doc.getAnimations = had[0];
         el.getAnimations = had[1];
      }
   });

   const dragQuery = async () => {
      await mount();
      const grip = screen.getByLabelText("Move Cell 4, query");
      grip.focus();
      fireEvent.keyDown(grip, { code: "Space", key: " " });
      await waitFor(() =>
         expect(cell("Cell 2, definition").getAttribute("data-drop")).toBe(
            "refused",
         ),
      );
      expect(cell("Cell 1, text").getAttribute("data-drop")).toBe("refused");
      expect(cell("Cell 3, text").getAttribute("data-drop")).toBeNull();
      // The cell in hand is the source, not a refused target.
      expect(cell("Cell 4, query").getAttribute("data-drop")).toBeNull();
      fireEvent.keyDown(grip, { code: "Escape", key: "Escape" });
      await waitFor(() =>
         expect(cell("Cell 1, text").getAttribute("data-drop")).toBeNull(),
      );
   };

   it("leaves the left and right arrow keys to the page", async () => {
      await mount();
      // `fireEvent` answers false when a handler called preventDefault.
      expect(fireEvent.keyDown(window, { key: "ArrowLeft" })).toBe(true);
      expect(fireEvent.keyDown(window, { key: "ArrowRight" })).toBe(true);
   });

   it.each([
      ["Cancel", () => fireEvent.click(button("Cancel"))],
      [
         "Escape",
         () =>
            fireEvent.keyDown(screen.getByLabelText("Markdown"), {
               key: "Escape",
            }),
      ],
      ["blur", () => fireEvent.blur(screen.getByLabelText("Markdown"))],
      ["Done", () => fireEvent.click(button("Done"))],
   ])(
      "removes a freshly added text cell that is left empty by %s",
      async (_name, leave) => {
         await mount();
         fireEvent.click(inCell("Cell 1, text", "Add text below"));
         expect(cells()).toHaveLength(5);
         leave();
         expect(cells()).toHaveLength(4);
      },
   );

   it("keeps a freshly added cell that has text, and Cancel on it removes it even with text typed", async () => {
      await mount();
      fireEvent.click(inCell("Cell 1, text", "Add text below"));
      fireEvent.change(screen.getByLabelText("Markdown"), {
         target: { value: "Typed." },
      });
      fireEvent.click(button("Cancel"));
      expect(cells()).toHaveLength(4);
      fireEvent.click(inCell("Cell 1, text", "Add text below"));
      fireEvent.change(screen.getByLabelText("Markdown"), {
         target: { value: "Typed." },
      });
      fireEvent.click(button("Done"));
      expect(cells()).toHaveLength(5);
   });

   it("cancels an edit to an existing cell without changing the document", async () => {
      await mount({ onSave: async () => {} });
      fireEvent.click(inCell("Cell 1, text", "Edit text"));
      fireEvent.change(screen.getByLabelText("Markdown"), {
         target: { value: "Dropped." },
      });
      fireEvent.click(button("Cancel"));
      expect(within(cell("Cell 1, text")).getByText("Intro.")).toBeDefined();
      expect(button("Saved")).toBeDefined();
   });

   it("never undoes from inside the markdown field", async () => {
      await mount();
      editText("Cell 1, text", "Changed.");
      fireEvent.click(inCell("Cell 1, text", "Edit text"));
      fireEvent.keyDown(screen.getByLabelText("Markdown"), {
         key: "z",
         ctrlKey: true,
         metaKey: true,
      });
      fireEvent.click(button("Done"));
      expect(within(cell("Cell 1, text")).getByText("Changed.")).toBeDefined();
   });
});

describe("NotebookBuilder: query results", () => {
   const withCell = (run: string) =>
      `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Why this matters.\n\n${run}\n`;

   it("shows a hidden source's cell with its text and prose, result unavailable", async () => {
      await mount({ source: withCell("run: hidden -> { select: x }") });
      await waitFor(() =>
         expect(screen.getByText(/^Result unavailable/)).toBeDefined(),
      );
      expect(screen.getByText("Why this matters.")).toBeDefined();
      expect(cell("Cell 3, query").textContent).toContain(
         "run: hidden -> { select: x }",
      );
   });

   it("shows an ordinary compile error as an error, not as a preview the editor cannot run", async () => {
      await mount({ source: withCell("run: a -> { select: missing_field }") });
      await waitFor(() =>
         expect(
            screen.getByText(/'missing_field' is not defined/),
         ).toBeDefined(),
      );
      expect(
         screen.queryByText(/^Preview unavailable in the editor/),
      ).toBeNull();
      expect(screen.queryByText(/^Result unavailable/)).toBeNull();
   });

   it("says a restricted construct cannot preview here, rather than failing", async () => {
      await mount({
         source: withCell('run: duckdb.sql("select 1 as x") -> { select: x }'),
      });
      await waitFor(() =>
         expect(
            screen.getByText(/^Preview unavailable in the editor/),
         ).toBeDefined(),
      );
      expect(screen.queryByText(/This cell could not be run/)).toBeNull();
   });

   it("keeps a result across a reorder, and re-runs when a given changes", async () => {
      await mount({ givens: [{ name: "REGION", type: "string" }] });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      fireEvent.keyDown(cell("Cell 4, query"), {
         key: "ArrowUp",
         altKey: true,
      });
      fireEvent.click(button("Undo"));
      fireEvent.click(inCell("Cell 3, text", "Move down"));
      await settled();
      expect(executeQueryModel).toHaveBeenCalledTimes(1);

      fireEvent.change(screen.getByLabelText("REGION"), {
         target: { value: "West" },
      });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
      expect(executeQueryModel.mock.calls[1][3].givens).toEqual({
         REGION: "West",
      });

      // Back to values it already ran with: the cached result, not another run.
      fireEvent.change(screen.getByLabelText("REGION"), {
         target: { value: "East" },
      });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(3));
      fireEvent.change(screen.getByLabelText("REGION"), {
         target: { value: "West" },
      });
      await settled();
      expect(executeQueryModel).toHaveBeenCalledTimes(3);
   });
});
