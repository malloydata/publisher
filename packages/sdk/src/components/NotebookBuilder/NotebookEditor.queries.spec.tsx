// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
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
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import { sha256Hex } from "../../utils/sha256";
import type { NotebookEvent } from "./telemetry";

const FILE = `## artifact { kind=notebook }
##(markdown) Intro.

source: a is duckdb.table('t')

run: a -> by_cat
`;

const NAMED_IMPORT = `## artifact { kind=notebook }
import { a } from "../shop.malloy"

run: a -> by_cat
`;
const WHOLE_IMPORT = NAMED_IMPORT.replace(
   'import { a } from "../shop.malloy"',
   'import "../shop.malloy"',
);

const modelOf = (path: string) => ({
   data: {
      modelPath: path,
      sourceText: FILE,
      givens: [],
      sources: [
         {
            name: "a",
            views: [{ name: "by_cat", annotations: ["# bar_chart\n"] }],
         },
      ],
   },
});

const getModel = mock(
   async (
      _env: string,
      _pkg: string,
      path: string,
      _versionId?: string,
      _hidden?: boolean,
   ) => {
      if (path === "gone.malloy") throw new Error("not found");
      if (path === "notebooks/broken.malloy")
         return {
            data: {
               modelPath: path,
               sourceText: NAMED_IMPORT.replace("shop", "gone"),
               givens: [],
               sources: [],
            },
         };
      // A curated package: the imported file's sources, and none of them in the notebook's own.
      if (path === "shop.malloy")
         return {
            data: {
               modelPath: path,
               modelInfo: JSON.stringify({
                  entries: [
                     { kind: "source", name: "a" },
                     { kind: "source", name: "other" },
                  ],
               }),
               sources: [
                  {
                     name: "a",
                     views: [
                        { name: "by_cat", annotations: ["# bar_chart\n"] },
                     ],
                  },
                  { name: "other", views: [{ name: "v" }] },
               ],
            },
         };
      if (
         path === "notebooks/named.malloy" ||
         path === "notebooks/whole.malloy"
      )
         return {
            data: {
               modelPath: path,
               sourceText:
                  path === "notebooks/named.malloy"
                     ? NAMED_IMPORT
                     : WHOLE_IMPORT,
               givens: [],
               sources: [],
            },
         };
      return modelOf(path);
   },
);
const getNotebook = mock(async () => ({ data: { autorun: true } }));
const updateModelSource = mock(
   async (
      _env: string,
      _pkg: string,
      path: string,
      _body: { source: string; expectedHash?: string },
   ) => ({ data: { path, contentHash: "h2", created: false } }),
);
mockServerProvider(
   {
      models: {
         getModel,
         executeQueryModel: mock(() => pending()),
         updateModelSource,
      },
      notebooks: { getNotebook },
   },
   { mutable: true, isLoadingStatus: false },
);

const { NotebookEditor } = await import("./NotebookEditor");

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

const settle = async () => {
   await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
   });
};

beforeEach(() => {
   cleanup();
   clearCache();
   getModel.mockClear();
   getNotebook.mockClear();
   updateModelSource.mockClear();
});

describe("NotebookEditor, adding a query", () => {
   it("offers the sources of the model it already fetched, and writes the cell through the package route", async () => {
      const onEvent = mock((_event: NotebookEvent) => {});
      render(
         <NotebookEditor
            environmentName="env"
            packageName="pkg"
            notebookName="tour"
            onEvent={onEvent}
         />,
         { wrapper: serverWrapper },
      );
      const query = await screen.findByRole("group", {
         name: "Cell 3, query",
         hidden: true,
      });
      const requests = [
         getModel.mock.calls.length,
         getNotebook.mock.calls.length,
      ];

      fireEvent.click(
         within(query).getByRole("button", {
            name: "Add query below",
            hidden: true,
         }),
      );
      fireEvent.click(button("View by_cat"));
      fireEvent.click(button("Add query"));
      expect(
         screen.getByRole("group", { name: "Cell 4, query", hidden: true }),
      ).toBeDefined();
      await settle();
      // The dialog read the cached model: no request of its own.
      expect([
         getModel.mock.calls.length,
         getNotebook.mock.calls.length,
      ]).toEqual(requests);

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      const body = updateModelSource.mock.calls[0][3];
      expect(body.source).toBe(`${FILE}\nrun: a -> by_cat\n`);
      expect(body.expectedHash).toBe(await sha256Hex(FILE));
      await waitFor(() =>
         expect(onEvent.mock.calls.at(-1)?.[0]).toMatchObject({
            type: "notebook.saved",
            structural: true,
            where: "package",
         }),
      );
   });
});

describe("NotebookEditor, adding a query on a curated package", () => {
   const open = async (name: string) => {
      render(
         <NotebookEditor
            environmentName="env"
            packageName="pkg"
            notebookName={name}
         />,
         { wrapper: serverWrapper },
      );
      return screen.findByRole("group", {
         name: "Cell 2, query",
         hidden: true,
      });
   };
   const importReads = () =>
      getModel.mock.calls.filter((call) => call[2] === "shop.malloy").length;

   for (const [name, file] of [
      ["named", NAMED_IMPORT],
      ["whole", WHOLE_IMPORT],
   ] as const)
      it(`offers the sources of a ${name} import that the notebook's own model left out, reading them only when the dialog opens`, async () => {
         const query = await open(name);
         const loadReads = getModel.mock.calls.length;
         // Nothing about the import is read on load or while idle.
         expect(importReads()).toBe(0);

         fireEvent.click(
            within(query).getByRole("button", {
               name: "Add query below",
               hidden: true,
            }),
         );
         fireEvent.click(
            await screen.findByRole("button", {
               name: "View by_cat",
               hidden: true,
            }),
         );
         expect(importReads()).toBe(1);
         expect(getModel.mock.calls.length).toBe(loadReads + 1);
         // A named import offers what it names; a whole one, everything the file exports.
         fireEvent.mouseDown(
            screen.getByRole("combobox", { name: /Source/, hidden: true }),
         );
         expect(
            within(screen.getAllByRole("listbox", { hidden: true }).at(-1)!)
               .getAllByRole("option", { hidden: true })
               .map((option) => option.textContent),
         ).toEqual(name === "named" ? ["a"] : ["a", "other"]);
         fireEvent.keyDown(
            screen.getAllByRole("listbox", { hidden: true }).at(-1)!,
            { key: "Escape" },
         );
         fireEvent.click(button("Add query"));

         fireEvent.click(button("Save changes"));
         await waitFor(() =>
            expect(updateModelSource).toHaveBeenCalledTimes(1),
         );
         expect(updateModelSource.mock.calls[0][3].source).toBe(
            `${file}\nrun: a -> by_cat\n`,
         );

         // A second look reads nothing again.
         await screen.findByRole("group", {
            name: "Cell 3, query",
            hidden: true,
         });
         fireEvent.click(
            within(
               screen.getByRole("group", {
                  name: "Cell 3, query",
                  hidden: true,
               }),
            ).getByRole("button", { name: "Add query below", hidden: true }),
         );
         await settle();
         expect(importReads()).toBe(1);
      });

   it("names an import that could not be read, once", async () => {
      const query = await open("broken");
      fireEvent.click(
         within(query).getByRole("button", {
            name: "Add query below",
            hidden: true,
         }),
      );
      expect(
         await screen.findAllByText(/Could not read gone\.malloy/),
      ).not.toHaveLength(0);
      expect(screen.queryByText(/reads no source/)).toBeNull();
      expect(
         getModel.mock.calls.filter((call) => call[2] === "gone.malloy"),
      ).toHaveLength(1);
   });
});
