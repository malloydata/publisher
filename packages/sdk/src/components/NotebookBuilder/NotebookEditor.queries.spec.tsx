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

const getModel = mock(
   async (
      _env: string,
      _pkg: string,
      path: string,
      _versionId?: string,
      _hidden?: boolean,
   ) => ({
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
   }),
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
      // The dialog read the cached model: no request of its own.
      expect([
         getModel.mock.calls.length,
         getNotebook.mock.calls.length,
      ]).toEqual(requests);

      fireEvent.click(button("Save changes"));
      const diff = (await screen.findByLabelText("File changes")).closest(
         '[role="dialog"]',
      ) as HTMLElement;
      fireEvent.click(
         within(diff).getByRole("button", { name: /Save/, hidden: true }),
      );
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
