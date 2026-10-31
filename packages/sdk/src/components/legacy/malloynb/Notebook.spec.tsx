// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A notebook's suggest queries are version-scoped like the rest of it.
 *
 * The notebook already versioned its own fetch and its cell execution while
 * its dropdowns ran unversioned, so a versioned notebook filled its controls
 * from whatever the latest version happened to hold. The forwarding is a bare
 * trailing optional argument, which is the kind that goes missing without a
 * test noticing, so it is asserted here rather than only on the hook.
 */
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { render, screen, waitFor } from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../../test/serverProvider";
import type { RawNotebook } from "../../../client";

const NOTEBOOK: RawNotebook = {
   notebookCells: [
      {
         type: "code",
         text: "import { orders } from './orders.malloy'",
         // An import cell with `newSources` is where the "Data sources" icon
         // used to appear; the test below asserts it no longer does.
         newSources: [JSON.stringify({ name: "orders" })],
      } as RawNotebook["notebookCells"][number],
   ],
   sources: [
      {
         name: "orders",
         givens: [
            {
               name: "REGION",
               control: "select",
               suggest: { source: "orders", dimension: "region" },
            },
            { name: "TENANT", type: "string" },
         ],
      },
   ],
};

let current: RawNotebook = NOTEBOOK;

const getNotebook = mock(
   (
      _environmentName: string,
      _packageName: string,
      _notebookPath: string,
      _versionId?: string,
   ) => Promise.resolve({ data: current }),
);
const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string },
   ) => pending(),
);
// The notebook's real run path for a code cell, distinct from
// `executeQueryModel`: without it, the import cell this spec adds fails with
// "not a function" on every render.
const executeNotebookCell = mock(
   (
      _environmentName: string,
      _packageName: string,
      _notebookPath: string,
      _cellIndex: number,
   ) => pending(),
);

mockServerProvider({
   notebooks: { getNotebook, executeNotebookCell },
   models: { executeQueryModel },
});

const { default: Notebook } = await import("./Notebook");

const URI =
   "publisher://environments/env/packages/pkg/models/notebooks/ops.malloynb";

beforeEach(() => {
   current = NOTEBOOK;
   clearCache();
   getNotebook.mockClear();
   executeQueryModel.mockClear();
   executeNotebookCell.mockClear();
});

it("runs its suggest queries against the version it was opened at", async () => {
   render(<Notebook resourceUri={`${URI}?versionId=v2`} />, {
      wrapper: serverWrapper,
   });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBe("v2");
   expect(cacheKeys("givenSuggest")[0]).toContain('"v2"');
});

it("sends none when the notebook was opened without one", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBeUndefined();
});

// An import cell used to open the Explorer on its sources. The Explorer wrote a
// drilled value into Malloy unescaped, so it is gone, and with it the button.
it("offers no 'Data sources' explorer on an import cell", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(screen.queryByLabelText("Data sources")).toBeNull();
});

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).
describe("deprecated: .malloynb notebooks", () => {
   it("renders a .malloynb as before: runs every code cell, no caption, no folded definition", async () => {
      current = {
         notebookCells: [
            { type: "markdown", text: "Prose" },
            { type: "code", text: "run: a -> b" },
            { type: "code", text: '#" x\nrun: a -> c' },
         ],
      } as RawNotebook;
      render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

      await screen.findByText("Prose");
      await waitFor(() => expect(executeNotebookCell).toHaveBeenCalledTimes(2));
      expect(executeNotebookCell.mock.calls.map((call) => call[3])).toEqual([
         1, 2,
      ]);
      expect(screen.queryByText("x")).toBeNull();
      expect(
         screen.queryByRole("button", { name: /^(run|source|import)/ }),
      ).toBeNull();
   });
});
