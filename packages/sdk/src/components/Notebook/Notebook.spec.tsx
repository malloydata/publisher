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
import { beforeEach, expect, it, mock } from "bun:test";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { RawNotebook } from "../../client";

const NOTEBOOK: RawNotebook = {
   notebookCells: [
      {
         type: "code",
         text: "import { orders } from './orders.malloy'",
         // `newSources` is what makes the cell's "Data sources" icon appear;
         // see `NotebookCell.tsx`'s `hasValidImport` check.
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
            // Plain text, unlike REGION: the "Data sources" dialog test below
            // needs a control `fireEvent.change` can drive directly, rather
            // than a select whose options load from a suggest query.
            { name: "TENANT", type: "string" },
         ],
      },
   ],
};

const getNotebook = mock(
   (
      _environmentName: string,
      _packageName: string,
      _notebookPath: string,
      _versionId?: string,
   ) => Promise.resolve({ data: NOTEBOOK }),
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
// `executeQueryModel`: without it, the cell this spec adds (for the "Data
// sources" dialog) fails with "not a function" on every render.
const executeNotebookCell = mock(() => pending());

// Stubbed rather than let the cell's "Data sources" dialog reach the real
// ModelExplorer: that pulls in the lazy-loaded, WASM-backed
// `@malloydata/malloy-explorer`, which is not this file's business. Only what
// the notebook hands it matters here, so the stub just records its props.
const dataSourcesDialogProps = mock(
   (_props: {
      open: boolean;
      startingGivens?: Record<string, string>;
      data?: { givens?: { name?: string }[] };
   }) => {},
);
mock.module("../Model/ModelExplorerDialog", () => ({
   ModelExplorerDialog: (props: {
      open: boolean;
      startingGivens?: Record<string, string>;
      data?: { givens?: { name?: string }[] };
   }) => {
      dataSourcesDialogProps(props);
      return null as ReactNode;
   },
}));

mockServerProvider({
   notebooks: { getNotebook, executeNotebookCell },
   models: { executeQueryModel },
});

const { default: Notebook } = await import("./Notebook");

const URI =
   "publisher://environments/env/packages/pkg/models/notebooks/ops.malloynb";

beforeEach(() => {
   clearCache();
   getNotebook.mockClear();
   executeQueryModel.mockClear();
   executeNotebookCell.mockClear();
   dataSourcesDialogProps.mockClear();
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

it("opens a cell's 'Data sources' dialog with the notebook's current values", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   const input = (await screen.findByLabelText("TENANT")) as HTMLInputElement;
   fireEvent.change(input, { target: { value: "acme" } });
   // Wait for the control's own re-render before opening the dialog: without
   // it the click can land on the render that has not yet carried the new
   // value into `cellStartingGivens`.
   await waitFor(() => expect(input.value).toBe("acme"));

   fireEvent.click(await screen.findByLabelText("Data sources"));

   await waitFor(() =>
      expect(dataSourcesDialogProps.mock.calls.at(-1)?.[0]?.open).toBe(true),
   );
   const lastCall = dataSourcesDialogProps.mock.calls.at(-1)?.[0];
   expect(lastCall?.startingGivens).toEqual({ TENANT: "acme" });
   // The values alone render nothing: the dialog needs the declarations too.
   expect(lastCall?.data?.givens?.map((given) => given.name)).toContain(
      "TENANT",
   );
});
