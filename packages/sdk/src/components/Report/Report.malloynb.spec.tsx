// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).

/**
 * What a `.malloynb`'s cells offer around their results, drawn by `Report`:
 * the cell's "Data sources" dialog, which is the Malloy Explorer and so is
 * offered only where the server's model page allows it and opens on the
 * report's current values; the copy-link icon on a leading markdown cell; and
 * suggest queries pinned to the version the report was opened at.
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
import type { ReportManifest } from "../../client";

const PATH = "notebooks/ops.malloynb";

const NOTEBOOK = {
   path: PATH,
   name: "ops",
   layout: "linear",
   editable: false,
   notebook: {
      format: "malloynb",
      notebookCells: [
         { type: "markdown", text: "Ops prose" },
         {
            type: "code",
            text: "import { orders } from './orders.malloy'",
            // What makes the cell's "Data sources" icon appear.
            newSources: [JSON.stringify({ name: "orders" })],
         },
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
               // Plain text, so the dialog test can type a value directly.
               { name: "TENANT", type: "string" },
            ],
         },
      ],
   },
} as ReportManifest;

let manifest: ReportManifest = NOTEBOOK;
const getReport = mock(() => Promise.resolve({ data: manifest }));
const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string },
   ): Promise<never> => pending(),
);

// The Explorer itself is lazy, WASM-backed and not under test: only what the
// cell hands it is, so the stub records its props.
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

// The server's /status answer: the model page a test switches to `viewer`.
const serverContext: { modelPage?: string; isLoadingStatus?: boolean } = {};
mockServerProvider(
   {
      reports: { getReport },
      models: { executeQueryModel, getModel: () => pending() },
   },
   serverContext,
);

// Imported after the stub is registered: a static import would hoist above it.
const { Report } = await import("./Report");

const URI = "publisher://environments/env/packages/pkg";

beforeEach(() => {
   manifest = NOTEBOOK;
   clearCache();
   getReport.mockClear();
   executeQueryModel.mockClear();
   dataSourcesDialogProps.mockClear();
});

it("runs its suggest queries on the notebook's path, at the version it was opened at", async () => {
   render(<Report resourceUri={`${URI}?versionId=v2`} path={PATH} />, {
      wrapper: serverWrapper,
   });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][2]).toBe(PATH);
   expect(executeQueryModel.mock.calls[0][3].versionId).toBe("v2");
   expect(cacheKeys("givenSuggest")[0]).toContain('"v2"');
});

it("sends no version on a suggest query when the report was opened without one", async () => {
   render(<Report resourceUri={URI} path={PATH} />, {
      wrapper: serverWrapper,
   });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBeUndefined();
});

it("opens a cell's 'Data sources' dialog with the report's current values", async () => {
   render(<Report resourceUri={URI} path={PATH} />, {
      wrapper: serverWrapper,
   });

   // The control sits in its filter button's popover.
   fireEvent.click(await screen.findByRole("button", { name: /^TENANT: / }));
   const input = (await screen.findByLabelText("TENANT")) as HTMLInputElement;
   fireEvent.change(input, { target: { value: "acme" } });
   // The control re-renders with the value before the dialog opens, or the
   // click lands on a render that has not carried it yet.
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

// A server with PUBLISHER_MODEL_PAGE=viewer takes the Explorer out of the
// Console, and the "Data sources" dialog is the Explorer.
it("offers no 'Data sources' dialog on a server whose model page is the viewer", async () => {
   serverContext.modelPage = "viewer";
   try {
      render(<Report resourceUri={URI} path={PATH} />, {
         wrapper: serverWrapper,
      });
      await screen.findByText("Ops prose");
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(screen.queryByLabelText("Data sources")).toBeNull();
      expect(dataSourcesDialogProps).not.toHaveBeenCalledWith(
         expect.objectContaining({ open: true }),
      );
   } finally {
      delete serverContext.modelPage;
   }
});

// Nor while /status loads: until the server has said which page it shows, a
// viewer server must not offer the Explorer even briefly.
it("offers no 'Data sources' dialog while /status loads", async () => {
   serverContext.isLoadingStatus = true;
   try {
      render(<Report resourceUri={URI} path={PATH} />, {
         wrapper: serverWrapper,
      });
      await screen.findByText("Ops prose");
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(screen.queryByLabelText("Data sources")).toBeNull();
   } finally {
      delete serverContext.isLoadingStatus;
   }
});

it("offers the 'Data sources' dialog once the server allows it", async () => {
   render(<Report resourceUri={URI} path={PATH} />, {
      wrapper: serverWrapper,
   });
   expect(await screen.findByLabelText("Data sources")).toBeTruthy();
});

it("puts the copy-link icon on a leading markdown cell", async () => {
   render(<Report resourceUri={URI} path={PATH} />, {
      wrapper: serverWrapper,
   });

   await screen.findByText("Ops prose");
   expect(screen.getAllByTestId("LinkOutlinedIcon")).toHaveLength(1);
   expect(screen.getByRole("button", { name: "Copy link" })).toBeTruthy();
});

it("keeps the copy-link icon off a notebook that opens with a code cell", async () => {
   manifest = {
      ...NOTEBOOK,
      notebook: {
         format: "malloynb",
         notebookCells: [
            { type: "code", text: "import { orders } from './orders.malloy'" },
            { type: "markdown", text: "Later prose" },
         ],
      },
   } as ReportManifest;
   render(<Report resourceUri={URI} path={PATH} />, {
      wrapper: serverWrapper,
   });

   await screen.findByText("Later prose");
   expect(screen.queryAllByTestId("LinkOutlinedIcon")).toHaveLength(0);
});
