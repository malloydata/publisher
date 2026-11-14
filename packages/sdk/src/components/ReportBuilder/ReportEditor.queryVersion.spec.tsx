// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
   TEST_SERVER,
} from "../../../test/serverProvider";
import { BrowserDocumentStorage } from "../DocumentStorage/BrowserDocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";
import { editInline } from "./testing/inline";
import { reportDraftKey } from "./useReportDraft";

/**
 * `queryVersionId`: the version the builder's queries run at, apart from the
 * document's identity. A host whose package republishes on every change opens
 * the builder on an unversioned `resourceUri` with its reader's version here,
 * so the builder reuses the reader's results while the kept draft, the save
 * target and the save caption stay with the unversioned document.
 */
const REPORT = "dashboards/overview.malloy";
const PACKAGE_FILE = `## artifact { title="Storefront" tiles=["a -> by_cat"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category
}`;

const getModel = mock(
   (_env: string, _pkg: string, path: string, _versionId?: string) =>
      Promise.resolve({
         data:
            path === REPORT
               ? { modelPath: path, sourceText: PACKAGE_FILE }
               : {
                    modelPath: path,
                    modelInfo: JSON.stringify({
                       entries: [{ kind: "source", name: "scoped_orders" }],
                    }),
                    sources: [
                       {
                          name: "scoped_orders",
                          views: [{ name: "by_category" }],
                       },
                    ],
                    sourceInfos: [
                       JSON.stringify({
                          name: "scoped_orders",
                          schema: { fields: [] },
                       }),
                    ],
                 },
      }),
);
const getReport = mock(
   (
      _env: string,
      _pkg: string,
      _path: string,
      _versionId?: string,
      _options?: unknown,
   ) =>
      Promise.resolve({
         data: {
            path: REPORT,
            name: "overview",
            title: "Storefront",
            layout: "grid",
            givens: [],
            tiles: [{ kind: "query", query: "a -> by_cat", givenNames: [] }],
         },
      }),
);
const listModels = mock((_env: string, _pkg: string, _versionId?: string) =>
   Promise.resolve({ data: [{ path: "data_app.malloy" }] }),
);
const listReports = mock((_env: string, _pkg: string, _versionId?: string) =>
   Promise.resolve({
      data: [{ path: REPORT, name: "overview", layout: "grid" }],
   }),
);
const updateModelSource = mock(
   (_env: string, _pkg: string, path: string, body: { source: string }) =>
      Promise.resolve({
         data: {
            path,
            contentHash: `hash-of-${body.source.length}`,
            created: false,
         },
      }),
);
// Answers by default, so the reader's tiles land in the cache; a test that
// wants nothing to settle swaps in `pending`.
let answer: () => Promise<unknown> = () =>
   Promise.resolve({ data: { result: "{}" } });
const executeQueryModel = mock(
   (
      _env: string,
      _pkg: string,
      _path: string,
      _body: { query?: string; givens?: unknown; versionId?: string },
   ) => answer(),
);

const serverContext = { mutable: false };
mockServerProvider(
   {
      models: { getModel, executeQueryModel, updateModelSource, listModels },
      reports: { getReport, listReports },
      notebooks: { getNotebook: () => pending() },
   },
   serverContext,
);

const { ReportEditor } = await import("./ReportEditor");
const { Report } = await import("../Report/Report");

const URI = "publisher://environments/env/packages/pkg";

const mountEditor = (
   props: { queryVersionId?: string; keepDraft?: boolean } = {},
   resourceUri = URI,
) =>
   render(
      <DocumentStorageProvider documentStorage={new BrowserDocumentStorage()}>
         <ReportEditor resourceUri={resourceUri} path={REPORT} {...props} />
      </DocumentStorageProvider>,
      { wrapper: serverWrapper },
   );

// The tile, as the reader and an untouched builder tile both ask for it.
const tileCalls = () =>
   executeQueryModel.mock.calls.filter((call) =>
      call[3].query?.includes("a -> by_cat"),
   );

beforeEach(() => {
   clearCache();
   localStorage.clear();
   for (const fn of [
      getModel,
      getReport,
      listModels,
      listReports,
      updateModelSource,
      executeQueryModel,
   ])
      fn.mockClear();
   answer = () => Promise.resolve({ data: { result: "{}" } });
   serverContext.mutable = false;
});

describe("queryVersionId", () => {
   it("serves the builder's tile from the result the reader cached at that version", async () => {
      const reader = render(
         <Report resourceUri={`${URI}?versionId=v7`} path={REPORT} />,
         { wrapper: serverWrapper },
      );
      await waitFor(() => expect(tileCalls()).toHaveLength(1));
      expect(tileCalls()[0][3].versionId).toBe("v7");
      await waitFor(() =>
         expect(
            cacheKeys("queryResult").some((key) => key.includes('"v7"')),
         ).toBe(true),
      );
      reader.unmount();

      mountEditor({ queryVersionId: "v7" });
      expect(await screen.findByText("Storefront")).toBeDefined();
      // Everything the builder reads besides the file itself has answered,
      // so a tile that was going to run has run by now.
      await waitFor(() =>
         expect(
            getModel.mock.calls.some((call) => call[2] === "data_app.malloy"),
         ).toBe(true),
      );
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(tileCalls()).toHaveLength(1);
      // Nothing was asked unversioned: no second copy of the tile's result.
      expect(
         cacheKeys("queryResult").every((key) => key.includes('"v7"')),
      ).toBe(true);
   });

   it("runs every query at that version, and reads the document unversioned", async () => {
      answer = () => pending();
      mountEditor({ queryVersionId: "v7" });
      expect(await screen.findByText("Storefront")).toBeDefined();
      await waitFor(() =>
         expect(
            getModel.mock.calls.some((call) => call[2] === "data_app.malloy"),
         ).toBe(true),
      );
      await waitFor(() => expect(tileCalls().length).toBeGreaterThan(0));

      // The document: the file the editor opens and saves.
      expect(getModel.mock.calls.find((call) => call[2] === REPORT)).toEqual([
         "env",
         "pkg",
         REPORT,
         undefined,
      ]);
      // The queries: the manifest, the listing, the catalog and the tiles.
      expect(getReport.mock.calls[0].slice(0, 4)).toEqual([
         "env",
         "pkg",
         REPORT,
         "v7",
      ]);
      expect(listReports.mock.calls[0]).toEqual(["env", "pkg", "v7"]);
      expect(listModels.mock.calls[0]).toEqual(["env", "pkg", "v7"]);
      expect(
         getModel.mock.calls.find((call) => call[2] === "data_app.malloy"),
      ).toEqual(["env", "pkg", "data_app.malloy", "v7"]);
      for (const call of tileCalls()) expect(call[3].versionId).toBe("v7");
   });

   it("keeps the draft under the document's key, not the query version's", async () => {
      answer = () => pending();
      mountEditor({ queryVersionId: "v7", keepDraft: true });
      await screen.findByText("Storefront");
      editInline("By category", "Tile title", "Categories");

      const unversioned = reportDraftKey({
         server: TEST_SERVER,
         environmentName: "env",
         packageName: "pkg",
         path: REPORT,
      });
      await waitFor(() =>
         expect(
            Object.keys(localStorage).some((key) => key.endsWith(unversioned)),
         ).toBe(true),
      );
      expect(
         Object.keys(localStorage).some((key) => key.includes("@v7/")),
      ).toBe(false);
   });

   it("saves to the package as an unpinned document, with no pinned-version caption", async () => {
      answer = () => pending();
      serverContext.mutable = true;
      mountEditor({ queryVersionId: "v7" });
      await screen.findByText("Storefront");
      expect(screen.queryByText(/Reading version/)).toBeNull();

      editInline("By category", "Tile title", "Categories");
      fireEvent.click(
         screen.getByRole("button", { name: "Save", hidden: true }),
      );
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      expect(updateModelSource.mock.calls[0].slice(0, 3)).toEqual([
         "env",
         "pkg",
         REPORT,
      ]);
   });

   it("unset, runs the queries at the resourceUri's version, pinned as before", async () => {
      answer = () => pending();
      serverContext.mutable = true;
      mountEditor({}, `${URI}?versionId=v7`);
      await screen.findByText("Storefront");
      await waitFor(() => expect(tileCalls().length).toBeGreaterThan(0));

      expect(getModel.mock.calls.find((call) => call[2] === REPORT)?.[3]).toBe(
         "v7",
      );
      for (const call of tileCalls()) expect(call[3].versionId).toBe("v7");
      expect(
         screen.getByText(
            "Reading version v7: a version is a fixed point in history, so Save is off.",
         ),
      ).toBeDefined();
   });

   it("unset with no version anywhere, asks every query unversioned", async () => {
      answer = () => pending();
      mountEditor();
      await screen.findByText("Storefront");
      await waitFor(() => expect(tileCalls().length).toBeGreaterThan(0));
      for (const call of tileCalls()) expect(call[3].versionId).toBeUndefined();
      expect(getReport.mock.calls[0][3]).toBeUndefined();
   });
});
