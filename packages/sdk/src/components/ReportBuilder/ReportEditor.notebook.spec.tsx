// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import { sha256Hex } from "../../utils/sha256";
import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type DocumentType,
   type Workspace,
} from "../DocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";
import type { ReportEvent } from "../Report/telemetry";
import { editInline } from "./testing/inline";

/**
 * The one editor opened as a notebook: how it reads the file, where it takes
 * the manifest from.
 */
const LAYOUT = `## artifact { layout=linear title="Tour" tiles=[intro { kind=text }] }

##|(markdown) intro
Hello.
|##
`;

let served = LAYOUT;
let servedHash = await sha256Hex(LAYOUT);
let writes: string[] = [];

const getModel = mock(
   async (
      _env: string,
      _pkg: string,
      modelPath: string,
      _versionId?: string,
      _authoring?: boolean,
   ) => ({
      data: {
         modelPath,
         sourceText: modelPath.endsWith("tour.malloy") ? served : served,
         givens: [{ name: "FROM_MODEL", type: "string" }],
      },
   }),
);
// One read for either layout, by path: the report's manifest.
const getReport = mock(
   async (_env: string, _pkg: string, modelPath: string) => ({
      data: {
         path: modelPath,
         layout: "linear",
         givens: [{ name: "FROM_MANIFEST", type: "string" }],
         tiles: [],
      },
   }),
);
const getDashboard = mock(() => pending());
const listModels = mock(async () => ({ data: [] }));
const executeQueryModel = mock(() => pending());
const updateModelSource = mock(
   async (
      _env: string,
      _pkg: string,
      modelPath: string,
      body: { source: string; expectedHash?: string },
   ) => {
      if (body.expectedHash !== servedHash)
         throw { response: { status: 409, data: { message: "changed" } } };
      served = body.source;
      writes.push(body.source);
      servedHash = `hash-${writes.length}`;
      return { data: { path: modelPath, contentHash: servedHash } };
   },
);

const serverContext: {
   mutable: boolean | undefined;
   isLoadingStatus: boolean;
} = { mutable: true, isLoadingStatus: false };
mockServerProvider(
   {
      models: { getModel, listModels, executeQueryModel, updateModelSource },
      reports: {
         getReport,
         listReports: mock(async () => ({ data: [] })),
      },
      dashboards: { getDashboard },
   },
   serverContext,
);

const { ReportEditor } = await import("./ReportEditor");
const { NotebookEditor } = await import("../legacy/NotebookEditor");

const button = (name: string | RegExp) =>
   screen.getByRole("button", { name, hidden: true });

const mount = (
   props: Partial<Parameters<typeof ReportEditor>[0]> & {
      dashboardName?: string;
   } = {},
   onEvent?: (event: ReportEvent) => void,
) =>
   render(
      <ReportEditor
         kind="notebook"
         environmentName="env"
         packageName="pkg"
         dashboardName="tour"
         {...(onEvent ? { onEvent } : {})}
         {...(props as object)}
      />,
      { wrapper: serverWrapper },
   );

beforeEach(async () => {
   cleanup();
   clearCache();
   localStorage.clear();
   served = LAYOUT;
   servedHash = await sha256Hex(LAYOUT);
   writes = [];
   serverContext.mutable = true;
   getModel.mockClear();
   getReport.mockClear();
   getDashboard.mockClear();
   updateModelSource.mockClear();
});

describe("ReportEditor as a notebook", () => {
   it("reads the file with the authoring flag, and the manifest from the report read", async () => {
      const onEvent = mock((_event: ReportEvent) => {});
      mount({}, onEvent);
      expect(await screen.findByText("Tour")).toBeDefined();
      expect(getModel.mock.calls[0].slice(0, 3)).toEqual([
         "env",
         "pkg",
         "notebooks/tour.malloy",
      ]);
      expect(getModel.mock.calls[0][4]).toBe(true);
      await waitFor(() => expect(getReport).toHaveBeenCalled());
      expect(getReport.mock.calls[0][2]).toBe("notebooks/tour.malloy");
      expect(getDashboard).not.toHaveBeenCalled();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "report.opened",
         surface: "edit",
         layout: "linear",
         from: "package",
         tiles: 1,
      });
   });

   it("opens the file where the host says it is, not where its kind would put it", async () => {
      mount({ path: "dashboards/tour.malloy" });
      expect(await screen.findByText("Tour")).toBeDefined();
      expect(getModel.mock.calls[0][2]).toBe("dashboards/tour.malloy");
   });

   it("opens the file NotebookEditor is told it is at", async () => {
      render(
         <NotebookEditor
            environmentName="env"
            packageName="pkg"
            notebookName="tour"
            path="dashboards/tour.malloy"
         />,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("Tour")).toBeDefined();
      expect(getModel.mock.calls[0][2]).toBe("dashboards/tour.malloy");
   });

   // DEPRECATED FEATURE COVERAGE — delete together with
   // `.malloynb` notebooks (see RELEASE_NOTES deprecations).
   describe("deprecated: .malloynb notebooks", () => {
      it("refuses a .malloynb notebook without fetching it", async () => {
         const onEvent = mock((_event: ReportEvent) => {});
         mount({ dashboardName: "old.malloynb" }, onEvent);
         expect(
            await screen.findByText(
               /a \.malloynb notebook is read, not edited/,
            ),
         ).toBeDefined();
         expect(getModel).not.toHaveBeenCalled();
         expect(onEvent.mock.calls[0][0]).toMatchObject({
            type: "report.load_failed",
            surface: "edit",
            category: "unsupported",
         });
      });
   });
});

describe("ReportEditor as a notebook: the host's record", () => {
   const RECORD: Workspace = {
      name: "Branch",
      writeable: true,
      description: "Saved to the draft branch",
      authoritative: true,
   };
   class Store implements DocumentStorage {
      readonly types: DocumentType[] = [];
      readonly saved = new Map<string, string>();
      constructor(private readonly record: string | undefined) {}
      async listWorkspaces(): Promise<Workspace[]> {
         return [RECORD];
      }
      async listDocuments(): Promise<DocumentLocator[]> {
         return [];
      }
      async getDocument(locator: DocumentLocator): Promise<string> {
         this.types.push(locator.type);
         if (this.record === undefined)
            throw new DocumentNotFoundError(locator.path);
         return this.record;
      }
      async saveDocument(locator: DocumentLocator, content: string) {
         this.types.push(locator.type);
         this.saved.set(locator.path, content);
      }
      async deleteDocument(): Promise<void> {}
      async moveDocument(): Promise<void> {}
   }

   it("reads it with the linear layout the deprecated kind names, and records that layout on a save", async () => {
      const store = new Store(LAYOUT.replace("Hello.", "From the record."));
      render(
         <DocumentStorageProvider documentStorage={store}>
            <ReportEditor
               kind="notebook"
               environmentName="env"
               packageName="pkg"
               dashboardName="tour"
            />
         </DocumentStorageProvider>,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("From the record.")).toBeDefined();
      expect(store.types).toEqual(["linear"]);
      editInline("Tour", "Report title", "Tour, again");
      fireEvent.click(button("Save"));
      await waitFor(() => expect(store.saved.size).toBe(1));
      expect(store.types).toEqual(["linear", "linear"]);
      expect(store.saved.get("env/pkg/notebooks/tour.malloy")).toContain(
         'title="Tour, again"',
      );
      // The record took the write; the package file was left alone.
      expect(updateModelSource).not.toHaveBeenCalled();
   });
});
