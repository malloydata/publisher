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
import * as fs from "fs";
import * as path from "path";
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
import type { BuilderEvent } from "./telemetry";
import { editInline } from "./testing/inline";

/**
 * The one editor opened as a notebook: how it reads the file, where it takes
 * the manifest from, and what it does with a notebook in the cell format.
 */
const REPO = path.resolve(import.meta.dir, "../../../../..");
const LEGACY = fs
   .readFileSync(
      path.join(REPO, "examples/storefront/notebooks/category-review.malloy"),
      "utf8",
   )
   .replace(/\r\n/g, "\n");

const LAYOUT = `## artifact { kind=notebook title="Tour" tiles=[intro { kind=text }] }

##|(markdown) intro
Hello.
|##
`;

const REFUSED = `## artifact { kind=notebook }
import { orders } from "../m.malloy"

run: orders extend { dimension: z is 1 } -> { select: z }
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
const getNotebook = mock(
   async (_env: string, _pkg: string, modelPath: string) => ({
      data: served.includes("tiles=")
         ? {
              dashboard: {
                 path: modelPath,
                 kind: "notebook",
                 givens: [{ name: "FROM_MANIFEST", type: "string" }],
                 tiles: [],
              },
           }
         : {},
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
      notebooks: { getNotebook },
      dashboards: {
         getDashboard,
         listDashboards: mock(async () => ({ data: [] })),
      },
   },
   serverContext,
);

const { DashboardEditor } = await import("./DashboardEditor");
const { NotebookEditor } = await import("./NotebookEditor");

const button = (name: string | RegExp) =>
   screen.getByRole("button", { name, hidden: true });

const mount = (
   props: Partial<Parameters<typeof DashboardEditor>[0]> & {
      dashboardName?: string;
   } = {},
   onEvent?: (event: BuilderEvent) => void,
) =>
   render(
      <DashboardEditor
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
   getNotebook.mockClear();
   getDashboard.mockClear();
   updateModelSource.mockClear();
});

describe("DashboardEditor as a notebook", () => {
   it("reads the file with the authoring flag, and the manifest from the notebook read", async () => {
      const onEvent = mock((_event: BuilderEvent) => {});
      mount({}, onEvent);
      expect(await screen.findByText("Tour")).toBeDefined();
      expect(getModel.mock.calls[0].slice(0, 3)).toEqual([
         "env",
         "pkg",
         "notebooks/tour.malloy",
      ]);
      expect(getModel.mock.calls[0][4]).toBe(true);
      await waitFor(() => expect(getNotebook).toHaveBeenCalled());
      expect(getDashboard).not.toHaveBeenCalled();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.opened",
         from: "package",
         cells: 1,
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

   it("refuses a .malloynb notebook without fetching it", async () => {
      const onEvent = mock((_event: BuilderEvent) => {});
      mount({ dashboardName: "old.malloynb" }, onEvent);
      expect(
         await screen.findByText(/a \.malloynb notebook is read, not edited/),
      ).toBeDefined();
      expect(getModel).not.toHaveBeenCalled();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.open_refused",
      });
   });

   it("offers a notebook in the cell format as a conversion, and Save writes it exactly", async () => {
      served = LEGACY;
      servedHash = await sha256Hex(LEGACY);
      const onEvent = mock((_event: BuilderEvent) => {});
      mount({}, onEvent);
      expect(
         await screen.findByText(/This notebook is in the cell format/),
      ).toBeDefined();
      expect(screen.getByLabelText("Tile revenue_by_month")).toBeDefined();

      fireEvent.click(button("Save"));
      fireEvent.click(
         await screen.findByRole("button", { name: "Convert and save" }),
      );
      await waitFor(() => expect(writes).toHaveLength(1));
      expect(writes[0]).toContain("tiles=[\n    text_1 { kind=text }");
      expect(writes[0]).toContain("view: revenue_by_month is sales_by_month");
      await waitFor(() =>
         expect(onEvent.mock.calls.map(([event]) => event.type)).toEqual([
            "notebook.opened",
            "notebook.saved",
         ]),
      );
      expect(onEvent.mock.calls[1][0]).toMatchObject({ converted: true });
   });

   it("says why a notebook cannot be converted, naming the line", async () => {
      served = REFUSED;
      const onEvent = mock((_event: BuilderEvent) => {});
      mount({}, onEvent);
      const alert = await screen.findByText(/cannot be opened in the builder/);
      expect(alert.textContent).toMatch(/line 4/i);
      expect(
         screen.queryByText(/This notebook is in the cell format/),
      ).toBeNull();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.open_refused",
      });
   });
});

describe("DashboardEditor as a notebook: the host's record", () => {
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
         this.saved.set(locator.path, content);
      }
      async deleteDocument(): Promise<void> {}
      async moveDocument(): Promise<void> {}
   }

   it("reads and writes it under the notebook type", async () => {
      const store = new Store(LAYOUT.replace("Hello.", "From the record."));
      render(
         <DocumentStorageProvider documentStorage={store}>
            <DashboardEditor
               kind="notebook"
               environmentName="env"
               packageName="pkg"
               dashboardName="tour"
            />
         </DocumentStorageProvider>,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("From the record.")).toBeDefined();
      expect(store.types).toEqual(["notebook"]);
      editInline("Tour", "Notebook title", "Tour, again");
      fireEvent.click(button("Save"));
      await waitFor(() => expect(store.saved.size).toBe(1));
      expect(store.saved.get("env/pkg/notebooks/tour.malloy")).toContain(
         'title="Tour, again"',
      );
      // The record took the write; the package file was left alone.
      expect(updateModelSource).not.toHaveBeenCalled();
   });
});
