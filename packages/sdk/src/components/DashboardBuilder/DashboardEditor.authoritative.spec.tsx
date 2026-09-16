// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
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
import { globalQueryClient } from "../../utils/queryClient";
import { sha256Hex } from "../../utils/sha256";
import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type DocumentType,
   type Workspace,
} from "../DocumentStorage";
import { BrowserDocumentStorage } from "../DocumentStorage/BrowserDocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";
import type { DashboardEvent } from "../Dashboard/telemetry";

/**
 * The editor against a host whose store is the record, and against the same
 * store when it is not: what it opens, what it hands back as `expectedHash`,
 * what it does with a read or a delete the backend could not answer, and what
 * happens to a reader's edits when a new version of the file arrives.
 *
 * The gate is proved by the SAME fixture with and without `authoritative`,
 * because the specs the Console already has cannot see it: `BrowserDocumentStorage`
 * never declares itself the record, so they exercise one side of every branch.
 */
const PACKAGE_FILE = `## artifact { title="Storefront" tiles=["a -> by_cat"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category
}`;
const withTitle = (title: string) =>
   PACKAGE_FILE.replace('title="Storefront"', `title="${title}"`);

/** The file as the server has it, so a save is a real compare-and-swap. */
let serverText = PACKAGE_FILE;

const getModel = mock(async (_env: string, _pkg: string, path: string) => {
   // Not instant, because the defect these specs are about is a local parse
   // of stale text winning the race against the refetch that would correct it.
   // A mock that resolves in the same tick removes the race and the proof.
   await new Promise((resolve) => setTimeout(resolve, 60));
   return {
      data:
         path === "dashboards/overview.malloy"
            ? { modelPath: path, sourceText: serverText }
            : { modelPath: path, sources: [], sourceInfos: [] },
   };
});
const updateModelSource = mock(
   async (
      _env: string,
      _pkg: string,
      path: string,
      body: { source: string; expectedHash?: string },
   ) => {
      if (body.expectedHash !== (await sha256Hex(serverText)))
         throw {
            response: {
               data: {
                  message:
                     "`dashboards/overview.malloy` changed in the package since you opened it.",
               },
            },
         };
      serverText = body.source;
      return {
         data: {
            path,
            contentHash: await sha256Hex(serverText),
            created: false,
         },
      };
   },
);

// Spread into `useServer()` on every render, so a test can change what the
// server says about itself between mounts.
const serverContext = { mutable: false };
mockServerProvider(
   {
      models: {
         getModel,
         executeQueryModel: mock(() => pending()),
         updateModelSource,
      },
      dashboards: {
         getDashboard: mock(() =>
            Promise.resolve({
               data: {
                  path: "dashboards/overview.malloy",
                  givens: [],
                  tiles: [],
               },
            }),
         ),
         listDashboards: mock(() =>
            Promise.resolve({ data: [{ name: "overview" }] }),
         ),
      },
   },
   serverContext,
);

const { DashboardEditor } = await import("./DashboardEditor");

const PATH = "env/pkg/dashboards/overview.malloy";
const RECORD: Workspace = {
   name: "Branch",
   writeable: true,
   description: "Saved to the draft branch",
   authoritative: true,
};
const BESIDE: Workspace = {
   name: "Branch",
   writeable: true,
   description: "Saved to the draft branch",
};

/**
 * A host store the spec can break on purpose: a read or a delete that rejects
 * with something other than absence is the case the editor used to swallow.
 */
class FakeStorage implements DocumentStorage {
   readonly documents = new Map<string, string>();
   readFailure: unknown;
   deleteFailure: unknown;
   constructor(private readonly workspace: Workspace) {}

   async listWorkspaces(): Promise<Workspace[]> {
      return [this.workspace];
   }
   async listDocuments(
      _workspace: Workspace,
      _type?: DocumentType,
   ): Promise<DocumentLocator[]> {
      return [...this.documents.keys()].map((path) => ({
         workspace: this.workspace.name,
         type: "dashboard" as const,
         path,
      }));
   }
   async getDocument(locator: DocumentLocator): Promise<string> {
      if (this.readFailure !== undefined) throw this.readFailure;
      const content = this.documents.get(locator.path);
      if (content === undefined)
         throw new DocumentNotFoundError(`No dashboard at ${locator.path}`);
      return content;
   }
   async saveDocument(
      locator: DocumentLocator,
      content: string,
   ): Promise<void> {
      this.documents.set(locator.path, content);
   }
   async deleteDocument(locator: DocumentLocator): Promise<void> {
      if (this.deleteFailure !== undefined) throw this.deleteFailure;
      if (!this.documents.has(locator.path))
         throw new DocumentNotFoundError(`No dashboard at ${locator.path}`);
      this.documents.delete(locator.path);
   }
   async moveDocument(): Promise<void> {
      throw new Error("not exercised");
   }
}

const mount = (
   storage: DocumentStorage,
   onDirtyChange?: (dirty: boolean) => void,
   onEvent?: (event: DashboardEvent) => void,
) =>
   render(
      <DocumentStorageProvider documentStorage={storage}>
         <DashboardEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="overview"
            {...(onDirtyChange ? { onDirtyChange } : {})}
            {...(onEvent ? { onEvent } : {})}
         />
      </DocumentStorageProvider>,
      { wrapper: serverWrapper },
   );

const button = (name: string | RegExp) =>
   screen.getByRole("button", { name, hidden: true });

/** Rename the one tile, which is a non-structural edit and saves directly. */
const renameTile = (to: string, from = "By category") => {
   fireEvent.click(screen.getByLabelText(`Settings for ${from}`));
   fireEvent.change(screen.getByLabelText("Tile title"), {
      target: { value: to },
   });
   fireEvent.keyDown(screen.getByLabelText("Tile title"), { key: "Escape" });
};

/**
 * Let everything the last interaction started finish. A re-open the editor
 * should not have started parses its text and remounts the builder a tick
 * later, so asserting the instant a save reports "Saved" asserts too early to
 * see it.
 */
const settle = async () => {
   await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
   });
};

/**
 * A version of the file arriving from somewhere other than this editor: a real
 * refetch of the query the editor reads, driven to completion rather than
 * waited on, so what follows sees the render it caused.
 */
const packageChangedTo = async (text: string) => {
   serverText = text;
   await act(async () => {
      await globalQueryClient.refetchQueries({
         queryKey: ["dashboard-editor-model"],
      });
      // react-query hands the new data to its subscribers on a scheduled
      // batch, so the refetch resolving is not yet the render it causes.
      await new Promise((resolve) => setTimeout(resolve, 0));
   });
};

beforeEach(() => {
   // Explicitly, because nothing registers testing-library's auto-cleanup
   // here: every test in this file mounts the same dashboard, so a container
   // left behind is a second answer to every query.
   cleanup();
   clearCache();
   localStorage.clear();
   serverText = PACKAGE_FILE;
   serverContext.mutable = false;
   updateModelSource.mockClear();
});

describe("DashboardEditor, when the host's store is the record", () => {
   it("opens the store's copy rather than the package file, and never offers to resume it", async () => {
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, withTitle("Recorded"));
      const onEvent = mock((_event: DashboardEvent) => {});
      mount(storage, undefined, onEvent);

      expect(await screen.findByText("Recorded")).toBeDefined();
      expect(screen.queryByText("Storefront")).toBeNull();
      // Nothing is pending, so there is nothing to resume.
      expect(screen.queryByText(/edits to this dashboard/)).toBeNull();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "dashboard.opened",
         from: "record",
      });
   });

   it("leaves that same copy merely offered when the workspace is not the record", async () => {
      const storage = new FakeStorage(BESIDE);
      storage.documents.set(PATH, withTitle("Recorded"));
      mount(storage);

      expect(await screen.findByText("Storefront")).toBeDefined();
      expect(await screen.findByText(/edits to this dashboard/)).toBeDefined();
   });

   it("opens the package file when the record has no copy yet, and the first save keeps the history", async () => {
      const storage = new FakeStorage(RECORD);
      mount(storage);

      expect(await screen.findByText("Storefront")).toBeDefined();
      renameTile("Categories");
      fireEvent.click(button("Save changes"));

      await waitFor(() =>
         expect(storage.documents.get(PATH)).toContain('# label="Categories"'),
      );
      await waitFor(() => expect(button("Saved")).toBeDefined());
      await settle();
      // The save wrote the text the builder is holding, so the builder was not
      // remounted onto it: its undo history is still there.
      expect(button("Undo").hasAttribute("disabled")).toBe(false);
   });

   it("writes to the record even when the server says it takes writes", async () => {
      // The precedence that matters in production: a host whose store is the
      // record commonly sits on a server that reports itself writable, and
      // writing the package there edits a deploy of the record, not the record.
      serverContext.mutable = true;
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, withTitle("Recorded"));
      mount(storage);

      expect(await screen.findByText("Recorded")).toBeDefined();
      renameTile("Categories");
      fireEvent.click(button("Save changes"));

      await waitFor(() =>
         expect(storage.documents.get(PATH)).toContain('# label="Categories"'),
      );
      expect(updateModelSource).not.toHaveBeenCalled();
   });

   it("says where the record is, in the workspace's own words", async () => {
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, PACKAGE_FILE);
      mount(storage);
      expect(
         await screen.findByText("Saved to the draft branch"),
      ).toBeDefined();
   });

   it("keeps a reader out when the record could not be read", async () => {
      const storage = new FakeStorage(RECORD);
      storage.readFailure = new Error("the branch could not be reached");
      mount(storage);

      expect(
         await screen.findByText(/the branch could not be reached/),
      ).toBeDefined();
      // Opening the package and arming Save here would publish a deploy of the
      // record over the record.
      expect(screen.queryByText("Storefront")).toBeNull();
   });
});

describe("DashboardEditor, when the copy is kept beside the package", () => {
   it("opens the package but withholds Save when the saved copy could not be read", async () => {
      const storage = new FakeStorage(BESIDE);
      storage.readFailure = new Error("the branch could not be reached");
      mount(storage);

      expect(await screen.findByText("Storefront")).toBeDefined();
      expect(
         screen.getByText(/saved copy could not be read, so Save is off/),
      ).toBeDefined();
      renameTile("Categories");
      expect(
         screen.queryByRole("button", { name: "Save changes", hidden: true }),
      ).toBeNull();
   });

   it("says where the Console's copy goes, in that store's own words", async () => {
      mount(new BrowserDocumentStorage());
      expect(
         await screen.findByText(/Stored in this browser only/),
      ).toBeDefined();
   });

   it("keeps the copy offered when superseding it fails for a reason other than absence", async () => {
      serverContext.mutable = true;
      const storage = new FakeStorage(BESIDE);
      storage.documents.set(PATH, withTitle("Drafted"));
      storage.deleteFailure = new Error("the branch could not be reached");
      mount(storage);

      await screen.findByText("Storefront");
      renameTile("Categories");
      fireEvent.click(button("Save changes"));

      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(
         await screen.findByText(/copy kept beside it could not be cleared/),
      ).toBeDefined();
      // Still there, so the state must not claim otherwise.
      expect(storage.documents.get(PATH)).toBeDefined();
   });

   it("keeps a resumed copy on screen after saving it into the package, and splices the next save onto it", async () => {
      serverContext.mutable = true;
      const storage = new FakeStorage(BESIDE);
      storage.documents.set(PATH, withTitle("Drafted"));
      mount(storage);

      await screen.findByText("Storefront");
      fireEvent.click(button("Resume"));
      expect(await screen.findByText("Drafted")).toBeDefined();

      renameTile("Categories");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      await settle();
      expect(updateModelSource).toHaveBeenCalledTimes(1);
      // The save's own text, not the package file it replaced.
      expect(screen.getByText("Drafted")).toBeDefined();
      expect(screen.queryByText("Storefront")).toBeNull();
      // And on the same mount, so the reader's undo history survived it.
      expect(button("Undo").hasAttribute("disabled")).toBe(false);

      renameTile("Regions", "Categories");
      fireEvent.click(button("Save changes"));
      // On "Saved" rather than on the call count, which rises before the write
      // it started has finished and would leave it running into the next test.
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(updateModelSource).toHaveBeenCalledTimes(2);
      const second = updateModelSource.mock.calls[1][3].source;
      expect(second).toContain('title="Drafted"');
      expect(second).toContain('# label="Regions"');
   });
});

describe("DashboardEditor, when a new version of the file arrives", () => {
   it("holds it back rather than replacing unsaved edits, and loads it when the reader says so", async () => {
      serverContext.mutable = true;
      mount(new FakeStorage(BESIDE));

      await screen.findByText("Storefront");
      renameTile("Categories");
      await packageChangedTo(withTitle("Elsewhere"));

      expect(screen.getByText(/changed since you opened it/)).toBeDefined();
      // The edit is still here, and so is the text it was made against.
      expect(screen.getByText("Storefront")).toBeDefined();
      expect(screen.getByLabelText("Settings for Categories")).toBeDefined();

      fireEvent.click(button("Load it"));
      expect(await screen.findByText("Elsewhere")).toBeDefined();
      await waitFor(() =>
         expect(screen.queryByText(/changed since you opened it/)).toBeNull(),
      );
      expect(button("Undo").hasAttribute("disabled")).toBe(true);
   });

   it("still hands back the hash of the text it opened, so the stale save is refused", async () => {
      serverContext.mutable = true;
      mount(new FakeStorage(BESIDE));

      await screen.findByText("Storefront");
      renameTile("Categories");
      await packageChangedTo(withTitle("Elsewhere"));
      expect(screen.getByText(/changed since you opened it/)).toBeDefined();

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      expect(updateModelSource.mock.calls[0][3].expectedHash).toBe(
         await sha256Hex(PACKAGE_FILE),
      );
      await waitFor(() =>
         expect(
            screen
               .getAllByRole("alert")
               .some((alert) =>
                  alert.textContent?.includes(
                     "changed in the package since you opened it",
                  ),
               ),
         ).toBe(true),
      );
      // Refused, so the file the other writer left is intact.
      expect(serverText).toBe(withTitle("Elsewhere"));
   });

   it("tells the host when there are edits the record does not have", async () => {
      serverContext.mutable = true;
      const seen: boolean[] = [];
      mount(new FakeStorage(BESIDE), (value) => seen.push(value));

      await screen.findByText("Storefront");
      await waitFor(() => expect(seen).toEqual([false]));
      renameTile("Categories");
      await waitFor(() => expect(seen.at(-1)).toBe(true));
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(seen.at(-1)).toBe(false));
      await settle();
      // Exactly those three: the builder fires this from an effect, so a
      // handler whose identity changed each render would repeat its answer.
      expect(seen).toEqual([false, true, false]);
   });
});
