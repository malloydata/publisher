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
import { lastSession } from "../../../test/builderSession";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
   TEST_SERVER,
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
import type { NotebookEvent } from "./telemetry";

/**
 * The notebook host with the server mocked at the client: what it opens, where
 * Save goes, the hash each package write carries, and what a refusal leaves
 * on screen.
 */
const PACKAGE_FILE = `## artifact { kind=notebook }
##(markdown) Intro.

source: a is duckdb.table('t')

run: a -> { select: x }
`;
const withIntro = (intro: string) => PACKAGE_FILE.replace("Intro.", intro);
const UNTAGGED = `source: shared is duckdb.table('t')
`;
const TWO_TEXT = `${PACKAGE_FILE}
##(markdown) Outro.
`;
const OTHER = withIntro("Other intro.");

/** The file and its hash as the server holds them, so a save is a real compare-and-swap. */
let serverText = PACKAGE_FILE;
let serverHash = await sha256Hex(PACKAGE_FILE);
let writes = 0;
/** Slower than the store's answer, so an open from the cached fetch would win the race. */
let fetchDelay = 0;
/** Set, and every model fetch fails: a background refetch the server could not answer. */
let fetchFailure: Error | undefined;

const getModel = mock(
   async (
      _env: string,
      _pkg: string,
      path: string,
      _versionId?: string,
      _includeHiddenFilesAndSources?: boolean,
   ) => {
      if (fetchDelay)
         await new Promise((resolve) => setTimeout(resolve, fetchDelay));
      if (fetchFailure) throw fetchFailure;
      // A curated package withholds the text of a notebook that reads an off-surface source.
      if (path === "notebooks/withheld.malloy")
         return { data: { modelPath: path, givens: [] } };
      return {
         data: {
            modelPath: path,
            sourceText:
               path === "notebooks/shared.malloy"
                  ? UNTAGGED
                  : path === "notebooks/other.malloy"
                    ? OTHER
                    : serverText,
            givens:
               path === "notebooks/settings.malloy"
                  ? [{ name: "REGION", type: "string" }]
                  : [],
         },
      };
   },
);
/** The viewer's notebook read, which carries the artifact tag's starting givens and autorun. */
const getNotebook = mock(async (_env: string, _pkg: string, path: string) => ({
   data:
      path === "notebooks/settings.malloy"
         ? { autorun: false, startingGivens: { REGION: "EU" } }
         : { autorun: true },
}));
const executeQueryModel = mock(() => pending());
const updateModelSource = mock(
   async (
      _env: string,
      _pkg: string,
      path: string,
      body: { source: string; expectedHash?: string },
   ) => {
      if (body.expectedHash !== serverHash)
         throw {
            response: {
               status: 409,
               data: {
                  code: 409,
                  message: `\`${path}\` changed in the package since you opened it.`,
               },
            },
         };
      serverText = body.source;
      // Not a sha256, so a second save that recomputed the hash itself would be refused.
      serverHash = `server-hash-${++writes}`;
      return { data: { path, contentHash: serverHash, created: false } };
   },
);

const serverContext: {
   mutable: boolean | undefined;
   isLoadingStatus: boolean;
} = { mutable: true, isLoadingStatus: false };
mockServerProvider(
   {
      models: {
         getModel,
         executeQueryModel,
         updateModelSource,
      },
      notebooks: { getNotebook },
   },
   serverContext,
);

const { NotebookEditor } = await import("./NotebookEditor");

const PATH = "env/pkg/notebooks/tour.malloy";
const RECORD: Workspace = {
   name: "Branch",
   writeable: true,
   description: "Saved to the draft branch",
   authoritative: true,
};

/** A host store the spec can break on purpose. */
class FakeStorage implements DocumentStorage {
   readonly documents = new Map<string, string>();
   readonly types: DocumentType[] = [];
   readFailure: unknown;
   saveFailure: unknown;
   /** Held until the spec releases it, so a read can be caught in flight. */
   gate: Promise<void> | undefined;
   constructor(private readonly workspace: Workspace) {}

   async listWorkspaces(writeableOnly: boolean): Promise<Workspace[]> {
      await this.gate;
      return writeableOnly && !this.workspace.writeable ? [] : [this.workspace];
   }
   async listDocuments(): Promise<DocumentLocator[]> {
      return [];
   }
   async getDocument(locator: DocumentLocator): Promise<string> {
      this.types.push(locator.type);
      if (this.readFailure !== undefined) throw this.readFailure;
      const content = this.documents.get(locator.path);
      if (content === undefined)
         throw new DocumentNotFoundError(`No notebook at ${locator.path}`);
      return content;
   }
   async saveDocument(
      locator: DocumentLocator,
      content: string,
   ): Promise<void> {
      if (this.saveFailure !== undefined) throw this.saveFailure;
      this.documents.set(locator.path, content);
   }
   async deleteDocument(locator: DocumentLocator): Promise<void> {
      if (!this.documents.delete(locator.path))
         throw new DocumentNotFoundError(`No notebook at ${locator.path}`);
   }
   async moveDocument(): Promise<void> {
      throw new Error("not exercised");
   }
}

const editor = (
   options: {
      name?: string;
      onEvent?: (event: NotebookEvent) => void;
      onDirtyChange?: (dirty: boolean) => void;
      reloadToken?: number;
   } = {},
) => (
   <NotebookEditor
      key={options.reloadToken ?? 0}
      environmentName="env"
      packageName="pkg"
      notebookName={options.name ?? "tour"}
      {...(options.onEvent ? { onEvent: options.onEvent } : {})}
      {...(options.onDirtyChange
         ? { onDirtyChange: options.onDirtyChange }
         : {})}
   />
);

const mount = (
   storage: DocumentStorage | undefined,
   options: Parameters<typeof editor>[0] = {},
) =>
   render(
      storage ? (
         <DocumentStorageProvider documentStorage={storage}>
            {editor(options)}
         </DocumentStorageProvider>
      ) : (
         editor(options)
      ),
      { wrapper: serverWrapper },
   );

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

const textCell = (n: number) =>
   screen.getByRole("group", { name: `Cell ${n}, text`, hidden: true });
const introCell = () => textCell(1);

const editCell = (n: number, next: string) => {
   fireEvent.click(
      within(textCell(n)).getByRole("button", {
         name: "Edit text",
         hidden: true,
      }),
   );
   fireEvent.change(screen.getByLabelText("Markdown"), {
      target: { value: next },
   });
   fireEvent.click(button("Done"));
};
const editIntro = (next: string) => editCell(1, next);

const noSave = () =>
   expect(
      screen.queryByRole("button", { name: "Save changes", hidden: true }),
   ).toBeNull();

const refusedWith = (
   onEvent: ReturnType<typeof mock<(event: NotebookEvent) => void>>,
   text: string,
) =>
   waitFor(() =>
      expect(
         onEvent.mock.calls.some(
            ([event]) =>
               event.type === "notebook.save_refused" &&
               event.reason.includes(text),
         ),
      ).toBe(true),
   );

const alertWith = (text: string) =>
   waitFor(() =>
      expect(
         screen
            .getAllByRole("alert")
            .some((alert) => alert.textContent?.includes(text)),
      ).toBe(true),
   );

const settle = async () => {
   await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
   });
};

beforeEach(async () => {
   cleanup();
   clearCache();
   localStorage.clear();
   serverText = PACKAGE_FILE;
   serverHash = await sha256Hex(PACKAGE_FILE);
   writes = 0;
   fetchDelay = 0;
   fetchFailure = undefined;
   serverContext.mutable = true;
   serverContext.isLoadingStatus = false;
   getModel.mockClear();
   getNotebook.mockClear();
   executeQueryModel.mockClear();
   updateModelSource.mockClear();
});

describe("NotebookEditor, undoing a package save", () => {
   const undoSave = () =>
      act(async () => {
         await lastSession.current?.undoSave();
      });

   it("writes the file back against the hash the save returned, and reports it", async () => {
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(new BrowserDocumentStorage(), { onEvent });
      await within(
         await screen.findByRole("group", {
            name: "Cell 1, text",
            hidden: true,
         }),
      ).findByText("Intro.");
      editIntro("Intro, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(lastSession.current?.canUndoSave).toBe(true);

      await undoSave();
      expect(updateModelSource).toHaveBeenCalledTimes(2);
      const body = updateModelSource.mock.calls[1][3];
      expect(body.source).toBe(PACKAGE_FILE);
      expect(body.expectedHash).toBe("server-hash-1");
      expect(serverText).toBe(PACKAGE_FILE);
      await waitFor(() => expect(button("Save changes")).toBeDefined());
      const undone = onEvent.mock.calls.at(-1)?.[0];
      expect(undone).toMatchObject({
         type: "notebook.save_undone",
         cells: 3,
         where: "package",
         structural: false,
      });
      expect(undone).not.toHaveProperty("workspace");

      // The edit is back and saves again against the hash the undo returned.
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(3));
      expect(updateModelSource.mock.calls[2][3].expectedHash).toBe(
         "server-hash-2",
      );
      expect(serverText).toBe(withIntro("Intro, edited."));
   });

   it("refuses the undo when the file changed since the save, and keeps the offer", async () => {
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(new BrowserDocumentStorage(), { onEvent });
      await within(
         await screen.findByRole("group", {
            name: "Cell 1, text",
            hidden: true,
         }),
      ).findByText("Intro.");
      editIntro("Intro, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      // Another writer lands on top of the save.
      serverText = withIntro("Someone else's.");
      serverHash = "theirs";

      await undoSave();
      await alertWith("changed in the package since you opened it");
      expect(serverText).toBe(withIntro("Someone else's."));
      expect(onEvent.mock.calls.at(-1)?.[0]).toMatchObject({
         type: "notebook.save_undo_refused",
      });
      expect(lastSession.current?.canUndoSave).toBe(true);
   });
});

describe("NotebookEditor, on a server that takes writes", () => {
   it("opens the package file, saves with its hash, then with the hash the server returned", async () => {
      const onEvent = mock((_event: NotebookEvent) => {});
      const storage = new BrowserDocumentStorage();
      const copy = {
         workspace: "Local",
         type: "notebook" as const,
         path: PATH,
      };
      await storage.saveDocument(copy, "stale copy");
      mount(storage, { onEvent });
      expect(
         await within(
            await screen.findByRole("group", {
               name: "Cell 1, text",
               hidden: true,
            }),
         ).findByText("Intro."),
      ).toBeDefined();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.opened",
         from: "package",
         cells: 3,
      });
      expect(
         screen.getByText("Save writes the file into the package."),
      ).toBeDefined();

      editIntro("Intro, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      const [env, pkg, path, body] = updateModelSource.mock.calls[0];
      expect([env, pkg, path]).toEqual(["env", "pkg", "notebooks/tour.malloy"]);
      expect(body.source).toBe(withIntro("Intro, edited."));
      expect(body.expectedHash).toBe(await sha256Hex(PACKAGE_FILE));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(onEvent.mock.calls.at(-1)?.[0]).toMatchObject({
         type: "notebook.saved",
         cells: 3,
         where: "package",
      });
      // A package write was taken by no workspace.
      expect(onEvent.mock.calls.at(-1)?.[0]).not.toHaveProperty("workspace");
      // A copy beside the package is neither offered back nor touched.
      expect(await storage.getDocument(copy)).toBe("stale copy");

      editIntro("Intro, again.");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(2));
      expect(updateModelSource.mock.calls[1][3].expectedHash).toBe(
         "server-hash-1",
      );
      expect(serverText).toBe(withIntro("Intro, again."));
   });

   it("keeps the edit dirty and shows the server's reason when the write is refused", async () => {
      const seen: boolean[] = [];
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(new BrowserDocumentStorage(), {
         onDirtyChange: (dirty) => seen.push(dirty),
         onEvent,
      });
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      editIntro("Intro, edited.");
      // Moved underneath, with nothing telling the editor.
      serverText = withIntro("Elsewhere.");
      serverHash = await sha256Hex(serverText);

      fireEvent.click(button("Save changes"));
      await alertWith("changed in the package since you opened it");
      expect(button("Save changes")).toBeDefined();
      expect(seen.at(-1)).toBe(true);
      expect(within(introCell()).getByText("Intro, edited.")).toBeDefined();
      expect(serverText).toBe(withIntro("Elsewhere."));
      await refusedWith(onEvent, "changed in the package since you opened it");
   });

   it("keeps unsaved edits through a failed refetch, and never saves the pre-save text back", async () => {
      serverText = TWO_TEXT;
      serverHash = await sha256Hex(TWO_TEXT);
      mount(new BrowserDocumentStorage());
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      editIntro("First save.");
      // The refetch the save's invalidation starts is the one that fails.
      fetchFailure = new Error("the server went away");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      await alertWith("could not be re-read from the server");

      // Still mounted: the saved edit stands, and a new one can be made.
      expect(within(introCell()).getByText("First save.")).toBeDefined();
      editCell(4, "Unsaved.");
      expect(within(textCell(4)).getByText("Unsaved.")).toBeDefined();

      fetchFailure = undefined;
      await act(async () => {
         await globalQueryClient.refetchQueries({
            queryKey: ["notebook-editor-model"],
         });
         await new Promise((resolve) => setTimeout(resolve, 0));
      });
      await waitFor(() =>
         expect(
            screen.queryByText(/could not be re-read from the server/),
         ).toBeNull(),
      );
      expect(within(introCell()).getByText("First save.")).toBeDefined();
      expect(within(textCell(4)).getByText("Unsaved.")).toBeDefined();

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(2));
      expect(updateModelSource.mock.calls[1][3].expectedHash).toBe(
         "server-hash-1",
      );
      expect(serverText).toBe(
         TWO_TEXT.replace("Intro.", "First save.").replace(
            "Outro.",
            "Unsaved.",
         ),
      );
   });

   it("follows a host that switches notebooks without a key, and never saves one into the other", async () => {
      const storage = new BrowserDocumentStorage();
      const view = render(
         <DocumentStorageProvider documentStorage={storage}>
            <NotebookEditor
               environmentName="env"
               packageName="pkg"
               notebookName="tour"
            />
         </DocumentStorageProvider>,
         { wrapper: serverWrapper },
      );
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      editIntro("Tour, unsaved.");
      view.rerender(
         <DocumentStorageProvider documentStorage={storage}>
            <NotebookEditor
               environmentName="env"
               packageName="pkg"
               notebookName="other"
            />
         </DocumentStorageProvider>,
      );
      await waitFor(() =>
         expect(within(introCell()).getByText("Other intro.")).toBeDefined(),
      );
      editIntro("Other, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      const [, , path, body] = updateModelSource.mock.calls[0];
      expect(path).toBe("notebooks/other.malloy");
      expect(body.source).toBe(withIntro("Other, edited."));
      expect(body.expectedHash).toBe(await sha256Hex(OTHER));
   });

   it("tells the host when there are edits the record does not have", async () => {
      const seen: boolean[] = [];
      mount(undefined, { onDirtyChange: (dirty) => seen.push(dirty) });
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      await waitFor(() => expect(seen).toEqual([false]));
      editIntro("Intro, edited.");
      await waitFor(() => expect(seen.at(-1)).toBe(true));
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(seen.at(-1)).toBe(false));
      await settle();
      expect(seen).toEqual([false, true, false]);
   });

   it("re-reads the file when the host remounts it with a new key", async () => {
      const storage = new BrowserDocumentStorage();
      const view = mount(storage, { reloadToken: 1 });
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      expect(within(introCell()).getByText("Intro.")).toBeDefined();

      serverText = withIntro("Reloaded.");
      fetchDelay = 60;
      view.rerender(
         <DocumentStorageProvider documentStorage={storage}>
            {editor({ reloadToken: 2 })}
         </DocumentStorageProvider>,
      );
      await waitFor(() =>
         expect(within(introCell()).getByText("Reloaded.")).toBeDefined(),
      );
      expect(getModel).toHaveBeenCalledTimes(2);
   });

   it("shows the error, not the cached copy, when a remount's re-read fails", async () => {
      const storage = new BrowserDocumentStorage();
      const view = mount(storage, { reloadToken: 1 });
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );

      fetchFailure = new Error("the server went away");
      view.rerender(
         <DocumentStorageProvider documentStorage={storage}>
            {editor({ reloadToken: 2 })}
         </DocumentStorageProvider>,
      );
      await waitFor(() => expect(getModel).toHaveBeenCalledTimes(2));
      await settle();
      // A boolean, so a failure does not print the whole builder.
      expect(
         screen.queryByRole("group", { name: "Cell 1, text", hidden: true }) ===
            null,
      ).toBe(true);
      expect(screen.getByText("Opening the notebook")).toBeDefined();
   });
});

describe("NotebookEditor, refusals", () => {
   it("opens an untagged file read-only, with the reason", async () => {
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(new BrowserDocumentStorage(), { name: "shared", onEvent });
      expect(
         await screen.findByText(/read-only here: .*no ## artifact note/),
      ).toBeDefined();
      expect(
         screen.queryByRole("button", { name: "Save changes", hidden: true }),
      ).toBeNull();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.open_refused",
      });
   });

   it("refuses a .malloynb without asking for a file it cannot edit", async () => {
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(new BrowserDocumentStorage(), { name: "legacy.malloynb", onEvent });
      expect(
         await screen.findByText(/\.malloynb notebook is read/),
      ).toBeDefined();
      expect(getModel).not.toHaveBeenCalled();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.open_refused",
      });
   });
});

describe("NotebookEditor, when the host's store is the record", () => {
   it("opens the record, saves back to it, and reports the save as the host's", async () => {
      // Writable, so a package write would be possible and must not happen.
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, withIntro("Recorded."));
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(storage, { onEvent });

      await waitFor(() =>
         expect(within(introCell()).getByText("Recorded.")).toBeDefined(),
      );
      expect(storage.types).toEqual(["notebook"]);
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.opened",
         from: "record",
      });
      expect(screen.getByText("Saved to the draft branch")).toBeDefined();

      editIntro("Recorded, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(storage.documents.get(PATH)).toBe(
            withIntro("Recorded, edited."),
         ),
      );
      expect(updateModelSource).not.toHaveBeenCalled();
      await waitFor(() =>
         expect(onEvent.mock.calls.at(-1)?.[0]).toMatchObject({
            type: "notebook.saved",
            where: "host",
            workspace: "Branch",
         }),
      );
   });

   it("says it is the package's model, not the record, that a failed refetch could not re-read", async () => {
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, withIntro("Recorded."));
      mount(storage);
      await waitFor(() =>
         expect(within(introCell()).getByText("Recorded.")).toBeDefined(),
      );
      fetchFailure = new Error("the server went away");
      await act(async () => {
         await globalQueryClient.refetchQueries({
            queryKey: ["notebook-editor-model"],
         });
         await new Promise((resolve) => setTimeout(resolve, 0));
      });
      await alertWith("The package's model, which previews and parameters use");
      expect(within(introCell()).getByText("Recorded.")).toBeDefined();
   });

   it("opens the package file when the record has no copy yet", async () => {
      const storage = new FakeStorage(RECORD);
      mount(storage);
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      editIntro("Intro, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(storage.documents.get(PATH)).toBe(withIntro("Intro, edited.")),
      );
   });

   it("keeps a reader out when the record could not be read", async () => {
      const storage = new FakeStorage(RECORD);
      storage.readFailure = new Error("the branch could not be reached");
      mount(storage);
      expect(
         await screen.findByText(
            /cannot be opened: the branch could not be reached/,
         ),
      ).toBeDefined();
      // Opening the package here would put a deploy of the record where the record belongs.
      expect(
         screen.queryByRole("group", { name: "Cell 1, text", hidden: true }),
      ).toBeNull();
   });

   it("shows a save the store refused inline, and keeps the edit", async () => {
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, PACKAGE_FILE);
      storage.saveFailure = new Error("the branch is locked");
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(storage, { onEvent });
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      editIntro("Intro, edited.");
      fireEvent.click(button("Save changes"));
      await alertWith("the branch is locked");
      expect(button("Save changes")).toBeDefined();
      await refusedWith(onEvent, "the branch is locked");
   });

   it("does not open the package first when the storage answer is slower", async () => {
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, withIntro("Recorded."));
      storage.gate = new Promise((resolve) => setTimeout(resolve, 150));
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(storage, { onEvent });
      await waitFor(() =>
         expect(within(introCell()).getByText("Recorded.")).toBeDefined(),
      );
      const opens = onEvent.mock.calls.filter(
         ([event]) => event.type === "notebook.opened",
      );
      expect(opens.map(([event]) => event)).toMatchObject([
         { type: "notebook.opened", from: "record" },
      ]);
   });

   it("shows the record to a reader who cannot write to it, and offers no Save", async () => {
      const storage = new FakeStorage({ ...RECORD, writeable: false });
      storage.documents.set(PATH, withIntro("Recorded."));
      mount(storage);
      await waitFor(() =>
         expect(within(introCell()).getByText("Recorded.")).toBeDefined(),
      );
      expect(
         screen.getByText(
            "Saved to the draft branch: you cannot save into it.",
         ),
      ).toBeDefined();
      editIntro("Recorded, edited.");
      noSave();
      expect(updateModelSource).not.toHaveBeenCalled();
   });

   it("keeps the next edit after a save to the record", async () => {
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, TWO_TEXT);
      mount(storage);
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      editIntro("First.");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      await settle();
      editCell(4, "Second.");
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(storage.documents.get(PATH)).toBe(
            TWO_TEXT.replace("Intro.", "First.").replace("Outro.", "Second."),
         ),
      );
   });

   it("holds Save while a new store re-reads, so the record is never written into the package", async () => {
      const first = new FakeStorage(RECORD);
      first.documents.set(PATH, withIntro("Recorded."));
      const view = mount(first);
      await waitFor(() =>
         expect(within(introCell()).getByText("Recorded.")).toBeDefined(),
      );
      editIntro("Recorded, edited.");

      const second = new FakeStorage(RECORD);
      second.documents.set(PATH, withIntro("Recorded."));
      let release = () => {};
      second.gate = new Promise((resolve) => {
         release = resolve;
      });
      view.rerender(
         <DocumentStorageProvider documentStorage={second}>
            {editor()}
         </DocumentStorageProvider>,
      );
      await settle();
      // Mid-read the server takes writes, which is exactly when Save must not fall to the package.
      noSave();
      expect(within(introCell()).getByText("Recorded, edited.")).toBeDefined();

      await act(async () => release());
      await waitFor(() => expect(button("Save changes")).toBeDefined());
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(second.documents.get(PATH)).toBe(
            withIntro("Recorded, edited."),
         ),
      );
      expect(updateModelSource).not.toHaveBeenCalled();
   });
});

describe("NotebookEditor, by resource URI", () => {
   const mountUri = (resourceUri: string) =>
      render(<NotebookEditor resourceUri={resourceUri} notebook="tour" />, {
         wrapper: serverWrapper,
      });

   it("opens the package's notebook and saves into it", async () => {
      mountUri("publisher://environments/env/packages/pkg");
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      editIntro("Intro, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      expect(updateModelSource.mock.calls[0].slice(0, 3)).toEqual([
         "env",
         "pkg",
         "notebooks/tour.malloy",
      ]);
   });

   it("reads a pinned version, with Save off", async () => {
      mountUri("publisher://environments/env/packages/pkg?versionId=v1");
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      expect(getModel.mock.calls[0]).toEqual([
         "env",
         "pkg",
         "notebooks/tour.malloy",
         "v1",
         true,
      ]);
      expect(screen.getByText(/Reading version v1/)).toBeDefined();
      editIntro("Intro, edited.");
      noSave();
   });

   it("says so when the URI does not name a package", async () => {
      mountUri("not a uri");
      expect(
         screen.getByText(/must name an environment and a package/),
      ).toBeDefined();
      expect(getModel).not.toHaveBeenCalled();
   });
});

describe("NotebookEditor, on a server that does not take writes", () => {
   // A copy in a workspace that is not the record is one this editor never reads back.
   it("offers no Save into a workspace that is not the record", async () => {
      serverContext.mutable = false;
      const storage = new BrowserDocumentStorage();
      mount(storage);
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      // The browser workspace is not where Save goes, so it is not named.
      expect(
         screen.getByText("This server does not take writes."),
      ).toBeDefined();
      expect(screen.queryByText(/Stored in this browser only/)).toBeNull();
      editIntro("Intro, edited.");
      noSave();
      await settle();
      await expect(
         storage.getDocument({
            workspace: "Local",
            type: "notebook",
            path: PATH,
         }),
      ).rejects.toBeDefined();
      expect(updateModelSource).not.toHaveBeenCalled();
   });

   it("holds Save until the server says whether it takes writes", async () => {
      serverContext.mutable = undefined;
      serverContext.isLoadingStatus = true;
      const storage = new BrowserDocumentStorage();
      const view = mount(storage);
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      expect(
         screen.getByText("Checking whether this server takes writes."),
      ).toBeDefined();
      editIntro("Intro, edited.");
      noSave();

      serverContext.isLoadingStatus = false;
      serverContext.mutable = true;
      view.rerender(
         <DocumentStorageProvider documentStorage={storage}>
            {editor()}
         </DocumentStorageProvider>,
      );
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      await expect(
         storage.getDocument({
            workspace: "Local",
            type: "notebook",
            path: PATH,
         }),
      ).rejects.toBeDefined();
   });
});

describe("NotebookEditor, when the server never says whether it takes writes", () => {
   it("says Save is off, rather than that it is still checking", async () => {
      // `/status` failed: no longer loading, and no answer.
      serverContext.mutable = undefined;
      mount(new BrowserDocumentStorage());
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      expect(
         screen.getByText(
            "This server did not say whether it takes writes, so Save is off.",
         ),
      ).toBeDefined();
      editIntro("Intro, edited.");
      noSave();
   });
});

describe("NotebookEditor, when the model read withholds the text", () => {
   it("asks for the hidden files, since the editor reads the file to author it", async () => {
      mount(undefined);
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      expect(getModel.mock.calls[0]).toEqual([
         "env",
         "pkg",
         "notebooks/tour.malloy",
         undefined,
         true,
      ]);
   });

   it("refuses to open, with the reason, rather than waiting for text that never comes", async () => {
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(new BrowserDocumentStorage(), { name: "withheld", onEvent });
      expect(
         await screen.findByText(
            /read-only here: .*did not send this notebook's text/,
         ),
      ).toBeDefined();
      expect(screen.queryByText("Opening the notebook…")).toBeNull();
      expect(onEvent.mock.calls.map(([event]) => event)).toMatchObject([
         { type: "notebook.open_refused" },
      ]);
   });

   it("opens from the record without the package's text, and saves back to the record", async () => {
      const storage = new FakeStorage(RECORD);
      const withheld = "env/pkg/notebooks/withheld.malloy";
      storage.documents.set(withheld, withIntro("Recorded."));
      const onEvent = mock((_event: NotebookEvent) => {});
      mount(storage, { name: "withheld", onEvent });
      await waitFor(() =>
         expect(within(introCell()).getByText("Recorded.")).toBeDefined(),
      );
      expect(screen.queryByText("Opening the notebook…")).toBeNull();
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.opened",
         from: "record",
      });
      editIntro("Recorded, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(storage.documents.get(withheld)).toBe(
            withIntro("Recorded, edited."),
         ),
      );
      expect(updateModelSource).not.toHaveBeenCalled();
   });

   it("refuses to open when the record has no copy and the package's text is withheld", async () => {
      mount(new FakeStorage(RECORD), { name: "withheld" });
      expect(
         await screen.findByText(
            /read-only here: .*did not send this notebook's text/,
         ),
      ).toBeDefined();
   });
});

describe("NotebookEditor, when the host's store changes while open", () => {
   const NOT_RECORD: Workspace = {
      name: "Scratch",
      writeable: true,
      description: "Kept in scratch",
   };
   const swapTo = (view: ReturnType<typeof mount>, storage: DocumentStorage) =>
      view.rerender(
         <DocumentStorageProvider documentStorage={storage}>
            {editor()}
         </DocumentStorageProvider>,
      );

   it("never writes package text over a record that appears mid-session", async () => {
      const view = mount(new FakeStorage(NOT_RECORD));
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      editIntro("Intro, edited.");
      const record = new FakeStorage(RECORD);
      record.documents.set(PATH, withIntro("Recorded."));
      swapTo(view, record);
      await alertWith("Where this notebook saves changed");
      noSave();
      expect(within(introCell()).getByText("Intro, edited.")).toBeDefined();
      expect(record.documents.get(PATH)).toBe(withIntro("Recorded."));
      expect(updateModelSource).not.toHaveBeenCalled();
   });

   it("never writes the record's text into the package when the record goes away mid-session", async () => {
      const record = new FakeStorage(RECORD);
      record.documents.set(PATH, withIntro("Recorded."));
      const view = mount(record);
      await waitFor(() =>
         expect(within(introCell()).getByText("Recorded.")).toBeDefined(),
      );
      editIntro("Recorded, edited.");
      swapTo(view, new FakeStorage(NOT_RECORD));
      await alertWith("Where this notebook saves changed");
      noSave();
      expect(within(introCell()).getByText("Recorded, edited.")).toBeDefined();
      expect(updateModelSource).not.toHaveBeenCalled();
      expect(serverText).toBe(PACKAGE_FILE);
   });
});

describe("NotebookEditor, the notebook's own control settings", () => {
   it("runs the first preview with the notebook's starting givens, and holds changes behind Apply", async () => {
      mount(undefined, { name: "settings" });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      const calls = executeQueryModel.mock.calls as unknown as [
         string,
         string,
         string,
         { givens?: Record<string, unknown> },
      ][];
      expect(calls[0][3].givens).toEqual({ REGION: "EU" });
      expect(getNotebook.mock.calls[0].slice(0, 3)).toEqual([
         "env",
         "pkg",
         "notebooks/settings.malloy",
      ]);
      expect(button("Apply")).toBeDefined();
      fireEvent.change(screen.getByLabelText("REGION"), {
         target: { value: "West" },
      });
      await settle();
      expect(executeQueryModel).toHaveBeenCalledTimes(1);
   });

   it("lets an autorun text control settle before its previews run, once for a burst of typing", async () => {
      getNotebook.mockImplementationOnce(async () => ({
         data: { autorun: true, startingGivens: { REGION: "EU" } },
      }));
      mount(undefined, { name: "settings" });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      for (const value of ["W", "We", "West"])
         fireEvent.change(screen.getByLabelText("REGION"), {
            target: { value },
         });
      await settle();
      expect(executeQueryModel).toHaveBeenCalledTimes(1);
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
      const last = executeQueryModel.mock.calls.at(-1) as unknown as [
         string,
         string,
         string,
         { givens?: Record<string, unknown> },
      ];
      expect(last[3].givens).toEqual({ REGION: "West" });
      await settle();
      expect(executeQueryModel).toHaveBeenCalledTimes(2);
   });

   it("waits for the model's givens before a record open runs its first preview", async () => {
      const storage = new FakeStorage(RECORD);
      storage.documents.set("env/pkg/notebooks/settings.malloy", PACKAGE_FILE);
      fetchDelay = 100;
      mount(storage, { name: "settings" });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      const calls = executeQueryModel.mock.calls as unknown as [
         string,
         string,
         string,
         { givens?: Record<string, unknown> },
      ][];
      expect(calls[0][3].givens).toEqual({ REGION: "EU" });
   });

   it("keeps the editor, its edits and its controls through a later settings re-read", async () => {
      mount(undefined, { name: "settings" });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      editIntro("Intro, edited.");
      fireEvent.change(screen.getByLabelText("REGION"), {
         target: { value: "West" },
      });
      // The re-read hangs, then answers with other settings.
      let answer = (_value: unknown) => {};
      getNotebook.mockImplementationOnce(
         () =>
            new Promise((resolve) => {
               answer = resolve;
            }) as ReturnType<typeof getNotebook>,
      );
      await act(async () => {
         void globalQueryClient.refetchQueries({
            predicate: (query) =>
               JSON.stringify(query.queryKey).includes("settings.malloy") &&
               !JSON.stringify(query.queryKey).includes(
                  "notebook-editor-model",
               ),
         });
         await new Promise((resolve) => setTimeout(resolve, 0));
      });
      expect(within(introCell()).getByText("Intro, edited.")).toBeDefined();
      await act(async () => {
         answer({ data: { autorun: true, startingGivens: { REGION: "US" } } });
         await new Promise((resolve) => setTimeout(resolve, 0));
      });
      expect(within(introCell()).getByText("Intro, edited.")).toBeDefined();
      expect((screen.getByLabelText("REGION") as HTMLInputElement).value).toBe(
         "West",
      );
      expect(button("Apply")).toBeDefined();
   });

   it("opens with the defaults when the notebook read fails", async () => {
      getNotebook.mockImplementationOnce(async () => {
         throw new Error("no notebook route");
      });
      mount(undefined, { name: "settings" });
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      expect(
         screen.queryByRole("button", { name: "Apply", hidden: true }),
      ).toBeNull();
   });
});

describe("NotebookEditor, when a save fails", () => {
   const setClipboard = (value: unknown) =>
      Object.defineProperty(navigator, "clipboard", {
         value,
         configurable: true,
      });

   it("says the reason without the Error: prefix", async () => {
      const storage = new FakeStorage(RECORD);
      storage.documents.set(PATH, PACKAGE_FILE);
      storage.saveFailure = new Error("the branch is locked");
      mount(storage);
      await waitFor(() =>
         expect(within(introCell()).getByText("Intro.")).toBeDefined(),
      );
      editIntro("Intro, edited.");
      fireEvent.click(button("Save changes"));
      await alertWith("Could not save: the branch is locked");
      expect(
         screen
            .getAllByRole("alert")
            .some((a) => a.textContent?.includes("Error:")),
      ).toBe(false);
   });

   it("refetches the model after a refused package write", async () => {
      mount(undefined);
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      await waitFor(() => expect(getModel).toHaveBeenCalled());
      editIntro("Intro, edited.");
      serverHash = "moved-on";
      const before = getModel.mock.calls.length;
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      await waitFor(() =>
         expect(getModel.mock.calls.length).toBeGreaterThan(before),
      );
   });

   it("offers the would-be file to copy, and copies it", async () => {
      const writeText = mock(async (_text: string) => {});
      setClipboard({ writeText });
      mount(undefined);
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      editIntro("Intro, edited.");
      serverHash = "moved-on";
      fireEvent.click(button("Save changes"));
      await alertWith("changed in the package");
      fireEvent.click(button("Copy my changes"));
      await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
      expect(writeText.mock.calls[0][0]).toBe(
         PACKAGE_FILE.replace("Intro.", "Intro, edited."),
      );
   });

   it("falls back to a selectable block of the file when the clipboard is unavailable", async () => {
      setClipboard(undefined);
      mount(undefined);
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      editIntro("Intro, edited.");
      serverHash = "moved-on";
      fireEvent.click(button("Save changes"));
      await alertWith("changed in the package");
      fireEvent.click(button("Copy my changes"));
      const block = (await screen.findByLabelText(
         "Your changes",
      )) as HTMLTextAreaElement;
      expect(block.value).toBe(
         PACKAGE_FILE.replace("Intro.", "Intro, edited."),
      );
      expect(block.readOnly).toBe(true);
   });
});

describe("NotebookEditor, after a package save", () => {
   it("drops the cached results of the saved model and leaves other models' alone", async () => {
      const key = (modelPath: string) => [
         "queryResult",
         "env",
         "pkg",
         undefined,
         modelPath,
         undefined,
         "run: a -> { select: x }",
         undefined,
         "{}",
         TEST_SERVER,
      ];
      globalQueryClient.setQueryData(key("notebooks/tour.malloy"), "old");
      globalQueryClient.setQueryData(key("notebooks/other.malloy"), "other");
      mount(undefined);
      await screen.findByRole("group", { name: "Cell 1, text", hidden: true });
      editIntro("Intro, edited.");
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      await waitFor(() =>
         expect(
            globalQueryClient.getQueryState(key("notebooks/tour.malloy"))
               ?.isInvalidated,
         ).toBe(true),
      );
      expect(
         globalQueryClient.getQueryState(key("notebooks/other.malloy"))
            ?.isInvalidated,
      ).toBe(false);
   });
});
