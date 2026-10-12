// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The contract the SDK holds a host's {@link DocumentStorage} to today: every
 * call each caller makes, with the exact locator. A host's store (the
 * Console's browser store, a platform's own) is written against these calls,
 * so a change to any of them is a change to that contract and must be made
 * here on purpose.
 *
 * A locator is `{ workspace, type, path }`: `path` is
 * `<environment>/<package>/<package path>` and is the document's identity,
 * and `type` is the report's layout (`grid` or `linear`), metadata a save
 * records and a listing reports. One path is one document whatever its layout.
 */
import {
   act,
   fireEvent,
   render,
   renderHook,
   screen,
   waitFor,
} from "@testing-library/react";
import { QueryClient } from "@tanstack/react-query";
import { beforeEach, describe, expect, it } from "bun:test";
import type { ReactNode } from "react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import { createDocument } from "../DocumentCreate/createDocument";
import { useHostCopy } from "../ReportBuilder/useHostCopy";
import { useSaveChannel } from "../ReportBuilder/useSaveChannel";
import { findDraftLayout } from "../Package/useDraftLayout";
import { BrowserDocumentStorage } from "./BrowserDocumentStorage";
import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type DocumentType,
   type Workspace,
} from "./DocumentStorage";
import { DocumentStorageProvider } from "./DocumentStorageProvider";

mockServerProvider(
   {
      packages: { getPackage: pending },
      reports: {
         listReports: () => Promise.resolve({ data: [] }),
         getReport: pending,
      },
      models: { listModels: () => Promise.resolve({ data: [] }) },
      databases: { listDatabases: () => Promise.resolve({ data: [] }) },
      dataApps: { listDataApps: () => Promise.resolve({ data: [] }) },
      materializations: {
         listMaterializations: () => Promise.resolve({ data: [] }),
      },
   },
   { mutable: true },
);
const { default: Package } = await import("../Package/Package");

const RECORD: Workspace = {
   name: "record",
   writeable: true,
   description: "The record",
   authoritative: true,
};
const SCRATCH: Workspace = {
   name: "scratch",
   writeable: true,
   description: "Scratch space",
};
const READ_ONLY: Workspace = {
   name: "archive",
   writeable: false,
   description: "Read only",
};

type Call = [method: string, ...args: unknown[]];

/** A store that answers from a fixed set of documents and records every call verbatim. */
function recordingStorage(
   workspaces: Workspace[],
   documents: (DocumentLocator & { content?: string })[] = [],
) {
   const calls: Call[] = [];
   const find = (l: DocumentLocator) =>
      documents.find((d) => d.workspace === l.workspace && d.path === l.path);
   const storage: DocumentStorage = {
      listWorkspaces: async (writeableOnly) => {
         calls.push(["listWorkspaces", writeableOnly]);
         return workspaces;
      },
      listDocuments: async (workspace, type) => {
         calls.push(["listDocuments", workspace.name, type]);
         return documents
            .filter(
               (d) =>
                  d.workspace === workspace.name &&
                  (type === undefined || d.type === type),
            )
            .map(({ workspace, type, path }) => ({ workspace, type, path }));
      },
      getDocument: async (locator) => {
         calls.push(["getDocument", locator]);
         const found = find(locator);
         if (!found) throw new DocumentNotFoundError("absent");
         return found.content ?? "";
      },
      saveDocument: async (locator, content) => {
         calls.push(["saveDocument", locator, content]);
      },
      deleteDocument: async (locator) => {
         calls.push(["deleteDocument", locator]);
      },
      moveDocument: async (from, to) => {
         calls.push(["moveDocument", from, to]);
      },
   };
   return { storage, calls };
}

const locator = (
   workspace: string,
   type: DocumentType,
   path: string,
): DocumentLocator => ({ workspace, type, path });

describe("DocumentStorage contract: create", () => {
   it("lists the workspace, reads the candidate, then saves it at `<env>/<pkg>/<path>`, recording the layout", async () => {
      const { storage, calls } = recordingStorage([RECORD]);
      await createDocument({
         layout: "grid",
         path: "sales/q3.malloy",
         document: {
            title: "Q3",
            modelPath: "models/storefront.malloy",
            source: "order_items",
            view: "by_category",
         },
         target: {
            route: "storage",
            storage,
            workspace: RECORD,
            environmentName: "env",
            packageName: "pkg",
         },
      });
      const at = locator("record", "grid", "env/pkg/sales/q3.malloy");
      expect(
         calls.map(([method, ...args]) =>
            method === "saveDocument" ? [method, args[0]] : [method, ...args],
         ),
      ).toEqual([
         ["listDocuments", "record", undefined],
         ["getDocument", at],
         ["saveDocument", at],
      ]);
   });

   it("records a linear report's layout", async () => {
      const { storage, calls } = recordingStorage([RECORD]);
      await createDocument({
         layout: "linear",
         path: "story.malloy",
         document: {
            title: "Story",
            modelPath: "models/storefront.malloy",
            source: "order_items",
            view: "by_category",
         },
         target: {
            route: "storage",
            storage,
            workspace: RECORD,
            environmentName: "env",
            packageName: "pkg",
         },
      });
      expect(calls[0]).toEqual(["listDocuments", "record", undefined]);
      expect(calls.at(-1)?.[1]).toEqual(
         locator("record", "linear", "env/pkg/story.malloy"),
      );
   });
});

describe("DocumentStorage contract: find a draft's layout", () => {
   it("lists every workspace, then the chosen one's documents of every layout, matching on the path alone and reading the layout it recorded", async () => {
      const { storage, calls } = recordingStorage(
         [SCRATCH, RECORD],
         [
            locator("record", "linear", "env/pkg/story.malloy"),
            locator("scratch", "grid", "env/pkg/story.malloy"),
         ],
      );
      expect(await findDraftLayout(storage, "env", "pkg", "story.malloy")).toBe(
         "linear",
      );
      expect(calls).toEqual([
         ["listWorkspaces", false],
         ["listDocuments", "record", undefined],
      ]);
   });

   it("asks a store that reads one document's layout for that document alone, never listing", async () => {
      const { storage, calls } = recordingStorage(
         [SCRATCH, RECORD],
         [
            locator("record", "linear", "env/pkg/story.malloy"),
            locator("scratch", "grid", "env/pkg/story.malloy"),
         ],
      );
      const documents = [locator("record", "linear", "env/pkg/story.malloy")];
      storage.getDocumentType = async (at) => {
         calls.push(["getDocumentType", at]);
         const found = documents.find(
            (d) => d.workspace === at.workspace && d.path === at.path,
         );
         if (!found) throw new DocumentNotFoundError("absent");
         return found.type;
      };
      expect(await findDraftLayout(storage, "env", "pkg", "story.malloy")).toBe(
         "linear",
      );
      expect(
         await findDraftLayout(storage, "env", "pkg", "missing.malloy"),
      ).toBeUndefined();
      expect(calls).toEqual([
         ["listWorkspaces", false],
         [
            "getDocumentType",
            { workspace: "record", path: "env/pkg/story.malloy" },
         ],
         ["listWorkspaces", false],
         [
            "getDocumentType",
            { workspace: "record", path: "env/pkg/missing.malloy" },
         ],
      ]);
   });

   it("passes on a store's failure to answer, which is not absence", async () => {
      const { storage } = recordingStorage([RECORD]);
      storage.getDocumentType = async () => {
         throw new Error("backend down");
      };
      await expect(
         findDraftLayout(storage, "env", "pkg", "story.malloy"),
      ).rejects.toThrow("backend down");
   });
});

describe("DocumentStorage contract: open (the host's copy)", () => {
   const open = (storage: DocumentStorage, layout: DocumentType) =>
      renderHook(() =>
         useHostCopy({
            storage,
            layout,
            notebook: layout === "linear",
            environmentName: "env",
            packageName: "pkg",
            modelPath: "sales/q3.malloy",
         }),
      );

   it("reads the copy at `<env>/<pkg>/<path>`, with the layout it opened with", async () => {
      const { storage, calls } = recordingStorage(
         [RECORD],
         [
            {
               ...locator("record", "grid", "env/pkg/sales/q3.malloy"),
               content: "saved",
            },
         ],
      );
      const { result } = open(storage, "grid");
      await waitFor(() => expect(result.current.draftChecked).toBe(true));
      expect(result.current.draft).toBe("saved");
      expect(calls).toEqual([
         ["listWorkspaces", false],
         ["getDocument", locator("record", "grid", "env/pkg/sales/q3.malloy")],
      ]);
   });

   it("finds the copy at its path whatever layout it recorded", async () => {
      const { storage } = recordingStorage(
         [RECORD],
         [
            {
               ...locator("record", "linear", "env/pkg/sales/q3.malloy"),
               content: "saved linear",
            },
         ],
      );
      const { result } = open(storage, "grid");
      await waitFor(() => expect(result.current.draftChecked).toBe(true));
      expect(result.current.draft).toBe("saved linear");
   });

   it("reads a notebook's copy only from an authoritative workspace", async () => {
      const { storage, calls } = recordingStorage([SCRATCH]);
      const { result } = open(storage, "linear");
      await waitFor(() => expect(result.current.draftChecked).toBe(true));
      expect(calls).toEqual([["listWorkspaces", false]]);
   });
});

describe("DocumentStorage contract: save", () => {
   it("saves to `<env>/<pkg>/<path>`, recording the layout the document has, or the one it opened with", async () => {
      const { storage, calls } = recordingStorage([RECORD]);
      const noop = () => {};
      const ref = { current: undefined };
      const { result } = renderHook(() =>
         useSaveChannel({
            storage,
            workspace: RECORD,
            layout: "linear",
            notebook: true,
            environmentName: "env",
            packageName: "pkg",
            modelPath: "story.malloy",
            versionId: undefined,
            apiClients: {} as never,
            queryClient: new QueryClient(),
            mutable: true,
            textHeld: false,
            readFailure: undefined,
            authoritative: true,
            resume: undefined,
            setResume: noop,
            setOpened: noop,
            setDraft: noop,
            setOffered: noop,
            setSeen: noop,
            setWrote: noop,
            savedHashRef: ref,
            packageBaseRef: ref,
            fetchedAtRef: { current: 0 },
         }),
      );
      expect(result.current.writer).toBe("storage");
      await act(() => result.current.save!("the text"));
      await act(() => result.current.save!("switched", "grid"));
      expect(calls).toEqual([
         [
            "saveDocument",
            locator("record", "linear", "env/pkg/story.malloy"),
            "the text",
         ],
         [
            "saveDocument",
            locator("record", "grid", "env/pkg/story.malloy"),
            "switched",
         ],
      ]);
   });
});

describe("DocumentStorage contract: list and remove (the package page's drafts)", () => {
   beforeEach(() => clearCache());

   const mount = (storage: DocumentStorage) =>
      render(
         <Package
            resourceUri="publisher://environments/env/packages/pkg"
            onClickPackageFile={() => {}}
         />,
         {
            wrapper: ({ children }: { children: ReactNode }) =>
               serverWrapper({
                  children: (
                     <DocumentStorageProvider documentStorage={storage}>
                        {children}
                     </DocumentStorageProvider>
                  ),
               }),
         },
      );

   it("lists each writeable workspace's documents of every layout, and removes a draft by the locator it was listed under", async () => {
      const listed = locator("scratch", "linear", "env/pkg/team/n.malloy");
      const { storage, calls } = recordingStorage(
         [RECORD, SCRATCH, READ_ONLY],
         [listed, locator("scratch", "grid", "env/other/x.malloy")],
      );
      mount(storage);
      fireEvent.click(
         await screen.findByRole("button", {
            name: "Delete draft team/n.malloy",
         }),
      );
      await waitFor(() =>
         expect(calls.some(([method]) => method === "deleteDocument")).toBe(
            true,
         ),
      );
      expect(calls.slice(0, 3)).toEqual([
         ["listWorkspaces", false],
         ["listDocuments", "record", undefined],
         ["listDocuments", "scratch", undefined],
      ]);
      expect(calls.find(([method]) => method === "deleteDocument")).toEqual([
         "deleteDocument",
         listed,
      ]);
   });
});

describe("DocumentStorage contract: the browser store", () => {
   beforeEach(() => localStorage.clear());

   it("keys a document by its path, so a save under another layout replaces it and records that layout", async () => {
      const storage = new BrowserDocumentStorage();
      const asGrid = locator("Local", "grid", "env/pkg/x.malloy");
      const asLinear = locator("Local", "linear", "env/pkg/x.malloy");
      await storage.saveDocument(asGrid, "grid text");
      await storage.saveDocument(asLinear, "linear text");
      expect(await storage.getDocument(asGrid)).toBe("linear text");
      const [workspace] = await storage.listWorkspaces(false);
      expect(await storage.listDocuments(workspace)).toEqual([asLinear]);
      expect(await storage.listDocuments(workspace, "grid")).toEqual([]);
   });
});
