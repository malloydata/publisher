// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The contract the SDK holds a host's {@link DocumentStorage} to today: every
 * call each caller makes, with the exact locator. A host's store (the
 * Console's browser store, a platform's own) is written against these calls,
 * so a change to any of them is a change to that contract and must be made
 * here on purpose.
 *
 * Today a locator is `{ workspace, type, path }` where `type` is the report's
 * layout in the legacy vocabulary (`dashboard` grid, `notebook` linear) and
 * `path` is `<environment>/<package>/<package path>`. The type is part of the
 * key: the same path under two types is two documents.
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
import { useHostCopy } from "../DashboardBuilder/useHostCopy";
import { useSaveChannel } from "../DashboardBuilder/useSaveChannel";
import { findDraftType } from "../Package/useDraftType";
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
      documents.find(
         (d) =>
            d.workspace === l.workspace &&
            d.type === l.type &&
            d.path === l.path,
      );
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
   it("lists the workspace by type, reads the candidate, then saves it, each at `<env>/<pkg>/<path>` under the layout's type", async () => {
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
      const at = locator("record", "dashboard", "env/pkg/sales/q3.malloy");
      expect(
         calls.map(([method, ...args]) =>
            method === "saveDocument" ? [method, args[0]] : [method, ...args],
         ),
      ).toEqual([
         ["listDocuments", "record", "dashboard"],
         ["getDocument", at],
         ["saveDocument", at],
      ]);
   });

   it("files a linear report under `notebook`", async () => {
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
      expect(calls[0]).toEqual(["listDocuments", "record", "notebook"]);
      expect(calls.at(-1)?.[1]).toEqual(
         locator("record", "notebook", "env/pkg/story.malloy"),
      );
   });
});

describe("DocumentStorage contract: find a draft's type", () => {
   it("lists every workspace, then the chosen one's documents of every type, matching on the path alone", async () => {
      const { storage, calls } = recordingStorage(
         [SCRATCH, RECORD],
         [
            locator("record", "notebook", "env/pkg/story.malloy"),
            locator("scratch", "dashboard", "env/pkg/story.malloy"),
         ],
      );
      expect(await findDraftType(storage, "env", "pkg", "story.malloy")).toBe(
         "notebook",
      );
      expect(calls).toEqual([
         ["listWorkspaces", false],
         ["listDocuments", "record", undefined],
      ]);
   });
});

describe("DocumentStorage contract: open (the host's copy)", () => {
   const open = (
      storage: DocumentStorage,
      kind: "dashboard" | "notebook",
      notebook: boolean,
   ) =>
      renderHook(() =>
         useHostCopy({
            storage,
            kind,
            notebook,
            environmentName: "env",
            packageName: "pkg",
            modelPath: "sales/q3.malloy",
         }),
      );

   it("reads the copy at `<env>/<pkg>/<path>` under the opened kind", async () => {
      const { storage, calls } = recordingStorage(
         [RECORD],
         [
            {
               ...locator("record", "dashboard", "env/pkg/sales/q3.malloy"),
               content: "saved",
            },
         ],
      );
      const { result } = open(storage, "dashboard", false);
      await waitFor(() => expect(result.current.draftChecked).toBe(true));
      expect(result.current.draft).toBe("saved");
      expect(calls).toEqual([
         ["listWorkspaces", false],
         [
            "getDocument",
            locator("record", "dashboard", "env/pkg/sales/q3.malloy"),
         ],
      ]);
   });

   it("does not find a copy filed under the other type", async () => {
      const { storage } = recordingStorage(
         [RECORD],
         [locator("record", "notebook", "env/pkg/sales/q3.malloy")],
      );
      const { result } = open(storage, "dashboard", false);
      await waitFor(() => expect(result.current.draftChecked).toBe(true));
      expect(result.current.draft).toBeUndefined();
   });

   it("reads a notebook's copy only from an authoritative workspace", async () => {
      const { storage, calls } = recordingStorage([SCRATCH]);
      const { result } = open(storage, "notebook", true);
      await waitFor(() => expect(result.current.draftChecked).toBe(true));
      expect(calls).toEqual([["listWorkspaces", false]]);
   });
});

describe("DocumentStorage contract: save", () => {
   it("saves to `<env>/<pkg>/<path>` under the opened kind", async () => {
      const { storage, calls } = recordingStorage([RECORD]);
      const noop = () => {};
      const ref = { current: undefined };
      const { result } = renderHook(() =>
         useSaveChannel({
            storage,
            workspace: RECORD,
            kind: "notebook",
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
      expect(calls).toEqual([
         [
            "saveDocument",
            locator("record", "notebook", "env/pkg/story.malloy"),
            "the text",
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

   it("lists each writeable workspace's documents of every type, and removes a draft by the locator it was listed under", async () => {
      const listed = locator("scratch", "notebook", "env/pkg/team/n.malloy");
      const { storage, calls } = recordingStorage(
         [RECORD, SCRATCH, READ_ONLY],
         [listed, locator("scratch", "dashboard", "env/other/x.malloy")],
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

   it("keys a document by its type and path, so one path under two types is two documents", async () => {
      const storage = new BrowserDocumentStorage();
      const asGrid = locator("Local", "dashboard", "env/pkg/x.malloy");
      const asLinear = locator("Local", "notebook", "env/pkg/x.malloy");
      await storage.saveDocument(asGrid, "grid text");
      await storage.saveDocument(asLinear, "linear text");
      expect(await storage.getDocument(asGrid)).toBe("grid text");
      expect(await storage.getDocument(asLinear)).toBe("linear text");
      const [workspace] = await storage.listWorkspaces(false);
      expect(
         (await storage.listDocuments(workspace)).sort((a, b) =>
            a.type.localeCompare(b.type),
         ),
      ).toEqual([asGrid, asLinear]);
      expect(await storage.listDocuments(workspace, "notebook")).toEqual([
         asLinear,
      ]);
   });
});
