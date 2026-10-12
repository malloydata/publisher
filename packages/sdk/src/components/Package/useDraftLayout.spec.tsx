// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "bun:test";
import type { ReactNode } from "react";
import {
   BrowserDocumentStorage,
   DocumentStorageProvider,
   type DocumentStorage,
} from "../DocumentStorage";
import { locatorFor } from "../DocumentCreate/documentPath";
import { findDraftLayout, useDraftLayout } from "./useDraftLayout";

describe("findDraftLayout", () => {
   beforeEach(() => localStorage.clear());

   it("finds the layout a draft was saved with, which the listing cannot say", async () => {
      const storage = new BrowserDocumentStorage();
      // A save records the layout beside the document.
      await storage.saveDocument(
         locatorFor("linear", "Local", "env", "pkg", "story.malloy"),
         "## artifact { layout=linear }",
      );
      await storage.saveDocument(
         locatorFor("grid", "Local", "env", "pkg", "sales/q3.malloy"),
         "## artifact { layout=grid }",
      );
      expect(await findDraftLayout(storage, "env", "pkg", "story.malloy")).toBe(
         "linear",
      );
      expect(
         await findDraftLayout(storage, "env", "pkg", "sales/q3.malloy"),
      ).toBe("grid");
   });

   it("reads the browser store's one document rather than listing them all", async () => {
      const storage = new BrowserDocumentStorage();
      await storage.saveDocument(
         locatorFor("linear", "Local", "env", "pkg", "story.malloy"),
         "x",
      );
      storage.listDocuments = () => {
         throw new Error("listed");
      };
      expect(await findDraftLayout(storage, "env", "pkg", "story.malloy")).toBe(
         "linear",
      );
      expect(
         await findDraftLayout(storage, "env", "pkg", "absent.malloy"),
      ).toBeUndefined();
   });

   it("is undefined when the host keeps no copy, or keeps it for another package", async () => {
      const storage = new BrowserDocumentStorage();
      await storage.saveDocument(
         locatorFor("linear", "Local", "env", "other", "story.malloy"),
         "x",
      );
      expect(
         await findDraftLayout(storage, "env", "pkg", "story.malloy"),
      ).toBeUndefined();
   });
});

describe("useDraftLayout", () => {
   beforeEach(() => localStorage.clear());

   const withStorage = (storage: DocumentStorage) =>
      function Wrapper({ children }: { children: ReactNode }) {
         return (
            <DocumentStorageProvider documentStorage={storage}>
               {children}
            </DocumentStorageProvider>
         );
      };

   it("is settled at once with no layout when the host keeps no store", () => {
      const { result } = renderHook(() =>
         useDraftLayout("env", "pkg", "story.malloy"),
      );
      expect(result.current).toEqual({ layout: undefined, settled: true });
   });

   it("is settled at once with no layout when not enabled", () => {
      const { result } = renderHook(
         () => useDraftLayout("env", "pkg", "story.malloy", false),
         { wrapper: withStorage(new BrowserDocumentStorage()) },
      );
      expect(result.current).toEqual({ layout: undefined, settled: true });
   });

   it("is unsettled until the host answers, then has the draft's layout", async () => {
      const storage = new BrowserDocumentStorage();
      await storage.saveDocument(
         locatorFor("linear", "Local", "env", "pkg", "story.malloy"),
         "x",
      );
      const { result } = renderHook(
         () => useDraftLayout("env", "pkg", "story.malloy"),
         { wrapper: withStorage(storage) },
      );
      expect(result.current).toEqual({ layout: undefined, settled: false });
      await waitFor(() =>
         expect(result.current).toEqual({ layout: "linear", settled: true }),
      );
   });

   it("settles with no layout when the host cannot answer", async () => {
      const failing = {
         listWorkspaces: () => Promise.reject(new Error("down")),
      } as unknown as DocumentStorage;
      const { result } = renderHook(
         () => useDraftLayout("env", "pkg", "story.malloy"),
         { wrapper: withStorage(failing) },
      );
      await waitFor(() =>
         expect(result.current).toEqual({ layout: undefined, settled: true }),
      );
   });

   it("asks again for another path, unsettled until it answers", async () => {
      const storage = new BrowserDocumentStorage();
      await storage.saveDocument(
         locatorFor("grid", "Local", "env", "pkg", "b.malloy"),
         "x",
      );
      const { result, rerender } = renderHook(
         ({ path }) => useDraftLayout("env", "pkg", path),
         {
            wrapper: withStorage(storage),
            initialProps: { path: "a.malloy" },
         },
      );
      await waitFor(() => expect(result.current.settled).toBe(true));
      expect(result.current.layout).toBeUndefined();
      rerender({ path: "b.malloy" });
      expect(result.current.settled).toBe(false);
      await waitFor(() =>
         expect(result.current).toEqual({ layout: "grid", settled: true }),
      );
   });
});
