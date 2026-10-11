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
import { findDraftType, useDraftType } from "./useDraftType";

describe("findDraftType", () => {
   beforeEach(() => localStorage.clear());

   it("finds the type a draft was saved under, which the listing cannot say", async () => {
      const storage = new BrowserDocumentStorage();
      // createDocument files a linear report under `notebook`.
      await storage.saveDocument(
         locatorFor("notebook", "Local", "env", "pkg", "story.malloy"),
         "## artifact { layout=linear }",
      );
      await storage.saveDocument(
         locatorFor("dashboard", "Local", "env", "pkg", "sales/q3.malloy"),
         "## artifact { layout=grid }",
      );
      expect(await findDraftType(storage, "env", "pkg", "story.malloy")).toBe(
         "notebook",
      );
      expect(
         await findDraftType(storage, "env", "pkg", "sales/q3.malloy"),
      ).toBe("dashboard");
   });

   it("is undefined when the host keeps no copy, or keeps it for another package", async () => {
      const storage = new BrowserDocumentStorage();
      await storage.saveDocument(
         locatorFor("notebook", "Local", "env", "other", "story.malloy"),
         "x",
      );
      expect(
         await findDraftType(storage, "env", "pkg", "story.malloy"),
      ).toBeUndefined();
   });
});

describe("useDraftType", () => {
   beforeEach(() => localStorage.clear());

   const withStorage = (storage: DocumentStorage) =>
      function Wrapper({ children }: { children: ReactNode }) {
         return (
            <DocumentStorageProvider documentStorage={storage}>
               {children}
            </DocumentStorageProvider>
         );
      };

   it("is settled at once with no type when the host keeps no store", () => {
      const { result } = renderHook(() =>
         useDraftType("env", "pkg", "story.malloy"),
      );
      expect(result.current).toEqual({ type: undefined, settled: true });
   });

   it("is settled at once with no type when not enabled", () => {
      const { result } = renderHook(
         () => useDraftType("env", "pkg", "story.malloy", false),
         { wrapper: withStorage(new BrowserDocumentStorage()) },
      );
      expect(result.current).toEqual({ type: undefined, settled: true });
   });

   it("is unsettled until the host answers, then has the draft's type", async () => {
      const storage = new BrowserDocumentStorage();
      await storage.saveDocument(
         locatorFor("notebook", "Local", "env", "pkg", "story.malloy"),
         "x",
      );
      const { result } = renderHook(
         () => useDraftType("env", "pkg", "story.malloy"),
         { wrapper: withStorage(storage) },
      );
      expect(result.current).toEqual({ type: undefined, settled: false });
      await waitFor(() =>
         expect(result.current).toEqual({ type: "notebook", settled: true }),
      );
   });

   it("settles with no type when the host cannot answer", async () => {
      const failing = {
         listWorkspaces: () => Promise.reject(new Error("down")),
      } as unknown as DocumentStorage;
      const { result } = renderHook(
         () => useDraftType("env", "pkg", "story.malloy"),
         { wrapper: withStorage(failing) },
      );
      await waitFor(() =>
         expect(result.current).toEqual({ type: undefined, settled: true }),
      );
   });

   it("asks again for another path, unsettled until it answers", async () => {
      const storage = new BrowserDocumentStorage();
      await storage.saveDocument(
         locatorFor("dashboard", "Local", "env", "pkg", "b.malloy"),
         "x",
      );
      const { result, rerender } = renderHook(
         ({ path }) => useDraftType("env", "pkg", path),
         {
            wrapper: withStorage(storage),
            initialProps: { path: "a.malloy" },
         },
      );
      await waitFor(() => expect(result.current.settled).toBe(true));
      expect(result.current.type).toBeUndefined();
      rerender({ path: "b.malloy" });
      expect(result.current.settled).toBe(false);
      await waitFor(() =>
         expect(result.current).toEqual({ type: "dashboard", settled: true }),
      );
   });
});
