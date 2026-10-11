// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it } from "bun:test";
import { BrowserDocumentStorage } from "./BrowserDocumentStorage";
import type { DocumentLocator, Workspace } from "./DocumentStorage";

const local: Workspace = { name: "Local", writeable: true, description: "" };
const grid: DocumentLocator = {
   workspace: "Local",
   type: "grid",
   path: "env/pkg/dashboards/overview.malloy",
};
const linear: DocumentLocator = {
   workspace: "Local",
   type: "linear",
   path: "env/pkg/tour.malloy",
};

describe("BrowserDocumentStorage", () => {
   beforeEach(() => localStorage.clear());

   it("round-trips a document", async () => {
      const store = new BrowserDocumentStorage();
      await store.saveDocument(grid, "## artifact { tiles=[] }");
      expect(await store.getDocument(grid)).toBe("## artifact { tiles=[] }");
   });

   it("lists its own documents with their layouts, by layout, and nothing else in localStorage", async () => {
      const store = new BrowserDocumentStorage();
      localStorage.setItem("publisher:themeMode", "dark");
      await store.saveDocument(grid, "a");
      await store.saveDocument(linear, "b");
      expect(await store.listDocuments(local)).toEqual(
         expect.arrayContaining([grid, linear]),
      );
      expect(await store.listDocuments(local)).toHaveLength(2);
      expect(await store.listDocuments(local, "linear")).toEqual([linear]);
   });

   it("keys a document by its path alone: a save under another layout replaces it and records that layout", async () => {
      const store = new BrowserDocumentStorage();
      await store.saveDocument(grid, "as a grid");
      const switched = { ...grid, type: "linear" as const };
      await store.saveDocument(switched, "as linear");
      expect(await store.getDocument(grid)).toBe("as linear");
      expect(await store.listDocuments(local)).toEqual([switched]);
   });

   it("keeps a path containing a colon intact", async () => {
      const store = new BrowserDocumentStorage();
      const odd = { ...grid, path: "env/pkg/dashboards/a:b.malloy" };
      await store.saveDocument(odd, "x");
      expect(await store.listDocuments(local, "grid")).toEqual([odd]);
   });

   it("rejects a read, delete or move of a document that is not there", async () => {
      const store = new BrowserDocumentStorage();
      await expect(store.getDocument(grid)).rejects.toThrow(/No report/);
      await expect(store.deleteDocument(grid)).rejects.toThrow();
      await expect(store.moveDocument(grid, linear)).rejects.toThrow();
   });

   it("moves a document, content intact, records the new layout, and forgets the old address", async () => {
      const store = new BrowserDocumentStorage();
      await store.saveDocument(grid, "content");
      const renamed = {
         ...grid,
         type: "linear" as const,
         path: "env/pkg/sales.malloy",
      };
      await store.moveDocument(grid, renamed);
      expect(await store.getDocument(renamed)).toBe("content");
      expect(await store.listDocuments(local)).toEqual([renamed]);
      await expect(store.getDocument(grid)).rejects.toThrow();
   });

   it("has one writeable workspace and lists nothing for any other", async () => {
      const store = new BrowserDocumentStorage();
      expect(await store.listWorkspaces(true)).toEqual([
         expect.objectContaining({ name: "Local", writeable: true }),
      ]);
      expect(
         await store.listDocuments({
            name: "Shared",
            writeable: false,
            description: "",
         }),
      ).toEqual([]);
   });

   it("ignores a key of its own prefix that does not hold a document", async () => {
      localStorage.setItem("publisher:report:env/pkg/x.malloy", "not json");
      const store = new BrowserDocumentStorage();
      expect(await store.listDocuments(local)).toEqual([]);
   });
});

describe("BrowserDocumentStorage: drafts saved under the old type-keyed scheme", () => {
   beforeEach(() => localStorage.clear());

   it("moves each to its path, its old type read as its layout, the first time the store is used", async () => {
      localStorage.setItem(
         "publisher:document:dashboard:env/pkg/sales/q3.malloy",
         "grid draft",
      );
      localStorage.setItem(
         "publisher:document:notebook:env/pkg/story.malloy",
         "linear draft",
      );
      const store = new BrowserDocumentStorage();
      expect(
         (await store.listDocuments(local)).sort((a, b) =>
            a.path.localeCompare(b.path),
         ),
      ).toEqual([
         { workspace: "Local", type: "grid", path: "env/pkg/sales/q3.malloy" },
         { workspace: "Local", type: "linear", path: "env/pkg/story.malloy" },
      ]);
      expect(
         await store.getDocument({ ...linear, path: "env/pkg/story.malloy" }),
      ).toBe("linear draft");
      expect(
         localStorage.getItem(
            "publisher:document:dashboard:env/pkg/sales/q3.malloy",
         ),
      ).toBeNull();
      expect(
         localStorage.getItem(
            "publisher:document:notebook:env/pkg/story.malloy",
         ),
      ).toBeNull();
   });

   it("is found by a read before any listing", async () => {
      localStorage.setItem(
         "publisher:document:notebook:env/pkg/tour.malloy",
         "kept",
      );
      expect(await new BrowserDocumentStorage().getDocument(linear)).toBe(
         "kept",
      );
   });

   it("never overwrites a document already at the path: the old copy stays where it was", async () => {
      const store = new BrowserDocumentStorage();
      await store.saveDocument(linear, "current");
      localStorage.setItem(
         "publisher:document:dashboard:env/pkg/tour.malloy",
         "older",
      );
      const fresh = new BrowserDocumentStorage();
      expect(await fresh.getDocument(linear)).toBe("current");
      expect(
         localStorage.getItem(
            "publisher:document:dashboard:env/pkg/tour.malloy",
         ),
      ).toBe("older");
   });
});
