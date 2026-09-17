// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it } from "bun:test";
import { BrowserDocumentStorage } from "./BrowserDocumentStorage";
import type { DocumentLocator, Workspace } from "./DocumentStorage";

const local: Workspace = { name: "Local", writeable: true, description: "" };
const dashboard: DocumentLocator = {
   workspace: "Local",
   type: "dashboard",
   path: "dashboards/overview.malloy",
};
const notebook: DocumentLocator = {
   workspace: "Local",
   type: "notebook",
   path: "tour.malloynb",
};

describe("BrowserDocumentStorage", () => {
   beforeEach(() => localStorage.clear());

   it("round-trips a document", async () => {
      const store = new BrowserDocumentStorage();
      await store.saveDocument(dashboard, "## artifact { tiles=[] }");
      expect(await store.getDocument(dashboard)).toBe(
         "## artifact { tiles=[] }",
      );
   });

   it("lists its own documents, by kind, and nothing else in localStorage", async () => {
      const store = new BrowserDocumentStorage();
      localStorage.setItem("publisher:themeMode", "dark");
      await store.saveDocument(dashboard, "a");
      await store.saveDocument(notebook, "b");
      expect(await store.listDocuments(local)).toEqual(
         expect.arrayContaining([dashboard, notebook]),
      );
      expect(await store.listDocuments(local)).toHaveLength(2);
      expect(await store.listDocuments(local, "notebook")).toEqual([notebook]);
   });

   it("keeps a path containing the separator intact", async () => {
      const store = new BrowserDocumentStorage();
      const odd = { ...dashboard, path: "dashboards/a:b.malloy" };
      await store.saveDocument(odd, "x");
      expect(await store.listDocuments(local, "dashboard")).toEqual([odd]);
   });

   it("rejects a read, delete or move of a document that is not there", async () => {
      const store = new BrowserDocumentStorage();
      await expect(store.getDocument(dashboard)).rejects.toThrow(
         /No dashboard/,
      );
      await expect(store.deleteDocument(dashboard)).rejects.toThrow();
      await expect(store.moveDocument(dashboard, notebook)).rejects.toThrow();
   });

   it("moves a document, content intact, and forgets the old address", async () => {
      const store = new BrowserDocumentStorage();
      await store.saveDocument(dashboard, "content");
      const renamed = { ...dashboard, path: "dashboards/sales.malloy" };
      await store.moveDocument(dashboard, renamed);
      expect(await store.getDocument(renamed)).toBe("content");
      await expect(store.getDocument(dashboard)).rejects.toThrow();
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
});
