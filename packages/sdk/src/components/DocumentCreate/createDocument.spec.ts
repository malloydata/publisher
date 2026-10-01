// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type DocumentType,
   type Workspace,
} from "../DocumentStorage/DocumentStorage";
import {
   createDocument,
   createRoute,
   MAX_SLUG_ATTEMPTS,
} from "./createDocument";

const WORKSPACE: Workspace = {
   name: "record",
   writeable: true,
   description: "The record",
   authoritative: true,
};

const DOC = {
   title: "Sales",
   modelPath: "models/storefront.malloy",
   source: "order_items",
   view: "by_category",
};

/** A store whose `saveDocument` replaces what is there, like the interface says. */
class FakeStorage implements DocumentStorage {
   readonly files = new Map<string, string>();
   readonly saves: string[] = [];
   /** What `listDocuments` reports; defaults to the files, a stale test sets it. */
   listing?: string[];
   getFailure?: Error;

   private key = (l: DocumentLocator) => `${l.type}:${l.path}`;
   seed(type: DocumentType, path: string, content = "original") {
      this.files.set(`${type}:${path}`, content);
   }
   async listWorkspaces() {
      return [WORKSPACE];
   }
   async listDocuments(_w: Workspace, type?: DocumentType) {
      const paths =
         this.listing ??
         [...this.files.keys()]
            .filter((k) => type === undefined || k.startsWith(`${type}:`))
            .map((k) => k.slice(k.indexOf(":") + 1));
      return paths.map((path) => ({
         workspace: WORKSPACE.name,
         type: type ?? "dashboard",
         path,
      }));
   }
   async getDocument(locator: DocumentLocator) {
      if (this.getFailure) throw this.getFailure;
      const text = this.files.get(this.key(locator));
      if (text === undefined) throw new DocumentNotFoundError("absent");
      return text;
   }
   async saveDocument(locator: DocumentLocator, content: string) {
      this.saves.push(locator.path);
      this.files.set(this.key(locator), content);
   }
   async deleteDocument() {}
}

const target = (storage: FakeStorage) =>
   ({
      route: "storage",
      storage,
      workspace: WORKSPACE,
      environmentName: "env",
      packageName: "pkg",
   }) as const;

describe("createDocument on the storage route", () => {
   it("writes a new locator and returns it", async () => {
      const storage = new FakeStorage();
      const created: unknown[] = [];
      const events: unknown[] = [];
      const result = await createDocument({
         kind: "notebook",
         document: DOC,
         target: target(storage),
         onCreated: (c) => created.push(c),
         onEvent: (e) => events.push(e),
      });
      expect(result.path).toBe("notebooks/sales.malloy");
      expect(result.slug).toBe("sales");
      expect(result.locator).toEqual({
         workspace: "record",
         type: "notebook",
         path: "env/pkg/notebooks/sales.malloy",
      });
      expect(storage.saves).toEqual(["env/pkg/notebooks/sales.malloy"]);
      expect(created).toEqual([result]);
      expect(events).toEqual([{ type: "notebook.created", where: "host" }]);
   });

   it("takes the next name when the listing shows the slug", async () => {
      const storage = new FakeStorage();
      storage.seed("dashboard", "env/pkg/dashboards/sales.malloy");
      const result = await createDocument({
         kind: "dashboard",
         document: DOC,
         target: target(storage),
      });
      expect(result.slug).toBe("sales-2");
      expect(
         storage.files.get("dashboard:env/pkg/dashboards/sales.malloy"),
      ).toBe("original");
   });

   it("never overwrites a file a stale listing misses", async () => {
      const storage = new FakeStorage();
      storage.seed("dashboard", "env/pkg/dashboards/sales.malloy");
      storage.seed("dashboard", "env/pkg/dashboards/sales-2.malloy", "two");
      storage.listing = [];
      const result = await createDocument({
         kind: "dashboard",
         document: DOC,
         target: target(storage),
      });
      expect(result.slug).toBe("sales-3");
      expect(storage.saves).toEqual(["env/pkg/dashboards/sales-3.malloy"]);
      expect(
         storage.files.get("dashboard:env/pkg/dashboards/sales.malloy"),
      ).toBe("original");
      expect(
         storage.files.get("dashboard:env/pkg/dashboards/sales-2.malloy"),
      ).toBe("two");
   });

   it("aborts, saving nothing, when the read fails for any reason but absence", async () => {
      const storage = new FakeStorage();
      storage.getFailure = new Error("backend down");
      await expect(
         createDocument({
            kind: "dashboard",
            document: DOC,
            target: target(storage),
         }),
      ).rejects.toThrow("backend down");
      expect(storage.saves).toEqual([]);
   });

   it("gives up after the attempt cap rather than looping", async () => {
      const storage = new FakeStorage();
      storage.listing = [];
      storage.seed("dashboard", "env/pkg/dashboards/sales.malloy");
      for (let n = 2; n <= MAX_SLUG_ATTEMPTS; n++)
         storage.seed("dashboard", `env/pkg/dashboards/sales-${n}.malloy`);
      await expect(
         createDocument({
            kind: "dashboard",
            document: DOC,
            target: target(storage),
         }),
      ).rejects.toThrow("No free file name");
      expect(storage.saves).toEqual([]);
   });

   it("refuses a title the writers cannot carry before touching the store", async () => {
      const storage = new FakeStorage();
      for (const title of ["a\nb", "x ##(authorize) y", "  "]) {
         await expect(
            createDocument({
               kind: "notebook",
               document: { ...DOC, title },
               target: target(storage),
            }),
         ).rejects.toThrow();
      }
      expect(storage.saves).toEqual([]);
   });
});

describe("createDocument on the package route", () => {
   it("writes create-only to a free name from the listing", async () => {
      const writes: Array<[string, string]> = [];
      const events: unknown[] = [];
      const result = await createDocument({
         kind: "dashboard",
         document: DOC,
         target: {
            route: "package",
            existing: ["dashboards/sales.malloy", "notebooks/sales.malloy"],
            write: async (path, source) => {
               writes.push([path, source]);
            },
         },
         onEvent: (e) => events.push(e),
      });
      expect(result).toEqual({
         kind: "dashboard",
         path: "dashboards/sales-2.malloy",
         slug: "sales-2",
      });
      expect(writes.map(([p]) => p)).toEqual(["dashboards/sales-2.malloy"]);
      expect(writes[0][1]).toContain("## artifact");
      expect(events).toEqual([{ type: "dashboard.created", where: "package" }]);
   });

   it("does not report a create the server refused", async () => {
      const events: unknown[] = [];
      await expect(
         createDocument({
            kind: "notebook",
            document: DOC,
            target: {
               route: "package",
               existing: [],
               write: async () => {
                  throw new Error("409");
               },
            },
            onEvent: (e) => events.push(e),
         }),
      ).rejects.toThrow("409");
      expect(events).toEqual([]);
   });
});

describe("createRoute", () => {
   it("is off for a browser workspace and on for a package or a record", () => {
      expect(
         createRoute({ authoritative: false, mutable: false, canStore: true }),
      ).toBeUndefined();
      expect(
         createRoute({ authoritative: false, mutable: true, canStore: true }),
      ).toBe("package");
      expect(
         createRoute({ authoritative: true, mutable: false, canStore: true }),
      ).toBe("storage");
      expect(
         createRoute({ authoritative: true, mutable: true, canStore: false }),
      ).toBeUndefined();
   });
});
