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
   reportPathProblem,
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

/** A store keyed by path whose `saveDocument` replaces what is there, like the interface says. */
class FakeStorage implements DocumentStorage {
   readonly files = new Map<string, string>();
   readonly layouts = new Map<string, DocumentType>();
   readonly saves: string[] = [];
   /** What `listDocuments` reports; defaults to the files, a stale test sets it. */
   listing?: string[];
   getFailure?: Error;

   seed(path: string, content = "original", layout: DocumentType = "grid") {
      this.files.set(path, content);
      this.layouts.set(path, layout);
   }
   async listWorkspaces() {
      return [WORKSPACE];
   }
   async listDocuments(_w: Workspace, type?: DocumentType) {
      const paths =
         this.listing ??
         [...this.files.keys()].filter(
            (path) => type === undefined || this.layouts.get(path) === type,
         );
      return paths.map((path) => ({
         workspace: WORKSPACE.name,
         type: this.layouts.get(path) ?? "grid",
         path,
      }));
   }
   async getDocument(locator: DocumentLocator) {
      if (this.getFailure) throw this.getFailure;
      const text = this.files.get(locator.path);
      if (text === undefined) throw new DocumentNotFoundError("absent");
      return text;
   }
   async saveDocument(locator: DocumentLocator, content: string) {
      this.saves.push(locator.path);
      this.files.set(locator.path, content);
      this.layouts.set(locator.path, locator.type);
   }
   async deleteDocument() {}
   async moveDocument() {}
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
         layout: "linear",
         document: DOC,
         target: target(storage),
         onCreated: (c) => created.push(c),
         onEvent: (e) => events.push(e),
      });
      expect(result.path).toBe("sales.malloy");
      expect(result.slug).toBe("sales");
      expect(result.locator).toEqual({
         workspace: "record",
         type: "linear",
         path: "env/pkg/sales.malloy",
      });
      expect(storage.saves).toEqual(["env/pkg/sales.malloy"]);
      expect(created).toEqual([result]);
      expect(events).toEqual([{ type: "notebook.created", where: "host" }]);
   });

   it("takes the next name when the listing shows the slug", async () => {
      const storage = new FakeStorage();
      storage.seed("env/pkg/sales.malloy");
      const result = await createDocument({
         layout: "grid",
         document: DOC,
         target: target(storage),
      });
      expect(result.slug).toBe("sales-2");
      expect(storage.files.get("env/pkg/sales.malloy")).toBe("original");
   });

   it("never overwrites a file a stale listing misses", async () => {
      const storage = new FakeStorage();
      storage.seed("env/pkg/sales.malloy");
      storage.seed("env/pkg/sales-2.malloy", "two");
      storage.listing = [];
      const result = await createDocument({
         layout: "grid",
         document: DOC,
         target: target(storage),
      });
      expect(result.slug).toBe("sales-3");
      expect(storage.saves).toEqual(["env/pkg/sales-3.malloy"]);
      expect(storage.files.get("env/pkg/sales.malloy")).toBe("original");
      expect(storage.files.get("env/pkg/sales-2.malloy")).toBe("two");
   });

   it("aborts, saving nothing, when the read fails for any reason but absence", async () => {
      const storage = new FakeStorage();
      storage.getFailure = new Error("backend down");
      await expect(
         createDocument({
            layout: "grid",
            document: DOC,
            target: target(storage),
         }),
      ).rejects.toThrow("backend down");
      expect(storage.saves).toEqual([]);
   });

   it("gives up after the attempt cap rather than looping", async () => {
      const storage = new FakeStorage();
      storage.listing = [];
      storage.seed("env/pkg/sales.malloy");
      for (let n = 2; n <= MAX_SLUG_ATTEMPTS; n++)
         storage.seed(`env/pkg/sales-${n}.malloy`);
      await expect(
         createDocument({
            layout: "grid",
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
               layout: "linear",
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
         layout: "grid",
         document: DOC,
         target: {
            route: "package",
            existing: ["sales.malloy", "other/sales.malloy"],
            write: async (path, source) => {
               writes.push([path, source]);
            },
         },
         onEvent: (e) => events.push(e),
      });
      expect(result).toEqual({
         layout: "grid",
         path: "sales-2.malloy",
         slug: "sales-2",
      });
      expect(writes.map(([p]) => p)).toEqual(["sales-2.malloy"]);
      expect(writes[0][1]).toContain("##| artifact { layout=grid");
      expect(events).toEqual([{ type: "dashboard.created", where: "package" }]);
   });

   it("does not report a create the server refused", async () => {
      const events: unknown[] = [];
      await expect(
         createDocument({
            layout: "linear",
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

describe("createDocument names", () => {
   it("on the storage route, skips a path the package listing has", async () => {
      const storage = new FakeStorage();
      const result = await createDocument({
         layout: "grid",
         document: DOC,
         target: {
            ...target(storage),
            existing: ["sales.malloy"],
         },
      });
      expect(result.slug).toBe("sales-2");
   });

   it("on the package route, moves to the next name when the server answers 409", async () => {
      const tried: string[] = [];
      const result = await createDocument({
         layout: "grid",
         document: DOC,
         target: {
            route: "package",
            existing: [],
            write: async (path) => {
               tried.push(path);
               if (tried.length < 3)
                  throw Object.assign(new Error("exists"), {
                     response: { status: 409 },
                  });
            },
         },
      });
      expect(tried).toEqual([
         "sales.malloy",
         "sales-2.malloy",
         "sales-3.malloy",
      ]);
      expect(result.slug).toBe("sales-3");
   });

   it("on the package route, any other failure is thrown, not retried", async () => {
      let writes = 0;
      await expect(
         createDocument({
            layout: "grid",
            document: DOC,
            target: {
               route: "package",
               existing: [],
               write: async () => {
                  writes++;
                  throw new Error("boom");
               },
            },
         }),
      ).rejects.toThrow("boom");
      expect(writes).toBe(1);
   });
});

describe("createRoute", () => {
   it("is off on a pinned version for the package, not for a record", () => {
      const host = { mutable: true, canStore: true, versionId: "v2" };
      expect(createRoute({ ...host, authoritative: false })).toBeUndefined();
      expect(createRoute({ ...host, authoritative: true })).toBe("storage");
   });

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

describe("createDocument at a path", () => {
   const write = () => {
      const writes: Array<[string, string]> = [];
      return {
         writes,
         target: {
            route: "package" as const,
            existing: ["taken.malloy"],
            write: async (path: string, source: string) => {
               writes.push([path, source]);
            },
         },
      };
   };

   it("writes `<slug>.malloy` at the package root, importing the model beside it", async () => {
      const { writes, target: t } = write();
      const result = await createDocument({
         layout: "linear",
         document: { ...DOC, modelPath: "storefront.malloy" },
         target: t,
      });
      expect(result).toEqual({
         layout: "linear",
         path: "sales.malloy",
         slug: "sales",
      });
      expect(writes[0][1]).toContain(
         'import { order_items } from "storefront.malloy"',
      );
      expect(writes[0][1]).toContain("layout=linear");
      expect(writes[0][1]).not.toContain("kind=notebook");
   });

   it("defaults to a linear report", async () => {
      const { target: t } = write();
      const result = await createDocument({ document: DOC, target: t });
      expect(result.layout).toBe("linear");
   });

   it("writes at a nested path, with the import relative to it", async () => {
      const { writes, target: t } = write();
      const result = await createDocument({
         layout: "grid",
         path: "team/sales.malloy",
         document: { ...DOC, modelPath: "m.malloy" },
         target: t,
      });
      expect(result.path).toBe("team/sales.malloy");
      expect(result.slug).toBe("sales");
      expect(writes[0][0]).toBe("team/sales.malloy");
      expect(writes[0][1]).toContain('from "../m.malloy"');
      expect(writes[0][1]).toContain("layout=grid");
   });

   it("puts a title-named file in a directory", async () => {
      const { writes, target: t } = write();
      await createDocument({
         directory: "/team/",
         document: DOC,
         target: t,
      });
      expect(writes[0][0]).toBe("team/sales.malloy");
   });

   it("refuses a taken path rather than renaming it", async () => {
      const { writes, target: t } = write();
      await expect(
         createDocument({ path: "taken.malloy", document: DOC, target: t }),
      ).rejects.toThrow("taken.malloy already exists");
      expect(writes).toEqual([]);
   });

   it("refuses a path the server answers 409 for, once", async () => {
      const tried: string[] = [];
      await expect(
         createDocument({
            path: "x.malloy",
            document: DOC,
            target: {
               route: "package",
               existing: [],
               write: async (path) => {
                  tried.push(path);
                  throw Object.assign(new Error("exists"), {
                     response: { status: 409 },
                  });
               },
            },
         }),
      ).rejects.toThrow("already exists");
      expect(tried).toEqual(["x.malloy"]);
   });

   it("refuses a path a report cannot live at, before writing", async () => {
      for (const path of [
         "public/x.malloy",
         "index.malloy",
         "../x.malloy",
         "a/../x.malloy",
         "/x.malloy",
         "x.txt",
         "x",
         "dir/.malloy",
      ]) {
         const { writes, target: t } = write();
         await expect(
            createDocument({ path, document: DOC, target: t }),
         ).rejects.toThrow();
         expect(writes).toEqual([]);
      }
   });
});

describe("createDocument in a directory", () => {
   const BAD_DIRECTORIES = [
      "../x",
      "a/../../x",
      "public",
      "Public/reports",
      "./x",
   ];

   it("refuses a directory a report cannot live in, before the package is written", async () => {
      for (const directory of BAD_DIRECTORIES) {
         const writes: string[] = [];
         await expect(
            createDocument({
               directory,
               document: DOC,
               target: {
                  route: "package",
                  existing: [],
                  write: async (path) => {
                     writes.push(path);
                  },
               },
            }),
         ).rejects.toThrow();
         expect(writes).toEqual([]);
      }
   });

   it("refuses it before the store is asked anything", async () => {
      for (const directory of BAD_DIRECTORIES) {
         const storage = new FakeStorage();
         let asked = 0;
         storage.listDocuments = async () => {
            asked++;
            return [];
         };
         storage.getDocument = async () => {
            asked++;
            throw new DocumentNotFoundError("absent");
         };
         await expect(
            createDocument({
               directory,
               document: DOC,
               target: target(storage),
            }),
         ).rejects.toThrow();
         expect(asked).toBe(0);
         expect(storage.saves).toEqual([]);
      }
   });

   it("still writes into a directory that is fine", async () => {
      const writes: string[] = [];
      const created = await createDocument({
         directory: "team/q3",
         document: DOC,
         target: {
            route: "package",
            existing: [],
            write: async (path) => {
               writes.push(path);
            },
         },
      });
      expect(created.path).toBe("team/q3/sales.malloy");
      expect(writes).toEqual(["team/q3/sales.malloy"]);
   });
});

describe("reportPathProblem", () => {
   it("accepts a path anywhere in the package outside public/", () => {
      for (const path of [
         "x.malloy",
         "sales/q3.malloy",
         "dashboards/x.malloy",
         "notebooks/a/b.malloy",
         "sub/index.malloy",
      ])
         expect(reportPathProblem(path)).toBeUndefined();
   });

   it("names why a path is refused", () => {
      expect(reportPathProblem("public/x.malloy")).toContain("public/");
      // On a case-insensitive file system these land in public/ too.
      expect(reportPathProblem("Public/x.malloy")).toContain("public/");
      expect(reportPathProblem("PUBLIC/a/x.malloy")).toContain("public/");
      expect(reportPathProblem("publications/x.malloy")).toBeUndefined();
      expect(reportPathProblem("index.malloy")).toContain("index.malloy");
      expect(reportPathProblem("../x.malloy")).toContain("inside the package");
      expect(reportPathProblem("x.malloynb")).toContain(".malloy");
   });
});

/**
 * A host store that files documents by layout, as a host may:
 * whatever path the locator asks for, the file lands in `dashboards/` or
 * `notebooks/` under its basename.
 */
class RelocatingStorage extends FakeStorage {
   readonly written = new Map<string, string>();
   relocate = (locator: DocumentLocator) => {
      const base = locator.path.slice(locator.path.lastIndexOf("/") + 1);
      return `${locator.type === "grid" ? "dashboards" : "notebooks"}/${base}`;
   };
   resolveDocumentPath(locator: DocumentLocator) {
      return this.relocate(locator);
   }
   async saveDocument(locator: DocumentLocator, content: string) {
      await super.saveDocument(locator, content);
      this.written.set(this.relocate(locator), content);
   }
}

describe("createDocument into a store that chooses the file's place", () => {
   it("writes the import for where the store puts the file, and returns that path", async () => {
      const storage = new RelocatingStorage();
      const created = await createDocument({
         layout: "grid",
         document: DOC,
         target: target(storage),
      });
      expect(created.path).toBe("dashboards/sales.malloy");
      expect(created.slug).toBe("sales");
      const text = storage.written.get("dashboards/sales.malloy")!;
      expect(text).toContain(
         'import { order_items } from "../models/storefront.malloy"',
      );
   });

   it("skips a name whose real place the package already has", async () => {
      const storage = new RelocatingStorage();
      const created = await createDocument({
         layout: "linear",
         document: DOC,
         target: {
            ...target(storage),
            existing: ["notebooks/sales.malloy"],
         },
      });
      expect(created.path).toBe("notebooks/sales-2.malloy");
   });

   it("falls back to the requested path for a store that says nothing", async () => {
      const storage = new FakeStorage();
      const created = await createDocument({
         layout: "grid",
         document: DOC,
         target: target(storage),
      });
      expect(created.path).toBe("sales.malloy");
      expect(storage.files.get("env/pkg/sales.malloy")).toContain(
         'from "models/storefront.malloy"',
      );
   });
});
