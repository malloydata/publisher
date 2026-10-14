// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { findViewOption } from "./testing/inline";
import {
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
import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type Workspace,
} from "../DocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";

/**
 * The editor on a document the host keeps as TEXT: the manifest is the
 * server's compile of that text for the viewer, and every tile runs as the
 * document's definitions followed by one `run:`.
 */
const DOCUMENT = `## artifact { title="Ops" tiles=["a -> by_cat", "gated -> total"] } dashboard { columns=12 }

source: a is orders extend {
  # label="By category"
  view: by_cat is by_category
}
`;

const DEFINITION = `source: a is orders extend {
  # label="By category"
  view: by_cat is by_category
}`;

const compiled = (givenNames: string[]) => ({
   data: {
      status: "success",
      problems: [],
      document: {
         kind: "dashboard",
         manifest: {
            name: "ops",
            title: "Ops",
            tiles: [
               { kind: "query", query: "a -> by_cat", givenNames },
               { kind: "query", query: "gated -> total", restricted: true },
            ],
            givens: [],
         },
         cells: [{ type: "code", kind: "definition", text: DEFINITION }],
      },
   },
});
const compileModelSource = mock(
   async (
      _env: string,
      _pkg: string,
      _path: string,
      _body: { source?: string; scope?: string; givens?: object },
   ) => compiled([]),
);
const executeQueryModel = mock(
   (
      _env: string,
      _pkg: string,
      _path: string,
      _body: { query?: string; queryName?: string },
   ) => pending(),
);
const getModel = mock((..._args: unknown[]): Promise<unknown> => pending());
const listModels = mock(
   (..._args: unknown[]): Promise<unknown> => Promise.resolve({ data: [] }),
);

mockServerProvider(
   {
      models: { compileModelSource, executeQueryModel, getModel, listModels },
      reports: {
         getReport: mock(() => pending()),
         listReports: mock(() => Promise.resolve({ data: [] })),
      },
   },
   { mutable: true },
);

const { ReportEditor } = await import("./ReportEditor");

const RECORD: Workspace = {
   name: "Host",
   writeable: true,
   description: "Saved by the host",
   authoritative: true,
};

class FakeStorage implements DocumentStorage {
   readonly documents = new Map<string, string>();
   constructor(private readonly workspace: Workspace = RECORD) {}
   async listWorkspaces(): Promise<Workspace[]> {
      return [this.workspace];
   }
   async listDocuments(): Promise<DocumentLocator[]> {
      return [];
   }
   async getDocument(locator: DocumentLocator): Promise<string> {
      const text = this.documents.get(locator.path);
      if (text === undefined) throw new DocumentNotFoundError("none");
      return text;
   }
   async saveDocument(locator: DocumentLocator, text: string): Promise<void> {
      this.documents.set(locator.path, text);
   }
   async deleteDocument(): Promise<void> {}
   async moveDocument(): Promise<void> {}
}

const mount = (
   storage: DocumentStorage,
   textSource: {
      modelPath: string;
      givens?: Record<string, string | string[]>;
      compileSource?: (text: string) => string | Promise<string>;
   } = {
      modelPath: "models/orders.malloy",
   },
) =>
   render(
      <DocumentStorageProvider documentStorage={storage}>
         <ReportEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="ops"
            textSource={textSource}
         />
      </DocumentStorageProvider>,
      { wrapper: serverWrapper },
   );

const documentStore = () => {
   const storage = new FakeStorage();
   storage.documents.set("env/pkg/dashboards/ops.malloy", DOCUMENT);
   return storage;
};

beforeEach(() => {
   cleanup();
   clearCache();
   compileModelSource.mockClear();
   executeQueryModel.mockClear();
   getModel.mockClear();
   listModels.mockClear();
});

describe("ReportEditor in text-source mode", () => {
   it("compiles the stored text for the viewer and reads the package model nowhere", async () => {
      mount(documentStore());

      await waitFor(() => expect(compileModelSource).toHaveBeenCalled());
      const [, , modelPath, body] = compileModelSource.mock.calls[0];
      expect(modelPath).toBe("models/orders.malloy");
      expect(body).toEqual({ source: DOCUMENT, scope: "append" });
      expect(getModel).not.toHaveBeenCalled();
   });

   it("sends the host's givens with the compile, so a gated source it can read is not marked restricted", async () => {
      mount(documentStore(), {
         modelPath: "models/orders.malloy",
         givens: { TENANTS: "acme" },
      });

      await waitFor(() => expect(compileModelSource).toHaveBeenCalled());
      expect(compileModelSource.mock.calls[0][3]).toEqual({
         source: DOCUMENT,
         scope: "append",
         givens: { TENANTS: "acme" },
      });
   });

   it("sends the same givens with a tile that reads one, and with nothing else", async () => {
      compileModelSource.mockImplementation(async () => compiled(["TENANTS"]));
      try {
         mount(documentStore(), {
            modelPath: "models/orders.malloy",
            givens: { TENANTS: "acme", UNRELATED: "x" },
         });

         await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
         expect(executeQueryModel.mock.calls[0][3]).toMatchObject({
            givens: { TENANTS: "acme" },
         });
         expect(
            JSON.stringify(executeQueryModel.mock.calls[0][3]),
         ).not.toContain("UNRELATED");
      } finally {
         compileModelSource.mockImplementation(async () => compiled([]));
      }
   });

   describe("a host given no control shows", () => {
      // A select control whose option source is gated on the host's given, beside a tile that reads it.
      const withSuggestControl = (given: string) =>
         compileModelSource.mockImplementation(async () => {
            const result = compiled([given]);
            result.data.document.manifest.givens = [
               {
                  name: "REGION",
                  type: "string",
                  control: "select",
                  suggest: {
                     source: "a",
                     dimension: "category",
                     givenNames: [given],
                  },
               },
            ] as never;
            return result;
         });
      const gatedDocumentStore = () => {
         const storage = new FakeStorage();
         storage.documents.set(
            "env/pkg/dashboards/ops.malloy",
            DOCUMENT.replace(
               "view: by_cat is by_category",
               "view: by_cat is by_category + { where: cat ~ $REGION }",
            ),
         );
         return storage;
      };
      const suggestBody = () =>
         executeQueryModel.mock.calls
            .map((call) => call[3])
            .find((body) => body.query?.includes("group_by: category"));

      it("is carried by a select control's suggest, which would otherwise be denied by the gate", async () => {
         withSuggestControl("TENANT");
         try {
            mount(gatedDocumentStore(), {
               modelPath: "models/orders.malloy",
               givens: { TENANT: "acme" },
            });
            await waitFor(() => expect(suggestBody()).toBeDefined());
            expect(suggestBody()).toMatchObject({
               givens: { TENANT: "acme" },
            });
         } finally {
            compileModelSource.mockImplementation(async () => compiled([]));
         }
      });

      it("reaches the compile, a tile run and a suggest when it is a list", async () => {
         withSuggestControl("TENANTS");
         try {
            const givens = { TENANTS: ["acme", "globex"] };
            mount(gatedDocumentStore(), {
               modelPath: "models/orders.malloy",
               givens,
            });
            await waitFor(() => expect(suggestBody()).toBeDefined());
            expect(compileModelSource.mock.calls[0][3].givens).toEqual(givens);
            expect(suggestBody()).toMatchObject({ givens });
            const tile = executeQueryModel.mock.calls
               .map((call) => call[3])
               .find(
                  (body) =>
                     body.query?.includes("run: a ->") &&
                     !body.query.includes("group_by: category"),
               );
            expect(tile).toMatchObject({ givens });
         } finally {
            compileModelSource.mockImplementation(async () => compiled([]));
         }
      });
   });

   it("runs a tile as the document's definitions followed by one run:, with no import, flag or given", async () => {
      mount(documentStore());

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      const [, , modelPath, body] = executeQueryModel.mock.calls[0];
      expect(modelPath).toBe("models/orders.malloy");
      expect(body.queryName).toBeUndefined();
      expect(body.query).toBe(`${DEFINITION}\n\nrun: a -> by_category`);
      expect(body.query).not.toMatch(/^(import|##!|given:)/m);
   });

   it("shows a restricted tile as a notice and runs nothing for it", async () => {
      mount(documentStore());

      expect(
         await screen.findByText("You don't have access to this data"),
      ).toBeDefined();
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      const ran = executeQueryModel.mock.calls.map((call) => call[3].query);
      expect(ran.some((query) => query?.includes("gated"))).toBe(false);
   });

   // A text-held document has no `import`, so it can only name what its run model lists, imported or not.
   it("offers only the run model's sources for a new tile, and writes no import for it", async () => {
      const exporting = (name: string) => ({
         modelPath: `models/${name}.malloy`,
         modelInfo: JSON.stringify({ entries: [{ kind: "source", name }] }),
         // orders also lists `other`, which it imports without exporting.
         sources: [
            { name, views: [{ name: "by_x" }] },
            ...(name === "orders"
               ? [{ name: "other", views: [{ name: "by_x" }] }]
               : []),
         ],
      });
      listModels.mockImplementation(() =>
         Promise.resolve({
            data: [
               { path: "models/other.malloy" },
               { path: "models/orders.malloy" },
            ],
         }),
      );
      getModel.mockImplementation((_env, _pkg, path) =>
         Promise.resolve({
            data: exporting(String(path).replace(/^models\/|\.malloy$/g, "")),
         }),
      );
      try {
         const storage = documentStore();
         mount(storage);

         fireEvent.click(
            await screen.findByRole("button", { name: "Add tile" }),
         );
         await findViewOption("by_x");
         expect(
            screen
               .getAllByRole("group", { hidden: true })
               .map((group) => group.getAttribute("aria-label"))
               .filter((name) => name === "orders" || name === "other")
               .sort(),
         ).toEqual(["orders", "other"]);
         fireEvent.click(await findViewOption("by_x"));
         fireEvent.click(
            screen.getByRole("button", { name: "Add tile", hidden: false }),
         );
         fireEvent.click(
            screen.getByRole("button", { name: "Save", hidden: true }),
         );

         await waitFor(() =>
            expect(
               storage.documents.get("env/pkg/dashboards/ops.malloy"),
            ).toContain("by_x_tile"),
         );
         const saved = storage.documents.get("env/pkg/dashboards/ops.malloy")!;
         expect(saved).not.toMatch(/^import /m);
      } finally {
         listModels.mockImplementation(() => Promise.resolve({ data: [] }));
         getModel.mockImplementation(() => pending());
      }
   });

   it("turns Add filter off, since the document holds no given: of its own", async () => {
      mount(documentStore());

      const add = await screen.findByRole("button", { name: "Add filter" });
      expect((add as HTMLButtonElement).disabled).toBe(true);
   });

   it("refuses a record that does not exist rather than opening the package", async () => {
      mount(new FakeStorage());

      expect(
         await screen.findByText(/no document at this location/),
      ).toBeDefined();
      expect(getModel).not.toHaveBeenCalled();
   });

   it("refuses a storage that is not the record", async () => {
      const beside = new FakeStorage({ ...RECORD, authoritative: false });
      beside.documents.set("env/pkg/dashboards/ops.malloy", DOCUMENT);
      mount(beside);

      expect(await screen.findByText(/authoritative/)).toBeDefined();
   });
});

describe("a host that compiles its own text (compileSource)", () => {
   /** A document that declares its own filter and binds it on its tile. */
   const WITH_FILTER = `##! experimental.givens
## artifact { title="Ops" tiles=["a -> by_cat"] } dashboard { columns=12 }

# label="Category"
given: CAT :: filter<string> is f'Outerwear'

source: a is orders extend {
  view: by_cat is by_category + { where: category ~ $CAT }
}
`;
   /** What a host does with it: no flag, no declaration, the value as a literal. */
   const resolve = (text: string) =>
      text
         .replace(/^##!.*\n/m, "")
         .replace(/^# label="Category"\ngiven: CAT .*\n/m, "")
         .replace(/\$CAT\b/g, "f'Outerwear'");

   const storeOf = (text: string) => {
      const storage = new FakeStorage();
      storage.documents.set("env/pkg/dashboards/ops.malloy", text);
      return storage;
   };

   it("compiles what the host makes of the text, and turns Add filter on", async () => {
      mount(storeOf(DOCUMENT), {
         modelPath: "models/orders.malloy",
         compileSource: (text) => `// resolved by the host\n${text}`,
      });

      await waitFor(() => expect(compileModelSource).toHaveBeenCalled());
      expect(compileModelSource.mock.calls[0][3]).toEqual({
         source: `// resolved by the host\n${DOCUMENT}`,
         scope: "append",
      });
      const add = await screen.findByRole("button", { name: "Add filter" });
      expect((add as HTMLButtonElement).disabled).toBe(false);
   });

   it("runs the document's own filter as a literal, never as a given the server is sent", async () => {
      mount(storeOf(WITH_FILTER), {
         modelPath: "models/orders.malloy",
         compileSource: resolve,
      });

      await waitFor(() => expect(compileModelSource).toHaveBeenCalled());
      const sent = compileModelSource.mock.calls[0][3].source ?? "";
      expect(sent).not.toContain("given:");
      expect(sent).not.toContain("$CAT");

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      const body = JSON.stringify(executeQueryModel.mock.calls[0][3]);
      expect(body).toContain("category ~ f'Outerwear'");
      expect(body).not.toContain('"CAT"');
      // The control is drawn from the document's own declaration.
      expect(
         await screen.findByRole("button", { name: /^Category: / }),
      ).toBeDefined();
   });

   it("says so when the host cannot prepare the text, and compiles nothing", async () => {
      mount(storeOf(DOCUMENT), {
         modelPath: "models/orders.malloy",
         compileSource: () => Promise.reject(new Error("CAT is the model's")),
      });

      expect(
         await screen.findByText(
            /could not be prepared to compile: CAT is the model's/,
         ),
      ).toBeDefined();
      expect(compileModelSource).not.toHaveBeenCalled();
   });
});
