// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
   within,
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
      dashboards: {
         getDashboard: mock(() => pending()),
         listDashboards: mock(() => Promise.resolve({ data: [] })),
      },
   },
   { mutable: true },
);

const { DashboardEditor } = await import("./DashboardEditor");

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
   } = {
      modelPath: "models/orders.malloy",
   },
) =>
   render(
      <DocumentStorageProvider documentStorage={storage}>
         <DashboardEditor
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

describe("DashboardEditor in text-source mode", () => {
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

   // A text-held document has no `import`, so it can only name what its run model exports.
   it("offers only the run model's sources for a new tile, and writes no import for it", async () => {
      const exporting = (name: string) => ({
         modelPath: `models/${name}.malloy`,
         modelInfo: JSON.stringify({ entries: [{ kind: "source", name }] }),
         sources: [{ name, views: [{ name: "by_x" }] }],
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
         fireEvent.mouseDown(
            await screen.findByRole("combobox", { name: /Source/ }),
         );
         const options = within(screen.getByRole("listbox")).getAllByRole(
            "option",
         );
         expect(options.map((o) => o.textContent)).toEqual(["orders"]);
         fireEvent.keyDown(screen.getByRole("listbox"), { key: "Escape" });
         fireEvent.click(await screen.findByLabelText("View by_x"));
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
