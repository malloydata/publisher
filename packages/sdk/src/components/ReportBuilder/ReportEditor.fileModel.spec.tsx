// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, within } from "@testing-library/react";
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
 * Which charts a tile can take come from the report file's own compiled
 * model, for the views it declares. That model is the package's file, so it
 * speaks for the builder only while the builder is on that text: a host's
 * record can declare a view of the same name with another shape.
 */
const file = (
   body: string,
) => `## artifact { title="Overview" tiles=["a -> kpis"] }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  # label="Key figures"
  view: kpis is { ${body} }
}`;
const PACKAGE_FILE = file("aggregate: revenue, order_count");
const RECORD_FILE = file("group_by: carrier; aggregate: revenue");

const total = (name: string) => ({
   kind: "dimension",
   name,
   type: { kind: "number_type" },
   annotations: [{ value: '#(malloy) reference_id = "x" calculation\n' }],
});
// The package file's compile: `kpis` is two totals and nothing else.
const SERVED = {
   modelPath: "dashboards/overview.malloy",
   sourceText: PACKAGE_FILE,
   sources: [{ name: "a", views: [{ name: "kpis" }] }],
   sourceInfos: [
      JSON.stringify({
         name: "a",
         schema: {
            fields: [
               {
                  kind: "view",
                  name: "kpis",
                  schema: { fields: [total("revenue"), total("order_count")] },
               },
            ],
         },
      }),
   ],
};

mockServerProvider(
   {
      models: {
         getModel: mock((_env: string, _pkg: string, path: string) =>
            Promise.resolve({
               data:
                  path === "dashboards/overview.malloy"
                     ? SERVED
                     : { modelPath: path, sources: [], sourceInfos: [] },
            }),
         ),
         executeQueryModel: mock(() => pending()),
         listModels: mock(() => Promise.resolve({ data: [] })),
      },
      reports: {
         getReport: mock(() =>
            Promise.resolve({
               data: {
                  path: "dashboards/overview.malloy",
                  givens: [],
                  tiles: [],
               },
            }),
         ),
         listReports: mock(() => Promise.resolve({ data: [] })),
      },
   },
   { mutable: true },
);

const { ReportEditor } = await import("./ReportEditor");

beforeEach(() => clearCache());

/** A host store that is the record, holding one document. */
class RecordStorage implements DocumentStorage {
   private readonly workspace: Workspace = {
      name: "Branch",
      writeable: true,
      description: "Saved to the draft branch",
      authoritative: true,
   };
   constructor(private readonly text: string) {}
   async listWorkspaces(): Promise<Workspace[]> {
      return [this.workspace];
   }
   async listDocuments(): Promise<DocumentLocator[]> {
      return [
         {
            workspace: this.workspace.name,
            type: "grid",
            path: "env/pkg/dashboards/overview.malloy",
         },
      ];
   }
   async getDocument(locator: DocumentLocator): Promise<string> {
      if (locator.path !== "env/pkg/dashboards/overview.malloy")
         throw new DocumentNotFoundError(`No dashboard at ${locator.path}`);
      return this.text;
   }
   async saveDocument(): Promise<void> {}
   async deleteDocument(): Promise<void> {}
   async moveDocument(): Promise<void> {
      throw new Error("not exercised");
   }
}

const editor = (
   <ReportEditor
      environmentName="env"
      packageName="pkg"
      dashboardName="overview"
   />
);

/** The Line choice in the open tile's Chart submenu. */
const lineChoice = async () => {
   fireEvent.click(await screen.findByLabelText("Settings for Key figures"));
   fireEvent.click(
      screen.getByRole("menuitem", { name: /^Chart/, hidden: true }),
   );
   const menu = screen.getByRole("menu", { name: "Chart", hidden: true });
   return within(menu)
      .getAllByRole("menuitemradio", { hidden: true })
      .find(
         (item) =>
            item.querySelector(".MuiListItemText-primary")?.textContent ===
            "Line",
      ) as HTMLElement;
};

describe("the chart choices of a view the report declares", () => {
   it("follow the package's compile while the builder is on the package's file", async () => {
      render(editor, { wrapper: serverWrapper });
      const line = await lineChoice();
      expect(line.getAttribute("aria-disabled")).toBe("true");
      expect(line.textContent).toContain(
         "This view has no field for the x-axis",
      );
   });

   it("do not follow it on a record whose view of the same name has another shape", async () => {
      render(
         <DocumentStorageProvider
            documentStorage={new RecordStorage(RECORD_FILE)}
         >
            {editor}
         </DocumentStorageProvider>,
         { wrapper: serverWrapper },
      );
      const line = await lineChoice();
      expect(line.getAttribute("aria-disabled")).not.toBe("true");
   });
});
