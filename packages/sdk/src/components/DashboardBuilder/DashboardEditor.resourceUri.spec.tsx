// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import { BrowserDocumentStorage } from "../DocumentStorage/BrowserDocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";

/**
 * `resourceUri` + `dashboard`, alongside the deprecated `environmentName` /
 * `packageName` / `dashboardName` form, and the `versionId` a `resourceUri`
 * can carry: what it reaches (every READ), what it never reaches (the write),
 * and what it does to Save when the write target is the package itself.
 */
const PACKAGE_FILE = `## artifact { title="Storefront" tiles=["a -> by_cat"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category
}`;

const getModel = mock(
   (_env: string, _pkg: string, path: string, _versionId?: string) =>
      Promise.resolve({
         data:
            path === "dashboards/overview.malloy"
               ? { modelPath: path, sourceText: PACKAGE_FILE }
               : {
                    modelPath: path,
                    sources: [
                       {
                          name: "scoped_orders",
                          views: [{ name: "by_category" }],
                       },
                    ],
                    sourceInfos: [
                       JSON.stringify({
                          name: "scoped_orders",
                          schema: { fields: [] },
                       }),
                    ],
                 },
      }),
);
const getDashboard = mock(
   (_env: string, _pkg: string, _slug: string, _versionId?: string) =>
      Promise.resolve({
         data: { path: "dashboards/overview.malloy", givens: [], tiles: [] },
      }),
);
const listDashboards = mock((_env: string, _pkg: string, _versionId?: string) =>
   Promise.resolve({ data: [{ name: "overview" }] }),
);
const updateModelSource = mock(
   (_env: string, _pkg: string, path: string, body: { source: string }) =>
      Promise.resolve({
         data: {
            path,
            contentHash: `hash-of-${body.source.length}`,
            created: false,
         },
      }),
);
const executeQueryModel = mock(() => pending());

// Mutated between tests: a `mutable` server is what puts Save into the
// package, which is the branch `versionId` has to gate.
const serverContext = { mutable: false };
mockServerProvider(
   {
      models: { getModel, executeQueryModel, updateModelSource },
      dashboards: { getDashboard, listDashboards },
   },
   serverContext,
);

const { DashboardEditor } = await import("./DashboardEditor");

const URI = "publisher://environments/env/packages/pkg";
const button = (name: string | RegExp) =>
   screen.getByRole("button", { name, hidden: true });

const mountByUri = (versionId?: string) =>
   render(
      <DocumentStorageProvider documentStorage={new BrowserDocumentStorage()}>
         <DashboardEditor
            resourceUri={
               versionId === undefined ? URI : `${URI}?versionId=${versionId}`
            }
            dashboard="overview"
         />
      </DocumentStorageProvider>,
      { wrapper: serverWrapper },
   );

const mountLegacy = () =>
   render(
      <DocumentStorageProvider documentStorage={new BrowserDocumentStorage()}>
         <DashboardEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="overview"
         />
      </DocumentStorageProvider>,
      { wrapper: serverWrapper },
   );

beforeEach(() => {
   clearCache();
   localStorage.clear();
   getModel.mockClear();
   getDashboard.mockClear();
   listDashboards.mockClear();
   updateModelSource.mockClear();
   // Uncleared, a tile query from an earlier test carries into the next one's
   // assertions, where it looks like the component under test made it.
   executeQueryModel.mockClear();
   serverContext.mutable = false;
});

describe("the two prop forms", () => {
   it("open the same dashboard", async () => {
      mountLegacy();
      expect(await screen.findByText("Storefront")).toBeDefined();
      const legacyCall = getModel.mock.calls.find(
         (call) => call[2] === "dashboards/overview.malloy",
      );
      cleanup();
      clearCache();
      getModel.mockClear();

      mountByUri();
      expect(await screen.findByText("Storefront")).toBeDefined();
      const uriCall = getModel.mock.calls.find(
         (call) => call[2] === "dashboards/overview.malloy",
      );

      // Same environment, package, path and (absent) version either way: the
      // union resolves to the same locals, not a second code path that
      // happens to agree today.
      expect(uriCall).toEqual(legacyCall);
      expect(uriCall).toEqual([
         "env",
         "pkg",
         "dashboards/overview.malloy",
         undefined,
      ]);
   });
});

describe("versionId", () => {
   it("reaches every read: the model, the manifest, the list, and the catalog", async () => {
      mountByUri("v7");
      expect(await screen.findByText("Storefront")).toBeDefined();

      await waitFor(() =>
         expect(
            getModel.mock.calls.some((call) => call[2] === "data_app.malloy"),
         ).toBe(true),
      );

      expect(
         getModel.mock.calls.find(
            (call) => call[2] === "dashboards/overview.malloy",
         ),
      ).toEqual(["env", "pkg", "dashboards/overview.malloy", "v7"]);
      expect(
         getModel.mock.calls.find((call) => call[2] === "data_app.malloy"),
      ).toEqual(["env", "pkg", "data_app.malloy", "v7"]);
      expect(getDashboard.mock.calls[0]).toEqual([
         "env",
         "pkg",
         "overview",
         "v7",
      ]);
      expect(listDashboards.mock.calls[0]).toEqual(["env", "pkg", "v7"]);

      // The key too, appended last, never spliced in the middle of it: a
      // version in the wrong slot is a key that cannot tell two versions of
      // one dashboard apart, and nothing about that failure is visible on
      // screen.
      expect(cacheKeys("dashboard-editor-model")[0]).toContain('"v7"');
      expect(cacheKeys("dashboard-editor-manifest")[0]).toContain('"v7"');
      expect(cacheKeys("dashboard-editor-dashboards")[0]).toContain('"v7"');
      await waitFor(() =>
         expect(cacheKeys("dashboard-editor-catalog")[0]).toContain('"v7"'),
      );
   });

   // The pin has to reach the TILE QUERIES too, not just the file, manifest
   // and catalog. Half-applied it is worse than absent: the editor shows v7's
   // text and field list while every tile runs against the current package, so
   // a view that changed between them displays data the file on screen does
   // not describe, and nothing says so.
   it("reaches the tile queries, not only the file the editor opens", async () => {
      mountByUri("v7");
      expect(await screen.findByText("Storefront")).toBeDefined();

      await waitFor(() =>
         expect(executeQueryModel.mock.calls.length).toBeGreaterThan(0),
      );
      for (const call of executeQueryModel.mock.calls)
         expect((call[3] as { versionId?: string }).versionId).toBe("v7");
   });

   it("never reaches the write", async () => {
      serverContext.mutable = true;
      mountByUri();
      await screen.findByText("Storefront");
      fireEvent.click(screen.getByLabelText("Settings for By category"));
      fireEvent.change(screen.getByLabelText("Tile title"), {
         target: { value: "Categories" },
      });
      fireEvent.keyDown(screen.getByLabelText("Tile title"), {
         key: "Escape",
      });
      fireEvent.click(button("Save changes"));

      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      // Four positional arguments, none of them a version: the client's
      // `updateModelSource` accepts one as a fifth, and Publisher answers 501
      // to it on this route, so threading it through here would make every
      // save from this component fail outright.
      expect(updateModelSource.mock.calls[0]).toHaveLength(4);
      expect(updateModelSource.mock.calls[0][4]).toBeUndefined();
   });

   it("does not stop a package save from invalidating the read it moved", async () => {
      serverContext.mutable = true;
      mountByUri();
      await screen.findByText("Storefront");
      const modelReadsBeforeSave = getModel.mock.calls.filter(
         (call) => call[2] === "dashboards/overview.malloy",
      ).length;

      fireEvent.click(screen.getByLabelText("Settings for By category"));
      fireEvent.change(screen.getByLabelText("Tile title"), {
         target: { value: "Categories" },
      });
      fireEvent.keyDown(screen.getByLabelText("Tile title"), {
         key: "Escape",
      });
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));

      // Proof, not a proxy for one: the live query's key now ends in a
      // version slot rather than at `modelPath`, and the invalidate call
      // that follows a save has to end in the same slot to still match it.
      // A version spliced into the middle of either key (as `Dashboard`
      // does for its own, unrelated, key) would leave this read stale
      // until something else happened to touch it.
      await waitFor(() =>
         expect(
            getModel.mock.calls.filter(
               (call) => call[2] === "dashboards/overview.malloy",
            ).length,
         ).toBeGreaterThan(modelReadsBeforeSave),
      );
   });
});

describe("Save, pinned to a version", () => {
   it("is off when the write target is the package, and the caption says why", async () => {
      serverContext.mutable = true;
      mountByUri("v7");
      await screen.findByText("Storefront");

      expect(
         screen.getByText(
            "Reading version v7: a version is a fixed point in history, so Save is off.",
         ),
      ).toBeDefined();
      expect(
         screen.queryByRole("button", { name: "Save changes", hidden: true }),
      ).toBeNull();
      expect(
         screen.queryByRole("button", { name: "Saved", hidden: true }),
      ).toBeNull();
   });

   it("is not off when the write target is storage instead of the package", async () => {
      serverContext.mutable = false;
      mountByUri("v7");
      await screen.findByText("Storefront");

      // Unaffected: a copy kept beside the package never goes through
      // `updateModelSource`, so the version pin has nothing to refuse.
      fireEvent.click(screen.getByLabelText("Settings for By category"));
      fireEvent.change(screen.getByLabelText("Tile title"), {
         target: { value: "Categories" },
      });
      fireEvent.keyDown(screen.getByLabelText("Tile title"), {
         key: "Escape",
      });
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
   });
});

/**
 * A render body that throws takes the host's whole tree down, which is a white
 * screen rather than a message. `parseResourceUri` throws on a string that is
 * not a `publisher://` URI at all and returns for one that merely names too
 * little, so both shapes have to reach the same display. An embedding host is
 * exactly who can pass either; the Console builds its URI from guarded params.
 */
describe("a malformed resourceUri", () => {
   const badUris = [
      ["names no package", "publisher://environments/env"],
      ["is not a URL at all", "nonsense"],
      ["is a URL of another scheme", "http://example.com/env"],
   ] as const;

   for (const [what, resourceUri] of badUris) {
      it(`degrades to an error display when it ${what}`, async () => {
         render(
            <DashboardEditor resourceUri={resourceUri} dashboard="overview" />,
            { wrapper: serverWrapper },
         );

         expect(
            await screen.findByText(
               /A dashboard resource URI must name an environment and a package/,
            ),
         ).toBeDefined();
      });
   }
});
