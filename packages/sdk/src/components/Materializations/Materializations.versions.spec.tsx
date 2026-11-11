// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A pinned version's runs are that version's: the section lists them for it,
 * and deleting one names it too, so the server refuses a run of another
 * version (404) or of an archived one (410) rather than deleting by id alone.
 *
 * The build plan the detail dialog shows is fetched only once a run is opened,
 * falls back to the package response's inline plan on a server without the
 * route (404), and reads as unavailable on any other failure.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

const RUN = {
   id: "run-1",
   status: "MANIFEST_FILE_READY",
   createdAt: "2026-10-01T00:00:00Z",
   updatedAt: "2026-10-01T00:01:00Z",
};

const listMaterializations = mock(
   (
      _environmentName: string,
      _packageName: string,
      _limit?: number,
      _offset?: number,
      _versionId?: string,
   ) => Promise.resolve({ data: [RUN] }),
);
const deleteMaterialization = mock(
   (
      _environmentName: string,
      _packageName: string,
      _materializationId: string,
      _dropTables?: boolean,
      _versionId?: string,
   ) => Promise.resolve({ data: undefined }),
);

const EMPTY_PLAN = { graphs: [], sources: {} };

const PLAN_SOURCE = {
   name: "orders_daily",
   sourceID: "orders_daily@model.malloy",
   connectionName: "duckdb",
   dialect: "duckdb",
   columns: [],
   sourceEntityId: "entity-1",
};

function planWith(name: string) {
   return {
      graphs: [],
      sources: { [name]: { ...PLAN_SOURCE, name, sourceID: name } },
   };
}

function httpError(status: number, data: Record<string, unknown>) {
   return Object.assign(
      new Error(`Request failed with status code ${status}`),
      { response: { status, data } },
   );
}

const getPackageBuildPlan = mock(
   (
      _environmentName: string,
      _packageName: string,
      _versionId?: string,
      _options?: unknown,
   ): Promise<{ data: unknown }> => Promise.resolve({ data: EMPTY_PLAN }),
);
const getPackage = mock(
   (
      _environmentName: string,
      _packageName: string,
      _versionId?: string,
   ): Promise<{ data: unknown }> => pending(),
);

mockServerProvider(
   {
      packages: { getPackage, getPackageBuildPlan },
      materializations: { listMaterializations, deleteMaterialization },
   },
   { mutable: true },
);

const { default: Materializations } = await import("./Materializations");

beforeEach(() => {
   clearCache();
   listMaterializations.mockClear();
   deleteMaterialization.mockClear();
   getPackageBuildPlan.mockReset();
   getPackageBuildPlan.mockImplementation(() =>
      Promise.resolve({ data: EMPTY_PLAN }),
   );
   getPackage.mockReset();
   getPackage.mockImplementation(() => pending());
});

async function openTheRun(resourceUri: string) {
   render(<Materializations resourceUri={resourceUri} />, {
      wrapper: serverWrapper,
   });
   fireEvent.click(
      await screen.findByRole("button", {
         name: "View materialization run-1 details",
      }),
   );
   await screen.findByRole("dialog");
}

async function deleteTheRun(resourceUri: string) {
   render(<Materializations resourceUri={resourceUri} />, {
      wrapper: serverWrapper,
   });
   fireEvent.click(
      await screen.findByRole("button", {
         name: "Materialization actions for run-1",
      }),
   );
   fireEvent.click(
      await screen.findByRole("menuitem", {
         name: "Delete materialization run-1",
      }),
   );
   fireEvent.click(await screen.findByRole("button", { name: "Delete run" }));
   await waitFor(() => expect(deleteMaterialization).toHaveBeenCalled());
}

it("lists and deletes a pinned version's run by that version", async () => {
   await deleteTheRun("publisher://environments/env/packages/pkg?versionId=v2");

   expect(listMaterializations.mock.calls[0]?.[4]).toBe("v2");
   expect(deleteMaterialization.mock.calls[0]).toEqual([
      "env",
      "pkg",
      "run-1",
      false,
      "v2",
   ]);
});

it("deletes with no version when none is pinned, as before", async () => {
   await deleteTheRun("publisher://environments/env/packages/pkg");

   expect(deleteMaterialization.mock.calls[0]).toEqual([
      "env",
      "pkg",
      "run-1",
      false,
      undefined,
   ]);
});

it("fetches the build plan only when a run is opened, for the pinned version", async () => {
   await openTheRun("publisher://environments/env/packages/pkg?versionId=v2");

   await waitFor(() => expect(getPackageBuildPlan).toHaveBeenCalledTimes(1));
   expect(getPackageBuildPlan.mock.calls[0]?.slice(0, 3)).toEqual([
      "env",
      "pkg",
      "v2",
   ]);
   expect(
      await screen.findByText("This package has no persist sources."),
   ).toBeTruthy();
});

it("does not fetch the build plan before a run is opened", async () => {
   render(
      <Materializations resourceUri="publisher://environments/env/packages/pkg" />,
      {
         wrapper: serverWrapper,
      },
   );
   await screen.findByRole("button", {
      name: "View materialization run-1 details",
   });

   expect(getPackageBuildPlan).not.toHaveBeenCalled();
});

it("shows the route's plan when the server answers it", async () => {
   getPackageBuildPlan.mockImplementation(() =>
      Promise.resolve({ data: planWith("orders_daily") }),
   );

   await openTheRun("publisher://environments/env/packages/pkg");

   expect(await screen.findByText("orders_daily")).toBeTruthy();
});

it("falls back to the package's inline plan on a server without the route", async () => {
   getPackageBuildPlan.mockImplementation(() =>
      Promise.reject(httpError(404, { message: "Not Found" })),
   );
   getPackage.mockImplementation(() =>
      Promise.resolve({
         data: { name: "pkg", buildPlan: planWith("inline_source") },
      }),
   );

   await openTheRun("publisher://environments/env/packages/pkg");

   expect(await screen.findByText("inline_source")).toBeTruthy();
   expect(
      screen.queryByText("This package has no persist sources."),
   ).toBeNull();
});

it("says the plan is unavailable, not empty, when the server cannot produce it", async () => {
   getPackageBuildPlan.mockImplementation(() =>
      Promise.reject(
         httpError(409, {
            code: "BUILD_PLAN_UNAVAILABLE",
            message: "The build plan failed at load",
         }),
      ),
   );

   await openTheRun("publisher://environments/env/packages/pkg");

   expect(
      await screen.findByText("The build plan is unavailable."),
   ).toBeTruthy();
   expect(
      screen.queryByText("This package has no persist sources."),
   ).toBeNull();
});
