// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A pinned version's runs are that version's: the section lists them for it,
 * and deleting one names it too, so the server refuses a run of another
 * version (404) or of an archived one (410) rather than deleting by id alone.
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

mockServerProvider(
   {
      packages: { getPackage: pending },
      materializations: { listMaterializations, deleteMaterialization },
   },
   { mutable: true },
);

const { default: Materializations } = await import("./Materializations");

beforeEach(() => {
   clearCache();
   listMaterializations.mockClear();
   deleteMaterialization.mockClear();
});

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
