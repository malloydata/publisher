// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A notebook's suggest queries are version-scoped like the rest of it.
 *
 * The notebook already versioned its own fetch and its cell execution while
 * its dropdowns ran unversioned, so a versioned notebook filled its controls
 * from whatever the latest version happened to hold. The forwarding is a bare
 * trailing optional argument, which is the kind that goes missing without a
 * test noticing, so it is asserted here rather than only on the hook.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { render, waitFor } from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { RawNotebook } from "../../client";

const NOTEBOOK: RawNotebook = {
   notebookCells: [],
   sources: [
      {
         name: "orders",
         givens: [
            {
               name: "REGION",
               control: "select",
               suggest: { source: "orders", dimension: "region" },
            },
         ],
      },
   ],
};

const getNotebook = mock(
   (
      _environmentName: string,
      _packageName: string,
      _notebookPath: string,
      _versionId?: string,
   ) => Promise.resolve({ data: NOTEBOOK }),
);
const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string },
   ) => pending(),
);

mockServerProvider({
   notebooks: { getNotebook },
   models: { executeQueryModel },
});

const { default: Notebook } = await import("./Notebook");

const URI =
   "publisher://environments/env/packages/pkg/models/notebooks/ops.malloynb";

beforeEach(() => {
   clearCache();
   getNotebook.mockClear();
   executeQueryModel.mockClear();
});

it("runs its suggest queries against the version it was opened at", async () => {
   render(<Notebook resourceUri={`${URI}?versionId=v2`} />, {
      wrapper: serverWrapper,
   });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBe("v2");
   expect(cacheKeys("givenSuggest")[0]).toContain('"v2"');
});

it("sends none when the notebook was opened without one", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBeUndefined();
});
