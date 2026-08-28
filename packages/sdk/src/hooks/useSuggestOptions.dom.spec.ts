// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a suggest query sends and what it is cached under.
 *
 * Separate from the pure-function tests next door because `mock.module` has to
 * run before the hook is imported, and a static import there would hoist above
 * it.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { renderHook, waitFor } from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../test/serverProvider";
import type { Given } from "../client";

const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string },
   ) => pending(),
);

mockServerProvider({ models: { executeQueryModel } });

const { useSuggestOptions } = await import("./useSuggestOptions");

const SPECS: Given[] = [
   {
      name: "REGION",
      control: "select",
      suggest: { source: "orders", dimension: "region" },
   },
];

const renderSuggest = (versionId?: string) =>
   renderHook(
      () =>
         useSuggestOptions(
            "env",
            "pkg",
            "dashboards/ops.malloy",
            SPECS,
            versionId,
         ),
      { wrapper: serverWrapper },
   );

beforeEach(() => {
   clearCache();
   executeQueryModel.mockClear();
});

it("sends the version with the suggest query and keys on it", async () => {
   renderSuggest("v2");

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBe("v2");
   expect(cacheKeys("givenSuggest")[0]).toContain('"v2"');
});

it("sends and keys nothing when no version was given", async () => {
   renderSuggest();

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBeUndefined();
   // `useQueries` goes through the context client rather than
   // `useQueryWithApiError`, so this key carries no trailing server.
   expect(cacheKeys("givenSuggest")[0]).toBe(
      '["givenSuggest","env","pkg",null,"dashboards/ops.malloy","REGION",' +
         'null,"orders","region"]',
   );
});

it("keeps two versions' option lists apart", async () => {
   const first = renderSuggest("v1");
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
   first.unmount();

   renderSuggest("v2");

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
   expect(new Set(cacheKeys("givenSuggest")).size).toBe(2);
});
