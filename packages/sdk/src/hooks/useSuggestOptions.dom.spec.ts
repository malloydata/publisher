// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a suggest query sends and what it is cached under.
 *
 * Separate from `useSuggestOptions.spec.ts` because `mock.module` has to run
 * before the hook is imported, and a static import in that file would hoist
 * above it. The pure-function tests stay there.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { createElement, type ReactNode } from "react";
import type { Given } from "../client";

const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string },
   ) => new Promise<never>(() => {}),
);

mock.module("../components/ServerProvider", () => ({
   ServerProvider: () => null,
   useServer: () => ({
      server: "http://localhost/api/v0",
      apiClients: { models: { executeQueryModel } },
   }),
}));

const { useSuggestOptions } = await import("./useSuggestOptions");

const SPECS: Given[] = [
   {
      name: "REGION",
      control: "select",
      suggest: { source: "orders", dimension: "region" },
   },
];

let client: QueryClient;

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
      {
         wrapper: ({ children }: { children: ReactNode }) =>
            createElement(QueryClientProvider, { client }, children),
      },
   );

const keys = () =>
   client
      .getQueryCache()
      .getAll()
      .map((query) => JSON.stringify(query.queryKey));

beforeEach(() => {
   client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
   executeQueryModel.mockClear();
});

it("sends the version with the suggest query and keys on it", async () => {
   renderSuggest("v2");

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBe("v2");
   expect(keys()[0]).toContain('"v2"');
});

it("is unchanged when no version was given", async () => {
   renderSuggest();

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBeUndefined();
});

it("keeps two versions' option lists apart", async () => {
   const first = renderSuggest("v1");
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
   first.unmount();

   renderSuggest("v2");

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
   expect(new Set(keys()).size).toBe(2);
});
