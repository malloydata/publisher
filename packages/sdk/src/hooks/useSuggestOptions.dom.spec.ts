// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a suggest query sends and what it is cached under.
 *
 * Separate from the pure-function tests next door because `mock.module` has to
 * run before the hook is imported, and a static import there would hoist above
 * it.
 */
import { beforeEach, describe, expect, it, mock } from "bun:test";
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

const { buildSuggestQuery, SUGGEST_OPTION_LIMIT, useSuggestOptions } =
   await import("./useSuggestOptions");

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

/** A suggest whose source is gated by TENANT; the page also filters by REGION. */
const GATED: Given[] = [
   {
      name: "BRAND",
      control: "select",
      suggest: { source: "orders", dimension: "brand", givenNames: ["TENANT"] },
   },
];
const APPLIED = {
   values: new Map<string, string>([
      ["TENANT", "acme"],
      ["REGION", "West"],
   ]),
   declaredTypes: new Map<string, string>([
      ["TENANT", "string"],
      ["REGION", "filter<string>"],
      ["BRAND", "filter<string>"],
   ]),
};
const renderGated = (applied = APPLIED) =>
   renderHook(
      () =>
         useSuggestOptions(
            "env",
            "pkg",
            "dashboards/ops.malloy",
            GATED,
            undefined,
            applied,
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
   // The trailing null is the givens slot: none carried, none keyed.
   expect(cacheKeys("givenSuggest")[0]).toBe(
      '["givenSuggest","env","pkg",null,"dashboards/ops.malloy","REGION",' +
         'null,"orders","region",null]',
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

it("sends only the givens the suggest names, and keys on them", async () => {
   renderGated();
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   const request = executeQueryModel.mock.calls[0][3] as { givens?: unknown };
   // TENANT rides along; REGION, applied on the page, does not: the option
   // list must not depend on the page's other filters.
   expect(request.givens).toEqual({ TENANT: "acme" });
   expect(cacheKeys("givenSuggest")[0]).toContain('{\\"TENANT\\":\\"acme\\"}');
});

it("sends no givens when the suggest names none, or the caller offers none", async () => {
   renderSuggest();
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(
      (executeQueryModel.mock.calls[0][3] as { givens?: unknown }).givens,
   ).toBeUndefined();
   executeQueryModel.mockClear();
   clearCache();
   renderGated({ values: new Map(), declaredTypes: APPLIED.declaredTypes });
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(
      (executeQueryModel.mock.calls[0][3] as { givens?: unknown }).givens,
   ).toBeUndefined();
});

it("runs a suggest as the document's definitions plus one run:, by name or by dimension, when a preamble is given", async () => {
   const specs: Given[] = [
      {
         name: "REGION",
         control: "select",
         suggest: { source: "a", dimension: "region" },
      },
      { name: "BRAND", control: "select", suggest: { query: "brands" } },
   ];
   executeQueryModel.mockClear();
   renderHook(
      () =>
         useSuggestOptions(
            "env",
            "pkg",
            "models/orders.malloy",
            specs,
            undefined,
            undefined,
            "source: a is orders",
         ),
      { wrapper: serverWrapper },
   );

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
   const requests = executeQueryModel.mock.calls.map(
      (call) => call[3] as { query?: string; queryName?: string },
   );
   expect(requests.every((request) => request.queryName === undefined)).toBe(
      true,
   );
   expect(requests.map((request) => request.query)).toEqual([
      `source: a is orders\n\n${buildSuggestQuery("a", "region")}`,
      "source: a is orders\n\nrun: brands",
   ]);
});

describe("a list cut off at the limit", () => {
   /** `n` distinct values for one column, `Value 0001` … in order. */
   const rows = (column: string, n: number) =>
      JSON.stringify(
         Array.from({ length: n }, (_, i) => ({
            [column]: `Value ${String(i).padStart(4, "0")}`,
         })),
      );
   const answer = (result: string) =>
      Promise.resolve({ data: { result } }) as never;
   const MANUFACTURER: Given[] = [
      {
         name: "MAKER",
         control: "select",
         suggest: { source: "recalls", dimension: "manufacturer" },
      },
   ];
   const render = (specs: Given[], preamble?: string) =>
      renderHook(
         () =>
            useSuggestOptions(
               "env",
               "pkg",
               "README.malloy",
               specs,
               undefined,
               undefined,
               preamble,
            ),
         { wrapper: serverWrapper },
      );

   it("is flagged, and its search finds a value past the cutoff on the server", async () => {
      executeQueryModel.mockImplementation((_e, _p, _m, request) => {
         const query = (request as { query?: string }).query ?? "";
         return answer(
            query.includes("where:")
               ? JSON.stringify([{ manufacturer: "General Motors, LLC" }])
               : rows("manufacturer", SUGGEST_OPTION_LIMIT + 1),
         );
      });
      const { result } = render(MANUFACTURER);
      await waitFor(() =>
         expect(result.current.truncated.has("MAKER")).toBe(true),
      );
      // The page holds the limit, not the extra row that proved it was cut off.
      expect(result.current.options.get("MAKER")).toHaveLength(
         SUGGEST_OPTION_LIMIT,
      );
      const search = result.current.search.get("MAKER");
      expect(search).toBeDefined();

      const page = await search!.run("General Motors");
      expect(page.values).toEqual(["General Motors, LLC"]);
      const sent = (
         executeQueryModel.mock.calls.at(-1)?.[3] as { query: string }
      ).query;
      expect(sent).toBe(
         buildSuggestQuery("recalls", "manufacturer", "General Motors"),
      );
      expect(sent).toContain(
         "where: strpos(lower(`manufacturer`::string), 'general motors') > 0",
      );
      expect(search!.key[0]).toBe("givenSuggestSearch");
      executeQueryModel.mockImplementation(() => pending());
   });

   it("is not, when the list fits: no search, and no request past the first", async () => {
      executeQueryModel.mockImplementation(() =>
         answer(rows("manufacturer", 3)),
      );
      const { result } = render(MANUFACTURER);
      await waitFor(() =>
         expect(result.current.options.get("MAKER")).toHaveLength(3),
      );
      expect(result.current.truncated.size).toBe(0);
      expect(result.current.search.size).toBe(0);
      expect(executeQueryModel).toHaveBeenCalledTimes(1);
      executeQueryModel.mockImplementation(() => pending());
   });

   it("is flagged for a named query at the server's cap, with no search to offer", async () => {
      executeQueryModel.mockImplementation(() =>
         answer(rows("brand", SUGGEST_OPTION_LIMIT)),
      );
      const { result } = render([
         { name: "BRAND", control: "select", suggest: { query: "brands" } },
      ]);
      await waitFor(() =>
         expect(result.current.truncated.has("BRAND")).toBe(true),
      );
      expect(result.current.search.has("BRAND")).toBe(false);
      executeQueryModel.mockImplementation(() => pending());
   });

   it("searches a text-held document as its definitions plus the search", async () => {
      executeQueryModel.mockImplementation((_e, _p, _m, request) => {
         const query = (request as { query?: string }).query ?? "";
         return answer(
            query.includes("where:")
               ? JSON.stringify([{ manufacturer: "General Motors, LLC" }])
               : rows("manufacturer", SUGGEST_OPTION_LIMIT + 1),
         );
      });
      const { result } = render(MANUFACTURER, "source: recalls is t");
      await waitFor(() =>
         expect(result.current.search.has("MAKER")).toBe(true),
      );
      await result.current.search.get("MAKER")!.run("general");
      expect(
         (executeQueryModel.mock.calls.at(-1)?.[3] as { query: string }).query,
      ).toBe(
         `source: recalls is t\n\n${buildSuggestQuery("recalls", "manufacturer", "general")}`,
      );
      // The definitions are part of what a search is cached under.
      expect(result.current.search.get("MAKER")!.key).toContain(
         "source: recalls is t",
      );
      executeQueryModel.mockImplementation(() => pending());
   });
});
