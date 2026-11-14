// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The builder's catalog leaves out the package's reports and the file being
 * edited, following the CURRENT listing and path: the models are cached once
 * per package version, and the exclusions are never baked into that cache.
 */
import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   serverWrapper,
   TEST_SERVER,
} from "../../../test/serverProvider";
import type { ListedReport } from "../Package/documentLocation";

const modelOf = (path: string, source: string) => ({
   modelPath: path,
   modelInfo: JSON.stringify({ entries: [{ kind: "source", name: source }] }),
   sources: [{ name: source, views: [{ name: "v" }] }],
});
const MODELS: Record<string, unknown> = {
   "data.malloy": modelOf("data.malloy", "orders"),
   "a.malloy": modelOf("a.malloy", "a_tiles"),
   "b.malloy": modelOf("b.malloy", "b_tiles"),
};
const listModels = mock(() =>
   Promise.resolve({ data: Object.keys(MODELS).map((path) => ({ path })) }),
);
const getModel = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({ data: MODELS[path] }),
);
mockServerProvider({ models: { listModels, getModel } });

const { useEditorCatalog } = await import("./useEditorCatalog");

const REPORTS: ListedReport[] = [
   { path: "a.malloy", layout: "grid" },
   { path: "b.malloy", layout: "linear" },
];

type Props = {
   modelPath: string;
   reports: readonly ListedReport[];
   reportsSettled: boolean;
};
const mountCatalog = (initialProps: Props) =>
   renderHook(
      (props: Props) =>
         useEditorCatalog({
            environmentName: "env",
            packageName: "pkg",
            ...props,
         }),
      { wrapper: serverWrapper, initialProps },
   );
const names = (catalog: { sources: { name: string }[] } | undefined) =>
   catalog?.sources.map((s) => s.name).sort();

beforeEach(() => {
   clearCache();
   listModels.mockClear();
   getModel.mockClear();
});

describe("useEditorCatalog", () => {
   it("offers and fetches nothing until the listing has answered", async () => {
      const { result } = mountCatalog({
         modelPath: "a.malloy",
         reports: [],
         reportsSettled: false,
      });
      await waitFor(() => expect(listModels).toHaveBeenCalledTimes(1));
      expect(getModel).not.toHaveBeenCalled();
      expect(result.current).toBeUndefined();
   });

   it("never fetches the file being edited or a listed report", async () => {
      const { result } = mountCatalog({
         modelPath: "a.malloy",
         reports: REPORTS,
         reportsSettled: true,
      });
      await waitFor(() => expect(names(result.current)).toEqual(["orders"]));
      expect(getModel.mock.calls.map((call) => call[2])).toEqual([
         "data.malloy",
      ]);
   });

   it("leaves the reports out once a failed listing recovers", async () => {
      // A failed listing is settled with no reports: nothing to exclude yet.
      const view = mountCatalog({
         modelPath: "a.malloy",
         reports: [],
         reportsSettled: true,
      });
      await waitFor(() =>
         expect(names(view.result.current)).toEqual(["b_tiles", "orders"]),
      );
      view.rerender({
         modelPath: "a.malloy",
         reports: REPORTS,
         reportsSettled: true,
      });
      expect(names(view.result.current)).toEqual(["orders"]);
      // The recovery reads the cached models; nothing is fetched again.
      expect(listModels).toHaveBeenCalledTimes(1);
      expect(getModel).toHaveBeenCalledTimes(2);
   });

   it("knows each model's path when the body leaves it out", async () => {
      // What a client or proxy that keeps only the spec's fields hands back:
      // `path` null, and the server's undeclared `modelPath` dropped.
      getModel.mockImplementation(
         (_env: string, _pkg: string, path: string) => {
            const { modelPath: _dropped, ...body } = MODELS[path] as {
               modelPath: string;
            };
            return Promise.resolve({ data: { ...body, path: null } });
         },
      );
      try {
         const { result } = mountCatalog({
            modelPath: "a.malloy",
            reports: REPORTS,
            reportsSettled: true,
         });
         await waitFor(() => expect(names(result.current)).toEqual(["orders"]));
         expect(result.current?.sources[0]?.modelPath).toBe("data.malloy");
      } finally {
         getModel.mockImplementation((_env, _pkg, path) =>
            Promise.resolve({ data: MODELS[path] }),
         );
      }
   });

   it("leaves each report out of its own catalog from one shared cache", async () => {
      const first = mountCatalog({
         modelPath: "a.malloy",
         reports: REPORTS,
         reportsSettled: true,
      });
      await waitFor(() =>
         expect(names(first.result.current)).toEqual(["orders"]),
      );
      // The file being edited is left out even when the listing lacks it.
      const second = mountCatalog({
         modelPath: "b.malloy",
         reports: [REPORTS[0]],
         reportsSettled: true,
      });
      await waitFor(() =>
         expect(names(second.result.current)).toEqual(["orders"]),
      );
      const other = mountCatalog({
         modelPath: "b.malloy",
         reports: [],
         reportsSettled: true,
      });
      await waitFor(() =>
         expect(names(other.result.current)).toEqual(["a_tiles", "orders"]),
      );
      // One listing entry for all three, and one entry per model: neither
      // the path being edited nor the report listing is in any key.
      expect(cacheKeys("report-editor-models")).toHaveLength(1);
      expect(cacheKeys("report-editor-model").sort()).toEqual(
         ["a.malloy", "data.malloy"].map((path) =>
            JSON.stringify([
               "report-editor-model",
               "env",
               "pkg",
               path,
               null,
               TEST_SERVER,
            ]),
         ),
      );
   });
});
