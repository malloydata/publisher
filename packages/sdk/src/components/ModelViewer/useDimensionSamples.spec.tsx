// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";
import type { ExplorerField } from "./schemaModel";

const result = JSON.stringify({
   data: {
      kind: "array_cell",
      array_value: [
         {
            kind: "record_cell",
            record_value: [
               { kind: "string_cell", string_value: "shipped" },
               { kind: "number_cell", number_value: 9 },
            ],
         },
         {
            kind: "record_cell",
            record_value: [
               { kind: "string_cell", string_value: "returned" },
               { kind: "number_cell", number_value: 2 },
            ],
         },
      ],
   },
});
const executeQueryModel = mock((..._args: unknown[]) =>
   Promise.resolve({ data: { result } }),
);
mockServerProvider({ models: { executeQueryModel } });

const { useDimensionSamples, isExpectedNoSamples } = await import(
   "./useDimensionSamples"
);

const STATUS: ExplorerField = {
   name: "status",
   kind: "dimension",
   indexed: true,
};
const run = { projectName: "env", packageName: "pkg", versionId: "1.0.0" };

beforeEach(() => {
   clearCache();
   executeQueryModel.mockClear();
});

describe("useDimensionSamples", () => {
   it("reads a tagged dimension's common values with one quoted query", async () => {
      const { result: hook } = renderHook(
         () =>
            useDimensionSamples({
               run,
               modelPath: "shop.malloy",
               sourceName: "orders",
               field: STATUS,
            }),
         { wrapper: serverWrapper },
      );
      await waitFor(() =>
         expect(hook.current.samples).toEqual(["shipped", "returned"]),
      );
      expect(executeQueryModel).toHaveBeenCalledTimes(1);
      const [env, pkg, path, request] = executeQueryModel.mock.calls[0] as [
         string,
         string,
         string,
         { query: string; versionId?: string },
      ];
      expect([env, pkg, path, request.versionId]).toEqual([
         "env",
         "pkg",
         "shop.malloy",
         "1.0.0",
      ]);
      expect(request.query).toContain("run: `orders` -> {");
      expect(request.query).toContain("group_by: `status`");
   });

   it("asks nothing for a dimension the model doesn't tag", () => {
      const { result: hook } = renderHook(
         () =>
            useDimensionSamples({
               run,
               modelPath: "shop.malloy",
               sourceName: "orders",
               field: { ...STATUS, indexed: false },
            }),
         { wrapper: serverWrapper },
      );
      expect(hook.current).toEqual({
         samples: [],
         loading: false,
         failed: false,
      });
      expect(executeQueryModel).not.toHaveBeenCalled();
   });

   it("asks a host's fetcher instead, once per build it serves", async () => {
      const fetchSamples = mock(() => Promise.resolve(["a", "b"]));
      const { result: hook, rerender } = renderHook(
         ({ generation }: { generation: string }) =>
            useDimensionSamples({
               run,
               modelPath: "shop.malloy",
               sourceName: "orders",
               field: STATUS,
               fetchSamples,
               generation,
            }),
         { wrapper: serverWrapper, initialProps: { generation: "r1" } },
      );
      await waitFor(() => expect(hook.current.samples).toEqual(["a", "b"]));
      rerender({ generation: "r1" });
      expect(fetchSamples).toHaveBeenCalledTimes(1);
      rerender({ generation: "r2" });
      await waitFor(() => expect(fetchSamples).toHaveBeenCalledTimes(2));
      expect(executeQueryModel).not.toHaveBeenCalled();
   });
});

describe("isExpectedNoSamples", () => {
   const withStatus = (status: number) => ({ response: { status } });
   it("reads a client error about the field as no values, and the rest as failures", () => {
      expect(isExpectedNoSamples(withStatus(400))).toBe(true);
      expect(isExpectedNoSamples(withStatus(404))).toBe(true);
      expect(isExpectedNoSamples(withStatus(401))).toBe(false);
      expect(isExpectedNoSamples(withStatus(429))).toBe(false);
      expect(isExpectedNoSamples(withStatus(500))).toBe(false);
      expect(isExpectedNoSamples(new Error("network"))).toBe(false);
   });
});
