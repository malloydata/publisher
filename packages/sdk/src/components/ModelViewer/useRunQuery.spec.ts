// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, mock } from "bun:test";
import type { ApiClients } from "../ServerProvider";
import { runQueryOptions } from "./useRunQuery";

// A run of a file the package's index.malloy hides asks with includeHiddenFilesAndSources, and
// is cached apart from the curated run of the same file and query, whose 404 must
// never be served in its place.
describe("runQueryOptions", () => {
   const call = (includeHiddenFilesAndSources?: boolean) => {
      const executeQueryModel = mock((..._args: unknown[]) =>
         Promise.resolve({ data: {} }),
      );
      const apiClients = {
         models: { executeQueryModel },
      } as unknown as ApiClients;
      const options = runQueryOptions({
         server: "http://demo.data.ms2.localhost/api/v0",
         apiClients,
         run: {
            projectName: "env",
            packageName: "pkg",
            versionId: "1.0.0",
            includeHiddenFilesAndSources,
         },
         modelPath: "customers.malloy",
         query: "run: customers -> { select: * }",
      });
      return { options, executeQueryModel };
   };

   it("asks with includeHiddenFilesAndSources for a hidden file, under its own cache key", async () => {
      const { options, executeQueryModel } = call(true);
      await options.queryFn({});
      expect(executeQueryModel.mock.calls[0][4]).toBe(true);
      expect(executeQueryModel.mock.calls[0][5]).toEqual({ signal: undefined });
      expect(options.queryKey.at(-1)).toBe("off-surface");
   });

   it("sends nothing extra, and keeps the shared key, for any other run", async () => {
      const { options, executeQueryModel } = call(undefined);
      await options.queryFn({});
      expect(executeQueryModel.mock.calls[0][4]).toBeUndefined();
      expect(executeQueryModel.mock.calls[0][5]).toEqual({ signal: undefined });
      expect(options.queryKey).not.toContain("off-surface");
      expect(options.queryKey).toEqual(call(false).options.queryKey);
   });
});
