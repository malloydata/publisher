// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's version listing: what reads as "no versions" and what reads as
 * a failure. A 404 (a package with none) and a 501 (a server, or a proxy in
 * front of one, without the route) are no versions; anything else is a
 * failure, so the page never treats a package it knows nothing about as one
 * it may edit.
 */
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { renderHook, waitFor } from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   serverWrapper,
   TEST_SERVER,
} from "../../../test/serverProvider";

let answer: () => Promise<unknown> = () => Promise.resolve({ data: [] });
const listPackageVersions = mock((_env: string, _pkg: string) => answer());

mockServerProvider({ versions: { listPackageVersions } });

const { usePackageVersions } = await import("./usePackageVersions");

const failing = (status: number) => () =>
   Promise.reject(
      Object.assign(new Error(`Request failed with status code ${status}`), {
         response: { status, data: { message: "no" } },
      }),
   );

beforeEach(() => {
   clearCache();
   listPackageVersions.mockClear();
});

describe("usePackageVersions", () => {
   it.each([404, 501])(
      "reads a %d as no versions, without an error",
      async (status) => {
         answer = failing(status);
         const { result } = renderHook(() => usePackageVersions("env", "pkg"), {
            wrapper: serverWrapper,
         });
         await waitFor(() => expect(result.current.isPending).toBe(false));
         expect(result.current.versions).toEqual([]);
         expect(result.current.isError).toBe(false);
      },
   );

   it.each([500, 403])(
      "says a %d failed, rather than that there are no versions",
      async (status) => {
         answer = failing(status);
         const { result } = renderHook(() => usePackageVersions("env", "pkg"), {
            wrapper: serverWrapper,
         });
         await waitFor(() => expect(result.current.isError).toBe(true));
         expect(result.current.versions).toEqual([]);
         // Asked once: a failure is not retried behind the page's back.
         expect(listPackageVersions).toHaveBeenCalledTimes(1);
      },
   );

   it("keys the listing by server, as every other query", async () => {
      answer = () => Promise.resolve({ data: [] });
      const { result } = renderHook(() => usePackageVersions("env", "pkg"), {
         wrapper: serverWrapper,
      });
      await waitFor(() => expect(result.current.isPending).toBe(false));
      expect(cacheKeys("packageVersions")).toEqual([
         JSON.stringify(["packageVersions", "env", "pkg", TEST_SERVER]),
      ]);
   });
});
