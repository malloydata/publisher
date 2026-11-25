// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** Where a link inside a package lands: on the version on screen. */
import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";

const navigate = mock((_to: string, _event?: MouseEvent) => {});

const sdk = await import("@malloy-publisher/sdk");
mock.module("@malloy-publisher/sdk", () => ({
   ...sdk,
   useRouterClickHandler: () => navigate,
}));

const { usePackageLinkNavigate } = await import("./usePackageLinkNavigate");

const follow = (to: string, versionSegment?: string) => {
   const { result } = renderHook(() =>
      usePackageLinkNavigate("env", "pkg", versionSegment),
   );
   result.current(to);
   return navigate.mock.calls.at(-1)?.[0];
};

beforeEach(() => {
   navigate.mockClear();
});

describe("usePackageLinkNavigate", () => {
   it("keeps a pinned version, for a file and for a folder named like a version", () => {
      expect(follow("/env/pkg/q.malloy", "1.0.0")).toBe(
         "/env/pkg/1.0.0/q.malloy",
      );
      expect(follow("/env/pkg/latest/q.malloy", "1.0.0")).toBe(
         "/env/pkg/1.0.0/latest/q.malloy",
      );
   });

   it("keeps latest spelled out", () => {
      expect(follow("/env/pkg/sales/q3.malloy", "latest")).toBe(
         "/env/pkg/latest/sales/q3.malloy",
      );
   });

   it("follows a link outside the package, or on an unversioned page, as written", () => {
      expect(follow("/env/other/q.malloy", "1.0.0")).toBe(
         "/env/other/q.malloy",
      );
      expect(follow("/env/pkg/q.malloy")).toBe("/env/pkg/q.malloy");
   });
});
