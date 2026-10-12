// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** Where a `# drill` click lands in the Console. */
import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";

let reports: { path: string; layout: string }[] = [];
const navigate = mock((_to: string, _event?: MouseEvent) => {});

const sdk = await import("@malloy-publisher/sdk");
mock.module("@malloy-publisher/sdk", () => ({
   ...sdk,
   useReportListing: () => ({ reports }),
   useRouterClickHandler: () => navigate,
}));

const { useDrillNavigate } = await import("./useDrillNavigate");

let outcome: string | undefined;
const drill = (env: string, pkg: string, to: string, givens = {}) => {
   const { result } = renderHook(() => useDrillNavigate(env, pkg));
   outcome = result.current({ path: to, dashboard: to, givens });
   return navigate.mock.calls.at(-1)?.[0];
};

beforeEach(() => {
   outcome = undefined;
   reports = [];
   navigate.mockClear();
});

describe("useDrillNavigate", () => {
   it("lands on the resolved report's path route, with the seeded givens", () => {
      reports = [{ path: "sales/q3.malloy", layout: "grid" }];
      expect(drill("env", "pkg", "sales/q3", { region: "west" })).toBe(
         "/env/pkg/sales/q3?region=west",
      );
      expect(outcome).toBe("resolved");
   });

   it("lands a bare name on the report at the root", () => {
      reports = [
         { path: "overview.malloy", layout: "linear" },
         { path: "dashboards/overview.malloy", layout: "grid" },
      ];
      expect(drill("env", "pkg", "overview")).toBe("/env/pkg/overview");
   });

   it("encodes the environment, the package and each path segment", () => {
      reports = [{ path: "a b/c#d.malloy", layout: "grid" }];
      expect(drill("my env", "my pkg", "a b/c#d")).toBe(
         "/my%20env/my%20pkg/a%20b/c%23d",
      );
   });

   it("goes nowhere for a target that is not a plain package path", () => {
      reports = [{ path: "sales/q3.malloy", layout: "grid" }];
      for (const to of ["./sales/q3", "/sales/q3", "a//b", "../x"]) {
         expect(drill("env", "pkg", to)).toBeUndefined();
         expect(outcome).toBe("malformed");
      }
   });

   it("sends an unresolved path to itself", () => {
      expect(drill("env", "pkg", "x/missing", { a: "1" })).toBe(
         "/env/pkg/x/missing?a=1",
      );
      expect(outcome).toBe("unresolved");
   });

   it("sends an unresolved bare name to its root path, never to a by-name alias", () => {
      expect(drill("env", "pkg", "absent")).toBe("/env/pkg/absent");
      reports = [
         { path: "dashboards/overview.malloy", layout: "grid" },
         { path: "notebooks/overview.malloy", layout: "linear" },
      ];
      expect(drill("env", "pkg", "overview")).toBe("/env/pkg/overview");
   });
});
