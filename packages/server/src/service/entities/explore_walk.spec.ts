// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   BoundaryDescents,
   guardDescent,
   guardHolds,
   holdsFor,
} from "./explore_walk";

describe("the descents a query boundary decides", () => {
   it("takes one provisionally, guarded by its root and target", () => {
      const boundary = new BoundaryDescents(true);
      expect(boundary.descent("orders", "customers")).toEqual({
         unlessQueryable: "customers",
      });
      // A source joined from itself would have to be both.
      expect(boundary.descent("orders", "orders")).toBe(false);
      expect(boundary.untaken).toEqual([]);
   });

   it("leaves one untaken otherwise, and says which, once each", () => {
      const boundary = new BoundaryDescents(false);
      expect(boundary.descent("orders", "customers")).toBe(false);
      expect(boundary.descent("orders", "customers")).toBe(false);
      expect(boundary.descent("items", "customers")).toBe(false);
      expect(boundary.untaken).toEqual([
         ["orders", "customers"],
         ["items", "customers"],
      ]);
   });

   it("holds a guard only while its root is queryable and its targets are not", () => {
      const guard = guardDescent(
         guardDescent(undefined, "orders", "customers"),
         "orders",
         "regions",
      );
      expect(guard).toEqual({
         root: "orders",
         nonQueryable: ["customers", "regions"],
      });
      expect(guardHolds(guard, new Set(["customers", "regions"]))).toBe(true);
      expect(guardHolds(guard, new Set(["customers"]))).toBe(false);
      expect(
         guardHolds(guard, new Set(["orders", "customers", "regions"])),
      ).toBe(false);
      expect(guardHolds(undefined, new Set(["orders"]))).toBe(true);
   });

   it("keeps an untaken extraction exact unless a descent it left would be taken", () => {
      const untaken: [string, string][] = [["orders", "customers"]];
      expect(holdsFor(untaken, new Set())).toBe(true);
      // The root is not queryable either: still not taken.
      expect(holdsFor(untaken, new Set(["orders", "customers"]))).toBe(true);
      expect(holdsFor(untaken, new Set(["customers"]))).toBe(false);
   });
});
