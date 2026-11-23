// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package warnings' fields a client branches on: the bounded `reason` a
 * materialization warning carries.
 */

import { describe, expect, it } from "bun:test";
import { relaxedCollision } from "./plan/load_gate_text";
import {
   annotationFindingWarnings,
   asOfBuildWarnings,
   dataInliningWarnings,
   PERSIST_READS_CURRENT_TIME_CODE,
   storageWarnings,
   verdictWarnings,
} from "./package_warnings";
import type { BuildPlan } from "./domain/types";

describe("a materialization warning's reason", () => {
   it("is the serve verdict's reason on a verdict warning", () => {
      const [w] = verdictWarnings([
         {
            sourceName: "orders",
            sourceID: "orders@file:///m.malloy",
            origin: "persist",
            tier: "storage",
            verdict: "live",
            reason: "destination_missing",
            message: "not configured.",
         },
      ]);
      expect(w).toMatchObject({
         code: "materialization-destination-missing",
         reason: "destination_missing",
      });
   });

   it("is storage_mode on materialization-storage-mode", () => {
      const plan = {
         graphs: [],
         sources: {
            "orders@file:///m.malloy": {
               name: "orders",
               modelPath: "m.malloy",
               annotationFields: { storage: "lake" },
            },
         },
      } as unknown as BuildPlan;
      expect(storageWarnings(plan, "off")).toEqual([
         expect.objectContaining({
            code: "materialization-storage-mode",
            reason: "storage_mode",
         }),
      ]);
   });

   it("names the database table, not live, as what answers a storage= source bound fresh there too", () => {
      const plan = {
         graphs: [],
         sources: {
            "orders@file:///m.malloy": {
               name: "orders",
               modelPath: "m.malloy",
               sourceEntityId: "abc",
               annotationFields: { storage: "lake" },
            },
            "daily@file:///m.malloy": {
               name: "daily",
               modelPath: "m.malloy",
               sourceEntityId: "def",
               annotationFields: { storage: "lake" },
            },
         },
      } as unknown as BuildPlan;
      for (const mode of ["write-only", "off"]) {
         const [orders, daily] = storageWarnings(
            plan,
            mode,
            (id) => id === "abc",
         ).map((w) => w.message);
         expect(orders).toContain(
            "answered from its table in the source's own database",
         );
         expect(orders).not.toContain("live");
         expect(daily).toContain("live");
      }
   });

   it("is table_name_collision on a collision refused alone under the opt-out", () => {
      const relaxed = relaxedCollision(
         {
            route: "@",
            tier: "malformed",
            code: "materialization-table-collision",
            message: "#@ persist on source a is invalid: it shares a table",
            model: "m.malloy",
            subject: "a",
         },
         false,
      );
      expect(annotationFindingWarnings([relaxed])).toEqual([
         expect.objectContaining({
            code: "persist-not-built",
            reason: "table_name_collision",
            severity: "warn",
         }),
      ]);
   });
});

describe("a table stored as of its build", () => {
   it("warns once per table, naming the source, view or query and where it reads the clock", () => {
      const warnings = asOfBuildWarnings(
         {
            "recent@file:///m.malloy": {
               name: "recent",
               modelPath: "m.malloy",
               reads: ["`where: ts ~ f'last 7 days'`"],
            },
            "orders__last7__view@file:///m.malloy": {
               name: "orders__last7__view",
               modelPath: "m.malloy",
               reads: [],
            },
            "daily__query@file:///m.malloy.persist-view.malloy": {
               name: "daily__query",
               reads: ["the field `today`"],
            },
         },
         {
            "orders__last7__view@file:///m.malloy": {
               sourceName: "orders",
               viewName: "last7",
            },
         },
      );
      expect(
         warnings.map((w) => [w.code, w.subject, w.severity, w.model]),
      ).toEqual([
         [PERSIST_READS_CURRENT_TIME_CODE, "recent", "warn", "m.malloy"],
         [
            PERSIST_READS_CURRENT_TIME_CODE,
            "orders -> last7",
            "warn",
            "m.malloy",
         ],
         [PERSIST_READS_CURRENT_TIME_CODE, "daily", "warn", undefined],
      ]);
      expect(warnings[0].message).toContain(
         "reads the current time (`where: ts ~ f'last 7 days'`)",
      );
      expect(warnings[0].message).toContain("as of its last build");
      expect(warnings[0].message).toContain("rebuilt on every run");
      expect(warnings[0].message).toContain("own extend block");
      expect(warnings[1].message).toContain('view "last7" of source "orders"');
      expect(warnings[2].message).toContain('query "daily"');
   });
});

describe("ducklake-data-inlining-on", () => {
   const plan = {
      graphs: [],
      sources: {
         "orders@file:///m.malloy": {
            name: "orders",
            modelPath: "m.malloy",
            connectionName: "warehouse",
            annotationFields: { storage: "lake" },
         },
         "items@file:///m.malloy": {
            name: "items",
            modelPath: "m.malloy",
            connectionName: "managed_db",
         },
         "users@file:///m.malloy": {
            name: "users",
            modelPath: "m.malloy",
            connectionName: "warehouse",
         },
      },
   } as unknown as BuildPlan;

   it("warns for each source stored where the catalog records inlining on, naming the limit", () => {
      const limits: Record<string, number> = { lake: 10, managed_db: 5 };
      const found = dataInliningWarnings(plan, (name) => limits[name]);
      expect(found.map((w) => [w.subject, w.code, w.severity])).toEqual([
         ["orders", "ducklake-data-inlining-on", "warn"],
         ["items", "ducklake-data-inlining-on", "warn"],
      ]);
      expect(found[0].message).toContain(
         "storage destination `lake`, whose DuckLake catalog records data_inlining_row_limit 10",
      );
      expect(found[1].message).toContain("connection `managed_db`");
   });

   it("warns nothing where no catalog was found recording a limit above 0", () => {
      expect(dataInliningWarnings(plan, () => undefined)).toEqual([]);
      expect(dataInliningWarnings(null, () => 10)).toEqual([]);
   });
});
