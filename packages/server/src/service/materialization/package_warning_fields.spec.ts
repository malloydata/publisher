// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package warnings' fields a client branches on: the bounded `reason` a
 * materialization warning carries, and a renamed code repeated under the code
 * 0.14.0 shipped, with the same message, for one release (a reader that reads
 * each problem once, the MCP reload tool, drops the repeat).
 */

import { describe, expect, it } from "bun:test";
import { relaxedCollision } from "./plan/load_gate_text";
import {
   annotationFindingWarnings,
   RETAINED_WARNING_ALIASES,
   storageWarnings,
   verdictWarnings,
   withoutRetainedAliases,
   withRetainedAliases,
} from "./package_warnings";
import type { BuildPlan } from "./domain/types";

const warning = (code: string) => ({
   model: "m.malloy",
   subject: "orders",
   code,
   severity: "warn" as const,
   message: "the same message",
});

describe("retained warning aliases", () => {
   it("names each renamed code with the code 0.14.0 shipped", () => {
      expect(RETAINED_WARNING_ALIASES).toEqual({
         "rollup-not-built": "preaggregate-not-built",
         "rollup-join-one-key-repeated": "preaggregate-join-one-key-repeated",
         "database-native-type-missing": "stored-native-schema-missing",
      });
   });

   it("repeats a renamed code under its old one, right after it, and nothing else", () => {
      expect(
         withRetainedAliases([
            warning("rollup-not-built"),
            warning("persist-not-built"),
            warning("database-native-type-missing"),
         ]).map((w) => w.code),
      ).toEqual([
         "rollup-not-built",
         "preaggregate-not-built",
         "persist-not-built",
         "database-native-type-missing",
         "stored-native-schema-missing",
      ]);
   });

   it("drops a repeat beside the warning it repeats, and keeps an old code that stands alone", () => {
      const doubled = withRetainedAliases([warning("rollup-not-built")]);
      expect(withoutRetainedAliases(doubled)).toEqual([
         warning("rollup-not-built"),
      ]);
      const alone = [warning("preaggregate-not-built")];
      expect(withoutRetainedAliases(alone)).toEqual(alone);
   });

   it("no longer produces 0.14.0's persist-source-refused", () => {
      expect(
         withRetainedAliases([warning("persist-not-built")]).map((w) => w.code),
      ).toEqual(["persist-not-built"]);
   });
});

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
