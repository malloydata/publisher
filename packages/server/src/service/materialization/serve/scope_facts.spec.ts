// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a bind commit records of each named source's gates and row scope
 * (`scope_facts.ts`) answers as the walk it replaces, for the snapshot it
 * was recorded over and no other.
 */

import { describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import { makeSnapshot } from "../domain/binding";
import { gateReadsOfSource } from "./gate_reads";
import {
   carriesAGate,
   recordedReads,
   recordScopeFacts,
   scopeFactsOf,
   showReads,
} from "./scope_facts";

const ORDERS = "orders@file:///pkg/m.malloy";

const modelDef = {
   contents: {
      orders: {
         type: "table",
         name: "orders",
         tablePath: "orders",
         connection: "duckdb",
         dialect: "duckdb",
         sourceID: ORDERS,
         fields: [{ type: "string", name: "region" }],
      },
   },
   exports: ["orders"],
} as unknown as ModelDef;

describe("recorded scope facts", () => {
   it("answer each named source as the walk does, for the snapshot recorded", () => {
      const runtime = {};
      const snapshot = makeSnapshot([
         { sourceIDs: [ORDERS], virtualHandle: "h" } as never,
      ]);
      recordScopeFacts(runtime, { modelDef, snapshot });
      const facts = scopeFactsOf(runtime, snapshot);
      expect(facts).toBeDefined();
      for (const stored of [new Set<string>(), new Set([ORDERS])]) {
         const walked = gateReadsOfSource(
            modelDef.contents.orders,
            modelDef,
            stored,
         );
         const looked = recordedReads(facts!, "orders", stored, new Set());
         expect(looked).not.toBe("unrecorded");
         expect(showReads(looked as never)).toBe(showReads(walked));
      }
      expect(facts!.flags.get("orders")).toEqual({
         carriesGate: false,
         compositeRowFilter: false,
      });
      // Another snapshot (a later bind) reads nothing recorded for this one.
      expect(scopeFactsOf(runtime, makeSnapshot([]))).toBeUndefined();
      expect(recordedReads(facts!, "elsewhere", new Set(), new Set())).toBe(
         "unrecorded",
      );
   });

   it("reads a gate anywhere beneath a source, failing closed past its depth", () => {
      expect(carriesAGate({ fields: [{ notes: ["#(doc) x"] }] })).toBe(false);
      let deep: Record<string, unknown> = {};
      const root = deep;
      for (let i = 0; i < 300; i++) {
         const next = {};
         deep.next = next;
         deep = next;
      }
      expect(carriesAGate(root)).toBe(true);
   });
});
