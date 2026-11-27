// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a bind commit records of each named source's gates and row scope
 * (`scope_facts.ts`) answers as the walk it replaces, for the snapshot it
 * was recorded over and no other.
 */

import { describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import { setEventLoopYieldBudgetMsForTesting } from "../../event_loop_yield";
import { makeSnapshot } from "../domain/binding";
import { gateReadsOfSource } from "./gate_reads";
import {
   carriesAGate,
   recordedReads,
   recordScopeFacts,
   scopeFactsOf,
} from "./scope_facts";
import type { GateReads } from "./gate_reads_types";

/**
 * A rendering of gate reads for comparing two of them: every field, sets
 * sorted. Undefined reads render as undefined.
 */
function showReads(r: GateReads | undefined): string | undefined {
   if (r === undefined) return undefined;
   return JSON.stringify(r, (_k, v: unknown) =>
      v instanceof Set ? [...v].sort() : v instanceof Map ? [...v].sort() : v,
   );
}

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
   it("answer each named source as the walk does, for the snapshot recorded", async () => {
      const runtime = {};
      const snapshot = makeSnapshot([
         { sourceIDs: [ORDERS], virtualHandle: "h" } as never,
      ]);
      await recordScopeFacts(runtime, { modelDef, snapshot });
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

   it("answer as the walk does over every bound set, those the walk never asks about included", async () => {
      // A query-shaped source whose scope reads its upstream table, beside
      // tables nothing scopes: the bindings name sets its walk asks about and
      // sets it does not, and the lane re-roots it.
      const ACCOUNTS = "accounts@m";
      const STORED = "stored@m";
      const table = (name: string, sourceID: string) => ({
         type: "table",
         name,
         tablePath: name,
         connection: "duckdb",
         dialect: "duckdb",
         sourceID,
         fields: [
            { type: "string", name: "region" },
            { type: "number", name: "amount" },
         ],
      });
      const accounts = table("accounts", ACCOUNTS);
      const others = Array.from({ length: 6 }, (_, i) =>
         table(`other_${i}`, `other_${i}@m`),
      );
      const stored = {
         type: "query_result",
         name: "stored",
         sourceID: STORED,
         query: {
            structRef: accounts,
            pipeline: [
               {
                  type: "project",
                  filterList: [
                     {
                        e: {
                           node: "=",
                           kids: {
                              left: { node: "field", path: ["region"] },
                              right: { node: "stringLiteral", literal: "us" },
                           },
                        },
                     },
                  ],
                  queryFields: [{ type: "fieldref", path: ["amount"] }],
               },
            ],
         },
         fields: [{ type: "number", name: "amount" }],
      };
      const model = {
         contents: Object.fromEntries([
            ["accounts", accounts],
            ["stored", stored],
            ...others.map((o) => [o.name, o]),
         ]),
         exports: ["accounts", "stored", ...others.map((o) => o.name)],
      } as unknown as ModelDef;
      const sets = [
         [STORED],
         [ACCOUNTS],
         [STORED, ACCOUNTS],
         ...others.map((o) => [o.sourceID]),
      ];
      const snapshot = makeSnapshot(
         sets.map(
            (sourceIDs, i) => ({ sourceIDs, virtualHandle: `h${i}` }) as never,
         ),
      );
      const lane = { stored: new Set([STORED]), rerooted: new Set([STORED]) };
      const runtime = {};
      await recordScopeFacts(runtime, { modelDef: model, snapshot, lane });
      const facts = scopeFactsOf(runtime, snapshot)!;
      expect(facts).toBeDefined();
      const cases: [ReadonlySet<string>, ReadonlySet<string>][] = [
         ...sets.flatMap((s): [ReadonlySet<string>, ReadonlySet<string>][] => [
            [new Set(s), new Set()],
            [new Set(s), lane.rerooted],
         ]),
         [lane.stored, lane.rerooted],
      ];
      for (const name of ["stored", "accounts", ...others.map((o) => o.name)]) {
         const struct = model.contents[name];
         for (const [set, rerooted] of cases) {
            const looked = recordedReads(facts, name, set, rerooted);
            expect(looked).not.toBe("unrecorded");
            expect(showReads(looked as never)).toBe(
               showReads(gateReadsOfSource(struct, model, set, rerooted)),
            );
         }
      }
   });

   it("hands the event loop a turn while it records", async () => {
      const previous = setEventLoopYieldBudgetMsForTesting(0);
      try {
         let done = false;
         let ranWhileRecording = false;
         setImmediate(() => {
            ranWhileRecording = !done;
         });
         const recording = Promise.resolve(
            recordScopeFacts(
               {},
               {
                  modelDef,
                  snapshot: makeSnapshot([
                     { sourceIDs: [ORDERS], virtualHandle: "h" } as never,
                  ]),
               },
            ),
         ).then(() => {
            done = true;
         });
         await recording;
         expect(ranWhileRecording).toBe(true);
      } finally {
         setEventLoopYieldBudgetMsForTesting(previous);
      }
   });
});
