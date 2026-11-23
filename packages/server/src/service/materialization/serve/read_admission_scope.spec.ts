// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Read admission looks up what an answer's row scope reads under the
 * answer's own stored and re-rooted sets: a served copy's answer that
 * re-roots what its lane does may still read other stored tables, and is
 * never judged by the lane's.
 */

import { describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import { makeSnapshot } from "../domain/binding";
import { withAdmissionMode } from "./admission_parity";
import type { GateReads } from "./gate_reads";
import { ReadAdmission } from "./read_admission";
import { recordScopeFacts } from "./scope_facts";

const ACCOUNTS = {
   type: "table",
   sourceID: "accounts@m",
   fields: [
      { name: "org_id", type: "number" },
      { name: "amount", type: "number" },
   ],
};

// A stored query-shaped source: its build applied its stage's where:, so
// over its table nothing of it is left to judge; read any other way, the
// stage's comparison on `amount` is.
const STORED_SOURCE = {
   type: "query_result",
   sourceID: "stored@m",
   query: {
      structRef: ACCOUNTS,
      pipeline: [
         {
            type: "project",
            filterList: [
               {
                  e: {
                     node: ">",
                     kids: {
                        left: { node: "field", path: ["amount"] },
                        right: 0,
                     },
                  },
               },
            ],
            queryFields: [{ type: "fieldref", path: ["org_id"] }],
         },
      ],
   },
   fields: [{ name: "org_id", type: "number" }],
};

const MODEL = {
   contents: { stored: STORED_SOURCE },
   exports: ["stored"],
} as unknown as ModelDef;

type ScopeReads = (
   tables: readonly unknown[],
   onTables: ReadonlySet<string>,
   lane?: boolean,
) => Promise<GateReads | undefined>;

describe("read admission's recorded row scope reads", () => {
   it("are the answer's own stored set's when it re-roots what the lane does, never the lane's", async () => {
      const runtime = {};
      const snapshot = makeSnapshot([]);
      // The lane stores the source and re-roots nothing.
      recordScopeFacts(runtime, {
         modelDef: MODEL,
         snapshot,
         lane: { stored: new Set(["stored@m"]), rerooted: new Set() },
      });
      const admission = new ReadAdmission(
         {
            queryString: "run: stored -> v",
            givens: undefined,
            authorRunnable: {
               runtime,
               getPreparedQuery: async () => ({
                  _query: { structRef: "stored" },
                  _modelDef: MODEL,
               }),
            } as never,
         },
         { materialize: true, gatedSources: true } as never,
         (async () => ({})) as never,
         snapshot,
      );
      const scopeReads = (
         admission as unknown as { gateReadsFor: ScopeReads }
      ).gateReadsFor.bind(admission);
      await withAdmissionMode("lookup", async () => {
         // The served copy's answer re-roots nothing, as its lane does, and
         // reads no stored table: the stage's where: is judged.
         const own = await scopeReads([], new Set(), true);
         expect(own?.scoped).toBe(true);
         expect(own?.columns).toEqual([["amount"]]);
         // One that reads the stored table is judged as the lane is.
         const lane = await scopeReads(
            [{ tier: "storage", sourceIDs: ["stored@m"] }],
            new Set(),
            true,
         );
         expect(lane?.scoped).toBe(false);
         expect(lane?.columns).toEqual([]);
      });
   });
});
