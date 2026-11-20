// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The router and the serve verdict, each held to an oracle neither computes:
 * the gate policy's frozen leg rows (`domain/gate_policy_frozen_rows.ts`),
 * which only the security review changes. For every leg, switch setting and
 * read the rows name, read with exact facts (every probe answers that the
 * table compares as its source does, no composite row filter):
 *
 * - `admitRead` admits exactly the reads a frozen row keeps, unless a
 *   `storage=` gate column is stored with a zone (its zone rule);
 * - the serve verdict's keep-gate path (`verdictKeepsGate`) says a source's
 *   own table keeps its gates exactly where the frozen `served` rows do for
 *   that one table, and a rollup's exactly where the frozen rows let the
 *   rollup leg past a gate and its plan proved the variant.
 *
 * A change that loosens or tightens the router and the verdict together, or
 * moves the verdict onto another leg, disagrees with the rows and fails here.
 */

import { describe, expect, it } from "bun:test";
import { admitRead, type ReadFacts } from "./domain/admission";
import type { TableRead } from "./domain/gate_policy";
import {
   FROZEN_LEG_ROWS,
   GATE_READS,
   switches,
   switchesLabel,
} from "./domain/gate_policy_frozen_rows";
import { LEGS, type Tier } from "./domain/vocabulary";
import { computeServeVerdicts } from "./serve/serve_verdict";
import { verdictKeepsGate } from "./serve_verdicts";

/** A read past an entry point's gate whose every probe finds it exact. */
const EXACT: ReadFacts = {
   pastEntryPointGate: true,
   compositeRowFilter: false,
   reads: { scoped: false, gated: true },
   columnsListed: true,
   probes: {
      scope: {
         source: { refusal: undefined },
         stored: { refusal: undefined },
      },
   },
};

/** Each frozen read again with its `storage=` gate columns stored zoned. */
const ZONED: Record<string, readonly TableRead[]> = Object.fromEntries(
   Object.entries(GATE_READS)
      .filter(([, tables]) => tables.some((t) => t.tier === "storage"))
      .map(([name, tables]) => [
         `${name}+zoned`,
         tables.map((t) =>
            t.tier === "storage"
               ? {
                    ...t,
                    gateColumnTypes: [
                       ...(t.gateColumnTypes ?? []),
                       "TIMESTAMP WITH TIME ZONE",
                    ],
                 }
               : t,
         ),
      ]),
);

/** A table of each tier whose gate columns are stored exactly. */
const EXACT_TABLE = {
   database: {},
   storage: { gateColumnTypes: ["BIGINT", "VARCHAR"] },
} as const;

// The serving context routes with own-gate keeping on (`g3=1`).
const keeps = (leg: string, read: string, label: string) =>
   FROZEN_LEG_ROWS.includes(`keeps ${leg} read=${read} ${label} g3=1`);
const mayPass = (leg: string, label: string) =>
   FROZEN_LEG_ROWS.includes(`may ${leg} ${label} g3=1`);

describe("admission and the serve verdict agree with the frozen gate policy", () => {
   it("admitRead admits exactly the reads a frozen row keeps", () => {
      const wrong: string[] = [];
      let admitted = 0;
      for (const leg of LEGS) {
         for (const sw of switches()) {
            const label = switchesLabel(sw);
            for (const [read, tables] of Object.entries({
               ...GATE_READS,
               ...ZONED,
            })) {
               const verdict = admitRead(leg, tables, EXACT, sw);
               expect(verdict.admit).not.toBe("needs_probe");
               const expected =
                  !read.endsWith("+zoned") && keeps(leg, read, label);
               if (verdict.admit === true) admitted++;
               if ((verdict.admit === true) !== expected) {
                  wrong.push(`${leg} read=${read} ${label}: ${verdict.admit}`);
               }
            }
         }
      }
      expect(wrong).toEqual([]);
      expect(admitted).toBeGreaterThan(0);
   });

   it("the verdict's keep-gate path says what the frozen served rows say of a source's own table", () => {
      const wrong: string[] = [];
      const outcomes = new Set<boolean>();
      for (const sw of switches()) {
         const label = switchesLabel(sw);
         for (const [read, tables] of Object.entries({
            ...GATE_READS,
            ...ZONED,
         })) {
            // The verdict asks of one table: the source's own.
            if (tables.length !== 1) continue;
            const said = verdictKeepsGate(
               { rollup: false, table: tables[0] },
               sw,
            );
            outcomes.add(said);
            const expected =
               !read.endsWith("+zoned") && keeps("served", read, label);
            if (said !== expected) wrong.push(`read=${read} ${label}: ${said}`);
         }
      }
      expect(wrong).toEqual([]);
      expect([...outcomes].sort()).toEqual([false, true]);
   });

   it("a rollup keeps its gates where its plan proved the variant and the frozen rollup rows keep its own table", () => {
      const wrong: string[] = [];
      const outcomes = new Set<boolean>();
      for (const sw of switches()) {
         const label = switchesLabel(sw);
         for (const [read, tables] of Object.entries({
            ...GATE_READS,
            ...ZONED,
         })) {
            // The verdict asks of one table: the rollup's own.
            if (tables.length !== 1) continue;
            for (const keptAtPlan of [false, true]) {
               const said = verdictKeepsGate(
                  { rollup: true, keptAtPlan, table: tables[0] },
                  sw,
               );
               outcomes.add(said);
               const expected =
                  keptAtPlan &&
                  mayPass("rollup", label) &&
                  !read.endsWith("+zoned") &&
                  keeps("rollup", read, label);
               if (said !== expected) {
                  wrong.push(
                     `read=${read} keptAtPlan=${keptAtPlan} ${label}: ${said}`,
                  );
               }
            }
         }
      }
      expect(wrong).toEqual([]);
      expect([...outcomes].sort()).toEqual([false, true]);
   });

   // A rollup whose base's gate reads a column the rollup stores inexactly
   // (`#(access_filter) w = $W` over a DOUBLE `w`): the router declines the
   // read on either placement, a `storage=` rollup's and one a host placed in
   // storage, and so does the verdict, whatever its plan proved.
   it("a storage rollup whose gate column is stored inexactly keeps no gate, as admitRead declines it", () => {
      for (const sw of switches()) {
         for (const marks of [
            {},
            { ownGateKept: true },
            { holdsOnlyUngatedRows: true },
         ]) {
            const table: TableRead = {
               tier: "storage",
               gateColumnTypes: ["DOUBLE"],
               ...marks,
            };
            expect(admitRead("rollup", [table], EXACT, sw).admit).not.toBe(
               true,
            );
            expect(
               verdictKeepsGate({ rollup: true, keptAtPlan: true, table }, sw),
            ).toBe(false);
         }
      }
   });

   // F-45: a database-origin source (no `storage=`) a host placed in a
   // storage destination is read from its storage binding, so the verdict
   // asks the gate policy of that table, and keeps a gate exactly where
   // admitRead admits the read.
   it("judges a host-placed storage table of a database-origin source as admitRead does, gated and ungated", () => {
      const sourceID = "orders@file:///pkg/m.malloy";
      const plan = {
         graphs: [],
         sources: {
            [sourceID]: {
               name: "orders",
               sourceID,
               connectionName: "duckdb",
               sourceEntityId: "entity-orders",
               sql: "SELECT 1",
               columns: [],
               modelPath: "m.malloy",
            },
         },
      };
      const wrong: string[] = [];
      const outcomes = new Set<string>();
      for (const sw of switches()) {
         const label = switchesLabel(sw);
         for (const gated of [false, true]) {
            const asked: string[] = [];
            const [verdict] = computeServeVerdicts({
               plan: plan as never,
               storageMode: "on",
               reachable: () => true,
               gated: () => gated,
               keepsGate: (_id, tier) => {
                  asked.push(tier);
                  return verdictKeepsGate(
                     { rollup: false, table: { tier, ...EXACT_TABLE[tier] } },
                     sw,
                  );
               },
               refusedAtBind: () => undefined,
               boundEntityIds: new Set(),
               boundStorageNames: new Set(["orders"]),
               boundStorageIds: new Set([sourceID]),
               servedStorageNames: new Set(["orders"]),
               materialize: () => undefined,
               persistedViewOf: () => undefined,
               destinationConfigured: () => undefined,
               connectionCanHoldTable: () => undefined,
            });
            outcomes.add(verdict.verdict);
            const admitted =
               admitRead(
                  "served",
                  [{ tier: "storage", ...EXACT_TABLE.storage }],
                  EXACT,
                  sw,
               ).admit === true;
            const expected = gated && !admitted ? "live" : "serves";
            if (verdict.tier !== "storage" || verdict.verdict !== expected) {
               wrong.push(
                  `gated=${gated} ${label}: ${verdict.tier} ${verdict.verdict}`,
               );
            }
            // Asked of the storage table, never the database one.
            if (asked.some((t) => t !== "storage")) {
               wrong.push(`gated=${gated} ${label}: asked ${asked}`);
            }
         }
      }
      expect(wrong).toEqual([]);
      expect([...outcomes].sort()).toEqual(["live", "serves"]);
   });

   // A database-origin source bound in both tiers, both tables fresh: the
   // served copy re-roots it on one table (written out below, not derived
   // from the rule the code uses), so the verdict names that tier and keeps
   // a gate exactly where admitRead admits the read of that table.
   it("judges a source bound fresh in both tiers on the tier the served copy reads, as admitRead does", () => {
      const sourceID = "orders@file:///pkg/m.malloy";
      const plan = {
         graphs: [],
         sources: {
            [sourceID]: {
               name: "orders",
               sourceID,
               connectionName: "duckdb",
               sourceEntityId: "entity-orders",
               sql: "SELECT 1",
               columns: [],
               modelPath: "m.malloy",
            },
         },
      };
      const wrong: string[] = [];
      const tiersSaid = new Set<string>();
      const outcomes = new Set<string>();
      for (const mode of ["on", "write-only", "off"] as const) {
         // The tiers whose binding the served copy may read under `mode`.
         // The table the served copy reads, written out: the storage table
         // while the mode reads it, else the database table.
         const read: Tier = {
            on: "storage",
            "write-only": "database",
            off: "database",
         }[mode] as Tier;
         for (const sw of switches()) {
            const label = switchesLabel(sw);
            for (const gated of [false, true]) {
               const asked: string[] = [];
               const [verdict] = computeServeVerdicts({
                  plan: plan as never,
                  storageMode: mode,
                  reachable: () => true,
                  gated: () => gated,
                  keepsGate: (_id, tier) => {
                     asked.push(tier);
                     return verdictKeepsGate(
                        {
                           rollup: false,
                           table: { tier, ...EXACT_TABLE[tier] },
                        },
                        sw,
                     );
                  },
                  refusedAtBind: () => undefined,
                  boundEntityIds: new Set(["entity-orders"]),
                  freshEntityIds: new Set(["entity-orders"]),
                  boundStorageNames: new Set(["orders"]),
                  boundStorageIds: new Set([sourceID]),
                  freshStorageIds: new Set([sourceID]),
                  servedStorageNames: new Set(["orders"]),
                  freshStorageNames: new Set(["orders"]),
                  materialize: () => undefined,
                  persistedViewOf: () => undefined,
                  destinationConfigured: () => undefined,
                  connectionCanHoldTable: () => undefined,
               });
               tiersSaid.add(verdict.tier);
               outcomes.add(verdict.verdict);
               const admitted =
                  admitRead(
                     "served",
                     [{ tier: read, ...EXACT_TABLE[read] }],
                     EXACT,
                     sw,
                  ).admit === true;
               const expected = gated && !admitted ? "live" : "serves";
               if (verdict.tier !== read || verdict.verdict !== expected) {
                  wrong.push(
                     `mode=${mode} gated=${gated} ${label}: ${verdict.tier} ${verdict.verdict} ${verdict.reason ?? ""}`,
                  );
               }
               if (asked.some((t) => t !== read)) {
                  wrong.push(
                     `mode=${mode} gated=${gated} ${label}: asked ${asked}`,
                  );
               }
            }
         }
      }
      expect(wrong).toEqual([]);
      expect([...tiersSaid].sort()).toEqual(["database", "storage"]);
      expect(outcomes.has("serves")).toBe(true);
   });

   // The same source shape read by a leg whose storage binding decides its
   // tier (a rollup): under `on` it reads the storage table; where the mode
   // reads none, it answers live (`storage_mode`) today. The r4 spec
   // both_tiers_bound holds the same literals against real queries.
   it("judges a rollup bound fresh in both tiers on the tier its leg reads, under every mode", () => {
      const sourceID = "orders__rollup__region@file:///pkg/m.malloy";
      const plan = {
         graphs: [],
         sources: {
            [sourceID]: {
               name: "orders__rollup__region",
               sourceID,
               origin: "rollup",
               connectionName: "duckdb",
               sourceEntityId: "entity-rollup",
               sql: "SELECT 1",
               columns: [],
               modelPath: "m.malloy",
               preaggregate: {
                  baseSourceName: "orders",
                  grainDimensions: ["region"],
               },
            },
         },
      };
      const wrong: string[] = [];
      for (const mode of ["on", "write-only", "off"] as const) {
         // Written out: the rollup leg reads its storage table under `on`,
         // and answers live under the other modes (until it falls back to a
         // database table there, when this and the verdict change together).
         const expected = {
            on: { tier: "storage", verdict: "serves" },
            "write-only": {
               tier: "storage",
               verdict: "live",
               reason: "storage_mode",
            },
            off: { tier: "storage", verdict: "live", reason: "storage_mode" },
         }[mode];
         const [verdict] = computeServeVerdicts({
            plan: plan as never,
            storageMode: mode,
            reachable: () => true,
            gated: () => false,
            keepsGate: () => true,
            refusedAtBind: () => undefined,
            boundEntityIds: new Set(["entity-rollup"]),
            freshEntityIds: new Set(["entity-rollup"]),
            boundStorageNames: new Set(["orders__rollup__region"]),
            boundStorageIds: new Set([sourceID]),
            freshStorageIds: new Set([sourceID]),
            servedStorageNames: new Set(["orders__rollup__region"]),
            freshStorageNames: new Set(["orders__rollup__region"]),
            materialize: () => undefined,
            persistedViewOf: () => undefined,
            destinationConfigured: () => undefined,
            connectionCanHoldTable: () => undefined,
         });
         const said = {
            tier: verdict.tier,
            verdict: verdict.verdict,
            ...(verdict.reason ? { reason: verdict.reason } : {}),
         };
         if (JSON.stringify(said) !== JSON.stringify(expected)) {
            wrong.push(`mode=${mode}: ${JSON.stringify(said)}`);
         }
      }
      expect(wrong).toEqual([]);
   });
});
