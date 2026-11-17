// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   type QueryMaterializer,
   SingleConnectionRuntime,
   type VirtualMap,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import {
   type Binding,
   EMPTY_SNAPSHOT,
   pin,
} from "./materialization/domain/binding";
import type { BuildManifest } from "./materialization/domain/types";
import type { GraftScope } from "./gate_classification";
import type { RouteDecision } from "./query_router";
import { installSettings } from "../test_helpers/settings_env";
import {
   answeredWith,
   canDegradeToLive,
   type LegAnswer,
   LIVE_ANSWER,
   liveFallbackAnswer,
   makeLegs,
   type RoutedAnswer,
   servedFromOf,
} from "./materialization/serve/leg";
import type { RollupVariantServe } from "./materialization/serve/rollup_variants";
import { tablesReadForGates } from "./materialization/serve/bindings";
import { answerOf, recordingManifest, variantsOf } from "./query_serving";

const runnable = { tag: "routed" } as unknown as QueryMaterializer;
const virtualMap = new Map() as unknown as VirtualMap;

// The author's compile the router reads an entry point's gate columns from:
// `gated` reads `acct`, `open` has no gate.
const authorConnection = new DuckDBConnection("duckdb", ":memory:");
const authorModel = new SingleConnectionRuntime({
   connection: authorConnection,
}).loadModel(`##! experimental { givens }
given: ACCT :: number
#(access_filter) acct = $ACCT
source: gated is duckdb.sql("SELECT 1 AS id, CAST(2 AS DECIMAL(20,0)) AS acct")
source: open is duckdb.sql("SELECT 1 AS id, CAST(2 AS DECIMAL(20,0)) AS acct")
#(access_filter) k = $ACCT
source: renamed is open -> { select: id, k is acct }
#(access_filter) k = $ACCT
source: renamed_twice is open -> { select: id, k1 is acct } -> { select: id, k is k1 }
given: ROLE :: string
#(authorize) 'analyst' = $ROLE
source: locked is open
`);
const author = (
   source: "gated" | "open" | "renamed" | "renamed_twice" | "locked",
): QueryMaterializer =>
   authorModel.loadQuery(`run: ${source} -> { select: id }`);
afterAll(async () => {
   await authorConnection.close();
});

const binding = (sourceName: string, extra: Partial<Binding> = {}): Binding =>
   ({
      kind: "persist",
      tier: "storage",
      sourceName,
      destinationName: "lake",
      virtualHandle: `eid-${sourceName}`,
      tablePath: `lake.t_${sourceName}`,
      // The gate column of the author compile below, stored exactly.
      schema: [{ name: "acct", type: "DECIMAL(38,0)" }],
      ...extra,
   }) as unknown as Binding;

const manifest = (keys: string[]): BuildManifest =>
   ({
      entries: Object.fromEntries(keys.map((k) => [k, { tableName: k }])),
   }) as unknown as BuildManifest;

const legAnswer = (extra: Partial<LegAnswer> = {}): LegAnswer => ({
   runnable,
   tablesRead: undefined,
   shapeBindings: [],
   virtualMap: undefined,
   graftScope: undefined,
   ...extra,
});

function storageRoute(bindings: Binding[]): RouteDecision<LegAnswer> {
   return {
      served: "variant",
      kind: "served",
      runnable,
      extra: legAnswer({
         virtualMap,
         shapeBindings: bindings,
         tablesRead: bindings,
      }),
      considered: [{ kind: "served", outcome: "used" }],
   };
}

const rollupRoute: RouteDecision<LegAnswer> = {
   served: "variant",
   kind: "rollup",
   runnable,
   extra: legAnswer({ rollupMember: "orders__by_day" }),
   considered: [
      { kind: "served", outcome: "skipped", reason: "does_not_answer" },
      { kind: "rollup", outcome: "used" },
   ],
};

describe("Answer", () => {
   it("reads a live route as live, keeping what the router considered", () => {
      const answer = answerOf({
         served: "live",
         considered: [
            { kind: "rollup", outcome: "skipped", reason: "row_level_gate" },
         ],
      });
      expect(answer.leg).toBe("live");
      expect("answer" in answer).toBe(false);
      expect(answer.considered).toHaveLength(1);
      expect(servedFromOf(answer)).toBeUndefined();
   });

   it("reads a storage route as a storage hit with its tables", () => {
      const bindings = [binding("daily")];
      const answer = answerOf(storageRoute(bindings)) as RoutedAnswer;
      expect(answer.leg).toBe("served");
      expect(answer.answer.runnable).toBe(runnable);
      expect(answer.answer.virtualMap).toBe(virtualMap);
      expect(answer.answer.shapeBindings).toEqual(bindings);
      expect(answer.answer.tablesRead).toEqual(bindings);
      expect(servedFromOf(answer)).toBe("storage");
   });

   it("reads a colocated rollup route as a colocated hit, not a storage hit", () => {
      const answer = answerOf(rollupRoute) as RoutedAnswer;
      expect(answer.leg).toBe("rollup");
      expect(answer.answer.rollupMember).toBe("orders__by_day");
      expect(answer.answer.virtualMap).toBeUndefined();
      expect(servedFromOf(answer)).toBe("database");
   });

   it("reads a storage= #(preaggregate) rollup route as a storage hit", () => {
      const b = { ...binding("rollup"), kind: "rollup" as const };
      const answer = answerOf({
         ...rollupRoute,
         extra: legAnswer({
            rollupMember: "orders__rollup__category",
            virtualMap,
            shapeBindings: [b],
         }),
      }) as RoutedAnswer;
      expect(answer.leg).toBe("rollup");
      expect(answer.answer.virtualMap).toBe(virtualMap);
      expect(answer.answer.shapeBindings).toEqual([b]);
      expect(servedFromOf(answer)).toBe("storage");
   });

   it("lets a join-to-rollup answer's query-source table decide whether a failed run degrades (known limitation 26)", () => {
      const rollup = { ...binding("rollup"), kind: "rollup" as const };
      const source = binding("flights", { freshnessFallback: "fail" });
      const answer = answerOf({
         ...rollupRoute,
         extra: legAnswer({
            rollupMember: "segments__rollup",
            virtualMap,
            shapeBindings: [source, rollup],
         }),
      }) as RoutedAnswer;
      expect(answer.answer.shapeBindings).toEqual([source, rollup]);
      expect(servedFromOf(answer)).toBe("storage");
      // The rollup alone would degrade; the query source's table vetoes.
      expect(
         canDegradeToLive(answer, new Error("store down"), {
            hasLiveRunnable: true,
            aborted: false,
         }),
      ).toBe(false);
   });

   it("never mutates: each stage returns a new frozen record", () => {
      const routed = answerOf(storageRoute([binding("daily")])) as RoutedAnswer;
      const failed = liveFallbackAnswer(routed);
      const read = new Set(["k1"]);
      const answered = answeredWith(failed, read);
      expect(Object.isFrozen(LIVE_ANSWER)).toBe(true);
      for (const a of [routed, failed, answered]) {
         expect(Object.isFrozen(a)).toBe(true);
      }
      expect(routed.runFailed).toBe(false);
      expect((failed as RoutedAnswer).runFailed).toBe(true);
      expect(failed.colocatedRead.size).toBe(0);
      expect(answered.colocatedRead).toBe(read);
      expect(LIVE_ANSWER.colocatedRead.size).toBe(0);
   });

   it("reads a failed storage run answered live as live_fallback, never a hit", () => {
      const answer = liveFallbackAnswer(
         answerOf(storageRoute([binding("daily")])) as RoutedAnswer,
      );
      expect(servedFromOf(answer)).toBe("live_fallback");
   });
});

describe("canDegradeToLive", () => {
   const open = { hasLiveRunnable: true, aborted: false };
   const storage = (fallback?: string) =>
      answerOf(
         storageRoute([
            binding("daily", {
               freshnessFallback: fallback,
            } as Partial<Binding>),
         ]),
      );

   it("degrades a storage answer whose shape's bindings all say live", () => {
      expect(
         canDegradeToLive(storage("live"), new Error("store down"), open),
      ).toBe(true);
   });

   it("never degrades a live or rollup answer (nothing stored was read)", () => {
      expect(canDegradeToLive(LIVE_ANSWER, new Error("x"), open)).toBe(false);
      expect(
         canDegradeToLive(answerOf(rollupRoute), new Error("x"), open),
      ).toBe(false);
   });

   it("never degrades a client error, an abort, or with no live compile", () => {
      const given = Object.assign(new Error("bad given"), {
         code: "runtime-given-missing",
      });
      expect(canDegradeToLive(storage("live"), given, open)).toBe(false);
      expect(
         canDegradeToLive(storage("live"), new Error("x"), {
            ...open,
            aborted: true,
         }),
      ).toBe(false);
      expect(
         canDegradeToLive(storage("live"), new Error("x"), {
            ...open,
            hasLiveRunnable: false,
         }),
      ).toBe(false);
   });

   it("lets an authored non-live binding veto the shape", () => {
      expect(canDegradeToLive(storage("stale_ok"), new Error("x"), open)).toBe(
         false,
      );
   });
});

describe("recordingManifest", () => {
   it("records exactly the entries looked up and found", () => {
      const read = new Set<string>();
      const wrapped = recordingManifest(manifest(["a", "b"]), read)!;
      void wrapped.entries["a"];
      void wrapped.entries["missing"];
      expect([...read]).toEqual(["a"]);
   });

   it("does not count any enumeration of the entries as a lookup", () => {
      const read = new Set<string>();
      const wrapped = recordingManifest(manifest(["a", "b"]), read)!;
      // Malloy checks every entry's table path before it compiles. Every
      // enumeration form, in any order, synchronously beside a lookup.
      for (const [, entry] of Object.entries(wrapped.entries)) void entry;
      void Object.values(wrapped.entries);
      void { ...wrapped.entries };
      void Object.assign({}, wrapped.entries);
      void JSON.stringify(wrapped.entries);
      void JSON.stringify(wrapped);
      const held = wrapped.entries;
      for (const key in held) void held[key];
      expect(Object.keys(wrapped.entries)).toEqual(["a", "b"]);
      expect([...read]).toEqual([]);
      // A lookup in the same synchronous step still records, plain or guarded.
      void wrapped.entries["b"];
      expect([...read]).toEqual(["b"]);
      const entries = wrapped.entries;
      if (Object.hasOwn(entries, "a")) void entries["a"];
      expect([...read].sort()).toEqual(["a", "b"]);
   });

   it("keeps the manifest's other fields", () => {
      const read = new Set<string>();
      const wrapped = recordingManifest(
         { ...manifest(["a"]), strict: true } as BuildManifest,
         read,
      )!;
      expect(wrapped.strict).toBe(true);
      expect(Object.keys(wrapped.entries)).toEqual(["a"]);
   });

   it("is undefined for no manifest", () => {
      expect(recordingManifest(undefined, new Set())).toBeUndefined();
   });
});

describe("the #(preaggregate) rollup candidate past an entry-point gate (row 1)", () => {
   const pinned = pin(EMPTY_SNAPSHOT, new Date(0));
   const rollupCandidate = (rollupVariants: unknown = {}) =>
      variantsOf(
         makeLegs(pinned, {
            rollupVariants: rollupVariants as RollupVariantServe,
         }),
         {
            queryString: "run: orders -> { aggregate: total }",
            givens: undefined,
            authorRunnable: author("gated"),
         },
         pinned,
         { modelPath: "m.malloy" },
      ).find((c) => c.kind === "rollup");

   for (const on of [true, false]) {
      it(`is tried past the gate with gated sources ${on ? "on" : "off (G3)"}, and keeps it off only for a colocated rollup`, async () => {
         const { restore } = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: on ? "true" : "false",
            },
         });
         try {
            const candidate = rollupCandidate();
            expect(candidate?.keepsContracts).toBe(true);
            const read = (tier: "colocated" | "storage") =>
               candidate?.keepsContractsFor?.({
                  runnable,
                  tablesRead: undefined,
                  gateTablesRead: [
                     tier === "storage"
                        ? {
                             tier,
                             schema: [{ name: "acct", type: "DECIMAL(38,0)" }],
                          }
                        : { tier },
                  ],
                  shapeBindings: [],
               } as never);
            expect(await read("colocated")).toBe(true);
            // A `storage=` rollup plan did not mark keeps the gate only
            // under the switch (P3/P4).
            expect(await read("storage")).toBe(on);
         } finally {
            restore();
         }
      });
   }

   it("carries every table a join branch in a storage destination reads into the route", async () => {
      const rollup = { ...binding("rollup"), kind: "rollup" as const };
      const source = binding("flights");
      const candidate = rollupCandidate({
         attempt: async () => ({
            runnable,
            rollupSourceName: "segments__rollup",
            virtualMap,
            binding: rollup,
            bindings: [source, rollup],
         }),
      });
      const routed = await candidate?.attempt();
      expect(routed?.extra).toMatchObject({
         rollupMember: "segments__rollup",
         virtualMap,
         shapeBindings: [source, rollup],
      });
   });

   it("carries the variant's graft scope into the answer, for govern()", () => {
      const graftScope = { cacheScope: "rollup" } as unknown as GraftScope;
      const answer = answerOf({
         served: "variant",
         kind: "rollup",
         runnable,
         extra: legAnswer({ rollupMember: "r", graftScope }),
         considered: [],
         pastEntryPointGate: true,
      }) as RoutedAnswer;
      expect(answer.answer.graftScope).toBe(graftScope);
      expect(answer.pastEntryPointGate).toBe(true);
   });
});

describe("the served copy past an entry-point gate under G3 (gated sources off)", () => {
   const pinned = pin(EMPTY_SNAPSHOT, new Date(0));
   const colocated = binding("c", {
      kind: "materialize",
      tier: "colocated",
      destinationName: "duckdb",
      tablePath: '"c_tbl"',
   });
   const storage = binding("s", {
      kind: "materialize",
      tablePath: "main.s_tbl",
   });
   // The storage table read unquoted beside the colocated one. Only the
   // quoting defeats the served copy's exact match against the bound
   // `"main"."s_tbl"` (sqlTableReads ignores case), so it finds only "c_tbl".
   const sql =
      'SELECT 1 FROM "c_tbl" AS base LEFT JOIN main.s_tbl AS j ON j.id = base.id';
   const servedCandidate = (
      gateTablesRead: Binding[] | undefined,
      authorRunnable: QueryMaterializer = author("gated"),
   ) =>
      variantsOf(
         makeLegs(pinned, {
            servedCopy: {
               hasBindings: () => true,
               loadQuery: async () => ({
                  runnable,
                  virtualMap,
                  bindings: [colocated, storage],
                  // The exact-match result, which gateTablesRead overrides.
                  tablesRead: [colocated],
                  gateTablesRead,
                  graftScope: undefined,
               }),
            } as never,
         }),
         {
            queryString: "run: gated -> { aggregate: n }",
            givens: undefined,
            authorRunnable,
         },
         pinned,
         { modelPath: "m.malloy" },
      ).find((c) => c.kind === "served")!;
   const withG3 = async <T>(run: () => Promise<T>): Promise<T> => {
      const { restore } = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
         },
      });
      try {
         return await run();
      } finally {
         restore();
      }
   };

   it("judges an answer by the tables it is judged to read for its gates, so a storage table spelled another way keeps it off G3", async () => {
      await withG3(async () => {
         const judged = tablesReadForGates(
            sql,
            [colocated, storage],
            [colocated],
         );
         expect(judged).toEqual([colocated, storage]);
         const candidate = servedCandidate(judged);
         // Tried past the gate (every table it may read could be colocated)…
         expect(candidate.keepsContracts).toBe(true);
         const routed = await candidate.attempt();
         // …but the answer it produced reads a storage table: not kept.
         expect(routed.extra?.gateTablesRead).toEqual([colocated, storage]);
         expect(await candidate.keepsContractsFor!(routed.extra!)).toBe(false);
      });
   });

   it("still keeps a colocated-only answer under G3", async () => {
      await withG3(async () => {
         const candidate = servedCandidate([colocated]);
         const routed = await candidate.attempt();
         expect(await candidate.keepsContractsFor!(routed.extra!)).toBe(true);
      });
   });

   it("keeps an answer reading only storage tables plan admitted past their own gate, and judges a mixed one by every table it may read", async () => {
      const kept = { ...storage, ownGateKept: true };
      const other = binding("o", {
         kind: "materialize",
         tablePath: "main.o_tbl",
      });
      await withG3(async () => {
         for (const [read, keeps] of [
            [[kept], true],
            [[colocated, kept], true],
            [[kept, other], false],
            [[storage], false],
         ] as const) {
            const candidate = servedCandidate([...read]);
            const routed = await candidate.attempt();
            expect(await candidate.keepsContractsFor!(routed.extra!)).toBe(
               keeps,
            );
         }
         // The kept table found, the other spelled another way: the fail
         // closed read still counts the other, so the answer stays live.
         const respelled =
            'SELECT 1 FROM "main"."s_tbl" AS base LEFT JOIN MAIN.O_TBL AS j ON TRUE';
         const judged = tablesReadForGates(respelled, [kept, other], [kept]);
         expect(judged).toEqual([kept, other]);
         const candidate = servedCandidate(judged);
         expect(
            await candidate.keepsContractsFor!(
               (await candidate.attempt()).extra!,
            ),
         ).toBe(false);
      });
   });

   it("L2: keeps no answer whose gate column a storage table stores as a float, under any switch", async () => {
      const stored = (type: string) =>
         binding("s", {
            kind: "materialize",
            tablePath: "main.s_tbl",
            ownGateKept: true,
            schema: [
               { name: "id", type: "BIGINT" },
               { name: "acct", type },
            ],
         });
      for (const gatedSources of ["false", "true"]) {
         const { restore } = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: gatedSources,
            },
         });
         try {
            for (const [type, keeps] of [
               ["DECIMAL(38,0)", true],
               ["BIGINT", true],
               ["DOUBLE", false],
               ["FLOAT", false],
               ["HUGEINT", false],
            ] as const) {
               const candidate = servedCandidate([stored(type)]);
               expect(
                  await candidate.keepsContractsFor!(
                     (await candidate.attempt()).extra!,
                  ),
                  `${type}, gated sources ${gatedSources}`,
               ).toBe(keeps);
            }
            // An entry point whose gate reads no float column is unaffected.
            const open = servedCandidate([stored("DOUBLE")], author("open"));
            expect(
               await open.keepsContractsFor!((await open.attempt()).extra!),
            ).toBe(true);
            // A query-shaped entry point whose gate reads a renamed stage
            // output: followed back to the stored column it renames.
            for (const source of ["renamed", "renamed_twice"] as const) {
               for (const [type, keeps] of [
                  ["DOUBLE", false],
                  ["DECIMAL(38,0)", true],
               ] as const) {
                  const c = servedCandidate([stored(type)], author(source));
                  expect(
                     await c.keepsContractsFor!((await c.attempt()).extra!),
                     `${source} over ${type}`,
                  ).toBe(keeps);
               }
            }
            // A gate column the table does not hold under any name: unproven.
            const missing = servedCandidate([
               binding("s", {
                  kind: "materialize",
                  tablePath: "main.s_tbl",
                  ownGateKept: true,
                  schema: [{ name: "id", type: "BIGINT" }],
               }),
            ]);
            expect(
               await missing.keepsContractsFor!(
                  (await missing.attempt()).extra!,
               ),
            ).toBe(false);
            // A lock reads no column: nothing to prove.
            const lock = servedCandidate(
               [
                  binding("s", {
                     kind: "materialize",
                     tablePath: "main.s_tbl",
                     ownGateKept: true,
                     schema: [{ name: "id", type: "DOUBLE" }],
                  }),
               ],
               author("locked"),
            );
            expect(
               await lock.keepsContractsFor!((await lock.attempt()).extra!),
            ).toBe(true);
            // A database table on a DuckLake catalog that holds the gate's
            // column as a DOUBLE (a 128-bit integer): live. Another column
            // held so leaves the gate alone.
            // A database table whose gate column the bind found altered
            // since its build: judged by the type it holds now.
            for (const [moved, keeps] of [
               [[{ name: "acct", type: "DOUBLE" }], false],
               [[{ name: "acct", type: "double precision" }], false],
               [[{ name: "acct", type: "DECIMAL(38,0)" }], true],
               [[{ name: "total", type: "DOUBLE" }], true],
            ] as const) {
               const lake = servedCandidate([
                  { ...colocated, retypedColumns: [...moved] },
               ]);
               expect(
                  await lake.keepsContractsFor!((await lake.attempt()).extra!),
               ).toBe(keeps);
            }
            // An author's compile that cannot be read: the served copy is
            // never answered from (its composite member cannot be checked).
            await expect(
               servedCandidate([stored("DECIMAL(38,0)")], runnable).attempt(),
            ).rejects.toThrow(/composite source/);
         } finally {
            restore();
         }
      }
   });
});
