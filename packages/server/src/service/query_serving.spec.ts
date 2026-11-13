// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryMaterializer, VirtualMap } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import type { ServeBinding } from "./materialization/serve/materialization_serve_transform";
import type { BuildManifest } from "./materialization/types";
import type { GraftScope } from "./gate_classification";
import type { RouteDecision } from "./query_router";
import { installSettings } from "../test_helpers/settings_env";
import {
   answeredWith,
   canDegradeToLive,
   LIVE_ANSWER,
   liveFallbackAnswer,
   recordingManifest,
   routeCandidates,
   type QueryServingHost,
   type RoutedExtra,
   routedAnswer,
   servedFromOf,
} from "./query_serving";

const runnable = { tag: "routed" } as unknown as QueryMaterializer;
const virtualMap = new Map() as unknown as VirtualMap;

const binding = (
   sourceName: string,
   extra: Partial<ServeBinding> = {},
): ServeBinding =>
   ({
      sourceName,
      destinationName: "lake",
      virtualHandle: `eid-${sourceName}`,
      tablePath: `lake.t_${sourceName}`,
      schema: [],
      ...extra,
   }) as unknown as ServeBinding;

const manifest = (keys: string[]): BuildManifest =>
   ({
      entries: Object.fromEntries(keys.map((k) => [k, { tableName: k }])),
   }) as unknown as BuildManifest;

function storageRoute(bindings: ServeBinding[]): RouteDecision<RoutedExtra> {
   return {
      served: "variant",
      kind: "storage",
      runnable,
      extra: {
         kind: "storage",
         virtualMap,
         bindings,
         origin: "persist",
         tablesRead: bindings,
      },
      considered: [{ kind: "storage", outcome: "used" }],
   };
}

const rollupRoute: RouteDecision<RoutedExtra> = {
   served: "variant",
   kind: "rollup",
   runnable,
   extra: { kind: "rollup", resolvedMember: "orders__by_day" },
   considered: [
      { kind: "storage", outcome: "skipped", reason: "does_not_answer" },
      { kind: "rollup", outcome: "used" },
   ],
};

describe("Answer", () => {
   it("reads a live route as live, keeping what the router considered", () => {
      const answer = routedAnswer({
         served: "live",
         considered: [
            { kind: "rollup", outcome: "skipped", reason: "row_level_gate" },
         ],
      });
      expect(answer.tier).toBe("live");
      expect(answer.routedRunnable).toBeUndefined();
      expect(answer.considered).toHaveLength(1);
      expect(servedFromOf(answer)).toBeUndefined();
   });

   it("reads a storage route as a storage hit with its tables", () => {
      const bindings = [binding("daily")];
      const answer = routedAnswer(storageRoute(bindings));
      expect(answer.tier).toBe("storage");
      expect(answer.routedRunnable).toBe(runnable);
      expect(answer.virtualMap).toBe(virtualMap);
      expect(answer.serveShapeBindings).toEqual(bindings);
      expect(answer.storageTablesRead).toEqual(bindings);
      expect(servedFromOf(answer)).toBe("storage");
   });

   it("reads a rollup route as a rollup, not a storage hit", () => {
      const answer = routedAnswer(rollupRoute);
      expect(answer.tier).toBe("rollup");
      expect(answer.rollupMember).toBe("orders__by_day");
      expect(answer.virtualMap).toBeUndefined();
      expect(servedFromOf(answer)).toBeUndefined();
   });

   it("reads a storage= #(preaggregate) rollup route as a storage hit", () => {
      const b = { ...binding("rollup"), origin: "preaggregate" as const };
      const answer = routedAnswer({
         ...rollupRoute,
         extra: {
            kind: "rollup",
            resolvedMember: "orders__rollup__category",
            virtualMap,
            binding: b,
         },
      });
      expect(answer.tier).toBe("rollup");
      expect(answer.virtualMap).toBe(virtualMap);
      expect(answer.serveShapeBindings).toEqual([b]);
      expect(servedFromOf(answer)).toBe("storage");
   });

   it("lets a join-to-rollup answer's query-source table decide whether a failed run degrades (known limitation 26)", () => {
      const rollup = { ...binding("rollup"), origin: "preaggregate" as const };
      const source = binding("flights", { freshnessFallback: "fail" });
      const answer = routedAnswer({
         ...rollupRoute,
         extra: {
            kind: "rollup",
            resolvedMember: "segments__rollup",
            virtualMap,
            binding: rollup,
            bindings: [source, rollup],
         },
      });
      expect(answer.serveShapeBindings).toEqual([source, rollup]);
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
      const routed = routedAnswer(storageRoute([binding("daily")]));
      const failed = liveFallbackAnswer(routed);
      const read = new Set(["k1"]);
      const answered = answeredWith(failed, read);
      expect(Object.isFrozen(LIVE_ANSWER)).toBe(true);
      for (const a of [routed, failed, answered]) {
         expect(Object.isFrozen(a)).toBe(true);
      }
      expect(routed.runFailed).toBe(false);
      expect(failed.runFailed).toBe(true);
      expect(failed.colocatedRead.size).toBe(0);
      expect(answered.colocatedRead).toBe(read);
      expect(LIVE_ANSWER.colocatedRead.size).toBe(0);
   });

   it("reads a failed storage run answered live as live_fallback, never a hit", () => {
      const answer = liveFallbackAnswer(
         routedAnswer(storageRoute([binding("daily")])),
      );
      expect(servedFromOf(answer)).toBe("live_fallback");
   });
});

describe("canDegradeToLive", () => {
   const open = { hasLiveRunnable: true, aborted: false };
   const storage = (fallback?: string) =>
      routedAnswer(
         storageRoute([
            binding("daily", {
               freshnessFallback: fallback,
            } as Partial<ServeBinding>),
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
         canDegradeToLive(routedAnswer(rollupRoute), new Error("x"), open),
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
   const host = {
      modelPath: "m.malloy",
      rollupVariants: {},
      persistedViews: {},
   } as unknown as QueryServingHost;
   const rollupCandidate = () =>
      routeCandidates(host, {
         queryString: "run: orders -> { aggregate: total }",
         surfaceGivens: undefined,
         buildManifest: undefined,
         servedCopyPossible: false,
         rollupVariantsFor: runnable,
      }).find((c) => c.kind === "rollup");

   for (const on of [true, false]) {
      it(`keeps the contracts only with gated sources on (${on ? "on" : "off"})`, () => {
         const { restore } = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: on ? "true" : "false",
            },
         });
         try {
            expect(rollupCandidate()?.keepsContracts).toBe(on);
         } finally {
            restore();
         }
      });
   }

   it("carries every table a join leg in a storage destination reads into the route", async () => {
      const rollup = { ...binding("rollup"), origin: "preaggregate" as const };
      const source = binding("flights");
      const reading = {
         ...host,
         rollupVariants: {
            attempt: async () => ({
               runnable,
               rollupSourceName: "segments__rollup",
               virtualMap,
               binding: rollup,
               bindings: [source, rollup],
            }),
         },
      } as unknown as QueryServingHost;
      const candidate = routeCandidates(reading, {
         queryString: "run: flights -> { aggregate: segments.seg_miles }",
         surfaceGivens: undefined,
         buildManifest: undefined,
         servedCopyPossible: false,
         rollupVariantsFor: runnable,
      }).find((c) => c.kind === "rollup");
      const routed = await candidate?.attempt();
      expect(routed?.extra).toMatchObject({
         kind: "rollup",
         binding: rollup,
         bindings: [source, rollup],
      });
   });

   it("carries the variant's graft scope into the answer, for govern()", () => {
      const graftScope = { cacheScope: "rollup" } as unknown as GraftScope;
      const answer = routedAnswer({
         served: "variant",
         kind: "rollup",
         runnable,
         extra: { kind: "rollup", resolvedMember: "r", graftScope },
         considered: [],
         pastEntryPointGate: true,
      });
      expect(answer.graftScope).toBe(graftScope);
      expect(answer.pastEntryPointGate).toBe(true);
   });
});
