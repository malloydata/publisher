// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The query pipeline over a fake leg list, one test per stage: each stage is
 * driven with fake legs, a fake governor and fake runnables, so what is
 * pinned in is what flows out of the stage and nothing reads a warehouse.
 */

import type { QueryMaterializer, VirtualMap } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   type Binding,
   makeSnapshot,
   type Pinned,
} from "./materialization/domain/binding";
import { LegDecline } from "./materialization/domain/leg_decline";
import type { BuildManifest } from "./materialization/domain/types";
import type { GraftScope } from "./gate_classification";
import {
   type Answer,
   type Leg,
   type LegAnswer,
   LIVE_ANSWER,
   runManifestOf,
   servedFromOf,
} from "./materialization/serve/leg";
import { InvalidQueryRequestError } from "../errors";
import { ModelServing, type ModelServingHost } from "./model_serving";
import type { QueryPrivileges } from "./model_query_request";

const runnable = (tag: string) => ({ tag }) as unknown as QueryMaterializer;
const virtualMap = new Map() as unknown as VirtualMap;

const binding = (sourceName: string, extra: Partial<Binding> = {}): Binding =>
   ({
      kind: "persist",
      tier: "storage",
      sourceName,
      destinationName: "lake",
      virtualHandle: `eid-${sourceName}`,
      tablePath: `lake.t_${sourceName}`,
      schema: [],
      ...extra,
   }) as unknown as Binding;

const answer = (extra: Partial<LegAnswer> = {}): LegAnswer => ({
   runnable: runnable("routed"),
   tablesRead: undefined,
   shapeBindings: [],
   virtualMap: undefined,
   graftScope: undefined,
   ...extra,
});

/** A leg that records the pin each call saw, and answers or declines. */
function fakeLeg(
   kind: Leg["kind"],
   behaviour: { applies?: boolean; answers?: LegAnswer; throws?: unknown },
   seen: Pinned[],
): Leg {
   return {
      kind,
      applies: (_q, p) => {
         seen.push(p);
         return behaviour.applies ?? true;
      },
      attempt: async (_q, p) => {
         seen.push(p);
         if (behaviour.answers) return behaviour.answers;
         throw behaviour.throws ?? new Error(`${kind} does not answer`);
      },
   };
}

const NO_PRIVILEGES = {
   bypassFilters: false,
   bypassAuthorize: false,
   includeHiddenFilesAndSources: false,
} as unknown as QueryPrivileges;

/** A host serving `legs`, with a governor that records each request. */
function host(
   legs: readonly Leg[],
   overrides: Partial<ModelServingHost> = {},
): { host: ModelServingHost; governed: unknown[]; snapshots: number } {
   const governed: unknown[] = [];
   const counts = { snapshots: 0 };
   const governor = {
      hasAnyAuthorizeNote: () => false,
      resolveAuthorizeSourceFromRunnable: async () => "orders",
      govern: async (request: unknown, target: { runnable: unknown }) => {
         governed.push(request);
         return {
            runnable: target.runnable,
            decision: { grafts: [], enforced: [] },
         };
      },
      unboundGateDenial: () => undefined,
      authorizeReferencedGivenNames: new Set<string>(),
   };
   const h = {
      packageName: "p",
      modelPath: "m.malloy",
      governor,
      snapshot: () => {
         counts.snapshots++;
         return makeSnapshot([], {});
      },
      legs: () => legs,
      withoutPreaggregateEntries: (m: BuildManifest | undefined) => m,
      assertFilterGivens: () => {},
      resolveQueryMetadata: () => undefined,
      ...overrides,
   } as unknown as ModelServingHost;
   return {
      host: h,
      governed,
      get snapshots() {
         return counts.snapshots;
      },
   };
}

/** The pipeline's private stages, driven one at a time. */
interface Stages {
   pin(): Pinned;
   queryStringOf(ctx: unknown): string;
   route(
      ctx: unknown,
      prepared: unknown,
      pinned: Pinned,
      admitted: unknown,
      servedCopy: unknown,
   ): Promise<Answer>;
   govern(
      ctx: unknown,
      admitted: unknown,
      prepared: unknown,
      answer: Answer,
   ): Promise<{ runnable: unknown; compiledSource: string | undefined }>;
   execute(
      ctx: unknown,
      args: unknown,
   ): Promise<{ answer: Answer; run: { result: unknown } }>;
}
const stages = (h: ModelServingHost) =>
   new ModelServing(h) as unknown as Stages;

const ctxOf = (request: Record<string, unknown> = {}) => ({
   audit: {},
   request: { query: "run: orders -> by_day", ...request },
   privileges: NO_PRIVILEGES,
   startTime: 0,
});
const prepared = {
   queryString: "\nrun: orders -> by_day",
   liveRunnable: {
      ...runnable("live"),
      // The author's compile the router reads a source's gates and row
      // scope from: an ungated source with no `where:`, so a leg reading a
      // storage table is judged on nothing it scopes.
      getPreparedQuery: async () => ({
         _query: { structRef: "orders" },
         _modelDef: {
            name: "m",
            exports: [],
            contents: {
               orders: {
                  type: "table",
                  name: "orders",
                  tablePath: "orders",
                  connection: "duckdb",
                  dialect: "duckdb",
                  fields: [{ name: "id", type: "number" }],
               },
            },
         },
      }),
   } as unknown as QueryMaterializer,
   surfaceGivens: undefined,
};
const admitted = {
   boundary: "cleared",
   callerRegion: undefined,
   earlySource: "orders",
};

describe("the query pipeline over a fake leg list", () => {
   it("pin: one snapshot read, and every leg reads that one pin", async () => {
      const seen: Pinned[] = [];
      const legs = [
         fakeLeg("view", { applies: false }, seen),
         fakeLeg(
            "rollup",
            { throws: new LegDecline("rollup_not_built") },
            seen,
         ),
         fakeLeg("served", { answers: answer() }, seen),
      ];
      const fake = host(legs);
      const s = stages(fake.host);
      const pinned = s.pin();
      await s.route(ctxOf(), prepared, pinned, admitted, {});
      expect(fake.snapshots).toBe(1);
      expect(seen.length).toBeGreaterThan(0);
      for (const p of seen) expect(p).toBe(pinned);
   });

   it("prepare: a named view is quoted, and a malformed request is refused", () => {
      const s = stages(host([]).host);
      expect(
         s.queryStringOf(
            ctxOf({ query: undefined, sourceName: "or ders", queryName: "v" }),
         ),
      ).toBe("\nrun: `or ders` -> `v`");
      expect(s.queryStringOf(ctxOf())).toBe("\nrun: orders -> by_day");
      expect(() =>
         s.queryStringOf(ctxOf({ query: undefined, sourceName: "orders" })),
      ).toThrow(InvalidQueryRequestError);
   });

   it("route: legs in order, the first that answers wins, each skip recorded with its decline", async () => {
      const seen: Pinned[] = [];
      const served = answer({ tablesRead: [binding("orders")], virtualMap });
      const s = stages(
         host([
            fakeLeg("view", { applies: false }, seen),
            fakeLeg(
               "rollup",
               {
                  throws: new LegDecline(
                     "rollup_grain_too_coarse",
                     "the rollup is by month",
                  ),
               },
               seen,
            ),
            fakeLeg("served", { answers: served }, seen),
         ]).host,
      );
      const a = await s.route(ctxOf(), prepared, s.pin(), admitted, {});
      expect(a.leg).toBe("served");
      expect(a.leg !== "live" && a.answer).toBe(served);
      expect(a.considered).toEqual([
         { kind: "view", outcome: "skipped", reason: "not_applicable" },
         {
            kind: "rollup",
            outcome: "skipped",
            reason: "does_not_answer",
            detail: "the rollup is by month",
            code: "rollup_grain_too_coarse",
            callerDetail: "the rollup is by month",
         },
         { kind: "served", outcome: "used" },
      ]);
   });

   it("route: a leg's own error is a skip with no code, and no answer is live", async () => {
      const s = stages(
         host([fakeLeg("served", { throws: new Error("boom") }, [])]).host,
      );
      const a = await s.route(ctxOf(), prepared, s.pin(), admitted, {});
      expect(a.leg).toBe("live");
      expect(a.considered).toEqual([
         {
            kind: "served",
            outcome: "skipped",
            reason: "does_not_answer",
            detail: "boom",
         },
      ]);
   });

   it("govern: the answer's graft scope, past-gate flag and served-copy ceiling reach govern(), on the routed runnable", async () => {
      const graftScope = { cacheScope: "served" } as unknown as GraftScope;
      const routed = answer({ graftScope });
      const fake = host([]);
      const s = stages(fake.host);
      const routedAnswer: Answer = {
         leg: "served",
         considered: [],
         answer: routed,
         pastEntryPointGate: true,
         runFailed: false,
         colocatedRead: new Set(),
      };
      const governed = await s.govern(
         ctxOf({ query: undefined, sourceName: "orders", queryName: "v" }),
         admitted,
         prepared,
         routedAnswer,
      );
      expect(governed.runnable).toBe(routed.runnable);
      expect(fake.governed[0]).toMatchObject({
         graftScope,
         routedPastEntryPointGate: true,
         ceiling: { answeredBy: "served_copy" },
      });
      const live = await s.govern(
         ctxOf({ query: undefined, sourceName: "orders", queryName: "v" }),
         admitted,
         prepared,
         LIVE_ANSWER,
      );
      expect(live.runnable).toBe(prepared.liveRunnable);
      expect(fake.governed[1]).toMatchObject({
         graftScope: undefined,
         routedPastEntryPointGate: false,
      });
   });

   it("run: a stored run that fails is retried live when every binding it read allows it", async () => {
      const result = { data: { rowCount: 1 } };
      const failing = {
         getPreparedResult: async () => {
            throw new Error("store down");
         },
      } as unknown as QueryMaterializer;
      const live = {
         getPreparedResult: async () => ({
            resultExplore: { limit: undefined },
            connectionName: "duckdb",
         }),
         run: async () => result,
      } as unknown as QueryMaterializer;
      const fake = host([]);
      const s = stages(fake.host);
      const routedAnswer: Answer = {
         leg: "served",
         considered: [{ kind: "served", outcome: "used" }],
         answer: answer({
            runnable: failing,
            virtualMap,
            shapeBindings: [binding("orders", { freshnessFallback: "live" })],
         }),
         pastEntryPointGate: false,
         runFailed: false,
         colocatedRead: new Set(),
      };
      const ran = await s.execute(ctxOf(), {
         pinned: s.pin(),
         viewManifest: undefined,
         admitted,
         prepared: { ...prepared, liveRunnable: live },
         answer: routedAnswer,
         governed: {
            runnable: failing,
            request: { ceiling: undefined },
            decision: { grafts: [], enforced: [] },
            compiledSource: "orders",
         },
      });
      expect(ran.run.result).toBe(result);
      expect(ran.answer.leg !== "live" && ran.answer.runFailed).toBe(true);
      expect(servedFromOf(ran.answer)).toBe("live_fallback");
      // The retry was governed in its own right, on the author's scope.
      expect(fake.governed[0]).toMatchObject({
         graftScope: undefined,
         routedPastEntryPointGate: false,
         bypassAlreadyNoted: true,
      });
   });

   it("run: each answer takes its own manifest or map", () => {
      const manifests = {
         pinned: "pinned",
         withoutRollups: "without rollups",
         live: "empty",
      };
      const routed = (
         leg: "view" | "rollup" | "served",
         extra: Partial<LegAnswer> = {},
      ): Answer => ({
         leg,
         considered: [],
         answer: answer(extra),
         pastEntryPointGate: false,
         runFailed: false,
         colocatedRead: new Set(),
      });
      expect(runManifestOf(LIVE_ANSWER, manifests)).toBe("empty");
      expect(runManifestOf(routed("rollup"), manifests)).toBe("pinned");
      expect(runManifestOf(routed("view"), manifests)).toBe("without rollups");
      expect(runManifestOf(routed("served", { virtualMap }), manifests)).toBe(
         undefined,
      );
      expect(
         runManifestOf(
            { ...(routed("rollup") as object), runFailed: true } as Answer,
            manifests,
         ),
      ).toBe("empty");
   });

   it("report: servedFrom reads only the answer", () => {
      const routed = (
         leg: "view" | "rollup" | "served",
         extra: Partial<LegAnswer>,
      ): Answer => ({
         leg,
         considered: [],
         answer: answer(extra),
         pastEntryPointGate: false,
         runFailed: false,
         colocatedRead: new Set(),
      });
      const colocated = binding("orders", { tier: "colocated" });
      expect(servedFromOf(LIVE_ANSWER)).toBeUndefined();
      expect(
         servedFromOf(routed("served", { tablesRead: [binding("orders")] })),
      ).toBe("storage");
      expect(servedFromOf(routed("served", { tablesRead: [colocated] }))).toBe(
         "database",
      );
      expect(servedFromOf(routed("rollup", {}))).toBe("database");
      expect(
         servedFromOf({ ...LIVE_ANSWER, colocatedRead: new Set(["a"]) }),
      ).toBe("database");
      expect(servedFromOf(routed("view", { virtualMap }))).toBe("storage");
   });
});
