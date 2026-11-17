// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Refusals and declines that had no end-to-end test, each through the real
 * package path (conformance/package_harness.ts): load, build and bind, then
 * every query answered against live before any table was bound, by the R4
 * comparator and the strict one, with the tier it must report and the code a
 * refusal carries.
 *
 *  - Naming: two `#@ persist name=` values that differ only in case, on a
 *    destination that folds case (`storage=` and database DuckDB); two
 *    persisted views that resolve to one table (under
 *    `PERSIST_COLLISION_ENFORCE=false`; by default each fails the load).
 *  - Rollups: two models' same-named bases rolled up at one grain, whose
 *    hashed names never meet (`rollup_name_collision` does not fire); a
 *    `#(rollup)` whose partial column clashes with a field
 *    (`preaggregate-name-clash`); unmergeable measures, failing the load (v1
 *    and v2); a v1 `storage=` rollup over a
 *    `storage=` base, served by the rollup leg.
 *  - Warnings: `materialization-unreachable`, for a persisted source nothing
 *    published reads.
 *  - Switches: `PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION` on, served;
 *    `PUBLISHER_MATERIALIZE=false` with a persisted view, not built.
 *  - Governance: a `#(secure)` given read by a stored source's own `where:`,
 *    per caller; a `storage=` source joining a gated source, refused through
 *    a plain join and built through an annotated one, which the eligibility
 *    scan does not follow; a v1 rollup base with an array column, whose
 *    rollups are refused alone as over a fan-out join.
 *  - Known limitation 36: a gate inherited through a query-shaped
 *    derivation answers live, filtered by the gate, while the rest of the
 *    model serves.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import {
   installSettings,
   useMaterializationOn,
} from "../../../src/test_helpers/settings_env";
import {
   computeSourceEntityId,
   plannedSources,
} from "../../../src/service/materialization/plan/addressing";
import { formatPublishRejections } from "../../../src/service/publish_checks";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { rowDifference } from "./harness";

type Tier = "live" | "storage" | "database" | "rollup" | "view";

interface Probe {
   name: string;
   model?: string;
   malloy: string;
   givens?: Record<string, unknown>;
   tier: Tier;
}

interface Scenario {
   files: Record<string, string>;
   env?: Record<string, string>;
   includeRollups?: boolean;
}

interface Loaded {
   fixture: LoadedFixture;
   live: Map<string, Outcome>;
   rejections: string | undefined;
   restore(): void;
}

/** Load, answer every probe live, then build and bind. */
async function load(s: Scenario, probes: Probe[]): Promise<Loaded> {
   const restore = installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE: "true",
         ...s.env,
      },
   }).restore;
   await freshLoadPool();
   const loader = await loadFixture({
      ...DATA_FILES,
      "raw.malloy": RAW_MODEL,
      ...s.files,
   });
   const live = new Map<string, Outcome>();
   for (const p of probes) {
      live.set(
         p.name,
         await loader.run(p.model ?? "m.malloy", p.malloy, p.givens as never),
      );
   }
   const rejections = formatPublishRejections(loader.pkg as never);
   const fixture = await loader.bind({ includeRollups: s.includeRollups });
   return { fixture, live, rejections, restore };
}

/** Assert a probe's served answer equals live's and reports its tier. */
async function expectAsLive(l: Loaded, p: Probe): Promise<void> {
   const live = l.live.get(p.name)!;
   const served = await l.fixture.run(
      p.model ?? "m.malloy",
      p.malloy,
      p.givens as never,
   );
   if ("error" in live) throw new Error(`live failed: ${live.error}`);
   if ("error" in served) throw new Error(`served failed: ${served.error}`);
   expect(rowDifference(live.answer.rows, served.answer.rows)).toBeUndefined();
   expect(
      strictRowDifference(live.answer.rows, served.answer.rows),
   ).toBeUndefined();
   switch (p.tier) {
      case "live":
         expect(served.answer.servedBy).toBe("live");
         return;
      case "rollup":
      case "view":
         expect(served.answer.servedBy).toBe(p.tier);
         return;
      case "database":
         expect(served.answer.servedBy).toBe("materialized");
         expect(served.answer.servedFrom).toBe("database");
         return;
      case "storage":
         expect(served.answer.servedBy).toBe("materialized");
         expect(served.answer.servedFrom).toBe("storage");
   }
}

const codes = (l: Loaded, code: string) =>
   l.fixture.warnings.filter((w) => w.code === code);

/** One scenario: load once, a test per probe, and the scenario's own checks. */
function scenario(
   title: string,
   s: Scenario,
   probes: Probe[],
   checks: Record<string, (l: Loaded) => void | Promise<void>> = {},
) {
   describe(title, () => {
      let l: Loaded | undefined;
      beforeAll(async () => {
         l = await load(s, probes);
      }, 120_000);
      afterAll(async () => {
         l?.restore();
         await l?.fixture.dispose();
      });
      for (const p of probes) {
         it(`${p.name}: ${p.tier === "live" ? "live" : `served (${p.tier})`}, equal to live`, async () => {
            await expectAsLive(l!, p);
         });
      }
      for (const [name, check] of Object.entries(checks)) {
         it(name, async () => {
            await check(l!);
         });
      }
   });
}

afterAll(freshLoadPool);

const HEADER = "##! experimental { persistence }";

// ---------------------------------------------------------------------------
// Naming and placement
// ---------------------------------------------------------------------------

for (const tier of ["storage", "database"] as const) {
   const at = tier === "storage" ? ` storage="lake"` : "";
   scenario(
      `two #@ persist names that differ only in case, ${tier} on DuckDB (folds case)`,
      {
         // By default the pair fails the load (`materialization-table-collision`,
         // below); this holds the serve path to the operator's opt-out.
         env: { PERSIST_COLLISION_ENFORCE: "false" },
         files: {
            "m.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="T_east"${at}
source: east is orders_raw -> { where: region = 'east'; select: id, region, amount } extend {
  measure: total is amount.sum()
}

#@ persist name="t_east"${at}
source: west is orders_raw -> { where: region = 'west'; select: id, region, amount } extend {
  measure: total is amount.sum()
}

#@ persist name="apart"${at}
source: apart is orders_raw -> { select: id, region, amount } extend {
  measure: total is amount.sum()
}
`,
         },
      },
      [
         {
            name: "the first of the pair",
            malloy: "run: east -> { group_by: region; aggregate: total }",
            tier: "live",
         },
         {
            name: "the second of the pair",
            malloy: "run: west -> { group_by: region; aggregate: total }",
            tier: "live",
         },
         {
            name: "a source with its own name beside them",
            malloy: "run: apart -> { group_by: region; aggregate: total }",
            tier,
         },
      ],
      {
         "under the opt-out, the pair is refused alone, naming both, and the package publishes":
            (l) => {
               expect(l.rejections).toBeUndefined();
               const refused = codes(l, "persist-not-built").map(
                  (w) => w.message ?? "",
               );
               expect(
                  refused.some((m) => /east/.test(m) && /west/.test(m)),
               ).toBe(true);
               expect(refused.some((m) => /apart/.test(m))).toBe(false);
            },
         "both of the pair are refused, the source beside them serves": (l) => {
            expect([...l.fixture.verdicts].sort()).toEqual([
               "apart:serves",
               "east:live:refused",
               "west:live:refused",
            ]);
         },
      },
   );
}

scenario(
   "two persisted views resolving to one table",
   {
      // By default the two fail the load (below); this holds the serve path
      // to the operator's opt-out.
      env: { PERSIST_COLLISION_ENFORCE: "false" },
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: orders is orders_raw extend {
  measure: total is amount.sum()
  #@ persist name="by_key"
  view: by_region is { group_by: region; aggregate: total }
  #@ persist name="by_key"
  view: by_status is { group_by: status; aggregate: total }
  #@ persist name="by_both"
  view: by_both is { group_by: region, status; aggregate: total }
}
`,
      },
   },
   [
      {
         name: "the first view",
         malloy: "run: orders -> by_region",
         tier: "live",
      },
      {
         name: "the second view",
         malloy: "run: orders -> by_status",
         tier: "live",
      },
      {
         name: "a view with its own name",
         malloy: "run: orders -> by_both",
         tier: "view",
      },
   ],
   {
      "under the opt-out, the package publishes, and the views are refused alone as views":
         (l) => {
            expect(l.rejections).toBeUndefined();
            const refused = codes(l, "persist-view-not-built").filter((w) =>
               /same materialized table "by_key"/.test(w.message ?? ""),
            );
            expect(refused).toHaveLength(1);
         },
      "neither colliding view is served from a table, the third is": (l) => {
         const views = l.fixture.pkg
            .materialization()
            .serveVerdicts()
            .map(
               (v) =>
                  `${v.sourceName}:${v.verdict}${v.reason ? `:${v.reason}` : ""}`,
            )
            .sort();
         expect(views).toEqual([
            "orders__by_both__view:serves",
            "orders__by_region__view:live:refused",
            "orders__by_status__view:live:refused",
         ]);
      },
   },
);

describe("names an author put on one table, by default", () => {
   useMaterializationOn();
   beforeAll(async () => {
      await freshLoadPool();
   });
   afterAll(async () => {
      await freshLoadPool();
   });

   for (const [label, body, names] of [
      [
         "two #@ persist names that differ only in case",
         `#@ persist name="T_east" storage="lake"
source: east is orders_raw -> { where: region = 'east'; select: id, region, amount }
#@ persist name="t_east" storage="lake"
source: west is orders_raw -> { where: region = 'west'; select: id, region, amount }`,
         ["east", "west"],
      ],
      [
         "two persisted views at one name=",
         `source: orders is orders_raw extend {
  measure: total is amount.sum()
  #@ persist name="by_key"
  view: by_region is { group_by: region; aggregate: total }
  #@ persist name="by_key"
  view: by_status is { group_by: status; aggregate: total }
}`,
         ["by_key"],
      ],
   ] as const) {
      it(`${label}: the load fails, materialization-table-collision`, async () => {
         let error: unknown;
         try {
            const loader = await loadFixture({
               ...DATA_FILES,
               "raw.malloy": RAW_MODEL,
               "m.malloy": `${HEADER}\nimport "raw.malloy"\n\n${body}\n`,
            });
            await loader.dispose();
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(Error);
         const message = (error as Error).message;
         expect(message).toContain("[materialization-table-collision]");
         for (const name of names) expect(message).toContain(name);
      }, 120_000);
   }
});

// ---------------------------------------------------------------------------
// Rollups
// ---------------------------------------------------------------------------

const rollupBase = (region: string) => `${HEADER}
import "raw.malloy"

source: base is orders_raw extend {
  where: region = '${region}'
  #@ preaggregate grain="status"
  measure: total is amount.sum()
}
`;

scenario(
   "two models' same-named bases rolled up at one grain, different content",
   {
      files: {
         "m.malloy": rollupBase("east"),
         "other.malloy": rollupBase("west"),
      },
      includeRollups: true,
   },
   [
      {
         name: "the first model's base",
         malloy: "run: base -> { group_by: status; aggregate: total }",
         tier: "rollup",
      },
      {
         name: "the second model's base",
         model: "other.malloy",
         malloy: "run: base -> { group_by: status; aggregate: total }",
         tier: "rollup",
      },
   ],
   {
      // A rollup's name carries a hash of its content, so two rollups of
      // different content never share a table: rollup_name_collision has
      // nothing to refuse, and each model reads its own rollup.
      "each rollup gets a table of its own, and none is refused": (l) => {
         const names = l.fixture.built.map((b) => b.name);
         expect(names).toHaveLength(2);
         expect(new Set(names).size).toBe(2);
         for (const name of names) {
            expect(name).toMatch(/^base__rollup__status__[0-9a-f]{8}$/);
         }
         expect(codes(l, "persist-source-refused")).toEqual([]);
      },
   },
);

scenario(
   "a #(rollup) measure whose partial column clashes with a field",
   {
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: orders is orders_raw extend {
  dimension: total__partial is amount * 2
  #(rollup) grain=["status"]
  measure: total is amount.sum()
  #(rollup) grain=["status"]
  measure: n is count()
}
`,
      },
      includeRollups: true,
   },
   [
      {
         name: "the clashing measure",
         malloy: "run: orders -> { group_by: status; aggregate: total }",
         tier: "live",
      },
      {
         name: "the other measure on its rollup",
         malloy: "run: orders -> { group_by: status; aggregate: n }",
         tier: "rollup",
      },
   ],
   {
      "warns rollup-not-built (name_clash) on the clashing measure, and publish allows it":
         (l) => {
            const clash = codes(l, "rollup-not-built");
            expect(clash.map((w) => w.subject)).toEqual(["orders.total"]);
            expect(clash[0].message).toContain("total__partial");
            expect(
               Object.values(
                  l.fixture.pkg.getBuildPlan()?.refusedSources ?? {},
               ).map((r) => r.reason),
            ).toEqual(["name_clash"]);
            expect(l.rejections ?? "").not.toContain("rollup-not-built");
         },
   },
);

describe("unmergeable rollup measures, refused through publish or load", () => {
   const model = (body: string) => `${HEADER}
import "raw.malloy"

source: orders is orders_raw extend {
  ${body}
}
`;
   useMaterializationOn();
   beforeAll(async () => {
      await freshLoadPool();
   });
   afterAll(async () => {
      await freshLoadPool();
   });

   it("v2 #(rollup) on avg and count_distinct: the load fails, preaggregate-not-mergeable naming both", async () => {
      let error: unknown;
      try {
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": model(`#(rollup) grain=["status"]
  measure: avg_amount is amount.avg()
  #(rollup) grain=["status"]
  measure: customers is count(customer_id)`),
         });
         await loader.dispose();
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(Error);
      const lines = (error as Error).message
         .split("\n")
         .filter((l) => l.includes("[preaggregate-not-mergeable]"));
      expect(lines.some((l) => l.includes("avg_amount"))).toBe(true);
      expect(lines.some((l) => l.includes("customers"))).toBe(true);
   }, 120_000);

   it("v1 #@ preaggregate on avg: the load fails, naming the measure (non_additive_measure)", async () => {
      let error: unknown;
      try {
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": model(`#@ preaggregate grain="status"
  measure: avg_amount is amount.avg()`),
         });
         await loader.dispose();
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toContain("`avg_amount` uses `avg`");
      expect((error as Error).message).toContain("cannot be re-aggregated");
   }, 120_000);
});

for (const materialize of ["true", "false"]) {
   scenario(
      `a v1 storage= rollup over a storage= base, PUBLISHER_MATERIALIZE=${materialize}`,
      {
         env: { PUBLISHER_MATERIALIZE: materialize },
         files: {
            "m.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="stored_base" storage="lake"
source: stored_base is orders_raw -> { select: id, region, status, amount } extend {
  #@ preaggregate grain="region" storage="lake"
  measure: total is amount.sum()
}
`,
         },
         includeRollups: true,
      },
      [
         {
            name: "a query the grain covers",
            malloy:
               "run: stored_base -> { group_by: region; aggregate: total }",
            tier: "rollup",
         },
      ],
      {
         // Planned as the rollup leg's, as #(rollup) over a stored base
         // is: built in the destination beside its base and served from there.
         "the rollup is planned and served, not left out as rollup_over_stored_base":
            async (l) => {
               const verdicts = l.fixture.pkg
                  .materialization()
                  .serveVerdicts()
                  .map(
                     (v) =>
                        `${v.origin}:${v.verdict}${v.reason ? `:${v.reason}` : ""}`,
                  )
                  .sort();
               expect(verdicts).toEqual(["persist:serves", "rollup:serves"]);
               expect(l.fixture.built.map((b) => b.tier)).toEqual([
                  "storage",
                  "storage",
               ]);
               const out = await l.fixture.run(
                  "m.malloy",
                  "run: stored_base -> { group_by: region; aggregate: total }",
               );
               if ("error" in out) throw new Error(out.error);
               expect(out.answer.servedFrom).toBe("storage");
            },
      },
   );
}

// ---------------------------------------------------------------------------
// Warnings
// ---------------------------------------------------------------------------

scenario(
   "a persisted source nothing published reads",
   {
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="published"
source: published is orders_raw -> { select: id, region, amount } extend {
  measure: total is amount.sum()
}
`,
         "hidden.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="orphan"
source: orphan is orders_raw -> { select: id, region, amount }
`,
         "index.malloy": `import "m.malloy"
export { published }
`,
      },
   },
   [
      {
         name: "the published source",
         malloy: "run: published -> { group_by: region; aggregate: total }",
         tier: "database",
      },
   ],
   {
      "warns materialization-unreachable for the source nothing reads": (l) => {
         const w = codes(l, "materialization-unreachable");
         expect(w.map((x) => x.subject)).toEqual(["orphan"]);
         expect(w[0].message ?? "").toContain("orphan");
      },
   },
);

// ---------------------------------------------------------------------------
// Switches
// ---------------------------------------------------------------------------

const ADDRESSED = {
   "m.malloy": `${HEADER}
import "raw.malloy"

#@ persist storage="lake"
source: stored is orders_raw -> { select: id, region, status, amount } extend {
  measure: total is amount.sum()
}

#@ persist
source: placed is orders_raw -> { select: id, region, amount } extend {
  measure: total is amount.sum()
}
`,
};

const ADDRESSED_PROBES: Probe[] = [
   {
      name: "the storage= source",
      malloy: "run: stored -> { group_by: region; aggregate: total }",
      tier: "storage",
   },
   {
      name: "the database source",
      malloy: "run: placed -> { group_by: region; aggregate: total }",
      tier: "database",
   },
];

describe("PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION", () => {
   for (const on of ["false", "true"]) {
      describe(`set ${on}`, () => {
         let l: Loaded | undefined;
         beforeAll(async () => {
            l = await load(
               {
                  files: ADDRESSED,
                  env: { PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: on },
               },
               ADDRESSED_PROBES,
            );
         }, 120_000);
         afterAll(async () => {
            l?.restore();
            await l?.fixture.dispose();
         });
         for (const p of ADDRESSED_PROBES) {
            it(`${p.name}: served (${p.tier}), equal to live`, async () => {
               await expectAsLive(l!, p);
            });
         }
      });
   }

   // One compile, fixed connection digests: only the switch differs.
   it("moves the storage= source's address and no database one", async () => {
      const duckdb = new DuckDBConnection("duckdb");
      try {
         await duckdb.runSQL(
            "CREATE TABLE ev AS SELECT * FROM (VALUES (1, 'a', 5.0)) AS t(id, region, amount)",
         );
         const runtime = new Runtime({
            urlReader: new InMemoryURLReader(
               new Map([
                  [
                     "file:///m.malloy",
                     `${HEADER}
#@ persist storage="lake"
source: stored is duckdb.table('ev') -> { select: id, region, amount }
#@ persist
source: placed is duckdb.table('ev') -> { select: id, region, amount }
`,
                  ],
               ]),
            ),
            connections: new FixedConnectionMap(
               new Map([["duckdb", duckdb]]),
               "duckdb",
            ),
         });
         const model = await runtime
            .loadModel(new URL("file:///m.malloy"))
            .getModel();
         const sources = Object.fromEntries(
            plannedSources(await runtime.getBuildTargets(model)).map((s) => [
               s.name,
               s,
            ]),
         );
         const digests = { duckdb: "fixed-digest" };
         const addresses = (on: string) => {
            const restore = installSettings({
               env: {
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: on,
               },
            }).restore;
            try {
               return {
                  stored: computeSourceEntityId(sources.stored, digests),
                  placed: computeSourceEntityId(sources.placed, digests),
               };
            } finally {
               restore();
            }
         };
         const off = addresses("false");
         const onAddr = addresses("true");
         expect(onAddr.stored).not.toBe(off.stored);
         expect(onAddr.placed).toBe(off.placed);
         // Stable within a position, so the difference is the switch.
         expect(addresses("false")).toEqual(off);
      } finally {
         await duckdb.close();
      }
   });
});

scenario(
   "PUBLISHER_MATERIALIZE=false with a persisted view",
   {
      env: { PUBLISHER_MATERIALIZE: "false" },
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: orders is orders_raw extend {
  measure: total is amount.sum()
  #@ persist name="by_region"
  view: by_region is { group_by: region; aggregate: total }
}
`,
      },
   },
   [{ name: "the view", malloy: "run: orders -> by_region", tier: "live" }],
   {
      "builds nothing for the view": (l) => {
         expect(l.fixture.built).toEqual([]);
      },
   },
);

// ---------------------------------------------------------------------------
// Known limitation 36
// ---------------------------------------------------------------------------

scenario(
   "a gate inherited through a query-shaped derivation (known limitation 36)",
   {
      files: {
         "m.malloy": `##! experimental { persistence givens }
import "raw.malloy"

given: REGIONS :: string[]

#(access_filter) region in $REGIONS
source: locked is orders_raw extend {
  measure: n is count()
}

#@ persist name="derived"
source: derived is locked extend {} -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

#@ persist name="open_t"
source: open_t is orders_raw -> { select: id, region, amount } extend {
  measure: total is amount.sum()
}
`,
      },
   },
   [
      {
         name: "the derivation, caller 1",
         malloy: "run: derived -> { group_by: region; aggregate: total }",
         givens: { REGIONS: ["north", "east"] },
         tier: "live",
      },
      {
         name: "the derivation, caller 2",
         malloy: "run: derived -> { group_by: region; aggregate: total }",
         givens: { REGIONS: ["west"] },
         tier: "live",
      },
      {
         name: "an ungated source beside it",
         malloy: "run: open_t -> { group_by: region; aggregate: total }",
         tier: "database",
      },
   ],
);

// ---------------------------------------------------------------------------
// Governance
// ---------------------------------------------------------------------------

scenario(
   "a #(secure) given read by the stored source's own where:, storage= and #(materialize)",
   {
      files: {
         "m.malloy": `##! experimental { persistence givens }
import "raw.malloy"

#(secure)
given: TENANTS :: string[]

#@ persist name="scoped_v1" storage="lake"
source: scoped_v1 is orders_raw -> {
  select: id, region, status, amount
} extend {
  where: region in $TENANTS
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: scoped_v2 is orders_raw extend {
  where: region in $TENANTS
  measure: total is amount.sum()
}
`,
      },
   },
   [
      ...(["scoped_v1", "scoped_v2"] as const).flatMap((source) => [
         {
            name: `${source}, caller 1`,
            malloy: `run: ${source} -> { group_by: region; aggregate: total }`,
            givens: { TENANTS: ["north", "east"] },
            tier: "storage" as const,
         },
         {
            name: `${source}, caller 2`,
            malloy: `run: ${source} -> { group_by: status; aggregate: total }`,
            givens: { TENANTS: ["west"] },
            tier: "storage" as const,
         },
      ]),
   ],
);

// The eligibility scan reads a gate on a plainly joined source but not
// through an annotated join, where Malloy replaces the target's
// annotations. Live does not apply an author's joined source's gate either
// (docs/authorize.md, "a join the author writes is not gated"), so the
// stored answer is live's.
scenario(
   "a storage= source joining a gated source, plainly and through an annotated join",
   {
      files: {
         "m.malloy": `##! experimental { persistence givens }
import "raw.malloy"

given: ROLE :: string

#(authorize) $ROLE = 'admin'
source: locked_customers is customers_raw extend {}

#@ persist name="plain_join" storage="lake"
source: plain_join is orders_raw extend {
  join_one: c is locked_customers on c.id = customer_id
} -> { group_by: segment is c.segment; aggregate: total is amount.sum() }

#@ persist name="tagged_join" storage="lake"
source: tagged_join is orders_raw extend {
  # label="Customer"
  join_one: c is locked_customers on c.id = customer_id
} -> { group_by: segment is c.segment; aggregate: total is amount.sum() }
`,
      },
   },
   [
      {
         name: "through the plain join",
         malloy:
            "run: plain_join -> { group_by: segment; aggregate: t is total.sum() }",
         givens: { ROLE: "viewer" },
         tier: "live",
      },
      {
         name: "through the annotated join",
         malloy:
            "run: tagged_join -> { group_by: segment; aggregate: t is total.sum() }",
         givens: { ROLE: "viewer" },
         tier: "storage",
      },
   ],
   {
      "refuses the plain join (authorize) and builds the annotated one": (
         l,
      ) => {
         expect([...l.fixture.verdicts].sort()).toEqual([
            "plain_join:live:refused",
            "tagged_join:serves",
         ]);
         const refused = codes(l, "persist-source-refused").filter(
            (w) => w.subject === "plain_join",
         );
         expect(refused).toHaveLength(1);
         expect(refused[0].message).toContain("#(authorize)");
      },
   },
);

describe("a v1 #@ preaggregate base with an array column", () => {
   it("refuses its rollups alone, naming the array as a fan-out join, and the package loads", async () => {
      const restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      try {
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            "m.malloy": `${HEADER}

source: listed is duckdb.sql("""
  SELECT * FROM (VALUES ('a', 1.5, [1, 2]), ('b', 4.0, [])) AS t(g, amount, lst)
""") extend {
  #@ preaggregate grain="g"
  measure: total is amount.sum()
}
`,
         });
         try {
            const refused = (
               loader.pkg.getPackageMetadata().warnings ?? []
            ).filter(
               (w) => (w as { code?: string }).code === "rollup-not-built",
            );
            expect(refused).toHaveLength(1);
            expect(refused[0]?.message).toContain(
               "joins `lst` with a fan-out join",
            );
            expect(
               Object.values(loader.pkg.getBuildPlan()?.sources ?? {}),
            ).toEqual([]);
         } finally {
            await loader.dispose();
         }
      } finally {
         restore();
      }
   }, 120_000);
});
