// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// E2.1 SPIKE — does manifest substitution fire for a persist source that is a
// compose() member?
//
// The whole pre-aggregation v1 design rests on one unverified compiler behavior:
// Publisher synthesizes an ordinary `#@ persist` rollup source, wraps the base in
// `compose(rollup, base)`, and expects the STOCK composite resolver to route each
// query while the STOCK colocated manifest substitution binds the winning member
// to its built table. Nothing had run that. If it does not hold, the mechanism is
// dead and the fallback is compiler work — a different project.
//
// So this file asserts on generated SQL, not on values. A correct number proves
// nothing here: the live path returns the same number by design, which is the
// entire point of the feature and also the reason a value assertion cannot tell
// routing from non-routing. Values are used only where the question is
// specifically "is the re-aggregated answer equal to the live one" (the count
// trap).
//
// Follows the real-compile pattern of incremental_compiler_contract.spec.ts:
// in-memory DuckDB + Runtime + getBuildPlan(), with a real CTAS so the table the
// manifest points at actually exists, and the serve-time manifest assembled by
// the production `filterFreshManifest` rather than by hand.
//
// WHAT THIS DOES NOT PROVE: the build leg is a CTAS of the source's own
// `getSQL()`, not a run through `materialization_service.buildOneSource` /
// `commitManifest`. Those need a package, storage and a control plane, and they
// are already covered for ordinary persist sources — a synthesized rollup is an
// ordinary persist source, and the seam they share with this file is
// `computeSourceEntityId`, which test 6 pins. If E2.2 changes how a rollup is
// built rather than merely which text is synthesized, that assumption is void.
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type {
   BuildManifest,
   FixedConnectionMap,
   PersistSource,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import type { FreshnessManifest } from "../storage/DatabaseInterface";
import { computeSourceEntityId } from "./build_plan";
import { filterFreshManifest } from "./freshness";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;
let digest: string;

beforeAll(async () => {
   ({ duckdb, connections } = duckdbTestConnections());
   digest = await duckdb.getDigest();
});

/** Four rows over two days and two categories; 260 total, 4 orders. */
const BASE_ROWS = `SELECT * FROM (VALUES
    (10, DATE '2024-01-01', 'A'),
    (20, DATE '2024-01-01', 'B'),
    (30, DATE '2024-01-02', 'A'),
    (200, DATE '2024-01-02', 'B')
  ) AS t(amount, order_date, category)`;

const ROLLUP_NAME = "orders__rollup__day_category";
const PHYSICAL_TABLE = "mz_orders_rollup_day_category";

/**
 * The model a synthesizer would emit, written by hand: a base carrying the
 * measures, one `#@ persist` rollup at (order_date, category) storing PARTIALS
 * and re-declaring each measure as the merge over its partial, and a compose()
 * with the base last.
 *
 * `members` is a parameter because freshness is expressed as membership: a stale
 * rollup leaves the serving set by dropping out of the member list, so the stale
 * case is literally `compose(orders__base)`.
 */
function modelText(members: string): string {
   return `##! experimental { persistence composite_sources }
source: orders__base is duckdb.sql("""${BASE_ROWS}""") extend {
  measure: total_revenue is amount.sum()
  measure: order_count is count()
}

#@ persist
source: ${ROLLUP_NAME} is orders__base -> {
  group_by: order_date, category
  aggregate:
    total_revenue__partial is amount.sum()
    order_count__partial is count()
} extend {
  measure: total_revenue is total_revenue__partial.sum()
  measure: order_count is order_count__partial.sum()
}

source: orders is compose(${members})
`;
}

const WITH_ROLLUP = modelText(`${ROLLUP_NAME}, orders__base`);
const BASE_ONLY = modelText("orders__base");

/** The rollup's PersistSource off a real getBuildPlan(). */
async function rollupSource(text: string): Promise<PersistSource> {
   const compiled = await loadTestModel(connections, text).getModel();
   const sources = Object.values(compiled.getBuildPlan().sources);
   const rollup = sources.find((s) => s.name === ROLLUP_NAME);
   if (!rollup) {
      throw new Error(
         `getBuildPlan() did not include ${ROLLUP_NAME}; saw [${sources
            .map((s) => s.name)
            .join(", ")}]`,
      );
   }
   return rollup;
}

/**
 * Build the rollup for real and return the manifest a serve-time query would be
 * handed. The wire entry goes through the production `filterFreshManifest`, so
 * the map that reaches the compiler is assembled by the same code the serve path
 * uses — including the freshness gate, which `freshness` exercises.
 *
 * `freshness` mirrors a control-plane-stamped entry: a window plus a `dataAsOf`
 * old enough to be outside it.
 */
async function buildRollup(
   text: string,
   freshness?: Pick<
      FreshnessManifest[string],
      "dataAsOf" | "freshnessWindowSeconds" | "freshnessFallback"
   >,
): Promise<{ manifest: BuildManifest; sourceEntityId: string }> {
   const rollup = await rollupSource(text);
   const sourceEntityId = computeSourceEntityId(rollup, { duckdb: digest });
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE ${PHYSICAL_TABLE} AS ${rollup.getSQL()}`,
   );
   const wire: FreshnessManifest = {
      [sourceEntityId]: {
         tableName: PHYSICAL_TABLE,
         connectionName: "duckdb",
         ...freshness,
      },
   };
   const { manifest } = filterFreshManifest(wire, new Date());
   return { manifest: { entries: manifest, strict: false }, sourceEntityId };
}

/** SQL for a query against `orders`, with the manifest threaded as production does. */
async function sqlFor(
   text: string,
   query: string,
   manifest?: BuildManifest,
): Promise<string> {
   return loadTestModel(connections, text)
      .loadQuery(query)
      .getSQL(manifest ? { buildManifest: manifest } : undefined);
}

/** Run a query and return its single row. */
async function rowFor(
   text: string,
   query: string,
   manifest?: BuildManifest,
): Promise<Record<string, number>> {
   const result = await loadTestModel(connections, text)
      .loadQuery(query)
      .run(manifest ? { buildManifest: manifest } : undefined);
   return result.data.toObject()[0] as unknown as Record<string, number>;
}

// A query whose group-by is covered by the rollup's grain: category only.
const COVERED =
   "run: orders -> { group_by: category; aggregate: total_revenue }";
const COVERED_COUNT =
   "run: orders -> { group_by: category; aggregate: order_count }";
// `amount` is a base column the rollup dropped, so the member must be rejected.
const UNCOVERED =
   "run: orders -> { group_by: category; aggregate: total_revenue; where: amount > 100 }";

describe("E2.1 spike: a persist source as a compose() member", () => {
   it("1. getBuildPlan() sees the rollup, and it builds", async () => {
      const rollup = await rollupSource(WITH_ROLLUP);
      // Reachable from modelDef.contents even though nothing outside compose()
      // references it — the fact that lets a synthesized rollup be built at all.
      expect(rollup.name).toBe(ROLLUP_NAME);
      expect(rollup.connectionName).toBe("duckdb");
      const { sourceEntityId } = await buildRollup(WITH_ROLLUP);
      expect(sourceEntityId).toBeTruthy();
      const built = await duckdb.runSQL(
         `SELECT count(*) AS n FROM ${PHYSICAL_TABLE}`,
      );
      // Four base rows collapse to four (day, category) groups here, but the
      // point is only that the CTAS produced a readable table.
      expect(Number((built.rows[0] as { n: number }).n)).toBeGreaterThan(0);
   });

   it("2. THE LOAD-BEARING ONE: substitution fires — routed SQL reads the built table", async () => {
      const { manifest } = await buildRollup(WITH_ROLLUP);
      const sql = await sqlFor(WITH_ROLLUP, COVERED, manifest);
      // The physical table, not a recomputed subquery over the base.
      expect(sql).toContain(PHYSICAL_TABLE);
      // And with no manifest the same query must NOT reference it, or the
      // assertion above proves nothing about the manifest.
      const live = await sqlFor(WITH_ROLLUP, COVERED);
      expect(live).not.toContain(PHYSICAL_TABLE);
   });

   it("3. the count trap is closed: sums the stored partial, and agrees with live", async () => {
      const { manifest } = await buildRollup(WITH_ROLLUP);
      const sql = await sqlFor(WITH_ROLLUP, COVERED_COUNT, manifest);
      expect(sql).toContain(PHYSICAL_TABLE);
      // Emitting the measure's own count() over rollup rows would return the
      // number of ROLLUP ROWS. It compiles, it runs, and it is wrong — so the
      // routed value is diffed against the base's answer explicitly.
      const routed = await rowFor(WITH_ROLLUP, COVERED_COUNT, manifest);
      const live = await rowFor(BASE_ONLY, COVERED_COUNT);
      expect(Number(routed.order_count)).toBe(Number(live.order_count));
   });

   it("4. fall-through: a filter on a dropped column rejects the member and serves live", async () => {
      const { manifest } = await buildRollup(WITH_ROLLUP);
      const sql = await sqlFor(WITH_ROLLUP, UNCOVERED, manifest);
      expect(sql).not.toContain(PHYSICAL_TABLE);
      const routed = await rowFor(WITH_ROLLUP, UNCOVERED, manifest);
      const live = await rowFor(BASE_ONLY, UNCOVERED);
      expect(Number(routed.total_revenue)).toBe(Number(live.total_revenue));
   });

   it("5. freshness: a stale rollup serves from the base, by window OR by absence", async () => {
      // Two independent ways a rollup leaves the serving set, both of which the
      // freshness design relies on, and neither of which may error.
      //
      // (a) The freshness gate drops the entry while the member is still in the
      //     compose(). This is the one that matters most, because the member
      //     list still advertises a rollup the manifest no longer backs — and
      //     strict:false is what makes that fall through instead of failing.
      const stale = await buildRollup(WITH_ROLLUP, {
         dataAsOf: new Date(Date.now() - 3600_000).toISOString(),
         freshnessWindowSeconds: 60,
         freshnessFallback: "live",
      });
      expect(Object.keys(stale.manifest.entries)).toHaveLength(0);
      expect(await sqlFor(WITH_ROLLUP, COVERED, stale.manifest)).not.toContain(
         PHYSICAL_TABLE,
      );
      const gated = await rowFor(WITH_ROLLUP, COVERED, stale.manifest);
      expect(Number(gated.total_revenue)).toBe(
         Number((await rowFor(BASE_ONLY, COVERED)).total_revenue),
      );

      // (b) The member is absent from compose() entirely (a resynthesis that
      //     left the stale rollup out), with a live manifest entry present.
      const { manifest } = await buildRollup(WITH_ROLLUP);
      expect(await sqlFor(BASE_ONLY, COVERED, manifest)).not.toContain(
         PHYSICAL_TABLE,
      );
      const absent = await rowFor(BASE_ONLY, COVERED, manifest);
      expect(Number(absent.total_revenue)).toBe(
         Number((await rowFor(BASE_ONLY, COVERED)).total_revenue),
      );
   });

   it("6. id stability: the build's id is the id the serve-time lookup wants", async () => {
      // If these ever disagree the feature silently does nothing while still
      // paying build cost, so it is asserted rather than inferred from test 2.
      const { sourceEntityId } = await buildRollup(WITH_ROLLUP);
      const recomputed = computeSourceEntityId(
         await rollupSource(WITH_ROLLUP),
         {
            duckdb: digest,
         },
      );
      expect(recomputed).toBe(sourceEntityId);
      // A wrong key must NOT bind, which is what makes the match above mean
      // something.
      const wrong: BuildManifest = {
         entries: { [`${sourceEntityId}x`]: { tableName: PHYSICAL_TABLE } },
      };
      expect(await sqlFor(WITH_ROLLUP, COVERED, wrong)).not.toContain(
         PHYSICAL_TABLE,
      );
   });
});
