// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for known limitation 24, join_many: `inline=` naming an `internal` or
 * `private` join_many stores its pair table, and the served copy declares
 * the join's record under the join's own modifier (read off the author's
 * compiled source), with every stored column the record reads renamed
 * `private` in the record's block. A caller reads `<join>.<field>` exactly
 * where live lets it (an internal join through its own `extend` or an
 * `include` lifting it, a private one never) and the stored columns never;
 * the author's own fields, measures and views through the hidden join answer
 * from storage with live's rows, childless parents and the parent-only rule
 * included. The same holds for a path of join_manys and for a table of
 * several blocks (#1681), and under row-level gates with gated serving on.
 *
 * Every refusal is checked three ways: live refuses, the routed query
 * refuses with no hidden value in its error, and the served copy's own
 * compile refuses (no stored column is left selectable for a fallback to
 * hide).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { MaterializePlan } from "../../../src/service/materialization/plan/build_file_generator";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   expectPairServedVia,
} from "./harness";

/** Measures that count through the inlined path (see {@link expectPairServedVia}). */
const PATH_COUNTS = [
   "seg_n",
   "long_legs",
   "opt_n",
   "crew_n",
   "by_carrier",
   "by_leg",
];

// Flights 1–6: 4 and 5 have no segment; flight 3 has four. Segment 99 pairs
// with no flight. Each segment's tag is a value no refusal may carry.
const seedSql = [
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0),
      (4, 'DL', 50.0), (5, 'UA', 75.0), (6, 'DL', 900.0)
    ) AS t(id, carrier, distance)`,
   `CREATE TABLE segments AS SELECT * FROM (VALUES
      (1, 1, 120.0, 'tagalpha', 'east'), (2, 1, 330.0, 'tagbravo', 'west'),
      (3, 2, 80.0, 'tagcharlie', 'east'), (4, 3, 410.0, 'tagdelta', 'east'),
      (5, 3, 15.0, 'tagecho', 'west'), (6, 3, 260.0, 'tagfoxtrot', 'west'),
      (7, 3, 99.0, 'taggolf', 'east'), (8, 6, 700.0, 'taghotel', 'west'),
      (99, 99, 10.0, 'tagindia', 'east')
    ) AS t(seg_id, flight_id, miles, tag, region)`,
   `CREATE TABLE opts AS SELECT * FROM (VALUES
      (101, 1, 'optkilo', 0.5), (102, 1, 'optlima', 0.25),
      (103, 4, 'optmike', 2.0), (104, 8, 'optnovember', 1.5)
    ) AS t(opt_id, seg_id, kind, extra)`,
   `CREATE TABLE crew AS SELECT * FROM (VALUES
      (201, 1, 'crewoscar'), (202, 1, 'crewpapa'), (203, 3, 'crewquebec'),
      (204, 6, 'crewromeo')
    ) AS t(crew_id, flight_id, crew_name)`,
];

/** The joined values a refusal must never carry. */
const HIDDEN =
   /tag(alpha|bravo|charlie|delta|echo|foxtrot|golf|hotel|india)|opt(kilo|lima|mike|november)|crew(oscar|papa|quebec|romeo)/;

const children = `source: opts is duckdb.table('opts') extend {
  primary_key: opt_id
  measure: extra_total is extra.sum()
}

source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  dimension: long_leg is miles > 200
  measure: seg_miles is miles.sum()
}

source: legs is segments extend {
  join_many: opts on opts.seg_id = seg_id
}

source: crew is duckdb.table('crew') extend {
  primary_key: crew_id
}
`;

const pairMeasures = `  measure:
    flight_count is count()
    total_distance is distance.sum()
    distance_per_flight is total_distance / flight_count
    seg_total is segments.miles.sum()
    seg_n is segments.count()
    tag_n is count(segments.tag)
    long_legs is segments.count() { where: segments.long_leg }`;

const model = `##! experimental { access_modifiers }
${children}
#(materialize) storage="lake" inline=["segments"]
source: hidden_many is duckdb.table('flights') extend {
  primary_key: id
  internal join_many: segments on segments.flight_id = id
  dimension: leg_tag is segments.tag
${pairMeasures}
  view: by_carrier is {
    group_by: carrier
    aggregate: flight_count, total_distance, seg_total, seg_n, tag_n
  }
  view: by_leg is {
    group_by: segments.long_leg
    aggregate: seg_total, seg_n
  }
}

#(materialize) storage="lake" inline=["segments"]
source: private_many is duckdb.table('flights') extend {
  primary_key: id
  private join_many: segments on segments.flight_id = id
  rename: dist is distance
  dimension: leg_tag is segments.tag
  measure:
    flight_count is count()
    total_distance is dist.sum()
    seg_total is segments.miles.sum()
    seg_n is segments.count()
    tag_n is count(segments.tag)
  view: by_carrier is {
    group_by: carrier
    aggregate: flight_count, total_distance, seg_total, seg_n, tag_n
  }
}

#(materialize) storage="lake" inline=["segments.opts"]
source: hidden_hop is duckdb.table('flights') extend {
  primary_key: id
  internal join_many: segments is legs on segments.flight_id = id
${pairMeasures}
    opt_n is segments.opts.count()
    extra_sum is segments.opts.extra.sum()
}

#(materialize) storage="lake" inline=["segments", "crew"]
source: hidden_blocks is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  private join_many: crew on crew.flight_id = id
${pairMeasures}
    crew_n is crew.count()
    crew_names is count(crew.crew_name)
}

#(materialize) storage="lake"
source: ext_many is hidden_many extend {
  measure: double_seg is seg_total * 2
}

#(materialize) storage="lake"
source: plain is duckdb.table('flights') extend {
  measure: flight_count is count()
}
`;

/** The error a run throws, or undefined when it answers. */
async function failure(run: () => Promise<unknown>): Promise<unknown> {
   try {
      await run();
      return undefined;
   } catch (error) {
      return error;
   }
}

/** Live refuses, the routed query refuses leaking nothing, and so does the copy. */
async function expectRefused(
   fixture: R4Fixture,
   malloy: string,
): Promise<void> {
   fixture.setBindings([]);
   const live = await failure(() => fixture.runAsIs(malloy));
   fixture.setBindings(fixture.bindings);
   const served = await failure(() => fixture.runAsIs(malloy));
   expect(live).toBeDefined();
   expect(served).toBeDefined();
   expect(String(served)).not.toMatch(HIDDEN);
   const copy = await failure(async () => {
      const { runnable, virtualMap } = await fixture
         .servedCopy()
         .loadQuery(malloy, {});
      await runnable.getSQL({ virtualMap } as never);
   });
   expect(copy).toBeDefined();
   expect(String(copy)).not.toMatch(HIDDEN);
   // And the copy's bare compile, past every check the router runs first:
   // the refusal is the compiler's, not only a decline a later change to
   // the checks could drop.
   const bare = await failure(() => compileOnCopy(fixture, malloy));
   expect(bare).toBeDefined();
   expect(String(bare)).not.toMatch(HIDDEN);
}

/** `malloy` compiled on the served copy as a caller's query, and nothing more. */
async function compileOnCopy(fixture: R4Fixture, malloy: string) {
   const compiled = await (
      fixture.servedCopy() as unknown as {
         ensureCompiled(): Promise<{
            materializer?: {
               loadRestrictedQuery(q: string): {
                  getPreparedQuery(): Promise<unknown>;
               };
            };
         }>;
      }
   ).ensureCompiled();
   if (!compiled.materializer) throw new Error("no served copy");
   return compiled.materializer.loadRestrictedQuery(malloy).getPreparedQuery();
}

/** Live answers, and the served copy answers from storage with its rows. */
async function expectServed(fixture: R4Fixture, malloy: string) {
   const run = await fixture.run({ name: malloy, malloy });
   expect(run.live.length).toBeGreaterThan(0);
   expectPairServedVia(run, PATH_COUNTS);
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   return run;
}

type Access = Record<string, Record<string, string>>;

async function servedAccess(fixture: R4Fixture): Promise<Access> {
   const def = (await fixture.servedCopy().compiledModelDef()) as {
      contents: Record<
         string,
         { fields?: { name: string; as?: string; accessModifier?: string }[] }
      >;
   };
   const out: Access = {};
   for (const [name, source] of Object.entries(def.contents)) {
      out[name] = Object.fromEntries(
         (source.fields ?? []).map((f) => [
            f.as ?? f.name,
            f.accessModifier ?? "public",
         ]),
      );
   }
   return out;
}

describe("R4: an inlined hidden join_many (known limitation 24)", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds each hidden join_many's pair table, its joined columns carrying the join's modifier", async () => {
      const plans = (
         fixture.servedCopy() as unknown as {
            materializePlans: Record<string, MaterializePlan>;
         }
      ).materializePlans;
      const byName = new Map(
         Object.values(plans).map((p) => [p.sourceName, p]),
      );
      expect([...byName.keys()].sort()).toEqual([
         "ext_many",
         "hidden_blocks",
         "hidden_hop",
         "hidden_many",
         "plain",
         "private_many",
      ]);
      const access = (name: string, column: string) =>
         byName.get(name)?.columns.find((c) => c.column === column)?.access;
      expect(access("hidden_many", "segments__tag")).toBe("internal");
      expect(access("private_many", "segments__tag")).toBe("private");
      expect(access("hidden_hop", "segments__opts__kind")).toBe("internal");
      expect(access("hidden_blocks", "crew__crew_name")).toBe("private");
      expect(access("hidden_blocks", "segments__tag")).toBeUndefined();
      const rows = await fixture.storedSQL(
         (t) =>
            `SELECT count(*) AS n, count(*) FILTER (WHERE __parent_row) AS parents FROM ${t("hidden_many")}`,
      );
      // 8 paired segments, one row each for the 2 childless flights.
      expect(rows.map((r) => [r.n, r.parents].map(Number))).toEqual([[10, 6]]);
      // An extension of a pair source builds over the raw chain and answers
      // live, as for a public join_many: its own copy would read the join.
      expect(await fixture.servedCopyProblems()).toEqual([
         {
            sourceName: "ext_many",
            reason: "cross_connection_join",
            join: "segments",
         },
      ]);
   });

   it("declares each record under its join's modifier, and its stored columns private", async () => {
      const access = await servedAccess(fixture);
      expect(access.hidden_many.segments).toBe("internal");
      expect(access.private_many.segments).toBe("private");
      expect(access.hidden_hop.segments).toBe("internal");
      expect(access.hidden_blocks.crew).toBe("private");
      expect(access.hidden_blocks.segments).toBe("public");
      for (const [source, column] of [
         ["hidden_many", "segments__tag__pair"],
         ["private_many", "segments__tag__pair"],
         ["hidden_hop", "segments__opts__kind__pair"],
         ["hidden_hop", "segments__tag__pair"],
         ["hidden_blocks", "crew__crew_name__pair"],
      ]) {
         expect(`${source}.${column}:${access[source][column]}`).toBe(
            `${source}.${column}:private`,
         );
      }
      // A public block's columns stay as a public pair table's are.
      expect(access.hidden_blocks.segments__tag__pair).toBe("internal");
      for (const source of ["hidden_many", "private_many"]) {
         expect(access[source].leg_tag).toBe("public");
         expect(access[source].seg_total).toBe("public");
      }
   });

   const served = [
      [
         "the author's measures through an internal join_many",
         "run: hidden_many -> { group_by: carrier; aggregate: flight_count, total_distance, distance_per_flight, seg_total, seg_n, tag_n, long_legs }",
      ],
      ["the author's view over it", "run: hidden_many -> by_carrier"],
      [
         "the author's view grouped by a child field",
         "run: hidden_many -> by_leg",
      ],
      [
         "the author's dimension through it",
         "run: hidden_many -> { group_by: leg_tag; aggregate: seg_n, seg_total }",
      ],
      [
         "childless parents, counted as live counts them",
         "run: hidden_many -> { group_by: id; aggregate: seg_n, tag_n, seg_total }",
      ],
      [
         "the author's measures through a private join_many, beside an author rename",
         "run: private_many -> { group_by: carrier; aggregate: flight_count, total_distance, seg_total, seg_n, tag_n }",
      ],
      [
         "the author's view over a private join_many",
         "run: private_many -> by_carrier",
      ],
      [
         "the author's dimension through a private join_many",
         "run: private_many -> { group_by: leg_tag; aggregate: seg_n }",
      ],
      [
         "the renamed field beside a private join_many",
         "run: private_many -> { group_by: dist; aggregate: seg_n }",
      ],
      [
         "an internal path's measures at every level",
         "run: hidden_hop -> { group_by: carrier; aggregate: opt_n, extra_sum }",
      ],
      [
         "an internal path's parent measures beside the leaf's",
         "run: hidden_hop -> { group_by: carrier; aggregate: flight_count, total_distance, opt_n }",
      ],
      [
         "a public block beside a private one",
         "run: hidden_blocks -> { group_by: carrier; aggregate: flight_count, seg_total, seg_n }",
      ],
      [
         "the author's measures through a private block",
         "run: hidden_blocks -> { group_by: carrier; aggregate: flight_count, total_distance, crew_n, crew_names }",
      ],
      ["a source beside them", "run: plain -> { aggregate: flight_count }"],
   ] as const;
   for (const [name, malloy] of served) {
      it(`answers ${name} from storage with live's rows`, async () => {
         await expectServed(fixture, malloy);
      });
   }

   // Live answers each (Malloy lets a caller's own `extend` or `include`
   // read an `internal` join, and a join's `count()` names no field of it).
   // The served copy declines a source a caller builds on a pair table, as
   // for a public join_many, so those answer live; either way the rows are
   // live's, never more.
   for (const [name, malloy] of [
      [
         "a caller's own extend over an internal join_many",
         "run: hidden_many extend { dimension: t is segments.tag } -> { group_by: t; aggregate: seg_n }",
      ],
      [
         "a caller's include lifting an internal join_many",
         "run: hidden_many include { public: segments, seg_n } -> { group_by: segments.long_leg; aggregate: seg_n }",
      ],
      [
         "a caller's count through an internal join_many",
         "run: hidden_many -> { group_by: carrier; aggregate: n is segments.count() }",
      ],
      [
         "an extension's measure over the hidden join_many",
         "run: ext_many -> { group_by: carrier; aggregate: double_seg, seg_n, flight_count }",
      ],
      [
         "a caller's count through a private join_many",
         "run: private_many -> { group_by: carrier; aggregate: n is segments.count() }",
      ],
   ] as const) {
      it(`answers ${name} with exactly live's rows, as live allows`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("answers live's count of a childless parent (none) through a hidden join_many, live", async () => {
      // Since malloydata/malloy#3208 live counts flight 4's segments as
      // none; the pair table's row for it would count one, so it is answered
      // live.
      const run = await expectServed(
         fixture,
         "run: hidden_many -> { where: id = 3 | 4; group_by: id; aggregate: seg_n }",
      );
      expect(run.servedVia).toBeUndefined();
      expect(
         run.live
            .map((r) => [Number(r.id), Number(r.seg_n)])
            .sort((a, b) => a[0] - b[0]),
      ).toEqual([
         [3, 4],
         [4, 0],
      ]);
   });

   // A query that reads nothing through the hidden join passes over the pair
   // table, as for a public one.
   for (const malloy of [
      "run: hidden_many -> { group_by: carrier; aggregate: flight_count, total_distance }",
      "run: private_many -> { group_by: carrier; aggregate: flight_count, total_distance }",
   ]) {
      it(`passes over the pair table, reading no child: ${malloy}`, async () => {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         const skipped = path?.considered?.find((c) => c.outcome === "skipped");
         expect(skipped?.reason).toBe("COUNT_SEMANTICS");
      });
   }

   const refused = [
      [
         "an internal join_many's field",
         "run: hidden_many -> { group_by: segments.tag }",
      ],
      [
         "an internal join_many's record",
         "run: hidden_many -> { select: segments }",
      ],
      [
         "an internal join_many's field in a nest",
         "run: hidden_many -> { group_by: carrier; nest: x is { group_by: segments.tag } }",
      ],
      [
         "an internal join_many's field indexed",
         "run: hidden_many -> { index: segments.tag }",
      ],
      [
         "a caller's own select of an internal join_many's field",
         "run: hidden_many -> { select: id, segments.tag }",
      ],
      [
         "an internal join_many's stored column",
         "run: hidden_many -> { group_by: segments__tag }",
      ],
      [
         "an internal join_many's stored column through a caller extend",
         "run: hidden_many extend { dimension: x is segments__tag } -> { group_by: x }",
      ],
      [
         "an internal join_many's internal name through a caller extend",
         "run: hidden_many extend { dimension: x is segments__tag__pair } -> { group_by: x }",
      ],
      [
         "an internal join_many's internal name through a caller rename",
         "run: hidden_many extend { rename: x is segments__tag__pair } -> { group_by: x }",
      ],
      [
         "an internal join_many's internal name made public by a caller include",
         "run: hidden_many include { public: segments__tag__pair } -> { group_by: segments__tag__pair }",
      ],
      [
         "an internal join_many's internal names after a caller's include wildcard",
         "run: hidden_many include { public: * } -> { group_by: segments__tag__pair }",
      ],
      [
         "the parent-row flag by its stored name",
         "run: hidden_many extend { dimension: x is __parent_row } -> { group_by: x }",
      ],
      [
         "the joined source's own measure through an internal join_many",
         "run: hidden_many -> { group_by: carrier; aggregate: m is segments.seg_miles }",
      ],
      [
         "an internal join_many's field through an extension",
         "run: ext_many -> { group_by: segments.tag }",
      ],
      [
         "a private join_many's field",
         "run: private_many -> { group_by: segments.tag }",
      ],
      [
         "a private join_many's field through a caller extend",
         "run: private_many extend { dimension: t is segments.tag } -> { group_by: t }",
      ],
      [
         "a private join_many's measure through a caller extend",
         "run: private_many extend { measure: m is segments.miles.sum() } -> { aggregate: m }",
      ],
      [
         "a private join_many lifted by a caller include",
         "run: private_many include { public: segments } -> { group_by: segments.tag }",
      ],
      [
         "a private join_many after a caller's include wildcard",
         "run: private_many include { public: * } -> { group_by: segments.tag }",
      ],
      [
         "a private join_many renamed by a caller",
         "run: private_many extend { rename: s is segments } -> { group_by: s.tag }",
      ],
      [
         "a private join_many through a caller join",
         "run: plain extend { join_one: p is private_many on id = p.id } -> { group_by: p.segments.tag }",
      ],
      [
         "a private join_many's internal name through a caller extend",
         "run: private_many extend { dimension: x is segments__tag__pair } -> { group_by: x }",
      ],
      [
         "raw SQL naming a private join_many's stored column",
         'run: private_many -> { group_by: x is sql_string("segments__tag") }',
      ],
      [
         "an internal path's leaf field",
         "run: hidden_hop -> { group_by: segments.opts.kind }",
      ],
      [
         "an internal path's leaf internal name through a caller extend",
         "run: hidden_hop extend { dimension: x is segments__opts__kind__pair } -> { group_by: x }",
      ],
      [
         "a private block's field",
         "run: hidden_blocks -> { group_by: crew.crew_name }",
      ],
      [
         "a private block's field through a caller extend",
         "run: hidden_blocks extend { dimension: c is crew.crew_name } -> { group_by: c }",
      ],
      [
         "a private block's internal name through a caller extend",
         "run: hidden_blocks extend { dimension: x is crew__crew_name__pair } -> { group_by: x }",
      ],
   ] as const;
   for (const [name, malloy] of refused) {
      it(`refuses ${name}, as live does`, async () => {
         await expectRefused(fixture, malloy);
      });
   }

   // The row flags' internal names are `internal` on every pair table (the
   // re-declared measures read them in a later block, where a private name
   // is unreadable), so the served copy's own compile reads one through a
   // caller's `extend`. The router never answers that from storage (a source
   // a caller builds on a pair table is declined), so the routed query
   // refuses as live does; a flag carries no hidden value either way.
   for (const malloy of [
      "run: hidden_many extend { dimension: x is __parent_row__pair } -> { group_by: x }",
      "run: private_many extend { dimension: x is __parent_row__pair } -> { group_by: x }",
      "run: hidden_blocks extend { dimension: x is __pair_lane__pair } -> { group_by: x }",
   ]) {
      it(`refuses a flag's internal name through a caller extend, as live does: ${malloy}`, async () => {
         fixture.setBindings([]);
         const live = await failure(() => fixture.runAsIs(malloy));
         fixture.setBindings(fixture.bindings);
         const served = await failure(() => fixture.runAsIs(malloy));
         expect(live).toBeDefined();
         expect(served).toBeDefined();
         expect(String(served)).not.toMatch(HIDDEN);
      });
   }

   it("never carries a hidden value in a refusal over a filter on it", async () => {
      // A filter that names a value of the hidden join: refused, and the
      // error neither confirms nor echoes the value beyond the query text.
      const malloy =
         "run: private_many -> { where: segments.tag = 'tagbravo'; aggregate: flight_count }";
      fixture.setBindings(fixture.bindings);
      const served = await failure(() => fixture.runAsIs(malloy));
      expect(served).toBeDefined();
      expect(String(served)).not.toMatch(/tag(alpha|charlie|delta|echo)/);
   });
});

const gatedModel = `##! experimental { access_modifiers givens }
given: C :: string
given: R :: string
${children}
#(access_filter) carrier = $C
#(materialize) storage="lake" inline=["segments"]
source: by_carrier is duckdb.table('flights') extend {
  primary_key: id
  private join_many: segments on segments.flight_id = id
${pairMeasures}
}

#(access_filter) segments.region = $R
#(materialize) storage="lake" inline=["segments"]
source: by_region is duckdb.table('flights') extend {
  primary_key: id
  internal join_many: segments on segments.flight_id = id
${pairMeasures}
}
`;

describe("R4: row-level gates over a hidden join_many, gated serving on", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": gatedModel },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("re-roots both gated sources", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
      const access = await servedAccess(fixture);
      expect(access.by_carrier.segments).toBe("private");
      expect(access.by_region.segments).toBe("internal");
   });

   for (const C of ["AA", "UA", "DL"]) {
      it(`answers each caller's rows under a parent gate through a private join_many (${C})`, async () => {
         const run = await fixture.run({
            name: `parent gate ${C}`,
            malloy:
               "run: by_carrier -> { group_by: carrier; aggregate: flight_count, total_distance, seg_total, seg_n, tag_n }",
            givens: { C },
         });
         expectPairServedVia(run, PATH_COUNTS);
         expect(run.served.map((r) => r.carrier)).toEqual([C]);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   for (const R of ["east", "west"]) {
      it(`answers child measures under a gate that reads an internal join_many (${R})`, async () => {
         const run = await fixture.run({
            name: `child gate ${R}`,
            malloy:
               "run: by_region -> { group_by: carrier; aggregate: seg_total, seg_n, tag_n }",
            givens: { R },
         });
         expectPairServedVia(run, PATH_COUNTS);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });

      it(`answers parent measures live under that gate, equal to live (${R})`, async () => {
         const run = await fixture.run({
            name: `child gate parents ${R}`,
            malloy:
               "run: by_region -> { group_by: carrier; aggregate: flight_count, total_distance, seg_n }",
            givens: { R },
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("refuses a caller reading the private join_many under its gate, leaking nothing", async () => {
      const malloy = "run: by_carrier -> { group_by: segments.tag }";
      fixture.setBindings([]);
      const live = await failure(() => fixture.runAsIs(malloy, { C: "AA" }));
      fixture.setBindings(fixture.bindings);
      const served = await failure(() => fixture.runAsIs(malloy, { C: "AA" }));
      expect(live).toBeDefined();
      expect(served).toBeDefined();
      expect(String(served)).not.toMatch(HIDDEN);
   });
});

// Colocated (no `storage=`): an extension's build can read its parent's
// build source in the same warehouse, which for a pair table holds each
// parent once per child. Built over the raw chain instead, the extension's
// table holds one row per parent and answers with live's rows, a public or a
// hidden join_many alike.
const colocatedModel = `##! experimental { access_modifiers }
${children}
#(materialize) name="r4h_pub" inline=["segments"]
source: pub_many is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
${pairMeasures}
}

#(materialize) name="r4h_pub_ext"
source: pub_ext is pub_many extend {
  measure: double_seg is seg_total * 2
}

#(materialize) name="r4h_hidden" inline=["segments"]
source: hidden_many is duckdb.table('flights') extend {
  primary_key: id
  internal join_many: segments on segments.flight_id = id
${pairMeasures}
}

#(materialize) name="r4h_hidden_ext"
source: hidden_ext is hidden_many extend {
  measure: double_seg is seg_total * 2
}

#(materialize) name="r4h_private" inline=["segments"]
source: private_many is duckdb.table('flights') extend {
  primary_key: id
  private join_many: segments on segments.flight_id = id
${pairMeasures}
}

#(materialize) name="r4h_private_ext"
source: private_ext is private_many extend {
  measure: double_seg is seg_total * 2
}
`;

describe("R4: a colocated extension of a pair table, hidden join_many or not", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "off", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": colocatedModel },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("stores each extension once per parent, over the raw chain", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
      for (const ext of ["pub_ext", "hidden_ext", "private_ext"]) {
         expect(`${ext}:${fixture.buildReads[ext]?.join(",") ?? ""}`).toBe(
            `${ext}:`,
         );
         const rows = await fixture.duckdb.runSQL(
            `SELECT count(*) AS n FROM ${fixture.physicalTables[ext]}`,
         );
         expect(`${ext}:${Number(rows.rows[0].n)}`).toBe(`${ext}:6`);
      }
   });

   for (const ext of ["pub_ext", "hidden_ext", "private_ext"]) {
      for (const aggregate of [
         "flight_count, total_distance",
         "double_seg, seg_total, seg_n, tag_n",
         "distance_per_flight, long_legs",
      ]) {
         const malloy = `run: ${ext} -> { group_by: carrier; aggregate: ${aggregate} }`;
         it(`answers with live's rows: ${malloy}`, async () => {
            const run = await fixture.run({ name: malloy, malloy });
            expect(run.live.length).toBeGreaterThan(0);
            // An extension's table holds one row per parent; its join is read
            // live, so its counts are served.
            expect(run.servedVia).toBe("served_copy");
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }
   }

   for (const malloy of [
      "run: hidden_ext -> { group_by: segments.tag }",
      "run: hidden_ext -> { group_by: segments__tag }",
      "run: private_ext -> { group_by: segments.tag }",
      "run: private_ext extend { dimension: t is segments.tag } -> { group_by: t }",
   ]) {
      it(`refuses a hidden join through the extension, as live does: ${malloy}`, async () => {
         fixture.setBindings([]);
         const live = await failure(() => fixture.runAsIs(malloy));
         fixture.setBindings(fixture.bindings);
         const served = await failure(() => fixture.runAsIs(malloy));
         expect(live).toBeDefined();
         expect(served).toBeDefined();
         expect(String(served)).not.toMatch(HIDDEN);
      });
   }
});
