// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for tenant isolation through a base, adversarial: every way the
 * inherited given-reading `where:` and the base's given-reading dimensions
 * could be built into the table with one caller's value, or dropped from the
 * served copy, so a second caller reads the first caller's rows (#1637's
 * shape).
 *
 * The given has a DEFAULT here, unlike the plain spec: a term or dimension the
 * build kept would then build silently with the default's value (one
 * tenant's rows) rather than fail to build, so a generator bug shows as a
 * served answer that differs from live for the other caller. The harness does
 * not run the eligibility gates, so the guard under test is the generator's
 * and the served copy's.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

const B = "2026-11-10 00:00:00";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'acme', 10.0, 'gold', TIMESTAMP '${B}' - INTERVAL 1 DAY),
      (2, 'acme', 20.0, 'tin', TIMESTAMP '${B}' - INTERVAL 40 DAY),
      (3, 'globex', 5.0, 'gold', TIMESTAMP '${B}' - INTERVAL 2 DAY),
      (4, 'globex', 7.0, 'tin', TIMESTAMP '${B}' - INTERVAL 3 DAY),
      (5, 'globex', 9.0, 'gold', TIMESTAMP '${B}' - INTERVAL 50 DAY),
      (6, 'initech', 3.0, 'gold', TIMESTAMP '${B}' - INTERVAL 1 DAY)
    ) AS t(id, org, amt, tier, ts)`,
   `CREATE TABLE tiers AS SELECT * FROM (VALUES
      ('acme', 'gold', 'A-gold'),
      ('acme', 'tin', 'A-tin'),
      ('globex', 'gold', 'G-gold'),
      ('globex', 'tin', 'G-tin'),
      ('initech', 'gold', 'I-gold')
    ) AS t(org, tier, label)`,
];

const HEAD = `##! experimental { givens access_modifiers }
given:
  ORG :: string is 'acme'
`;

const otherFile = `${HEAD}
source: far_base is duckdb.sql("select * from ev") extend {
  where: org = $ORG
  dimension: far_mine is org = $ORG
}
`;

// The given is declared once, in other.malloy, which this file imports.
const modelOver = (
   ev: string,
   tiers: string,
) => `##! experimental { givens access_modifiers }
import "other.malloy"

source: ev is ${ev}
source: tiers is ${tiers}

source: base is ev extend {
  where: org = $ORG
  dimension: mine is org = $ORG
  dimension: tag is pick 'me' when org = $ORG else 'other'
  measure: mine_total is amt.sum() { where: org = $ORG }
  measure: n is count()
  view: by_mine is { group_by: mine; aggregate: n }
}

#(materialize) storage="lake"
source: plain is base extend {
  measure: total is amt.sum()
}

// A parenthesised head: the base's block the served copy keeps is only
// its trailing extend, so the term in the head cannot be kept.
source: paren is (ev extend { where: org = $ORG }) extend { dimension: x is 1 }
#(materialize) storage="lake"
source: paren_ext is paren extend { measure: total is amt.sum() }

// The base re-declared under another name.
source: b2 is base
#(materialize) storage="lake"
source: renamed_ext is b2 extend { measure: total is amt.sum() }

// Two steps of renames and extends.
source: b3 is b2 extend { dimension: big is amt > 6 }
source: b4 is b3
#(materialize) storage="lake"
source: rename_chain is b4 extend { measure: total is amt.sum() }

source: base_x is ev extend {
  where: org = $ORG
  dimension: xmine is org = $ORG
  dimension: dbl is amt * 2
}
// except: of the given-reading dimension.
#(materialize) storage="lake"
source: excepts is base_x extend {
  except: xmine
  measure: total is amt.sum()
}
// include of everything but it.
#(materialize) storage="lake"
source: includes is base_x include { public: *; except: xmine } extend {
  measure: total is amt.sum()
}

// Hidden given-reading fields on the base.
source: base_h is ev extend {
  where: org = $ORG
  internal dimension: hmine is org = $ORG
  private dimension: pmine is org = $ORG
  dimension: shows is pick 'me' when hmine else 'other'
  dimension: pshows is pick 'me' when pmine else 'other'
}
#(materialize) storage="lake"
source: hidden is base_h extend { measure: total is amt.sum() }

// A join of the source reads the base the copy takes the term out of.
#(materialize) storage="lake"
source: joins_base is base extend {
  join_one: peer is base on peer.id = id
  measure: total is amt.sum()
}

// An inherited term reading a joined field.
source: base_j is ev extend {
  join_one: t is tiers on t.org = org and t.tier = tier
  where: t.org = $ORG
}
#(materialize) storage="lake"
source: joined_term is base_j extend { measure: total is amt.sum() }

// A term in the base's second trailing extend.
source: base_two is ev extend { dimension: a is 1 } extend { where: org = $ORG }
#(materialize) storage="lake"
source: two_blocks is base_two extend { measure: total is amt.sum() }

// A term with a fixed half, and a comma list.
source: base_mixed is ev extend {
  where: org = $ORG and amt > 4, tier != 'none'
}
#(materialize) storage="lake"
source: mixed is base_mixed extend { measure: total is amt.sum() }

// The base in another file: must not build without its term.
#(materialize) storage="lake"
source: cross_file is far_base extend { measure: total is amt.sum() }

// A base's term and a middle source's own term, and the source's own.
source: mid is base extend { where: tier = 'gold' or org = $ORG }
#(materialize) storage="lake"
source: three_terms is mid extend {
  where: amt >= 0 or org = $ORG
  measure: total is amt.sum()
}

// A rolling window of the source's own over the scoped base.
#(materialize) storage="lake"
source: rolling is base extend {
  where: ts >= now - 30 days
  measure: total is amt.sum()
}

// The source re-hides, or only renames, the column the base's term reads.
#(materialize) storage="lake"
source: rehides is base include { internal: org; public: * } extend {
  measure: total is amt.sum()
}
#(materialize) storage="lake"
source: renames_only is base extend {
  rename: company is org
  measure: total is amt.sum()
}

// An inlined join whose condition reads the given.
#(materialize) storage="lake" inline=["t"]
source: inlined is base extend {
  join_one: t is tiers on t.org = org and t.tier = tier and t.org = $ORG
  measure: total is amt.sum()
}
`;

const CALLERS = [{ ORG: "acme" }, { ORG: "globex" }];
const INTERLEAVED = [...CALLERS, ...CALLERS];
const ORGS = ["acme", "globex", "initech"];

async function asEachCaller(
   fixture: R4Fixture,
   name: string,
   malloy: string,
   options: {
      served?: boolean;
      orgsOf?: (row: Record<string, unknown>) => unknown;
   } = {},
): Promise<R4Run[]> {
   const served = options.served ?? true;
   const runs: R4Run[] = [];
   for (const givens of INTERLEAVED) {
      const run = await fixture.run({
         name: `${name} as ${JSON.stringify(givens)}`,
         malloy,
         givens,
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBe(served ? "served_copy" : undefined);
      if (served) expect(run.servedFrom).toBe("storage");
      if (options.orgsOf) {
         for (const row of run.served) {
            expect(options.orgsOf(row)).toBe(givens.ORG);
         }
      }
      runs.push(run);
   }
   expect(runs[0].live).not.toEqual(runs[1].live);
   expect(runs[2].served).toEqual(runs[0].served);
   expect(runs[3].served).toEqual(runs[1].served);
   return runs;
}

// Over a table and over a SQL source: Malloy records an extension's base
// differently for each, and the kept walk reads both.
// What each shape builds. Over a table, Malloy records no base edge for an
// extension, and the walk infers it only while the extension adds no
// `where:` or join and is not a rename (`b2 is base`), so those are refused
// and served live there, fail closed.
const BUILT_EVERYWHERE = ["hidden", "mixed", "plain", "two_blocks"];
for (const [shape, ev, tiers, expectedBuilt] of [
   ["table", "duckdb.table('ev')", "duckdb.table('tiers')", BUILT_EVERYWHERE],
   [
      "sql",
      'duckdb.sql("select * from ev")',
      'duckdb.sql("select * from tiers")',
      [
         ...BUILT_EVERYWHERE,
         "rename_chain",
         "renamed_ext",
         "rolling",
         "three_terms",
      ],
   ],
] as const) {
   const model = modelOver(ev, tiers);
   describe(`R4 adversarial: a base scoped by a given, with a defaulted given (over a ${shape})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      let built: string[];
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model, "other.malloy": otherFile },
            seedSql,
            materialize: true,
            clock: B,
         });
         built = fixture.bindings
            .flatMap((b) => b.sourceIDs ?? [])
            .map((id) => id.split("@")[0])
            .sort();
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("builds only what the served copy can keep, each with every caller's rows", async () => {
         expect(built).toEqual([...expectedBuilt].sort());
         expect(await fixture.servedCopyProblems()).toEqual([]);
         // Every built table holds every caller's rows: none was built with the
         // default's value.
         for (const source of built) {
            const orgs = await fixture.storedSQL(
               (t) => `SELECT DISTINCT org FROM ${t(source)} ORDER BY org`,
            );
            expect({ source, orgs: orgs.map((r) => r.org) }).toEqual({
               source,
               orgs: ORGS,
            });
         }
      });

      for (const [source, malloy, orgs] of [
         [
            "plain",
            "run: plain -> { group_by: org, mine, tag; aggregate: total, mine_total, n }",
            true,
         ],
         ["plain", "run: plain -> by_mine", false],
         [
            "plain",
            "run: plain -> { group_by: org; nest: m is { group_by: mine, tag; aggregate: n } }",
            true,
         ],
         [
            "plain",
            "run: plain -> { where: mine; group_by: org; aggregate: n }",
            true,
         ],
         [
            "plain",
            "run: plain -> { where: tag = 'me'; aggregate: n, total }",
            false,
         ],
         [
            "paren_ext",
            "run: paren_ext -> { group_by: org; aggregate: total }",
            true,
         ],
         [
            "renamed_ext",
            "run: renamed_ext -> { group_by: org, mine; aggregate: total }",
            true,
         ],
         [
            "rename_chain",
            "run: rename_chain -> { group_by: org, big, tag; aggregate: total }",
            true,
         ],
         [
            "excepts",
            "run: excepts -> { group_by: org; aggregate: total }",
            true,
         ],
         [
            "includes",
            "run: includes -> { group_by: org; aggregate: total }",
            true,
         ],
         [
            "hidden",
            "run: hidden -> { group_by: org, shows, pshows; aggregate: total }",
            true,
         ],
         [
            "joins_base",
            "run: joins_base -> { group_by: org; aggregate: total }",
            true,
         ],
         [
            "joined_term",
            "run: joined_term -> { group_by: org; aggregate: total }",
            true,
         ],
         [
            "two_blocks",
            "run: two_blocks -> { group_by: org, a; aggregate: total }",
            true,
         ],
         ["mixed", "run: mixed -> { group_by: org; aggregate: total }", true],
         [
            "cross_file",
            "run: cross_file -> { group_by: org, far_mine; aggregate: total }",
            true,
         ],
         [
            "three_terms",
            "run: three_terms -> { group_by: org, tier; aggregate: total }",
            true,
         ],
         [
            "rolling",
            "run: rolling -> { group_by: org; aggregate: total }",
            true,
         ],
         [
            "inlined",
            "run: inlined -> { group_by: org, t.label; aggregate: total }",
            true,
         ],
         [
            "rehides",
            "run: rehides -> { group_by: tier, mine; aggregate: total }",
            false,
         ],
      ] as const) {
         it(`${source}: ${malloy}`, async () => {
            await asEachCaller(fixture, source, malloy, {
               served: (expectedBuilt as readonly string[]).includes(source),
               orgsOf: orgs ? (r) => r.org : undefined,
            });
         });
      }

      it("a rename of the column the base's term reads is denied, as live denies it", async () => {
         for (const malloy of [
            "run: renames_only -> { group_by: company, mine; aggregate: total }",
            "run: rename_chain extend { rename: o is org } -> { group_by: o, mine; aggregate: total }",
         ]) {
            for (const givens of CALLERS) {
               let refusal: unknown;
               try {
                  await fixture.run({ name: malloy, malloy, givens });
               } catch (error) {
                  refusal = error;
               }
               expect(refusal).toBeInstanceOf(AccessDeniedError);
            }
         }
      });

      it("a caller's own extend cannot shadow the dimension or the term's column", async () => {
         for (const malloy of [
            "run: plain extend { dimension: mine is true } -> { group_by: org, mine; aggregate: total }",
            "run: plain extend { rename: o is org } -> { group_by: o; aggregate: total }",
            "run: plain extend { dimension: org2 is 'acme' } -> { group_by: org, org2; aggregate: total }",
            "run: plain extend { where: org = 'initech' or true } -> { group_by: org; aggregate: total }",
         ]) {
            for (const givens of INTERLEAVED) {
               const attempt = async (f: () => Promise<R4Run>) => {
                  try {
                     return { run: await f() };
                  } catch (error) {
                     return { error: String(error) };
                  }
               };
               const out = await attempt(() =>
                  fixture.run({ name: malloy, malloy, givens }),
               );
               if ("run" in out) {
                  expect(
                     rowDifference(out.run.live, out.run.served),
                  ).toBeUndefined();
                  for (const row of out.run.served) {
                     const org = row.org ?? row.o;
                     if (org !== undefined) expect(org).toBe(givens.ORG);
                  }
               }
            }
         }
      });
   });
}

const chainModel = `##! experimental { givens access_modifiers }
given:
  ORG :: string is 'acme'

source: ev is duckdb.table('ev')
// Stored in the same destination, so a join to it stays a join at read.
#(materialize) storage="lake"
source: tiers is duckdb.table('tiers')

source: base is ev extend {
  where: org = $ORG
  dimension: mine is org = $ORG
}

// A stored dimension of the source's own that reads the base's given-reading
// dimension: it depends on the caller too, so it must not be stored.
#(materialize) storage="lake"
source: derived is base extend {
  dimension: mine_label is pick 'Y' when mine else 'N'
  dimension: amt2 is amt * 2
  measure: total is amt.sum()
}

// A join whose condition reads the base's given-reading dimension.
#(materialize) storage="lake"
source: join_on_mine is base extend {
  join_one: t is tiers on t.org = org and t.tier = tier and mine
  measure: total is amt.sum()
}

// An extension table over a materialized parent over the scoped base.
#(materialize) storage="lake"
source: parent_m is base extend {
  dimension: big is amt > 6
}
#(materialize) storage="lake"
source: child_m is parent_m extend {
  measure: total is amt.sum()
}

// A materialized source scoped by its own term, and an extension table of it.
#(materialize) storage="lake"
source: scoped_m is ev extend {
  where: org = $ORG
  dimension: smine is org = $ORG
}
#(materialize) storage="lake"
source: scoped_child is scoped_m extend {
  dimension: big is amt > 6
  measure: total is amt.sum()
}

// A source that joins a materialized extension of the scoped base.
source: viewer is tiers extend {
  join_many: d is derived on d.org = org and d.tier = tier
}
`;

describe("R4 adversarial: stored dimensions, joins and extension tables over a scoped base", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   let built: string[];
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": chainModel },
         seedSql,
         materialize: true,
      });
      built = fixture.bindings
         .flatMap((b) => b.sourceIDs ?? [])
         .map((id) => id.split("@")[0])
         .sort();
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("stores every caller's rows and no caller-dependent column", async () => {
      expect(built).toEqual([
         "child_m",
         "derived",
         "join_on_mine",
         "parent_m",
         "scoped_child",
         "scoped_m",
         "tiers",
      ]);
      expect(await fixture.servedCopyProblems()).toEqual([]);
      for (const source of built) {
         const rows = await fixture.storedSQL(
            (t) => `SELECT * FROM ${t(source)}`,
         );
         expect(new Set(rows.map((r) => r.org))).toEqual(new Set(ORGS));
         for (const column of ["mine", "mine_label", "smine"]) {
            expect(Object.keys(rows[0])).not.toContain(column);
         }
      }
   });

   for (const [source, malloy, orgs] of [
      [
         "derived",
         "run: derived -> { group_by: org, mine_label, amt2; aggregate: total }",
         true,
      ],
      [
         "join_on_mine",
         "run: join_on_mine -> { group_by: org, t.label; aggregate: total }",
         true,
      ],
      [
         "parent_m",
         "run: parent_m -> { group_by: org, big, mine; aggregate: n is count() }",
         true,
      ],
      [
         "child_m",
         "run: child_m -> { group_by: org, big, mine; aggregate: total }",
         true,
      ],
      [
         "scoped_m",
         "run: scoped_m -> { group_by: org, smine; aggregate: n is count() }",
         true,
      ],
      [
         "scoped_child",
         "run: scoped_child -> { group_by: org, big, smine; aggregate: total }",
         true,
      ],
   ] as const) {
      it(`${source}: ${malloy}`, async () => {
         await asEachCaller(fixture, source, malloy, {
            orgsOf: orgs ? (r) => r.org : undefined,
         });
      });
   }

   it("a source joining the materialized extension sees only the caller's rows through the join", async () => {
      for (const givens of INTERLEAVED) {
         const run = await fixture.run({
            name: `viewer as ${givens.ORG}`,
            malloy:
               "run: viewer -> { group_by: org, d.mine_label; aggregate: n is d.count(), t is d.total }",
            givens,
         });
         expect(run.servedVia).toBe("served_copy");
         for (const row of run.served) {
            if (row.org !== givens.ORG) expect(row.n).toBe(0);
         }
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      }
   });

   it("a caller who supplies no given gets the default's rows, as live", async () => {
      fixture.setBindings([]);
      const live = (
         await fixture.runAsIs(
            "run: derived -> { group_by: org, mine; aggregate: total }",
         )
      ).rows;
      fixture.setBindings(fixture.bindings);
      const served = await fixture.runAsIs(
         "run: derived -> { group_by: org, mine; aggregate: total }",
      );
      expect(served.servedFrom).toBe("storage");
      expect(rowDifference(live, served.rows)).toBeUndefined();
      expect(live.map((r) => r.org)).toEqual(["acme"]);
   });
});

const colocatedModel = `##! experimental { givens access_modifiers }
given:
  ORG :: string is 'acme'

source: base is duckdb.table('ev') extend {
  where: org = $ORG
  dimension: mine is org = $ORG
}

#(materialize)
source: colocated is base extend { measure: total is amt.sum() }

#(materialize) storage="lake" refresh="incremental" watermark="ts"
source: incremental is base extend { measure: total is amt.sum() }
`;

describe("R4 adversarial: colocated and incremental over a scoped base", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   let built: string[];
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": colocatedModel },
         seedSql,
         materialize: true,
         colocated: ["colocated"],
      });
      built = fixture.bindings
         .flatMap((b) => b.sourceIDs ?? [])
         .map((id) => id.split("@")[0])
         .sort();
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("never builds one caller's rows", async () => {
      // Colocated #(materialize) is not built yet; the incremental one is.
      expect(built).toEqual(["incremental"]);
      for (const source of Object.keys(fixture.physicalTables)) {
         const orgs = await fixture.storedSQL(
            (t) => `SELECT DISTINCT org FROM ${t(source)} ORDER BY org`,
         );
         expect(orgs.map((r) => r.org)).toEqual(ORGS);
      }
   });

   for (const source of ["colocated", "incremental"]) {
      it(`${source} answers each caller as live`, async () => {
         const runs: R4Run[] = [];
         for (const givens of INTERLEAVED) {
            const run = await fixture.run({
               name: `${source} as ${givens.ORG}`,
               malloy: `run: ${source} -> { group_by: org, mine; aggregate: total }`,
               givens,
            });
            expect(run.servedVia).toBe(
               source === "incremental" ? "served_copy" : undefined,
            );
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            for (const row of run.served) expect(row.org).toBe(givens.ORG);
            runs.push(run);
         }
         expect(runs[0].live).not.toEqual(runs[1].live);
      });
   }
});
