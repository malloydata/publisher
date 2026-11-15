// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for P3, adversarial, as one matrix: with a `#(materialize)` source's
 * private and internal fields in the stored table, every way a caller could
 * reach a hidden column is run live and served, per scenario, and the served
 * answer must be live's own: refused exactly where live refuses (carrying no
 * hidden value, and refused by the served copy's own compile too), and
 * otherwise live's rows, read from storage. A shape that live answers (Malloy
 * lets a caller's `extend` or `include` read an `internal` field) must not
 * answer more from storage.
 *
 * The scenarios:
 *
 * - hidden fields on a source and its base in another file. Raw SQL and the
 *   storage connection itself are covered too: restricted mode refuses
 *   `sql_*`, `<conn>.table(…)` and `<conn>.sql(…)`, and a caller cannot write
 *   `<conn>.virtual(…)`, so the stored table is reachable only through the
 *   served copy's re-rooted source, which re-hides its hidden columns. A
 *   hidden source the queried model holds only through a join (#6643) is the
 *   conformance shape "joined persisted source with hidden fields, from an
 *   index": this harness plans `#(materialize)` in its entry file only.
 * - hidden fields on extension tables (P3 with P10). A parent declares or
 *   hides fields (`private`, `internal`, in an `include` and in its own
 *   `extend`); a `storage=` extension of it, built over the parent's stored
 *   table (chained), reads one of them, is gated on one, and declares a
 *   private field of its own; a second extension reaches the parent through
 *   an unmaterialized source that hides a field of its own. The chained build
 *   re-roots the parent on its build source, whose columns are plain (public)
 *   in the build copy; the extension still stores only the hidden fields its
 *   own build copy over the raw chain could select, so a field private on the
 *   parent is never in the extension's table.
 *
 * An inlined join_one's hidden fields, last, are a scenario of their own
 * shape: what a caller reads through the join's record.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'acme', 'east', 10.0), (2, 'acme', 'west', 20.0),
      (3, 'globex', 'east', 30.0), (4, 'globex', 'west', 40.0),
      (5, 'initech', 'east', 50.0)
    ) AS t(id, org_id, region, amount)`,
];

/** The hidden values a refusal must never carry. */
const HIDDEN_VALUES = /acme|globex|initech|EAST|WEST/;

/** The base, in its own file: `id` private, `org_id` and `region_code` internal. */
const base = `##! experimental { access_modifiers }

source: base is duckdb.table('orders') include { private: id; internal: org_id; public: * } extend {
  internal dimension: region_code is upper(region)
}
`;

const p3Model = `##! experimental { access_modifiers }
import "base.malloy"

#(materialize) storage="lake"
source: orders is base extend {
  private dimension: secret is amount * 2
  private measure: secret_m is secret.sum()
  internal view: by_org is { group_by: org_id; aggregate: total }
  measure: total is amount.sum(), n is count()
}

#(materialize) storage="lake"
source: wide is duckdb.table('orders') include { private: amount; public: * } extend {
  measure: amt is amount.sum()
}

#(materialize) storage="lake"
source: joiner is duckdb.table('orders') extend {
  join_one: o is orders on region = o.region and amount = o.amount
}

source: rollup is orders -> { group_by: region; aggregate: total }
`;

/** Live refuses each; the served copy must too, carrying no hidden value. */
const p3Refused: [string, string][] = [
   ["a private computed column", "run: orders -> { select: secret }"],
   ["an internal column", "run: orders -> { group_by: org_id }"],
   ["an inherited internal column", "run: orders -> { group_by: region_code }"],
   ["an ancestor-private column", "run: orders -> { group_by: id }"],
   ["a private measure", "run: orders -> { aggregate: secret_m }"],
   ["an internal view", "run: orders -> by_org"],
   [
      "a private column through a caller extend",
      "run: orders extend { dimension: s is secret } -> { group_by: s }",
   ],
   [
      "a private column through a caller rename",
      "run: orders extend { rename: s is secret } -> { group_by: s }",
   ],
   [
      "a renamed internal column, still internal",
      "run: orders extend { rename: o is org_id } -> { group_by: o }",
   ],
   [
      "a private column through a caller include",
      "run: orders include { public: * } -> { select: secret }",
   ],
   [
      "a private column made public by a caller include",
      "run: orders include { public: secret } -> { select: secret }",
   ],
   [
      "a private head column made public by a caller include",
      "run: wide include { public: amount } -> { select: amount }",
   ],
   [
      "a private column in a nest",
      "run: orders -> { nest: x is { group_by: secret } }",
   ],
   ["an internal column indexed by name", "run: orders -> { index: org_id }"],
   [
      "a private column after a wildcard",
      "run: orders -> { select: *, secret }",
   ],
   [
      "an internal column through the author's join",
      "run: joiner -> { group_by: o.org_id }",
   ],
   [
      "a private column through the author's join",
      "run: joiner -> { group_by: o.secret }",
   ],
   [
      "a private column through a caller join",
      "run: wide extend { join_one: o is orders on region = o.region } -> { group_by: o.secret }",
   ],
   [
      "a private head column through a caller join",
      "run: orders extend { join_one: w is wide on region = w.region } -> { group_by: w.amount }",
   ],
   [
      "a private column of a query-shaped source over it",
      "run: rollup -> { group_by: secret }",
   ],
   [
      "raw SQL naming a stored private column",
      'run: orders -> { group_by: x is sql_number("secret") }',
   ],
   [
      "raw SQL interpolating a private column",
      'run: orders -> { group_by: x is sql_number("${secret}") }',
   ],
];

/**
 * Live answers each (wildcards list public fields only; Malloy lets a
 * caller's own `extend` or `include` read an `internal` field); the served
 * copy must answer from storage with exactly live's rows, no more columns.
 */
const p3Answered: [string, string][] = [
   ["select: *", "run: orders -> { select: * }"],
   [
      "select: * through a query stage",
      "run: orders -> { select: * } -> { select: * }",
   ],
   ["index: *", "run: orders -> { index: * }"],
   [
      "a measure over a private column",
      "run: orders -> { aggregate: total, n }",
   ],
   [
      "an internal column through a caller extend",
      "run: orders extend { dimension: o is org_id } -> { group_by: o; aggregate: total }",
   ],
   [
      "an internal column lifted by a caller include",
      "run: orders include { public: org_id, total } -> { group_by: org_id; aggregate: total }",
   ],
   [
      "a private head column's measure",
      "run: wide -> { group_by: region; aggregate: amt }",
   ],
   ["a query-shaped source over it", "run: rollup -> { select: * }"],
   [
      "the author's join into it",
      "run: joiner -> { group_by: o.region; aggregate: o.total, o.n }",
   ],
];

// Hidden fields on extension tables (P3 with P10).
const extensionModel = `##! experimental { access_modifiers }

#(materialize) storage="lake"
source: parent is duckdb.table('orders') include { private: id; internal: org_id; public: * } extend {
  internal dimension: region_code is upper(region)
  private dimension: secret is amount * 2
  private measure: secret_m is secret.sum()
  measure: total is amount.sum(), n is count()
  measure: secret_total is secret.sum()
}

#(materialize) storage="lake"
source: ext is parent extend {
  where: region_code = 'EAST'
  dimension: org_upper is upper(org_id)
  private dimension: mine is amount + 1
  measure: org_count is count(org_id)
  measure: mine_total is mine.sum()
}

source: mid is parent extend {
  internal dimension: tier is amount > 20
}

#(materialize) storage="lake"
source: ext2 is mid extend {
  dimension: tier_label is pick 'big' when tier else 'small'
}

#(materialize) storage="lake"
source: narrowed is parent include { private: region; public: * } extend {
  measure: region_count is count(region)
}
`;

/** Live refuses each; the served copy must too, carrying no hidden value. */
const extensionRefused: [string, string][] = [
   [
      "the parent's internal computed column, on the extension",
      "run: ext -> { group_by: region_code }",
   ],
   [
      "the parent's include-internal column, on the extension",
      "run: ext -> { group_by: org_id }",
   ],
   [
      "the parent's include-private column, on the extension",
      "run: ext -> { group_by: id }",
   ],
   [
      "the parent's private computed column, on the extension",
      "run: ext -> { select: secret }",
   ],
   [
      "the parent's private measure, on the extension",
      "run: ext -> { aggregate: secret_m }",
   ],
   ["the extension's own private column", "run: ext -> { select: mine }"],
   [
      "the parent's private column through a caller extend",
      "run: ext extend { dimension: s is secret } -> { group_by: s }",
   ],
   [
      "the extension's private column through a caller extend",
      "run: ext extend { dimension: s is mine } -> { group_by: s }",
   ],
   [
      "the parent's private column made public by a caller include",
      "run: ext include { public: secret } -> { select: secret }",
   ],
   [
      "the extension's private column through a caller include",
      "run: ext include { public: * } -> { select: mine }",
   ],
   [
      "the parent's include-private column made public by a caller include",
      "run: ext include { public: id } -> { select: id }",
   ],
   [
      "a renamed internal column, still internal",
      "run: ext extend { rename: o is region_code } -> { group_by: o }",
   ],
   [
      "a private column in a nest",
      "run: ext -> { nest: x is { group_by: mine } }",
   ],
   ["an internal column indexed by name", "run: ext -> { index: region_code }"],
   ["a private column after a wildcard", "run: ext -> { select: *, mine }"],
   [
      "raw SQL naming the parent's private column",
      'run: ext -> { group_by: x is sql_number("secret") }',
   ],
   [
      "raw SQL naming the extension's private column",
      'run: ext -> { group_by: x is sql_number("mine") }',
   ],
   [
      "an intermediate source's internal column, on its extension table",
      "run: ext2 -> { group_by: tier }",
   ],
   [
      "the parent's private column, through an unmaterialized intermediate",
      "run: ext2 -> { select: secret }",
   ],
   [
      "the parent's internal column, through an unmaterialized intermediate",
      "run: ext2 -> { group_by: region_code }",
   ],
   [
      "a column the extension's own include makes private",
      "run: narrowed -> { group_by: region }",
   ],
   [
      "a column the extension's own include makes private, through a caller extend",
      "run: narrowed extend { dimension: r is region } -> { group_by: r }",
   ],
   [
      "a column the extension's own include makes private, lifted by a caller include",
      "run: narrowed include { public: region } -> { group_by: region }",
   ],
];

/**
 * Live answers each (wildcards list public fields only; Malloy lets a
 * caller's own `extend` or `include` read an `internal` field); the served
 * copy must answer from storage with exactly live's rows, no more columns.
 */
const extensionAnswered: [string, string][] = [
   ["select: * on the extension", "run: ext -> { select: * }"],
   ["index: * on the extension", "run: ext -> { index: * }"],
   [
      "the parent's measures, one over a private column, on the extension",
      "run: ext -> { aggregate: total, n, secret_total }",
   ],
   [
      "the extension's public fields over hidden ones",
      "run: ext -> { group_by: org_upper; aggregate: org_count, mine_total }",
   ],
   [
      "an inherited internal column through a caller extend",
      "run: ext extend { dimension: o is org_id } -> { group_by: o; aggregate: total }",
   ],
   [
      "an inherited internal column lifted by a caller include",
      "run: ext include { public: region_code, total } -> { group_by: region_code; aggregate: total }",
   ],
   [
      "a dimension over an intermediate's internal column",
      "run: ext2 -> { group_by: tier_label; aggregate: total, secret_total }",
   ],
   ["select: * on the second extension", "run: ext2 -> { select: * }"],
   [
      "a measure over a column the extension's include makes private, and the parent's dimension over it",
      "run: narrowed -> { group_by: tier is amount > 20; aggregate: region_count, total }",
   ],
   ["select: * on the narrowed extension", "run: narrowed -> { select: * }"],
];

// Hidden stored columns beside an author's rename: timestamps, so a
// timeframe field is stored, and a regions table for an inlined join.
const renameSeed = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'acme', 'east', 10.0, TIMESTAMP '2026-01-03 10:00:00'),
      (2, 'acme', 'west', 20.0, TIMESTAMP '2026-02-11 11:00:00'),
      (3, 'globex', 'east', 30.0, TIMESTAMP '2026-02-20 09:00:00'),
      (4, 'globex', 'west', 40.0, TIMESTAMP '2026-03-05 16:00:00'),
      (5, 'initech', 'east', 50.0, TIMESTAMP '2026-03-28 08:00:00')
    ) AS t(id, org_id, region, amount, ordered_at)`,
   `CREATE TABLE regions AS SELECT * FROM (VALUES
      ('east', 'East'), ('west', 'West')
    ) AS t(code, label)`,
];

const renameBase = `##! experimental { access_modifiers }

source: regions is duckdb.table('regions') extend { primary_key: code }

source: tbase is duckdb.table('orders') extend {
  internal dimension: order_month is ordered_at.month
}

source: cbase is duckdb.table('orders') include { internal: org_id; public: * }
`;

const renameModel = `##! experimental { access_modifiers }
import "base.malloy"

#(materialize) storage="lake"
source: renamed is tbase extend {
  rename: amt is amount
  measure: total is amt.sum()
  view: by_month is { group_by: order_month; aggregate: total }
}

#(materialize) storage="lake" inline=["r"]
source: renamed_join is duckdb.table('orders') extend {
  rename: amt is amount
  internal join_one: r is regions with region
  dimension: region_label is r.label
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: beside is duckdb.table('orders') extend {
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: renamed_hidden is cbase extend {
  rename: amt is amount
  measure: total is amt.sum()
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

async function expectParity(
   fixture: R4Fixture,
   malloy: string,
   answers: boolean,
   hiddenValues: RegExp = HIDDEN_VALUES,
): Promise<void> {
   fixture.setBindings([]);
   let live: Row[] | undefined;
   const liveError = await failure(async () => {
      live = (await fixture.runAsIs(malloy)).rows;
   });
   fixture.setBindings(fixture.bindings);
   let served: { rows: Row[]; servedFrom: string | null } | undefined;
   const servedError = await failure(async () => {
      served = await fixture.runAsIs(malloy);
   });
   if (!answers) {
      expect(liveError).toBeDefined();
      expect(servedError).toBeDefined();
      expect(String(servedError)).not.toMatch(hiddenValues);
      // The served copy's own compile refuses it too: no stored column is
      // left selectable for a fallback to hide.
      const copyError = await failure(async () => {
         const { runnable, virtualMap } = await fixture
            .servedCopy()
            .loadQuery(malloy, {});
         await runnable.getSQL({ virtualMap } as never);
      });
      expect(copyError).toBeDefined();
      return;
   }
   expect(liveError).toBeUndefined();
   expect(live?.length ?? 0).toBeGreaterThan(0);
   expect(servedError).toBeUndefined();
   expect(served?.servedFrom).toBe("storage");
   expect(rowDifference(live ?? [], served?.rows ?? [])).toBeUndefined();
}

/** One scenario: its package, what live refuses and answers, and its own checks. */
interface HiddenScenario {
   title: string;
   files: Record<string, string>;
   /** The seed tables, when not the shared {@link seedSql}. */
   seedSql?: string[];
   /** The hidden values a refusal must never carry, when not {@link HIDDEN_VALUES}. */
   hiddenValues?: RegExp;
   /** The test names' wording, when not the matrix's own. */
   phrases?: { refused: string; answered: string };
   refused: [string, string][];
   answered: [string, string][];
   /** Checks of the scenario's own, ahead of the parity rows. */
   before: (fixture: () => R4Fixture) => void;
   /** Checks of the scenario's own, after the parity rows. */
   after: (fixture: () => R4Fixture) => void;
}

const scenarios: HiddenScenario[] = [
   {
      title: "R4 P3 adversarial: a caller reads no hidden column from storage that live refuses",
      files: { "m.malloy": p3Model, "base.malloy": base },
      refused: p3Refused,
      answered: p3Answered,
      before: (get) => {
         it("re-roots both sources, with no problem", async () => {
            expect(await get().servedCopyProblems()).toEqual([]);
         });

         it("keeps hidden measures and views hidden on the served source", async () => {
            const def = (await get().servedCopy().compiledModelDef()) as {
               contents: Record<
                  string,
                  {
                     fields: {
                        name: string;
                        as?: string;
                        accessModifier?: string;
                     }[];
                  }
               >;
            };
            const access = Object.fromEntries(
               def.contents.orders.fields.map((f) => [
                  f.as ?? f.name,
                  f.accessModifier ?? "public",
               ]),
            );
            expect(access).toMatchObject({
               secret: "private",
               secret_m: "private",
               by_org: "internal",
               org_id: "internal",
               region_code: "internal",
               total: "public",
            });
            expect(access.id).toBeUndefined();
         });
      },
      after: (get) => {
         it("never lets a caller name the storage connection or its virtual source", async () => {
            const def = (await get().servedCopy().compiledModelDef()) as {
               contents: Record<string, { connection?: string; name?: string }>;
            };
            const { connection, name } = def.contents.orders;
            expect(connection).toMatch(/^__publisher_storage_/);
            for (const malloy of [
               `run: ${connection}.virtual('${name}')::orders__served -> { select: * }`,
               `run: ${connection}.virtual('${name}') -> { select: * }`,
               `run: ${connection}.table('orders') -> { select: * }`,
               `run: ${connection}.sql("select * from orders") -> { select: * }`,
            ]) {
               get().setBindings(get().bindings);
               const error = await failure(() => get().runAsIs(malloy));
               expect(error).toBeDefined();
               expect(String(error)).not.toMatch(HIDDEN_VALUES);
            }
         });
      },
   },
   {
      title: "R4 P3 with P10 adversarial: hidden fields on extension tables",
      files: { "m.malloy": extensionModel },
      refused: extensionRefused,
      answered: extensionAnswered,
      before: (get) => {
         it("builds each extension over its parent's stored table, in the destination", () => {
            expect(get().buildReads).toEqual({
               parent: [],
               ext: ["parent"],
               ext2: ["parent"],
               // An `include` hop is no extend: it builds over the raw chain.
               narrowed: [],
            });
            for (const name of ["ext", "ext2"]) {
               expect(get().storedBuildSQL[name]).toContain(
                  get().physicalTables.parent,
               );
               expect(get().storedBuildSQL[name]).not.toMatch(/\borders\b/);
            }
         });

         it("re-roots every source but the include-narrowed one, which fails closed", async () => {
            // `narrowed`'s base is `parent include { … }`, so no ancestor block is
            // kept; its re-root would lose the parent's fields, and it is left as
            // written (it reads the parent's re-root by name).
            expect(await get().servedCopyProblems()).toEqual([
               { reason: "fields_not_carried", sourceName: "narrowed" },
            ]);
         });

         it("stores the extension's selectable hidden fields, and never a field private on its parent", async () => {
            const columns = async (name: string) =>
               (
                  await get().storedSQL(
                     (t) => `DESCRIBE SELECT * FROM ${t(name)}`,
                  )
               ).map((c) => String(c.column_name));
            // The parent stores its own hidden fields (P3).
            expect(await columns("parent")).toEqual(
               expect.arrayContaining([
                  "id",
                  "org_id",
                  "region_code",
                  "secret",
               ]),
            );
            const ext = await columns("ext");
            expect(ext).toEqual(
               expect.arrayContaining(["org_id", "region_code", "mine"]),
            );
            // Private on the parent: the extension cannot read it, live or in its
            // build over the raw chain, so its chained build never stores it.
            expect(ext).not.toContain("id");
            expect(ext).not.toContain("secret");
            const ext2 = await columns("ext2");
            expect(ext2).toEqual(
               expect.arrayContaining(["org_id", "region_code", "tier"]),
            );
            expect(ext2).not.toContain("id");
            expect(ext2).not.toContain("secret");
         });

         it("keeps every field of each served extension exactly as hidden as the author's", async () => {
            const def = (await get().servedCopy().compiledModelDef()) as {
               contents: Record<
                  string,
                  {
                     fields: {
                        name: string;
                        as?: string;
                        accessModifier?: string;
                     }[];
                  }
               >;
            };
            const access = (name: string) =>
               Object.fromEntries(
                  def.contents[name].fields.map((f) => [
                     f.as ?? f.name,
                     f.accessModifier ?? "public",
                  ]),
               );
            expect(access("ext")).toMatchObject({
               org_id: "internal",
               region_code: "internal",
               secret: "private",
               secret_m: "private",
               mine: "private",
               org_upper: "public",
               total: "public",
            });
            // Left as written: the author's own include (its `public: *` lifts the
            // parent's internal fields, as live).
            expect(access("narrowed")).toMatchObject({
               region: "private",
               region_count: "public",
            });
            expect(access("ext2")).toMatchObject({
               tier: "internal",
               region_code: "internal",
               org_id: "internal",
               tier_label: "public",
            });
         });
      },
      after: () => {},
   },
   {
      // An author's `rename:` beside a stored hidden timeframe field or a
      // hidden inlined join: the re-root hides their stored columns with
      // labelled renames in its first block, not an `include`, since Malloy
      // refuses a `rename:` in the block just after an include and the served
      // copy (every source in the model) would not compile. Each source
      // answers from storage with live's rows, a source beside them too, and
      // every refusal holds. A hidden column kept under its own name still
      // needs the include, so that source alone is left live (known
      // limitation 25).
      title: "R4: hidden stored columns beside an author's rename",
      files: { "m.malloy": renameModel, "base.malloy": renameBase },
      seedSql: renameSeed,
      hiddenValues: /acme|globex|initech|2026-0[123]|East|West/,
      phrases: {
         refused: "from the served copy as from live",
         answered: "from storage with live's rows",
      },
      refused: [
         [
            "the internal timeframe field",
            "run: renamed -> { group_by: order_month }",
         ],
         [
            "its stored column through a caller extend",
            "run: renamed extend { dimension: x is order_month__stored } -> { group_by: x }",
         ],
         [
            "its stored column made public by a caller include",
            "run: renamed include { public: order_month__stored } -> { group_by: order_month__stored }",
         ],
         [
            "the internal join's field",
            "run: renamed_join -> { group_by: r.label }",
         ],
         [
            "the join's stored column through a caller extend",
            "run: renamed_join extend { dimension: x is r__label__stored } -> { group_by: x }",
         ],
      ],
      answered: [
         ["the source beside them", "run: beside -> { aggregate: total }"],
         ["the author's view over the timeframe", "run: renamed -> by_month"],
         [
            "a caller's extend over the internal timeframe field",
            "run: renamed extend { dimension: m is order_month } -> { group_by: m; aggregate: total }",
         ],
         [
            "the author's dimension over the internal join",
            "run: renamed_join -> { group_by: region_label; aggregate: total }",
         ],
         [
            "a caller's extend over the internal join",
            "run: renamed_join extend { dimension: l is r.label } -> { group_by: l; aggregate: total }",
         ],
      ],
      before: (get) => {
         // A hidden column kept under its own name needs the include, and the
         // inverse rename block would follow it, which Malloy refuses (known
         // limitation 25): that source alone is left live, never the whole copy.
         it("compiles the served copy with every other source re-rooted", async () => {
            expect(await get().servedCopyProblems()).toEqual([
               {
                  sourceName: "renamed_hidden",
                  reason: "definition_not_parsed",
               },
            ]);
         });

         it("answers the source it could not re-root live, with live's rows", async () => {
            const run = await get().run({
               name: "renamed_hidden",
               malloy:
                  "run: renamed_hidden -> { group_by: region; aggregate: total }",
            });
            expect(run.servedFrom).not.toBe("storage");
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      },
      after: () => {},
   },
];

for (const scenario of scenarios) {
   describe(scenario.title, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: scenario.files,
            seedSql: scenario.seedSql ?? seedSql,
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      scenario.before(() => fixture);

      const phrases = scenario.phrases ?? {
         refused: "as live does",
         answered: "with exactly live's rows",
      };

      for (const [name, malloy] of scenario.refused) {
         it(`refuses ${name}, ${phrases.refused}`, async () => {
            await expectParity(fixture, malloy, false, scenario.hiddenValues);
         });
      }

      for (const [name, malloy] of scenario.answered) {
         it(`answers ${name} ${phrases.answered}`, async () => {
            await expectParity(fixture, malloy, true, scenario.hiddenValues);
         });
      }

      scenario.after(() => fixture);
   });
}

/**
 * An inlined join_one (#1665) is served from its stored `<join>__<field>`
 * columns as a record. The joined source's hidden fields are never stored,
 * so the record cannot carry them. A join the author hides (known limitation
 * 24) is inlined too: its build reads the join through the lifted build copy,
 * and the served record keeps the join's own modifier, its stored columns
 * renamed `private`, so a caller reads `r.<field>` exactly where live lets it
 * (an internal join through its own `extend`, a private one never), while the
 * author's fields, measures and views over the join answer from storage.
 */
describe("R4 P3 adversarial: an inlined join_one's hidden fields and access", () => {
   const regionsSeed = [
      ...seedSql,
      `CREATE TABLE regions AS SELECT * FROM (VALUES
         ('east', 'East', 'acme-east', 5.0), ('west', 'West', 'globex-west', 7.0)
       ) AS t(code, label, owner, w)`,
   ];
   const inlineModel = `##! experimental { access_modifiers }

source: regions is duckdb.table('regions') include { internal: owner; public: * } extend {
  primary_key: code
}

#(materialize) storage="lake" inline=["r"]
source: by_region is duckdb.table('orders') extend {
  join_one: r is regions with region
  measure: total is amount.sum()
}

#(materialize) storage="lake" inline=["r"]
source: hidden_join is duckdb.table('orders') extend {
  internal join_one: r is regions with region
  dimension: region_label is r.label
  measure: total is amount.sum(), labels is count(r.label), w_of_regions is r.w.sum()
  view: by_label is { group_by: r.label; aggregate: total }
}

#(materialize) storage="lake" inline=["r"]
source: private_join is duckdb.table('orders') extend {
  private join_one: r is regions with region
  dimension: region_label is r.label
  measure: total is amount.sum(), labels is count(r.label)
  view: by_label is { group_by: r.label; aggregate: total }
}
`;
   /** The joined values a refusal must never carry. */
   const JOINED = /acme-east|globex-west|East|West/;
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": inlineModel },
         seedSql: regionsSeed,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds every inlined join, hidden ones too, and never stores the joined source's internal field", async () => {
      const plans = (
         fixture.servedCopy() as unknown as {
            materializePlans: Record<string, { sourceName: string }>;
         }
      ).materializePlans;
      expect(
         Object.values(plans)
            .map((p) => p.sourceName)
            .sort(),
      ).toEqual(["by_region", "hidden_join", "private_join"]);
      for (const name of ["by_region", "hidden_join", "private_join"]) {
         const described = await fixture.storedSQL(
            (t) => `DESCRIBE SELECT * FROM ${t(name)}`,
         );
         const names = described.map((c) => String(c.column_name));
         expect(names).toContain("r__label");
         expect(names.some((n) => n.includes("owner"))).toBe(false);
      }
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   it("keeps each join's modifier on its record, and its stored columns private", async () => {
      const def = (await fixture.servedCopy().compiledModelDef()) as {
         contents: Record<
            string,
            { fields: { name: string; as?: string; accessModifier?: string }[] }
         >;
      };
      for (const [source, access] of [
         ["by_region", "public"],
         ["hidden_join", "internal"],
         ["private_join", "private"],
      ] as const) {
         const fields = Object.fromEntries(
            def.contents[source].fields.map((f) => [
               f.as ?? f.name,
               f.accessModifier ?? "public",
            ]),
         );
         expect(fields.r).toBe(access);
         if (access !== "public") {
            expect(fields.region_label).toBe("public");
            expect(fields.r__label__stored).toBe("private");
         }
      }
   });

   for (const [name, malloy] of [
      [
         "a public joined field",
         "run: by_region -> { group_by: r.label; aggregate: total }",
      ],
      [
         "the author's dimension over an internal join",
         "run: hidden_join -> { group_by: region_label; aggregate: total, labels }",
      ],
      [
         "the author's view over an internal join",
         "run: hidden_join -> by_label",
      ],
      [
         "a caller's own extend over an internal join, as live allows",
         "run: hidden_join extend { dimension: l is r.label } -> { group_by: l; aggregate: total }",
      ],
      [
         "a caller's include lifting an internal join, as live allows",
         "run: hidden_join include { public: r, total } -> { group_by: r.label; aggregate: total }",
      ],
      [
         "the author's dimension over a private join",
         "run: private_join -> { group_by: region_label; aggregate: total, labels }",
      ],
      [
         "the author's view over a private join",
         "run: private_join -> by_label",
      ],
   ] as const) {
      it(`answers ${name} from storage with live's rows`, async () => {
         await expectParity(fixture, malloy, true);
      });
   }

   // Declined as for a public join (#1665): the record would aggregate per
   // row, live per joined row. Answered live, with live's rows.
   it("answers the author's join-grain measure over an internal join live (COUNT_SEMANTICS)", async () => {
      const malloy =
         "run: hidden_join -> { group_by: region_label; aggregate: w_of_regions }";
      const run = await fixture.run({ name: "join grain", malloy });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(path?.considered?.map((c) => c.reason)).toContain(
         "COUNT_SEMANTICS",
      );
   });

   // Live answers each; the served copy cannot (a join-grain aggregate a
   // caller's own `extend` declares, a joined field the table does not
   // store), so each is answered live, with live's rows, never from storage.
   for (const [name, malloy] of [
      [
         "a caller's join-grain aggregate over an internal join",
         "run: hidden_join extend { measure: x is r.w.sum() } -> { aggregate: x }",
      ],
      [
         "the joined source's internal field through a caller extend",
         "run: hidden_join extend { dimension: o is r.owner } -> { group_by: o }",
      ],
   ] as const) {
      it(`answers ${name} live`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(run.servedVia).toBeUndefined();
         expect(run.servedFrom).not.toBe("storage");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   for (const [name, malloy] of [
      [
         "the joined source's internal field",
         "run: by_region -> { group_by: r.owner }",
      ],
      ["an internal join's field", "run: hidden_join -> { group_by: r.label }"],
      ["an internal join's record", "run: hidden_join -> { select: r }"],
      [
         "an internal join's field in a nest",
         "run: hidden_join -> { nest: x is { group_by: r.label } }",
      ],
      [
         "an internal join's field indexed",
         "run: hidden_join -> { index: r.label }",
      ],
      [
         "an internal join's stored column",
         "run: hidden_join -> { group_by: r__label__stored }",
      ],
      [
         "an internal join's stored column through a caller extend",
         "run: hidden_join extend { dimension: x is r__label__stored } -> { group_by: x }",
      ],
      [
         "an internal join's stored column under its stored name",
         "run: hidden_join extend { dimension: x is r__label } -> { group_by: x }",
      ],
      [
         "an internal join's stored column made public by a caller include",
         "run: hidden_join include { public: r__label__stored } -> { group_by: r__label__stored }",
      ],
      ["a private join's field", "run: private_join -> { group_by: r.label }"],
      [
         "a private join's field through a caller extend",
         "run: private_join extend { dimension: l is r.label } -> { group_by: l }",
      ],
      [
         "a private join lifted by a caller include",
         "run: private_join include { public: r } -> { group_by: r.label }",
      ],
      [
         "a private join after a caller's include wildcard",
         "run: private_join include { public: * } -> { group_by: r.label }",
      ],
      [
         "a private join through a caller join",
         "run: by_region extend { join_one: p is private_join on id = p.id } -> { group_by: p.r.label }",
      ],
      [
         "a private join's stored column through a caller extend",
         "run: private_join extend { dimension: x is r__label__stored } -> { group_by: x }",
      ],
      [
         "raw SQL naming a private join's stored column",
         'run: private_join -> { group_by: x is sql_string("r__label") }',
      ],
   ] as const) {
      it(`refuses ${name}, as live does`, async () => {
         fixture.setBindings([]);
         const live = await failure(() => fixture.runAsIs(malloy));
         fixture.setBindings(fixture.bindings);
         const served = await failure(() => fixture.runAsIs(malloy));
         expect(live).toBeDefined();
         expect(served).toBeDefined();
         expect(String(served)).not.toMatch(JOINED);
         const copy = await failure(async () => {
            const { runnable, virtualMap } = await fixture
               .servedCopy()
               .loadQuery(malloy, {});
            await runnable.getSQL({ virtualMap } as never);
         });
         expect(copy).toBeDefined();
      });
   }
});
