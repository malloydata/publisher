// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for P3, adversarial: with a `#(materialize)` source's private and
 * internal fields now in the stored table, every way a caller could reach a
 * hidden column is run live and served, and the served answer must be live's
 * own: refused exactly where live refuses, and otherwise live's rows, read
 * from storage. A shape that live answers (Malloy lets a caller's `extend` or
 * `include` read an `internal` field) must not answer more from storage.
 *
 * Raw SQL and the storage connection itself are covered too: restricted mode
 * refuses `sql_*`, `<conn>.table(…)` and `<conn>.sql(…)`, and a caller cannot
 * write `<conn>.virtual(…)`, so the stored table is reachable only through
 * the served copy's re-rooted source, which re-hides its hidden columns.
 *
 * A hidden source the queried model holds only through a join (#6643) is the
 * conformance shape "joined persisted source with hidden fields, from an
 * index": this harness plans `#(materialize)` in its entry file only.
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

const model = `##! experimental { access_modifiers }
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
const refused: [string, string][] = [
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
const answered: [string, string][] = [
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
      expect(String(servedError)).not.toMatch(HIDDEN_VALUES);
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
   expect(servedError).toBeUndefined();
   expect(served?.servedFrom).toBe("storage");
   expect(rowDifference(live ?? [], served?.rows ?? [])).toBeUndefined();
}

describe("R4 P3 adversarial: a caller reads no hidden column from storage that live refuses", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model, "base.malloy": base },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("re-roots both sources, with no problem", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   it("keeps hidden measures and views hidden on the served source", async () => {
      const def = (await fixture.servedCopy().compiledModelDef()) as {
         contents: Record<
            string,
            { fields: { name: string; as?: string; accessModifier?: string }[] }
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

   for (const [name, malloy] of refused) {
      it(`refuses ${name}, as live does`, async () => {
         await expectParity(fixture, malloy, false);
      });
   }

   for (const [name, malloy] of answered) {
      it(`answers ${name} with exactly live's rows`, async () => {
         await expectParity(fixture, malloy, true);
      });
   }

   it("never lets a caller name the storage connection or its virtual source", async () => {
      const def = (await fixture.servedCopy().compiledModelDef()) as {
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
         fixture.setBindings(fixture.bindings);
         const error = await failure(() => fixture.runAsIs(malloy));
         expect(error).toBeDefined();
         expect(String(error)).not.toMatch(HIDDEN_VALUES);
      }
   });
});

/**
 * An inlined join_one (#1665) is served from its stored `<join>__<field>`
 * columns as a record. The joined source's hidden fields are never stored,
 * so the record cannot carry them; and the record must keep the join's own
 * access, or the access check refuses the source and it is answered live.
 */
describe("R4 P3 adversarial: an inlined join_one's hidden fields and access", () => {
   const regionsSeed = [
      ...seedSql,
      `CREATE TABLE regions AS SELECT * FROM (VALUES
         ('east', 'East', 'acme-east'), ('west', 'West', 'globex-west')
       ) AS t(code, label, owner)`,
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
  measure: total is amount.sum()
}
`;
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

   it("never stores the joined source's internal field", async () => {
      const described = await fixture.storedSQL(
         (t) => `DESCRIBE SELECT * FROM ${t("by_region")}`,
      );
      const names = described.map((c) => String(c.column_name));
      expect(names).toContain("r__label");
      expect(names.some((n) => n.includes("owner"))).toBe(false);
   });

   it("answers a public joined field from storage with live's rows", async () => {
      await expectParity(
         fixture,
         "run: by_region -> { group_by: r.label; aggregate: total }",
         true,
      );
   });

   for (const [name, malloy] of [
      [
         "the joined source's internal field",
         "run: by_region -> { group_by: r.owner }",
      ],
      ["an internal join's field", "run: hidden_join -> { group_by: r.label }"],
   ] as const) {
      it(`refuses ${name}, as live does`, async () => {
         fixture.setBindings([]);
         const live = await failure(() => fixture.runAsIs(malloy));
         fixture.setBindings(fixture.bindings);
         const served = await failure(() => fixture.runAsIs(malloy));
         expect(live).toBeDefined();
         expect(served).toBeDefined();
         expect(String(served)).not.toMatch(/acme-east|globex-west/);
      });
   }

   it("refuses to build an internal inlined join, so it serves live", async () => {
      // Refused at plan time (`materialize-not-built`): its build could not
      // read the join, and its record would not carry the join's access.
      const plans = (
         fixture.servedCopy() as unknown as {
            materializePlans: Record<string, { sourceName: string }>;
         }
      ).materializePlans;
      expect(Object.values(plans).map((p) => p.sourceName)).toEqual([
         "by_region",
      ]);
      fixture.setBindings(fixture.bindings);
      const run = await fixture.runAsIs(
         "run: hidden_join -> { group_by: region_label; aggregate: total }",
      );
      expect(run.servedFrom).not.toBe("storage");
   });
});
