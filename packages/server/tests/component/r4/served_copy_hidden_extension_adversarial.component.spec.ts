// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for P3 with P10, adversarial: hidden fields on extension tables. A
 * parent declares or hides fields (`private`, `internal`, in an `include` and
 * in its own `extend`); a `storage=` extension of it, built over the parent's
 * stored table (chained), reads one of them, is gated on one, and declares a
 * private field of its own; a second extension reaches the parent through an
 * unmaterialized source that hides a field of its own. Every way a caller
 * could reach a hidden column through the extension is run live and served:
 * the served answer must be live's own, refused exactly where live refuses,
 * and otherwise live's rows read from storage.
 *
 * The chained build re-roots the parent on its build source, whose columns
 * are plain (public) in the build copy; the extension still stores only the
 * hidden fields its own build copy over the raw chain could select, so a field
 * private on the parent is never in the extension's table.
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

const model = `##! experimental { access_modifiers }

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
const refused: [string, string][] = [
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
const answered: [string, string][] = [
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

describe("R4 P3 with P10 adversarial: hidden fields on extension tables", () => {
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

   it("builds each extension over its parent's stored table, in the destination", () => {
      expect(fixture.buildReads).toEqual({
         parent: [],
         ext: ["parent"],
         ext2: ["parent"],
         // An `include` hop is no extend: it builds over the raw chain.
         narrowed: [],
      });
      for (const name of ["ext", "ext2"]) {
         expect(fixture.storedBuildSQL[name]).toContain(
            fixture.physicalTables.parent,
         );
         expect(fixture.storedBuildSQL[name]).not.toMatch(/\borders\b/);
      }
   });

   it("re-roots every source but the include-narrowed one, which fails closed", async () => {
      // `narrowed`'s base is `parent include { … }`, so no ancestor block is
      // kept; its re-root would lose the parent's fields, and it is left as
      // written (it reads the parent's re-root by name).
      expect(await fixture.servedCopyProblems()).toEqual([
         { reason: "fields_not_carried", sourceName: "narrowed" },
      ]);
   });

   it("stores the extension's selectable hidden fields, and never a field private on its parent", async () => {
      const columns = async (name: string) =>
         (
            await fixture.storedSQL((t) => `DESCRIBE SELECT * FROM ${t(name)}`)
         ).map((c) => String(c.column_name));
      // The parent stores its own hidden fields (P3).
      expect(await columns("parent")).toEqual(
         expect.arrayContaining(["id", "org_id", "region_code", "secret"]),
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
      const def = (await fixture.servedCopy().compiledModelDef()) as {
         contents: Record<
            string,
            { fields: { name: string; as?: string; accessModifier?: string }[] }
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
});
