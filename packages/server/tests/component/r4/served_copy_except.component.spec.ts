// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P2-29 (F-08): an `except:` on a `#(materialize)` source names a column its
 * table never stores, so the re-root drops the item (`dropExceptItems`), as a
 * v1 re-root always has: the source is served from its table, its answers
 * equal live, the excluded column stays out of reach, and the served copy
 * reports no problem. Before, the re-root kept the item, Malloy refused it,
 * and every query answered live behind a `served-copy-not-served` warning.
 *
 * The same holds when the base declares the measures (the campaign's shape,
 * `orders_raw is <table> extend { measure: … }`): the compiled model records
 * no base for a table-backed extension that drops a field, so the re-root
 * reads the base its declaration names and keeps that base's block. Before,
 * the re-root lost the base's measures and the source answered live
 * (`fields_not_carried`).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'east', 'x', 10.5, 'm1'),
      (2, 'east', 'y', 20.0, 'm2'),
      (3, 'west', 'x', 5.5, 'm3'),
      (4, 'west', 'z', 11.0, 'm4'),
      (5, 'east', 'z', 2.0, 'm5')
    ) AS t(id, region, sub, amt, mark)`,
];

const model = `source: ev is duckdb.table('ev')

#(materialize) name="one_except"
source: one_except is ev extend {
  except: mark
}

#(materialize) name="two_except"
source: two_except is ev extend {
  except: mark, sub
  measure: total is amt.sum()
}

source: clipped_base is ev extend {
  except: mark
  measure: total is amt.sum()
}

#(materialize) name="clipped_base_mat"
source: clipped_root is clipped_base extend {
  where: amt > 3
}

// The base declares the measures (the campaign's shape): the re-root keeps
// its block, which the except: follows.
source: ev_measured is duckdb.table('ev') extend {
  measure: n_rows is count()
  measure: amt_total is amt.sum()
}

#(materialize) name="measured_except"
source: measured_except is ev_measured extend { except: mark }

#(materialize) name="clipped_ext"
source: clipped_ext is clipped_root extend {
  where: region = 'east'
}
`;

describe("P2-29: an except: on a #(materialize) source is served from its table", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   const served = [
      [
         "an except: of one column",
         "run: one_except -> { group_by: region, sub; aggregate: n is count() }",
      ],
      [
         "an except: of two columns",
         "run: two_except -> { group_by: region; aggregate: total }",
      ],
      [
         "an except: on a source whose base declares its measures",
         "run: measured_except -> { group_by: region, sub; aggregate: amt_total, n_rows }",
      ],
      [
         "an extension table whose base, in the same file, writes except:",
         "run: clipped_ext -> { group_by: sub; aggregate: total }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(name, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(run.servedBy?.kind).toBe("materialized");
         expect(run.servedFrom).not.toBeNull();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("keeps the excluded column out of reach, as live does", async () => {
      fixture.setBindings(fixture.bindings);
      await expect(
         fixture.runAsIs("run: one_except -> { group_by: mark }"),
      ).rejects.toThrow(/mark/);
      await expect(
         fixture.runAsIs("run: measured_except -> { group_by: mark }"),
      ).rejects.toThrow(/mark/);
   });

   it("raises no served-copy-not-served problem", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });
});

describe("F-08: an except: over a base in an imported file that declares the measures", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "x.malloy": `import "base.malloy"

#(materialize) name="imported_except"
source: imported_except is ev_measured extend { except: mark }
`,
            "base.malloy": `source: ev_measured is duckdb.table('ev') extend {
  measure: n_rows is count()
  measure: amt_total is amt.sum()
}
`,
         },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("is served from its table, equal to live", async () => {
      const run = await fixture.run({
         name: "imported",
         malloy:
            "run: imported_except -> { group_by: region; aggregate: amt_total, n_rows }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(run.servedBy?.kind).toBe("materialized");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });
});

describe("F-08: a kept base block that reads the excluded column", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: ev_east is duckdb.table('ev') extend {
  measure: amt_total is amt.sum()
  measure: east_amt is amt.sum() { where: region = 'east' }
}

#(materialize) name="east_except"
source: east_except is ev_east extend { except: region }
`,
         },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("answers that source live, equal to live, and names both real reasons", async () => {
      const run = await fixture.run({
         name: "east",
         malloy: "run: east_except -> { group_by: sub; aggregate: amt_total }",
      });
      expect(run.servedBy?.kind).toBe("live");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      const reasons = (await fixture.servedCopyProblems())
         .filter((p) => p.sourceName === "east_except")
         .map((p) => p.reason)
         .sort();
      expect(reasons).toEqual(["fields_not_carried", "reroot_not_compiled"]);
   });
});
