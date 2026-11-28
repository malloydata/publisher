// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored extension whose base, declared in another file, joins a source
 * that file imports (gtme follow-up to #2066).
 *
 * `users.malloy` stores `users`; `base.malloy` imports it and declares
 * `activity_base`, unstored, joining `users`; `activity.malloy` imports
 * `base.malloy` and stores `activity is activity_base extend { … }`;
 * `ext.malloy` imports `activity.malloy`. `users` is no name in
 * `activity.malloy` (Malloy imports are not transitive), so the re-root,
 * which compiles `activity_base`'s kept block in `activity.malloy`, imports it
 * there selectively. Before, the block was not carried (`fields_not_carried`)
 * and every query answered live.
 *
 * Also: where `activity.malloy` declares a `users` of its own, the block is
 * not carried (the join would read the wrong table), and a file that cannot
 * name an unstored declarer is not warned that its join crosses connections.
 *
 * Through a real package (plan, build, bind, each model's served copy, the
 * router), on DuckDB.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";

const ACTIVITY = "id,user_id,kind\n1,1,a\n2,1,b\n3,2,a\n";
const USERS = "id,name\n1,x\n2,y\n";
const EVIL = "id,name\n1,evil\n2,evil\n";
const STORED_USERS = `#(materialize) name="users_t" storage=lake
source: users is duckdb.table('users.csv')
`;
const BASE = `import "users.malloy"

source: activity_base is duckdb.table('activity.csv') extend {
  join_one: users on user_id = users.id
  measure: n is count()
}
`;
const ACTIVITY_OVER = (head: string) => `${head}
#(materialize) name="activity_t" storage=lake
source: activity is activity_base extend { dimension: k2 is kind }
`;

const QUERIES = {
   "no join":
      "run: activity -> { group_by: kind; aggregate: n; order_by: kind }",
   "the join":
      "run: activity -> { group_by: users.name; aggregate: n; order_by: name }",
};

function answer(o: Outcome | undefined): { servedBy: string; rows: string } {
   if (!o) return { servedBy: "missing", rows: "" };
   if ("error" in o) return { servedBy: "error", rows: o.error };
   return {
      servedBy: o.answer.via ?? o.answer.servedBy,
      rows: JSON.stringify(o.answer.rows, (_k, v: unknown) =>
         typeof v === "bigint" ? Number(v) : v,
      ),
   };
}

interface Run {
   live: Record<string, Outcome>;
   served: Record<string, Outcome>;
   fixture: LoadedFixture;
   dispose: () => Promise<void>;
}

async function runShape(
   files: Record<string, string>,
   via: readonly string[],
): Promise<Run> {
   await freshLoadPool();
   const loader = await loadFixture({
      ...files,
      "activity.csv": ACTIVITY,
      "users.csv": USERS,
      "evil.csv": EVIL,
   });
   const live: Record<string, Outcome> = {};
   const served: Record<string, Outcome> = {};
   for (const model of via) {
      for (const [name, q] of Object.entries(QUERIES)) {
         live[`${model} ${name}`] = await loader.run(model, q);
      }
   }
   const fixture = await loader.bind();
   for (const model of via) {
      for (const [name, q] of Object.entries(QUERIES)) {
         served[`${model} ${name}`] = await fixture.run(model, q);
      }
   }
   return {
      live,
      served,
      fixture,
      dispose: async () => {
         await fixture.dispose();
         await loader.dispose().catch(() => {});
      },
   };
}

const reasons = (run: Run | undefined, model: string) =>
   (
      (run?.fixture.pkg.getModel(model)?.servedCopyProblems() ?? []) as {
         sourceName: string;
         reason: string;
      }[]
   ).map((p) => `${p.sourceName}: ${p.reason}`);

describe("a stored extension of a base in another file that joins a source that file imports", () => {
   let restore: () => void;
   beforeAll(() => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
   });
   afterAll(async () => {
      restore?.();
      await freshLoadPool();
   });

   describe("the join target stored", () => {
      const VIA = ["activity.malloy", "ext.malloy"];
      let run: Run | undefined;
      beforeAll(async () => {
         run = await runShape(
            {
               "users.malloy": STORED_USERS,
               "base.malloy": BASE,
               "activity.malloy": ACTIVITY_OVER(`import "base.malloy"`),
               "ext.malloy": `import "activity.malloy"\n`,
            },
            VIA,
         );
      }, 600_000);
      afterAll(async () => run?.dispose());

      it("built both tables", () => {
         expect(run?.fixture.buildErrors).toEqual({});
         expect(run?.fixture.built.map((b) => b.name).sort()).toEqual([
            "activity",
            "users",
         ]);
      });

      for (const model of VIA) {
         for (const name of Object.keys(QUERIES)) {
            it(`serves a query using ${name} through ${model} from the tables, equal to live`, () => {
               const before = answer(run?.live[`${model} ${name}`]);
               expect(before.servedBy).toBe("live");
               expect(answer(run?.served[`${model} ${name}`])).toEqual({
                  servedBy: "served_copy",
                  rows: before.rows,
               });
            });
         }
      }

      it("warns only the files that can name the unstored base", () => {
         // `activity_base` is a name in activity.malloy (a whole import of
         // base.malloy), and a query of it with the join answers live there.
         expect(reasons(run, "activity.malloy")).toEqual([
            "activity_base: cross_connection_join",
         ]);
         expect(reasons(run, "ext.malloy")).toEqual([]);
      });
   });

   describe("the stored source's own file declares another source by the joined name", () => {
      let run: Run | undefined;
      beforeAll(async () => {
         run = await runShape(
            {
               "users.malloy": STORED_USERS,
               "base.malloy": BASE,
               "activity.malloy": ACTIVITY_OVER(
                  `import "base.malloy"\nsource: users is duckdb.table('evil.csv')`,
               ),
            },
            ["activity.malloy"],
         );
      }, 600_000);
      afterAll(async () => run?.dispose());

      it("does not carry the base's join, so no query reads the other source", () => {
         expect(reasons(run, "activity.malloy")).toContain(
            "activity: fields_not_carried",
         );
         for (const name of Object.keys(QUERIES)) {
            const before = answer(run?.live[`activity.malloy ${name}`]);
            expect(before.rows).not.toContain("evil");
            expect(answer(run?.served[`activity.malloy ${name}`])).toEqual(
               before,
            );
         }
      });
   });

   describe("an unstored source the served compile leaves out whole, which the entry file names", () => {
      // Its `where:` reads the join, so the join cannot be left out alone:
      // the whole declaration is (gtme's org_domains, reached by index.malloy).
      let run: Run | undefined;
      beforeAll(async () => {
         run = await runShape(
            {
               "users.malloy": STORED_USERS,
               "activity.malloy": `import "users.malloy"

source: activity is duckdb.table('activity.csv') extend {
  join_one: users on user_id = users.id
  measure: n is count()
  where: users.name = 'x'
}
`,
               "entry.malloy": `import "activity.malloy"\nimport "users.malloy"\n`,
            },
            ["entry.malloy"],
         );
      }, 600_000);
      afterAll(async () => run?.dispose());

      it("is still warned there, where a caller can name it", () => {
         expect(reasons(run, "entry.malloy")).toEqual([
            "activity: cross_connection_join",
         ]);
         for (const name of Object.keys(QUERIES)) {
            expect(answer(run?.served[`entry.malloy ${name}`])).toEqual(
               answer(run?.live[`entry.malloy ${name}`]),
            );
         }
      });
   });

   describe("a base whose join reads a source through a SQL block's %{ } embed", () => {
      // Its file binds `lookup` to users.csv; the stored source's file, which
      // imports the base selectively, declares a `lookup` of its own.
      let run: Run | undefined;
      beforeAll(async () => {
         run = await runShape(
            {
               "base.malloy": `source: lookup is duckdb.table('users.csv')

source: activity_base is duckdb.table('activity.csv') extend {
  join_one: users is duckdb.sql("""select * from %{ lookup -> { select: * } }""") on user_id = users.id
  measure: n is count()
}
`,
               "activity.malloy": `import { activity_base } from "base.malloy"
source: lookup is duckdb.table('evil.csv')

#(materialize) name="activity_t"
source: activity is activity_base extend { dimension: k2 is kind }
`,
            },
            ["activity.malloy"],
         );
      }, 600_000);
      afterAll(async () => run?.dispose());

      it("does not carry the block, so no query reads the other source", () => {
         expect(reasons(run, "activity.malloy")).toContain(
            "activity: fields_not_carried",
         );
         for (const name of Object.keys(QUERIES)) {
            const before = answer(run?.live[`activity.malloy ${name}`]);
            expect(before.rows).not.toContain("evil");
            expect(answer(run?.served[`activity.malloy ${name}`])).toEqual(
               before,
            );
         }
      });
   });

   describe("an unstored source reached only through a named query the served compile leaves out whole", () => {
      let run: Run | undefined;
      beforeAll(async () => {
         run = await runShape(
            {
               "users.malloy": STORED_USERS,
               "mid.malloy": `import "users.malloy"

source: d is duckdb.table('activity.csv') extend {
  join_one: users on user_id = users.id
  measure: n is count()
}
`,
               "q.malloy": `import "mid.malloy"

query: by_name is d -> { group_by: users.name; aggregate: n; order_by: name }
`,
               "own.malloy": `import "q.malloy"\nimport "users.malloy"\n`,
            },
            [],
         );
      }, 600_000);
      afterAll(async () => run?.dispose());

      it("keeps the warning where a caller can run that query", () => {
         expect(reasons(run, "own.malloy")).toEqual([
            "d: cross_connection_join",
         ]);
      });
   });

   describe("the unstored base declared beside the stored source", () => {
      const ACT = `${BASE}
#(materialize) name="activity_t" storage=lake
source: activity is activity_base extend { dimension: k2 is kind }
`;
      for (const [label, ext, warned] of [
         [
            "a selective import of the stored source",
            `import { activity } from "activity.malloy"\n`,
            false,
         ],
         [
            "a whole import, which names the base too",
            `import "activity.malloy"\n`,
            true,
         ],
      ] as const) {
         describe(`through ${label}`, () => {
            let run: Run | undefined;
            beforeAll(async () => {
               run = await runShape(
                  {
                     "users.malloy": STORED_USERS,
                     "activity.malloy": ACT,
                     "ext.malloy": ext,
                  },
                  ["ext.malloy"],
               );
            }, 600_000);
            afterAll(async () => run?.dispose());

            it("serves both queries from the tables, equal to live", () => {
               for (const name of Object.keys(QUERIES)) {
                  const before = answer(run?.live[`ext.malloy ${name}`]);
                  expect(answer(run?.served[`ext.malloy ${name}`])).toEqual({
                     servedBy: "served_copy",
                     rows: before.rows,
                  });
               }
            });

            it(
               warned
                  ? "warns the importing file that the base's join crosses connections"
                  : "does not warn the importing file of a base it cannot name",
               () => {
                  expect(reasons(run, "activity.malloy")).toEqual([
                     "activity_base: cross_connection_join",
                  ]);
                  expect(reasons(run, "ext.malloy")).toEqual(
                     warned ? ["activity_base: cross_connection_join"] : [],
                  );
               },
            );
         });
      }
   });
});
