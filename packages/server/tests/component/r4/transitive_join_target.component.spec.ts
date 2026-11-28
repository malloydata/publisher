// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * S2 (gtme feedback): a stored source whose join target is declared in a
 * third file, which a file importing the stored source reaches only through
 * the stored source's own imports (Malloy imports are not transitive).
 *
 * `users.malloy` stores `users` in the lake; `activity.malloy` imports it and
 * stores `activity`, which joins `users`; `ext.malloy` imports only
 * `activity.malloy`, so `users` is no name of its own there. Both tables are
 * in one destination, so the join never crosses connections: every file
 * serves both a query that uses the join and one that does not from the
 * tables, equal to live, and no file is warned that the join crosses
 * connections. #2041 covered a base's measures and `where:` across files;
 * this covers a join target.
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

const SHAPES: { label: string; files: Record<string, string> }[] = [
   {
      label: "#(materialize)",
      files: {
         "users.malloy": `#(materialize) name="users_t" storage=lake
source: users is duckdb.table('users.csv')
`,
         "activity.malloy": `import "users.malloy"

#(materialize) name="activity_t" storage=lake
source: activity is duckdb.table('activity.csv') extend {
  join_one: users on user_id = users.id
  measure: n is count()
}
`,
         "ext.malloy": `import "activity.malloy"\n`,
      },
   },
   {
      label: "#@ persist",
      files: {
         "users.malloy": `#@ persist name="users_t" storage=lake
source: users is duckdb.table('users.csv') -> { select: id, name }
`,
         "activity.malloy": `import "users.malloy"

#@ persist name="activity_t" storage=lake
source: activity is duckdb.table('activity.csv') -> {
  select: id, user_id, kind
} extend {
  join_one: users on user_id = users.id
  measure: n is count()
}
`,
         "ext.malloy": `import "activity.malloy"\n`,
      },
   },
];

const QUERIES = {
   "no join":
      "run: activity -> { group_by: kind; aggregate: n; order_by: kind }",
   "the join":
      "run: activity -> { group_by: users.name; aggregate: n; order_by: name }",
};

const VIA = ["activity.malloy", "ext.malloy"];

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

for (const shape of SHAPES) {
   describe(`S2: a ${shape.label} source joining a source declared in a third file`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      let fixture: LoadedFixture | undefined;
      const live: Record<string, Outcome> = {};
      const served: Record<string, Outcome> = {};

      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({
            ...shape.files,
            "activity.csv": ACTIVITY,
            "users.csv": USERS,
         });
         for (const model of VIA) {
            for (const [name, q] of Object.entries(QUERIES)) {
               live[`${model} ${name}`] = await loader.run(model, q);
            }
         }
         fixture = await loader.bind();
         for (const model of VIA) {
            for (const [name, q] of Object.entries(QUERIES)) {
               served[`${model} ${name}`] = await fixture.run(model, q);
            }
         }
      }, 600_000);

      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      it("built both tables", () => {
         expect(fixture?.buildErrors).toEqual({});
         expect(fixture?.built.map((b) => b.name).sort()).toEqual([
            "activity",
            "users",
         ]);
      });

      for (const model of VIA) {
         for (const name of Object.keys(QUERIES)) {
            it(`serves a query using ${name} through ${model} from the tables, equal to live`, () => {
               const before = answer(live[`${model} ${name}`]);
               expect(before.servedBy).toBe("live");
               expect(answer(served[`${model} ${name}`])).toEqual({
                  servedBy: "served_copy",
                  rows: before.rows,
               });
            });
         }
      }

      it("warns no file that the join crosses connections", () => {
         const warnings = (
            (fixture?.pkg.getPackageMetadata().warnings ?? []) as {
               code?: string;
            }[]
         ).filter((w) => w.code === "served-copy-not-served");
         expect(warnings).toEqual([]);
         for (const model of VIA) {
            expect([
               model,
               fixture?.pkg.getModel(model)?.servedCopyProblems(),
            ]).toEqual([model, []]);
         }
      });
   });
}
