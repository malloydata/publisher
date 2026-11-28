// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An entry file that reaches a stored base only through a file it imports.
 *
 * `activity.malloy` stores `activity_events`; `eng.malloy` imports it and
 * declares `org_activity is activity_events extend { … }`; `index.malloy`
 * imports only `eng.malloy` (gtme's published surface). Malloy imports are
 * not transitive and record no edge for an extension of a table-backed
 * source, so `index.malloy`'s compile holds no node for `activity_events`.
 * The served copy re-rooted nothing there, so every query answered live with
 * no warning, and the package warned that `activity_events` was unreachable
 * from the published surface. The declared base, read from `eng.malloy`'s
 * text, is now re-rooted in its own file and proven through the sources
 * `index.malloy` names over it.
 *
 * Also gtme's fuller shape: a stored query-shaped join target and a second,
 * unstored source over the same base with the same join, in its own file.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";

const ACTIVITY = "id,user_email,kind\n1,a@x,a\n2,a@x,b\n3,b@x,a\n4,c@x,a\n";
const USERS =
   "email,is_prospect_user,name\na@x,true,A\nb@x,false,B\nc@x,true,C\n";

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

const SHAPES: {
   label: string;
   files: Record<string, string>;
   queries: Record<string, string>;
}[] = [];
for (const tier of ["storage=lake", ""]) {
   SHAPES.push({
      label: `an unstored extension over a ${tier ? "lake" : "database"} table`,
      files: {
         "activity.malloy": `#(materialize) name="activity_t" ${tier}\nsource: activity_events is duckdb.table('activity.csv')\n`,
         "eng.malloy": `import "activity.malloy"\nsource: org_activity is activity_events extend { measure: n is count() }\n`,
         "index.malloy": `import "eng.malloy"\nexport { org_activity }\n`,
      },
      queries: {
         "org_activity by kind":
            "run: org_activity -> { group_by: kind; aggregate: n; order_by: kind }",
      },
   });
}
SHAPES.push({
   label: "gtme's shape: a stored join target, and two files over the base",
   files: {
      "users.malloy": `#@ persist name="gtm_prospect_users" storage=lake
source: prospect_users is duckdb.table('users.csv') -> { select: email, is_prospect_user, name }
`,
      "activity.malloy": `#(materialize) name="activity_t" storage=lake
source: activity_events is duckdb.table('activity.csv')
`,
      "org_domains.malloy": `import 'activity.malloy'
import 'users.malloy'

source: org_domains is activity_events extend {
  join_one: prospect_users on user_email = prospect_users.email
  measure: n is count()
  where: prospect_users.is_prospect_user
}
`,
      "org_engagement.malloy": `import 'activity.malloy'
import 'users.malloy'

source: org_activity is activity_events extend {
  join_one: prospect_users on user_email = prospect_users.email
  measure: n is count()
}
`,
      "index.malloy": `import "org_domains.malloy"\nimport "org_engagement.malloy"\nexport { org_domains, org_activity }\n`,
   },
   queries: {
      "org_domains by kind":
         "run: org_domains -> { group_by: kind; aggregate: n; order_by: kind }",
      "org_domains by joined name":
         "run: org_domains -> { group_by: prospect_users.name; aggregate: n; order_by: name }",
      "org_activity by kind":
         "run: org_activity -> { group_by: kind; aggregate: n; order_by: kind }",
      "org_activity by joined name":
         "run: org_activity -> { group_by: prospect_users.name; aggregate: n; order_by: name }",
   },
});

// A stored base that filters its rows (its own where:, or a stored
// intermediate's) stays answered live through such an entry, never denied.
const FILTERED: { label: string; files: Record<string, string> }[] = [
   {
      label: "a stored base with its own where:",
      files: {
         "activity.malloy": `#(materialize) name="activity_t" storage=lake\nsource: activity_events is duckdb.table('activity.csv') extend { where: kind != 'c' }\n`,
         "eng.malloy": `import "activity.malloy"\nsource: org_activity is activity_events extend { measure: n is count() }\n`,
         "index.malloy": `import "eng.malloy"\nexport { org_activity }\n`,
      },
   },
   {
      label: "a stored intermediate with its own where:",
      files: {
         "activity.malloy": `#(materialize) name="activity_t" storage=lake\nsource: activity_events is duckdb.table('activity.csv')\n`,
         "mid.malloy": `import "activity.malloy"\n#(materialize) name="mid_t" storage=lake\nsource: mid is activity_events extend { where: kind = 'a' }\n`,
         "eng.malloy": `import "mid.malloy"\nsource: org_activity is mid extend { measure: n is count() }\n`,
         "index.malloy": `import "eng.malloy"\nexport { org_activity }\n`,
      },
   },
];
for (const shape of FILTERED) {
   describe(`an entry file reaching a filtered stored base through an import: ${shape.label}`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      let fixture: LoadedFixture | undefined;
      const q =
         "run: org_activity -> { group_by: kind; aggregate: n; order_by: kind }";
      let live: Outcome | undefined;
      let served: Outcome | undefined;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({
            ...shape.files,
            "activity.csv": ACTIVITY,
         });
         live = await loader.run("index.malloy", q);
         fixture = await loader.bind();
         served = await fixture.run("index.malloy", q);
      }, 600_000);
      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });
      it("answers with the filtered rows, as live, never denied", () => {
         const before = answer(live);
         expect(before.servedBy).toBe("live");
         expect(answer(served).rows).toBe(before.rows);
      });
   });
}

for (const shape of SHAPES) {
   describe(`an entry file reaching a stored base through an import: ${shape.label}`, () => {
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
         for (const [name, q] of Object.entries(shape.queries)) {
            live[name] = await loader.run("index.malloy", q);
         }
         fixture = await loader.bind();
         for (const [name, q] of Object.entries(shape.queries)) {
            served[name] = await fixture.run("index.malloy", q);
         }
      }, 600_000);

      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      for (const name of Object.keys(shape.queries)) {
         it(`serves ${name} through index.malloy from the tables, equal to live`, () => {
            const before = answer(live[name]);
            expect(before.servedBy).toBe("live");
            expect(answer(served[name])).toEqual({
               servedBy: "served_copy",
               rows: before.rows,
            });
         });
      }

      it("warns no source unreachable, and index.malloy's copy leaves nothing as written", () => {
         const codes = (
            (fixture?.pkg.getPackageMetadata().warnings ?? []) as {
               code?: string;
               subject?: string;
            }[]
         ).filter(
            (w) =>
               w.code === "materialization-unreachable" ||
               w.code === "served-copy-not-served",
         );
         expect(codes).toEqual([]);
         expect(
            fixture?.pkg.getModel("index.malloy")?.servedCopyProblems(),
         ).toEqual([]);
      });
   });
}
