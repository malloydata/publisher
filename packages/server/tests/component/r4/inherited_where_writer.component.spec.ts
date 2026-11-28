// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The inherited-filter guard checks an inherited `where:` against the
 * declaration that wrote it.
 *
 * Its walk up a source's derivation stops at the last ancestor the model's
 * compile holds. Malloy imports are not transitive, so an importing file
 * often holds an extension but not the base whose `where:` it inherits; the
 * walk could also follow a wrong link to a neighbour declared beside the
 * source. The guard then compared the condition against that ancestor, which
 * only inherited it, so a redefinition the ancestor shared with the executed
 * source proved nothing:
 *
 *  - a bare rename of an extension that redefines the field the `where:`
 *    reads, in a file that does not import the base's file, was answered
 *    without the filter;
 *  - a caller's own inline redefinition of that field was answered without
 *    the filter when the walk landed on a neighbour that redefines it, and
 *    an honest query on the source was denied there.
 *
 * Every case here answers live (no table is bound). Honest renames and
 * extensions keep answering.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";

const EVENTS = "deal_id,event_type\n1,x\n2,clarify:delete\n3,x\n4,x\n5,x\n";
const REDEFINE = `except: is_deleted\n  dimension: is_deleted is false`;
const count = (s: string) => `run: ${s} -> { aggregate: n is count() }`;

function answer(o: Outcome | undefined): string {
   if (!o) return "missing";
   // The reason after the source name (INHERITED_FILTER_UNVERIFIED) is not asserted here.
   if ("error" in o)
      return o.error.replace(
         /^(Access denied for source "[^"]*")[\s\S]*$/,
         "$1.",
      );
   return JSON.stringify(o.answer.rows, (_k, v: unknown) =>
      typeof v === "bigint" ? Number(v) : v,
   );
}
const DENIED = (s: string) => `Access denied for source "${s}".`;

describe("the inherited-filter guard checks an inherited where: against the declaration that wrote it", () => {
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

   describe("a chain across files, each importing only the one before", () => {
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      beforeAll(async () => {
         await freshLoadPool();
         loader = await loadFixture({
            "base.malloy": `source: t is duckdb.table('events.csv') extend {
  dimension: is_deleted is event_type = 'clarify:delete'
  where: not is_deleted
}
`,
            "mid.malloy": `import "base.malloy"
source: t2 is t extend { measure: c is count() }
source: t2_renamed is t
`,
            "redefines.malloy": `import "mid.malloy"
source: t3 is t2 extend {
  ${REDEFINE}
}
source: t3_honest is t2 extend { dimension: k is event_type }
source: t3_renamed is t2_renamed
`,
            "far.malloy": `import "redefines.malloy"
source: t4 is t3
source: t5 is t3 extend { measure: z is count() }
source: t4_honest is t3_honest
source: t5_honest is t3_renamed extend { dimension: q is 1 }
query: q_honest is t3_honest -> { aggregate: n is count() }
`,
            "events.csv": EVENTS,
         });
      }, 120_000);
      afterAll(async () => loader?.dispose());

      it("denies a bare rename of the redefining source where the base's file is not imported", async () => {
         expect(answer(await loader?.run("far.malloy", count("t4")))).toBe(
            DENIED("t4"),
         );
         expect(answer(await loader?.run("far.malloy", count("t5")))).toBe(
            DENIED("t5"),
         );
         expect(
            answer(await loader?.run("redefines.malloy", count("t3"))),
         ).toBe(DENIED("t3"));
      });

      it("keeps answering honest renames, extensions and queries, filtered", async () => {
         for (const s of [
            "t4_honest",
            "t5_honest",
            "t3_honest",
            "t3_renamed",
         ]) {
            expect([
               s,
               answer(await loader?.run("far.malloy", count(s))),
            ]).toEqual([s, '[{"n":4}]']);
         }
         expect(answer(await loader?.run("far.malloy", "run: q_honest"))).toBe(
            '[{"n":4}]',
         );
         expect(
            answer(
               await loader?.run(
                  "far.malloy",
                  "run: t4_honest extend { where: event_type = 'x' } -> { aggregate: n is count() }",
               ),
            ),
         ).toBe('[{"n":4}]');
      });

      it("denies a caller's own redefinition of an honest source there", async () => {
         for (const s of ["t4_honest", "t5_honest"]) {
            expect(
               answer(
                  await loader?.run(
                     "far.malloy",
                     `run: ${s} extend { ${REDEFINE} } -> { aggregate: n is count() }`,
                  ),
               ),
            ).toBe(DENIED(s));
         }
      });
   });

   describe("a redefining source and its aliases, through files that do and do not import the base's", () => {
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      const DEALS = `source: deal_events is duckdb.table('events.csv')
source: deals is deal_events extend {
  dimension: is_deleted is event_type = 'clarify:delete'
  where: not is_deleted
}
`;
      beforeAll(async () => {
         await freshLoadPool();
         loader = await loadFixture({
            "deals.malloy": DEALS,
            "rebound.malloy": `import "deals.malloy"
source: rebound_deals is deals extend {
  ${REDEFINE}
}
source: rebound_alias is rebound_deals
`,
            "alias.malloy": `import "rebound.malloy"
source: alias_of_rebound is rebound_deals
`,
            "entry.malloy": `import "deals.malloy"\nimport "rebound.malloy"\nimport "alias.malloy"\n`,
            "events.csv": EVENTS,
         });
      }, 120_000);
      afterAll(async () => loader?.dispose());

      for (const model of ["rebound.malloy", "alias.malloy", "entry.malloy"]) {
         it(`denies every one of them through ${model}`, async () => {
            for (const s of [
               "rebound_deals",
               "rebound_alias",
               "alias_of_rebound",
            ]) {
               const got = answer(await loader?.run(model, count(s)));
               if (got.includes("undefined object")) continue;
               expect([s, got]).toEqual([s, DENIED(s)]);
            }
         });
      }
   });

   describe("a caller's inline redefinition, beside a neighbour that redefines the field", () => {
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      beforeAll(async () => {
         await freshLoadPool();
         loader = await loadFixture({
            "deals.malloy": `##! experimental.access_modifiers
source: deal_events is duckdb.table('events.csv')
source: deal_activity is deal_events -> {
  group_by: deal_id
  aggregate: delete_event_count is count() { where: event_type = 'clarify:delete' }
}
source: deal_progress is deal_events extend {
  join_one: deal_activity on deal_id = deal_activity.deal_id
} -> {
  group_by:
    deal_id
    is_deleted is deal_activity.delete_event_count > 0
}

#(materialize) name="gtm_deals_including_deleted" storage=lake
source: deals_including_deleted is deal_progress
include {
  public: deal_id, is_deleted
}
extend {
  measure: deal_count is count()
}

source: deals is deals_including_deleted extend {
  where: not is_deleted
}
`,
            "funnel.malloy": `##! experimental.access_modifiers
import 'deals.malloy'
source: funnel_deals is deals extend { measure: c3 is count() }
source: funnel_rebound is deals extend {
  ${REDEFINE}
}
`,
            "entry.malloy": `##! experimental.access_modifiers
import 'deals.malloy'
import 'funnel.malloy'
`,
            "events.csv": EVENTS,
         });
      }, 120_000);
      afterAll(async () => loader?.dispose());

      for (const model of ["entry.malloy", "funnel.malloy"]) {
         it(`denies it, and answers the honest query, through ${model}`, async () => {
            expect(
               answer(
                  await loader?.run(
                     model,
                     `run: funnel_deals extend { ${REDEFINE} } -> { aggregate: n is count() }`,
                  ),
               ),
            ).toBe(DENIED("funnel_deals"));
            expect(
               answer(await loader?.run(model, count("funnel_deals"))),
            ).toBe('[{"n":4}]');
         });
      }
   });
   describe("on the served copy of a stored base, through files that do and do not import the base's", () => {
      const STORED = `##! experimental.access_modifiers
source: deal_events is duckdb.table('events.csv')
source: deal_progress is deal_events -> {
  group_by: deal_id
  aggregate: delete_count is count() { where: event_type = 'clarify:delete' }
} extend {
  dimension: is_deleted is delete_count > 0
}

#(materialize) name="iww_deals_including_deleted" storage=lake
source: deals_including_deleted is deal_progress
include {
  public: deal_id, is_deleted
}

source: deals is deals_including_deleted extend {
  where: not is_deleted
}
`;
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      let served:
         | Awaited<ReturnType<Awaited<ReturnType<typeof loadFixture>>["bind"]>>
         | undefined;
      beforeAll(async () => {
         await freshLoadPool();
         loader = await loadFixture({
            "deals.malloy": STORED,
            "rebound.malloy": `##! experimental.access_modifiers
import 'deals.malloy'
source: rebound_deals is deals extend {
  ${REDEFINE}
}
source: rebound_alias is rebound_deals
source: honest is deals extend { measure: m is count() }
`,
            "alias.malloy": `##! experimental.access_modifiers
import 'rebound.malloy'
source: alias_of_rebound is rebound_deals
source: alias_of_honest is honest
`,
            "entry.malloy": `##! experimental.access_modifiers
import 'deals.malloy'
import 'rebound.malloy'
import 'alias.malloy'
`,
            "events.csv": EVENTS,
         });
         served = await loader.bind();
      }, 120_000);
      afterAll(async () => {
         await served?.dispose();
         await loader?.dispose();
      });

      for (const model of ["alias.malloy", "entry.malloy"]) {
         it(`denies the redefining aliases and a caller's redefinition, and answers the honest alias, through ${model}`, async () => {
            for (const [route, run] of [
               ["live", (m: string, q: string) => loader?.run(m, q)],
               ["served", (m: string, q: string) => served?.run(m, q)],
            ] as const) {
               for (const s of ["rebound_alias", "alias_of_rebound"]) {
                  expect([
                     route,
                     s,
                     answer(await run(model, count(s))),
                  ]).toEqual([route, s, DENIED(s)]);
               }
               expect([
                  route,
                  answer(
                     await run(
                        model,
                        `run: alias_of_honest extend { ${REDEFINE} } -> { aggregate: n is count() }`,
                     ),
                  ),
               ]).toEqual([route, DENIED("alias_of_honest")]);
               expect([
                  route,
                  answer(await run(model, count("alias_of_honest"))),
               ]).toEqual([route, '[{"n":4}]']);
               expect([
                  route,
                  answer(
                     await run(
                        model,
                        `source: mine is alias_of_honest extend { measure: z is count() }\nrun: mine -> { aggregate: n is count() }`,
                     ),
                  ),
               ]).toEqual([route, '[{"n":4}]']);
               expect([
                  route,
                  answer(
                     await run(
                        model,
                        `source: mine is alias_of_honest extend { ${REDEFINE} }\nrun: mine -> { aggregate: n is count() }`,
                     ),
                  ),
               ]).toEqual([route, DENIED("mine")]);
            }
         });
      }
   });
   describe("a source declared in the request text, over a table-backed source whose where: another file writes", () => {
      const H = "##! experimental.access_modifiers";
      const N = 40;
      const files: Record<string, string> = {
         "s.malloy": `${H}
source: raw is duckdb.table('events.csv')
source: deals is raw extend {
  dimension: is_deleted is event_type = 'clarify:delete'
  where: not is_deleted
}
source: two is deals extend { measure: c is count() }
source: two_rebound is deals extend {
  ${REDEFINE}
}
`,
         "u.malloy": `${H}
import 's.malloy'
source: three is deals extend { measure: c3 is count() }
`,
         "t_sel.malloy": `${H}
import { two, two_rebound } from 's.malloy'
`,
         "t_chain.malloy": `${H}
import 'u.malloy'
source: four is three extend { measure: c4 is count() }
`,
         "events.csv": EVENTS,
      };
      // A chain of files, each importing only the one before.
      files["f0.malloy"] = `${H}
import 's.malloy'
source: s0 is deals extend { measure: m0 is count() }
`;
      for (let i = 1; i < N; i++) {
         files[`f${i}.malloy`] = `${H}
import 'f${i - 1}.malloy'
source: s${i} is s${i - 1} extend { measure: m${i} is count() }
`;
      }
      const request = (base: string, body: string, run = "") =>
         `source: mine is ${base} extend { ${body} }\nrun: mine -> { aggregate: n is ${run}count() }`;
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      beforeAll(async () => {
         await freshLoadPool();
         loader = await loadFixture(files);
      }, 120_000);
      afterAll(async () => loader?.dispose());

      it("answers an honest one, filtered, through a selective import and a chain of files", async () => {
         for (const [model, q] of [
            ["t_sel.malloy", request("two", "measure: z is count()")],
            ["t_chain.malloy", request("three", "measure: z is count()")],
            ["t_chain.malloy", request("four", "measure: z is count()")],
            [
               "t_chain.malloy",
               request(
                  "four",
                  "join_one: o is four on deal_id = o.deal_id",
                  "o.",
               ),
            ],
         ]) {
            expect([q, answer(await loader?.run(model, q))]).toEqual([
               q,
               '[{"n":4}]',
            ]);
         }
      });

      it("denies one that redefines the field, even beside a neighbour that redefines it the same way", async () => {
         for (const [model, base] of [
            ["t_sel.malloy", "two"],
            ["t_chain.malloy", "four"],
         ]) {
            expect(
               answer(await loader?.run(model, request(base, REDEFINE))),
            ).toBe(DENIED("mine"));
         }
         expect(
            answer(await loader?.run("t_sel.malloy", count("two_rebound"))),
         ).toBe(DENIED("two_rebound"));
      });

      it(`proves a source ${N} files down a chain of imports`, async () => {
         const last = `f${N - 1}.malloy`;
         expect(answer(await loader?.run(last, count(`s${N - 1}`)))).toBe(
            '[{"n":4}]',
         );
         expect(
            answer(
               await loader?.run(
                  last,
                  `run: s${N - 1} extend { ${REDEFINE} } -> { aggregate: n is count() }`,
               ),
            ),
         ).toBe(DENIED(`s${N - 1}`));
      });
   });
});
