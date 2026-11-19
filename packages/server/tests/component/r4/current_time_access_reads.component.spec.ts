// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored relation that reads the current time is built as of its build
 * only when no access decision reads it: no source, query, view or stored
 * relation of the package that carries an `#(authorize)` or
 * `#(access_filter)` gate, or reads a given, depends on it, whether it joins
 * it (directly, through an alias, through another source's join, from
 * another file), extends it, derives from it with `->`, composes it, or is a
 * `#(materialize)` source, persisted view or persisted query over one that
 * does. Otherwise it is refused (`current_time`) and answered live, and so is
 * every access decision over it.
 *
 * Each case grants a document whose grant expires a little after the load.
 * After it expires, every answer leaves the document out, as live does; a
 * table as of its build would still hold the grant. Run on both tiers.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   loadFixture,
   type LoadedFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";

/** How far after the load the moving grant expires. */
const LEAD_MS = 25_000;

function files(moving: string, storage: boolean): Record<string, string> {
   const out: Record<string, string> = {
      // doc 10: granted forever; doc 20: granted until shortly after the
      // load; doc 30: the grant expired long ago.
      "data/grants.csv": `doc_id,user_id,expires_at\n10,1,2100-01-01 00:00:00\n20,1,${moving}\n30,1,2001-01-01 00:00:00\n`,
      "data/docs.csv": `id,org\n10,1\n20,1\n30,1\n`,
      "grants_lib.malloy": `
source: g_imp_raw is duckdb.sql("SELECT * FROM read_csv_auto('data/grants.csv')")
#@ persist name="g_imp"
source: g_imp is g_imp_raw -> { where: expires_at > now; select: doc_id, user_id }
`,
      "m.malloy": `##! experimental { givens composite_sources }
import "grants_lib.malloy"
given: USER :: number
given: ORG :: number

${[
   "s1",
   "s2",
   "s3",
   "s4",
   "s5",
   "s6",
   "s7",
   "s8",
   "s9",
   "t1",
   "t2",
   "t3",
   "t4",
   "t5",
   "t6",
   "t7",
   "t8",
   "t9",
   "t10",
   "t11",
]
   .map(
      (s) =>
         `source: graw_${s} is duckdb.sql("SELECT *, '${s}' AS tag FROM read_csv_auto('data/grants.csv')")\nsource: draw_${s} is duckdb.sql("SELECT *, '${s}' AS tag FROM read_csv_auto('data/docs.csv')")`,
   )
   .join("\n")}

// S1 control: grant table joined by a STORED gated source (should be refused)
#@ persist name="g_s1"
source: g_s1 is graw_s1 -> { where: expires_at > now; select: doc_id, user_id }
#(access_filter) g.user_id = $USER
#@ persist name="d_s1"
source: d_s1 is draw_s1 -> { select: id } extend { join_one: g is g_s1 on g.doc_id = id }

// S2: grant table joined by an UNSTORED gated source (documented limitation)
#@ persist name="g_s2"
source: g_s2 is graw_s2 -> { where: expires_at > now; select: doc_id, user_id }
#(access_filter) g.user_id = $USER
source: d_s2 is draw_s2 extend { join_one: g is g_s2 on g.doc_id = id }

// S3: stored gated source joins the grant table under another name (an extend alias)
#@ persist name="g_s3"
source: g_s3 is graw_s3 -> { where: expires_at > now; select: doc_id, user_id }
source: g_s3_alias is g_s3 extend {}
#(access_filter) g.user_id = $USER
#@ persist name="d_s3"
source: d_s3 is draw_s3 -> { select: id } extend { join_one: g is g_s3_alias on g.doc_id = id }

// S4: unstored gated source EXTENDS a stored relation whose build applied the grant expiry
#@ persist name="vis_s4"
source: vis_s4 is draw_s4 extend { join_one: g is graw_s4 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
#(access_filter) user_id = $USER
source: d_s4 is vis_s4 extend {}

// S5: STORED gated source extends that stored relation (gated one refused; is its live read frozen?)
#@ persist name="vis_s5"
source: vis_s5 is draw_s5 extend { join_one: g is graw_s5 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
#(access_filter) user_id = $USER
#@ persist name="d_s5"
source: d_s5 is vis_s5 extend {}

// S6: given-scoped (ungated) consumer reads visibility from a frozen grant table via a dimension
#@ persist name="g_s6"
source: g_s6 is graw_s6 -> { where: expires_at > now; select: doc_id, user_id }
source: d_s6 is draw_s6 extend {
  where: org = $ORG
  join_one: g is g_s6 on g.doc_id = id
  dimension: visible is g.user_id = $USER
}

// S7: grant table imported from another model, joined by a stored gated source
#(access_filter) g.user_id = $USER
#@ persist name="d_s7"
source: d_s7 is draw_s7 -> { select: id } extend { join_one: g is g_imp on g.doc_id = id }

// S8: relative filter string grant, unstored gated joiner
#@ persist name="g_s8"
source: g_s8 is graw_s8 -> { where: expires_at ~ f'after now'; select: doc_id, user_id }
#(access_filter) g.user_id = $USER
source: d_s8 is draw_s8 extend { join_one: g is g_s8 on g.doc_id = id }

// S9: gated joiner via a join chain: gated -> mid (unstored, ungated) -> stored grant table
#@ persist name="g_s9"
source: g_s9 is graw_s9 -> { where: expires_at > now; select: doc_id, user_id }
source: mid_s9 is draw_s9 extend { join_one: g is g_s9 on g.doc_id = id }
#(access_filter) m.g.user_id = $USER
#@ persist name="d_s9"
source: d_s9 is draw_s9 -> { select: id } extend { join_one: m is mid_s9 on m.id = id }
// S10: gated #(materialize) over an as-of-build stored relation
#@ persist name="vis_s10"
source: vis_s10 is draw_t1 extend { join_one: g is graw_t1 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
#(materialize)
#(access_filter) user_id = $USER
source: d_s10 is vis_s10 extend { measure: n is count() }

// S11: unstored gated query-derivation (->) of an as-of-build stored relation
#@ persist name="vis_s11"
source: vis_s11 is draw_t2 extend { join_one: g is graw_t2 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
#(access_filter) user_id = $USER
source: d_s11 is vis_s11 -> { select: * }

// S12: persisted named query over the S5 gated extension
#@ persist name="vis_s12"
source: vis_s12 is draw_t3 extend { join_one: g is graw_t3 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
#(access_filter) user_id = $USER
source: d_s12 is vis_s12 extend { measure: n is count() }
#@ persist
query: q_s12 is d_s12 -> { group_by: id }

// S13: gated source joins the as-of-build relation (visibility via a join, not an extend), gated joiner unstored
#@ persist name="vis_s13"
source: vis_s13 is draw_t4 extend { join_one: g is graw_t4 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
#(access_filter) v.user_id = $USER
source: d_s13 is draw_t5 extend { join_one: v is vis_s13 on v.id = id }

// S14: gated persisted view on an extension of the as-of-build relation
#@ persist name="vis_s14"
source: vis_s14 is draw_t6 extend { join_one: g is graw_t6 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
#(access_filter) user_id = $USER
source: d_s14 is vis_s14 extend {
  measure: n is count()
  #@ persist
  view: by_id is { group_by: id; aggregate: n }
}

// S15: a composite over the as-of-build relation, scoped by a given
#@ persist name="vis_s15"
source: vis_s15 is draw_t7 extend { join_one: g is graw_t7 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
source: c_s15 is compose(vis_s15 extend { dimension: k is 1 }, vis_s15)
source: d_s15 is c_s15 extend { where: user_id = $USER }

// A3: a model query reads a given over it.
#@ persist name="vis_a3"
source: vis_a3 is draw_t8 extend { join_one: g is graw_t8 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
query: q_a3 is vis_a3 -> { where: user_id = $USER; group_by: id; order_by: id }

// A4: a gated extension of a plain alias of it.
#@ persist name="vis_a4"
source: vis_a4 is draw_t9 extend { join_one: g is graw_t9 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
source: vis_a4_alias is vis_a4
#(access_filter) user_id = $USER
source: d_a4 is vis_a4_alias extend {}

// A7: a model query refines a view of it with a given.
#@ persist name="vis_a7"
source: vis_a7 is draw_t10 extend { join_one: g is graw_t10 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}
source: vis_a7v is vis_a7 extend { view: by_id is { group_by: id, user_id; order_by: id } }
query: q_a7 is vis_a7v -> by_id + { where: user_id = $USER }

// A9: no access decision of the model reads it, so it is stored as of its
// build; the caller's own query joins it and filters by a given.
#@ persist name="vis_a9"
source: vis_a9 is draw_t11 extend { join_one: g is graw_t11 on g.doc_id = id } -> {
  where: g.expires_at > now
  select: id, user_id is g.user_id
}`,
   };
   if (storage) {
      for (const k of Object.keys(out)) {
         out[k] = out[k].replace(
            /#@ persist name="([a-z0-9_]+)"/g,
            '#@ persist name="$1" storage="lake"',
         );
      }
   }
   return out;
}

/** Each access decision, the query it answers, and the relation it reads. */
const CASES: Record<
   string,
   {
      query: string;
      /** The relation refused at plan; absent: built, and answered live at read. */
      frozen?: string;
      what: string;
   }
> = {
   s1: {
      query: "run: d_s1 -> { group_by: id; order_by: id }",
      frozen: "g_s1",
      what: "a stored gated source joins it",
   },
   s2: {
      query: "run: d_s2 -> { group_by: id; order_by: id }",
      frozen: "g_s2",
      what: "an unstored gated source joins it",
   },
   s3: {
      query: "run: d_s3 -> { group_by: id; order_by: id }",
      frozen: "g_s3",
      what: "a gated source joins an alias of it",
   },
   s4: {
      query: "run: d_s4 -> { group_by: id; order_by: id }",
      frozen: "vis_s4",
      what: "an unstored gated source extends it",
   },
   s5: {
      query: "run: d_s5 -> { group_by: id; order_by: id }",
      frozen: "vis_s5",
      what: "a stored gated source extends it",
   },
   s6: {
      query: "run: d_s6 -> { where: visible; group_by: id; order_by: id }",
      frozen: "g_s6",
      what: "an ungated source reads a given through a join to it",
   },
   s7: {
      query: "run: d_s7 -> { group_by: id; order_by: id }",
      frozen: "g_imp",
      what: "a gated source joins it from another file",
   },
   s8: {
      query: "run: d_s8 -> { group_by: id; order_by: id }",
      frozen: "g_s8",
      what: "a gated source joins it, its window a relative filter string",
   },
   s9: {
      query: "run: d_s9 -> { group_by: id; order_by: id }",
      frozen: "g_s9",
      what: "a gated source reaches it through another source's join",
   },
   s10: {
      query: "run: d_s10 -> { group_by: id; order_by: id }",
      frozen: "vis_s10",
      what: "a gated #(materialize) source extends it",
   },
   s11: {
      query: "run: d_s11 -> { group_by: id; order_by: id }",
      frozen: "vis_s11",
      what: "a gated source derives from it with ->",
   },
   s12: {
      query: "run: q_s12",
      frozen: "vis_s12",
      what: "a persisted query reads a gated extension of it",
   },
   s12b: {
      query: "run: d_s12 -> { group_by: id; order_by: id }",
      frozen: "vis_s12",
      what: "a gated extension of it",
   },
   s13: {
      query: "run: d_s13 -> { group_by: id; order_by: id }",
      frozen: "vis_s13",
      what: "an unstored gated source joins it",
   },
   s14: {
      query: "run: d_s14 -> by_id",
      frozen: "vis_s14",
      what: "a persisted view of a gated extension of it",
   },
   s15: {
      query: "run: d_s15 -> { group_by: id; order_by: id }",
      frozen: "vis_s15",
      what: "a given-scoped extension of a composite over it",
   },
   a3: {
      query: "run: q_a3",
      frozen: "vis_a3",
      what: "a model query reads a given over it",
   },
   a4: {
      query: "run: d_a4 -> { group_by: id; order_by: id }",
      frozen: "vis_a4",
      what: "a gated extension of an alias of it",
   },
   a7: {
      query: "run: q_a7",
      frozen: "vis_a7",
      what: "a model query refines a view of it with a given",
   },
   a9: {
      query: "run: draw_t11 extend { join_one: v is vis_a9 on v.id = id } -> { where: v.user_id = $USER; group_by: id; order_by: id }",
      what: "the caller's own query joins it and filters by a given",
   },
};
const GIVENS = { USER: 1, ORG: 1 };

function ids(o: Outcome): number[] | string {
   if ("error" in o) return `ERROR ${o.error.slice(0, 200)}`;
   return o.answer.rows.map((r) => Number(r.id)).sort((x, y) => x - y);
}

for (const storage of [false, true]) {
   describe(`an access decision over a relation that reads the current time (${storage ? "storage=" : "database"})`, () => {
      let fixture: LoadedFixture;
      let movingAt: number;
      const live: Record<string, number[] | string> = {};
      useMaterializationOn();
      beforeAll(async () => {
         await freshLoadPool();
         movingAt = Math.ceil((Date.now() + LEAD_MS) / 1000) * 1000;
         const ts = new Date(movingAt)
            .toISOString()
            .replace("T", " ")
            .slice(0, 19);
         const loaded = await loadFixture(files(ts, storage));
         for (const [s, { query }] of Object.entries(CASES)) {
            live[s] = ids(await loaded.run("m.malloy", query, GIVENS));
         }
         fixture = await loaded.bind();
      }, 300_000);
      afterAll(async () => {
         await fixture?.dispose();
         await freshLoadPool();
      });

      it("refuses each relation an access decision reads, as current_time", () => {
         const refused = new Map(
            Object.values(fixture.pkg.getBuildPlan()?.refusedSources ?? {}).map(
               (r) => [r.name, r.reason],
            ),
         );
         // Every case at once, so a failure names each one that is wrong.
         const wrong = Object.entries(CASES).flatMap(([s, { frozen, what }]) =>
            frozen === undefined ||
            (refused.get(frozen) === "current_time" &&
               !fixture.built.some((b) => b.name === frozen))
               ? []
               : [
                    `${s} (${what}): ${frozen} is ${refused.get(frozen) ?? "built"}`,
                 ],
         );
         expect(wrong).toEqual([]);
      });

      it("stores a relation no access decision of the model reads, as of its build", () => {
         expect(fixture.built.some((b) => b.name === "vis_a9")).toBe(true);
      });

      it("leaves the expired grant out of every answer once it expires, as live does", async () => {
         expect(
            Object.entries(live).filter(
               ([, ids]) => JSON.stringify(ids) !== "[10,20]",
            ),
         ).toEqual([]);
         expect(Date.now()).toBeLessThan(movingAt);
         await Bun.sleep(Math.max(0, movingAt - Date.now()) + 2_000);
         const wrong: string[] = [];
         for (const [s, { query, what }] of Object.entries(CASES)) {
            const after = ids(await fixture.run("m.malloy", query, GIVENS));
            if (JSON.stringify(after) !== "[10]") {
               wrong.push(`${s} (${what}): ${JSON.stringify(after)}`);
            }
         }
         expect(wrong).toEqual([]);
         // The same table, read by a query that is no access decision,
         // answers as of its build: the expired grant is still there.
         const plain = await fixture.run(
            "m.malloy",
            "run: vis_a9 -> { group_by: id; order_by: id }",
         );
         expect(ids(plain)).toEqual([10, 20]);
         expect("answer" in plain && plain.answer.servedBy).toBe(
            "materialized",
         );
      }, 120_000);
   });
}
