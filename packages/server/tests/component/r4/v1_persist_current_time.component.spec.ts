// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A v1 `#@ persist` whose build reads the current time, through the real
 * package path (load, plan, build, bind, serve). An ungated one builds like
 * any stored table and is served as of its build: its table holds the rows
 * as of the build, and the package carries a `persist-reads-current-time`
 * warning naming it and where it reads the clock. A neighbour whose `now` is
 * in its own extend block is applied at every read, so its table answers as
 * live, with no warning.
 *
 * One row's timestamp lies a little after the load. Every source is queried
 * before it passes and again after: a table as of its build still leaves it
 * out, while the live answer and a table read under its own `now` take it in.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   loadFixture,
   type LoadedFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";

/** How far after the load the moving row's timestamp lies. */
const LEAD_MS = 20_000;

const root = (x: string) =>
   `source: ev_${x} is duckdb.sql("SELECT id, ts, '${x}' AS tag FROM read_csv_auto('data/ev.csv')")`;

function files(movingTs: string): Record<string, string> {
   return {
      "data/ev.csv": `id,ts\n1,2001-01-01 00:00:00\n2,2100-01-01 00:00:00\n3,${movingTs}\n`,
      "bases.malloy": `
${root("imp")}
source: imp_base is ev_imp -> { where: ts < now; select: id, ts }
`,
      "now.malloy": `
import "bases.malloy"
${root("q")}
${root("qb")}
${root("qb2")}
${root("up")}
${root("jq")}
${root("jh")}
${root("st")}
${root("sq")}
${root("bw")}
${root("bd")}
${root("vw")}
${root("own")}
${root("sown")}
${root("plain")}
source: base_where is ev_bw extend { where: ts < now }
source: base_dim is ev_bd extend { dimension: past is ts < now }
source: base_view is ev_vw extend { view: recent is { where: ts < now; select: id, ts } }

#@ persist name="q_now"
source: q_now is ev_q -> { where: ts < now; select: id, ts }

#@ persist name="sq_now" storage="lake"
source: sq_now is ev_sq -> { where: ts < now; select: id, ts }

#@ persist name="base_where_now"
source: base_where_now is base_where -> { select: id, ts }

#@ persist name="base_dim_now"
source: base_dim_now is base_dim -> { select: id, ts, past }

#@ persist name="view_now"
source: view_now is base_view -> recent

source: qbase is ev_qb -> { where: ts < now; select: id, ts }
#@ persist name="qb_now"
source: qb_now is qbase -> { select: id, ts }

source: qbase2a is ev_qb2 -> { where: ts < now; select: id, ts }
source: qbase2 is qbase2a -> { select: * }
#@ persist name="qb2_now"
source: qb2_now is qbase2 extend { dimension: k is 1 }

#@ persist name="imp_now"
source: imp_now is imp_base -> { select: id, ts }

#@ persist name="up_now"
source: up_now is ev_up -> { where: ts < now; select: id, ts }
#@ persist name="down_now"
source: down_now is up_now -> { select: id, ts }

source: jq_base is ev_jq -> { where: ts < now; select: id }
source: jhost is ev_jh extend { join_one: r is jq_base on r.id = id }
#@ persist name="join_now"
source: join_now is jhost -> { where: r.id is not null; select: id, ts }

source: star_base is ev_st extend { dimension: past is ts < now }
#@ persist name="star_now"
source: star_now is star_base -> { select: * }

#@ persist name="own_now"
source: own_now is ev_own extend { where: ts < now }

#@ persist name="sown_now" storage="lake"
source: sown_now is ev_sown extend { where: ts < now }

#@ persist name="plain"
source: plain is ev_plain -> { select: id, ts }
`,
   };
}

const AS_OF_BUILD = [
   "q_now",
   "sq_now",
   "base_where_now",
   "base_dim_now",
   "view_now",
   "qb_now",
   "qb2_now",
   "imp_now",
   "up_now",
   "down_now",
   "join_now",
   "star_now",
];
const BUILT = ["own_now", "sown_now", "plain"];

/** The query each source is asked, and the ids it answers before and after the moving row's time. */
const QUERY: Record<string, string> = {
   base_dim_now:
      "run: base_dim_now -> { where: past; group_by: id; order_by: id }",
   star_now: "run: star_now -> { where: past; group_by: id; order_by: id }",
};
const query = (s: string) =>
   QUERY[s] ?? `run: ${s} -> { group_by: id; order_by: id }`;
const BEFORE = (s: string) => (s === "plain" ? [1, 2, 3] : [1]);
const AFTER = (s: string) =>
   s === "plain" ? [1, 2, 3] : AS_OF_BUILD.includes(s) ? [1] : [1, 3];

function ids(outcome: Outcome): { ids: number[]; servedBy: string } {
   if ("error" in outcome) throw new Error(outcome.error);
   return {
      ids: outcome.answer.rows.map((r) => Number(r.id)),
      servedBy: outcome.answer.servedBy,
   };
}

describe("v1 #@ persist over now, through a real package", () => {
   let restore: () => void;
   let fixture: LoadedFixture;
   let movingAt: number;
   const live: Record<string, number[]> = {};

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      await freshLoadPool();
      movingAt = Math.ceil((Date.now() + LEAD_MS) / 1000) * 1000;
      const ts = new Date(movingAt)
         .toISOString()
         .replace("T", " ")
         .slice(0, 19);
      const loaded = await loadFixture(files(ts));
      for (const s of [...AS_OF_BUILD, ...BUILT]) {
         live[s] = ids(await loaded.run("now.malloy", query(s))).ids;
      }
      fixture = await loaded.bind();
   }, 300_000);

   afterAll(async () => {
      await fixture?.dispose();
      restore();
      await freshLoadPool();
   });

   it("builds each source whose build reads now, and says its answers are as of its build", () => {
      const refused = Object.values(
         fixture.pkg.getBuildPlan()?.refusedSources ?? {},
      );
      for (const s of AS_OF_BUILD) {
         expect({ s, built: fixture.built.some((b) => b.name === s) }).toEqual({
            s,
            built: true,
         });
         expect(refused.find((r) => r.name === s)).toBeUndefined();
         const warning = fixture.warnings.find(
            (w) => w.subject === s && w.code === "persist-reads-current-time",
         );
         expect({ s, message: warning?.message }).toEqual({
            s,
            message: expect.stringContaining("as of its last build"),
         });
      }
      const q = fixture.warnings.find(
         (w) =>
            w.subject === "q_now" && w.code === "persist-reads-current-time",
      );
      expect(q?.message).toContain("`where: ts < now`");
      for (const s of BUILT) {
         expect(
            fixture.warnings.some(
               (w) =>
                  w.subject === s && w.code === "persist-reads-current-time",
            ),
         ).toBe(false);
      }
   });

   it("still builds a neighbour whose now is in its own extend block", () => {
      const refused = Object.values(
         fixture.pkg.getBuildPlan()?.refusedSources ?? {},
      );
      for (const s of BUILT) {
         expect(fixture.built.some((b) => b.name === s)).toBe(true);
         expect(refused.find((r) => r.name === s)).toBeUndefined();
      }
      expect(fixture.buildErrors).toEqual({});
   });

   it("answers every source as live, before and after the moving row's time", async () => {
      // The fixture must reach this point before the row's time, or the
      // "before" answers say nothing about a snapshot.
      expect(Date.now()).toBeLessThan(movingAt);
      const sources = [...AS_OF_BUILD, ...BUILT];
      const before: Record<string, ReturnType<typeof ids>> = {};
      for (const s of sources) {
         before[s] = ids(await fixture.run("now.malloy", query(s)));
      }
      await Bun.sleep(Math.max(0, movingAt - Date.now()) + 1_500);
      const after: Record<string, ReturnType<typeof ids>> = {};
      for (const s of sources) {
         after[s] = ids(await fixture.run("now.malloy", query(s)));
      }
      // Rows first: a table as of its build still answers [1] after the
      // moving row's time; one whose window is read at every read takes it in.
      for (const s of sources) {
         expect({ s, live: live[s] }).toEqual({ s, live: BEFORE(s) });
         expect({ s, before: before[s].ids }).toEqual({ s, before: BEFORE(s) });
         expect({ s, after: after[s].ids }).toEqual({ s, after: AFTER(s) });
      }
      for (const s of sources) {
         expect({ s, servedBy: before[s].servedBy }).toEqual({
            s,
            servedBy: "materialized",
         });
         expect({ s, servedBy: after[s].servedBy }).toEqual({
            s,
            servedBy: "materialized",
         });
      }
   }, 120_000);
});
