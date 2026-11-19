// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which stored relations that read the current time build as of their
 * build, and which are still refused (`current_time`), through the real
 * package path with PUBLISHER_MATERIALIZE on. Ungated v1 `#@ persist`
 * sources, persisted views and persisted named queries build, serve from
 * their tables and carry a `persist-reads-current-time` warning. A source
 * with its own row-level gate, a persisted view over a gated source, and a
 * grant table a gated source joins are refused, and answer live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   loadFixture,
   type LoadedFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";

const day = (offset: number) =>
   new Date(Date.now() + offset * 86_400_000).toISOString().slice(0, 10);

const ROWS = `id,org,ts\n1,1,${day(-3)} 12:00:00\n2,2,${day(-3)} 12:00:00\n3,1,2001-01-01 00:00:00\n`;

const MODEL = `##! experimental { givens }
given: ORG :: number

source: ev is duckdb.sql("SELECT id, org, ts FROM read_csv_auto('data/ev.csv')")

#@ persist name="recent"
source: recent is ev -> { where: ts ~ f'last 7 days'; select: id, org }

#@ persist
query: recent_q is ev -> { where: ts ~ f'last 7 days'; group_by: id }

source: ev_views is ev extend {
  measure: n is count()
  #@ persist
  view: last7 is { where: ts ~ f'last 7 days'; group_by: id; aggregate: n }
}

#(access_filter) org = $ORG
#@ persist name="gated_recent"
source: gated_recent is ev -> { where: ts ~ f'last 7 days'; select: id, org } extend {}

#(access_filter) org = $ORG
source: gated_views is ev extend {
  measure: n is count()
  #@ persist
  view: last7 is { where: ts ~ f'last 7 days'; group_by: id; aggregate: n }
}

#@ persist name="grants"
source: grants is ev -> { where: ts ~ f'last 7 days'; select: id }

#(access_filter) g.id = $ORG
#@ persist name="visible"
source: visible is ev -> { select: id, org } extend {
  join_one: g is grants on g.id = id
}
`;

function answer(outcome: Outcome): { ids: number[]; servedBy: string } {
   if ("error" in outcome) throw new Error(outcome.error);
   return {
      ids: outcome.answer.rows.map((r) => Number(r.id)).sort(),
      servedBy: outcome.answer.servedBy,
   };
}

describe("stored relations that read the current time, by route", () => {
   let fixture: LoadedFixture;
   useMaterializationOn();
   beforeAll(async () => {
      await freshLoadPool();
      fixture = await (
         await loadFixture({ "data/ev.csv": ROWS, "m.malloy": MODEL })
      ).bind();
   }, 300_000);
   afterAll(async () => {
      await fixture?.dispose();
      await freshLoadPool();
   });

   const refusedReason = (name: string) =>
      Object.values(fixture.pkg.getBuildPlan()?.refusedSources ?? {}).find(
         (r) => r.name === name,
      )?.reason;
   const asOfBuild = (subject: string) =>
      fixture.warnings.find(
         (w) =>
            w.subject === subject && w.code === "persist-reads-current-time",
      );

   it("builds an ungated source, view and query, each with a warning", () => {
      for (const name of [
         "recent",
         "recent_q__query",
         "ev_views__last7__view",
      ]) {
         expect({ name, refused: refusedReason(name) }).toEqual({
            name,
            refused: undefined,
         });
      }
      expect(fixture.built.map((b) => b.name)).toEqual(
         expect.arrayContaining(["recent"]),
      );
      expect(asOfBuild("recent")?.message).toContain(
         "`where: ts ~ f'last 7 days'`",
      );
      expect(asOfBuild("recent_q")?.message).toContain('query "recent_q"');
      expect(asOfBuild("ev_views -> last7")?.message).toContain(
         'view "last7" of source "ev_views"',
      );
   });

   it("serves each from its table", async () => {
      expect(
         answer(
            await fixture.run("m.malloy", "run: recent -> { group_by: id }"),
         ),
      ).toEqual({ ids: [1, 2], servedBy: "materialized" });
      expect(
         answer(await fixture.run("m.malloy", "run: recent_q")).servedBy,
      ).toBe("query");
      expect(
         answer(await fixture.run("m.malloy", "run: ev_views -> last7"))
            .servedBy,
      ).toBe("view");
   });

   it("says so in the serve verdict", () => {
      const verdict = fixture.pkg
         .materialization()
         .serveVerdicts()
         .find((v) => v.sourceName === "recent");
      expect(verdict?.verdict).toBe("serves");
      expect(verdict?.message).toContain("as of its last build");
   });

   it("refuses a source with its own row-level gate as current_time, and answers it live", async () => {
      expect(refusedReason("gated_recent")).toBe("current_time");
      expect(asOfBuild("gated_recent")).toBeUndefined();
      expect(
         answer(
            await fixture.run(
               "m.malloy",
               "run: gated_recent -> { group_by: id }",
               { ORG: 1 },
            ),
         ),
      ).toEqual({ ids: [1], servedBy: "live" });
   });

   it("refuses a persisted view over a gated source as current_time", () => {
      const warning = fixture.warnings.find(
         (w) =>
            w.code === "persist-view-not-built" &&
            w.subject === "gated_views -> last7",
      );
      expect(warning?.message).toContain("reads the current time");
      expect(asOfBuild("gated_views -> last7")).toBeUndefined();
   });

   it("refuses a grant table a gated source joins as current_time", () => {
      expect(refusedReason("grants")).toBe("current_time");
      const warning = fixture.warnings.find(
         (w) => w.subject === "grants" && w.code === "persist-not-built",
      );
      expect(warning?.message).toContain("feed an access decision");
   });
});
