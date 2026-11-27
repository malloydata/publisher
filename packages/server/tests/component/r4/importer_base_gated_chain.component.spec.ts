// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * N-06, gated chains: the four-file shape of
 * `importer_base_measures.component.spec.ts` with a control on the
 * intermediate source `trips_mid` in `mid.malloy`, which `materialize.malloy`
 * imports and re-roots `#(materialize) trips_2015` over.
 *
 * Resolving the base in the declaring file's scope must never carry a gate
 * into a table that serves without it. What each control does today:
 *
 * - `#(access_filter)` and a given-reading `where:` are not built; the source
 *   answers live, with each caller's rows.
 * - `#(authorize)` is built and served from the table, and the gate still
 *   denies a caller it does not admit.
 * - a plain `where:` is carried into the table: served equals live.
 *
 * `ext.malloy` imports `materialize.malloy` but does not surface the given, so
 * every gated variant is refused there rather than served.
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

const TRIPS =
   "id,start_date,station,duration\n" +
   "1,2014-06-01,a,10\n2,2015-02-01,a,20\n3,2015-07-01,b,5\n" +
   "4,2015-11-01,b,11\n5,2016-01-01,a,2\n6,2015-03-01,a,1\n";

const BIKES = `source: trips is duckdb.table('trips.csv') extend {
  measure: trip_count is count()
  measure: total_duration is duration.sum()
}
`;

const GIVENS = `##! experimental { persistence givens }\n`;

/** `mid.malloy`, with a control annotating `trips_mid` or inside its block. */
function mid(opts: { before?: string; inside?: string; given: boolean }) {
   return `${opts.given ? GIVENS : ""}import "bikes.malloy"
${opts.given ? "given: ST :: string\n" : ""}
${opts.before ?? ""}source: trips_mid is trips extend {
${opts.inside ?? ""}  measure: longest is duration.max()
}
`;
}

function materialize(given: boolean) {
   return `${given ? GIVENS : ""}import "mid.malloy"

#(materialize) name="trips_2015"
source: trips_2015 is trips_mid extend { where: start_date ? @2015 }
`;
}

const QUERY =
   "run: trips_2015 -> { group_by: station; aggregate: trip_count, total_duration, longest; order_by: station }";

const ROW_A = {
   station: "a",
   trip_count: 2,
   total_duration: "21",
   longest: "20",
};
const ROW_B = {
   station: "b",
   trip_count: 2,
   total_duration: "16",
   longest: "11",
};

/** Where an answer came from, and its rows (or its error). */
function answer(o: Outcome | undefined): { servedBy: string; rows: unknown } {
   if (!o) return { servedBy: "missing", rows: undefined };
   if ("error" in o) return { servedBy: "error", rows: o.error };
   return {
      servedBy: o.answer.via ?? o.answer.servedBy,
      rows: JSON.parse(
         JSON.stringify(o.answer.rows, (_k, v: unknown) =>
            typeof v === "bigint" ? v.toString() : v,
         ),
      ),
   };
}

/** One package per variant: live answers, the bind, and served answers. */
function chain(midText: string, given: boolean) {
   const state: {
      fixture?: LoadedFixture;
      live: Record<string, Outcome>;
      served: Record<string, Outcome>;
   } = { live: {}, served: {} };
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
   const callers: Record<string, Record<string, string> | undefined> = given
      ? { a: { ST: "a" }, b: { ST: "b" } }
      : { none: undefined };

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({
         "bikes.malloy": BIKES,
         "mid.malloy": midText,
         "materialize.malloy": materialize(given),
         "ext.malloy": `import "materialize.malloy"\n`,
         "trips.csv": TRIPS,
      });
      for (const model of ["materialize.malloy", "ext.malloy"]) {
         for (const [who, g] of Object.entries(callers)) {
            state.live[`${model} ${who}`] = await loader.run(model, QUERY, g);
         }
      }
      state.fixture = await loader.bind();
      for (const model of ["materialize.malloy", "ext.malloy"]) {
         for (const [who, g] of Object.entries(callers)) {
            state.served[`${model} ${who}`] = await state.fixture.run(
               model,
               QUERY,
               g,
            );
         }
      }
   }, 600_000);

   afterAll(async () => {
      await state.fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   return state;
}

describe("N-06 gated chain: #(access_filter) on the intermediate source", () => {
   const s = chain(
      mid({ before: "#(access_filter) station = $ST\n", given: true }),
      true,
   );

   it("is not built", () => {
      expect(s.fixture?.buildErrors).toEqual({});
      expect(s.fixture?.built.map((b) => b.name)).toEqual([]);
   });

   it("answers live through materialize.malloy, with each caller's rows", () => {
      expect(answer(s.served["materialize.malloy a"])).toEqual({
         servedBy: "live",
         rows: [ROW_A],
      });
      expect(answer(s.served["materialize.malloy b"])).toEqual({
         servedBy: "live",
         rows: [ROW_B],
      });
      expect(answer(s.served["materialize.malloy a"])).toEqual(
         answer(s.live["materialize.malloy a"]),
      );
   });

   it("is refused through ext.malloy, which does not surface the given", () => {
      for (const who of ["a", "b"]) {
         expect(answer(s.served[`ext.malloy ${who}`]).servedBy).toBe("error");
      }
   });
});

describe("N-06 gated chain: a given-reading where: on the intermediate source", () => {
   const s = chain(
      mid({ inside: "  where: station = $ST\n", given: true }),
      true,
   );

   it("is not built", () => {
      expect(s.fixture?.buildErrors).toEqual({});
      expect(s.fixture?.built.map((b) => b.name)).toEqual([]);
   });

   it("answers live through materialize.malloy, with each caller's rows", () => {
      expect(answer(s.served["materialize.malloy a"])).toEqual({
         servedBy: "live",
         rows: [ROW_A],
      });
      expect(answer(s.served["materialize.malloy b"])).toEqual({
         servedBy: "live",
         rows: [ROW_B],
      });
      expect(answer(s.served["materialize.malloy a"])).toEqual(
         answer(s.live["materialize.malloy a"]),
      );
   });

   it("is refused through ext.malloy, which does not surface the given", () => {
      for (const who of ["a", "b"]) {
         expect(answer(s.served[`ext.malloy ${who}`]).servedBy).toBe("error");
      }
   });
});

describe("N-06 gated chain: #(authorize) on the intermediate source", () => {
   const s = chain(
      mid({ before: "#(authorize) $ST = 'a'\n", given: true }),
      true,
   );

   it("is built", () => {
      expect(s.fixture?.buildErrors).toEqual({});
      expect(s.fixture?.built.map((b) => b.name)).toEqual(["trips_2015"]);
   });

   it("serves an admitted caller from the table through materialize.malloy, equal to live", () => {
      const before = answer(s.live["materialize.malloy a"]);
      expect(before).toEqual({ servedBy: "live", rows: [ROW_A, ROW_B] });
      expect(answer(s.served["materialize.malloy a"])).toEqual({
         servedBy: "served_copy",
         rows: before.rows,
      });
   });

   it("still denies ST=b through materialize.malloy", () => {
      const denied = answer(s.served["materialize.malloy b"]);
      expect(denied.servedBy).toBe("error");
      expect(String(denied.rows)).toContain("Access denied");
   });

   it("is refused through ext.malloy, which does not surface the given", () => {
      for (const who of ["a", "b"]) {
         const refused = answer(s.served[`ext.malloy ${who}`]);
         expect(refused.servedBy).toBe("error");
         expect(String(refused.rows)).toContain("Access denied");
      }
   });
});

describe("N-06 gated chain: a plain where: on the intermediate source", () => {
   const s = chain(
      mid({ inside: "  where: duration > 3\n", given: false }),
      false,
   );

   it("is built", () => {
      expect(s.fixture?.buildErrors).toEqual({});
      expect(s.fixture?.built.map((b) => b.name)).toEqual(["trips_2015"]);
   });

   for (const model of ["materialize.malloy", "ext.malloy"]) {
      it(`is carried: served from the table through ${model}, equal to live`, () => {
         const before = answer(s.live[`${model} none`]);
         // The where: drops trip 6 (duration 1), so station a counts one trip.
         expect(before).toEqual({
            servedBy: "live",
            rows: [
               {
                  station: "a",
                  trip_count: 1,
                  total_duration: "20",
                  longest: "20",
               },
               ROW_B,
            ],
         });
         expect(answer(s.served[`${model} none`])).toEqual({
            servedBy: "served_copy",
            rows: before.rows,
         });
      });
   }

   it("raises no served-copy problem in any model", () => {
      for (const model of ["materialize.malloy", "ext.malloy"]) {
         expect([
            model,
            s.fixture?.pkg.getModel(model)?.servedCopyProblems(),
         ]).toEqual([model, []]);
      }
   });
});
