// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * N-06: a `#(materialize)` source whose base declares measures is served from
 * its table in every file that imports it, not only in the file that
 * declares it.
 *
 * The campaign's shape: `bikes.malloy` declares `trips is <table> extend {
 * measure: … }`, `materialize.malloy` imports it and declares
 * `#(materialize) source: trips_2015 is trips extend { where: … }`, and
 * `ext.malloy` imports `materialize.malloy`. Malloy records no base for an
 * extension of a table-backed source, so the re-root reads the base the
 * declaration names. It looked that name up in the importing model, which
 * holds neither the name nor the declaration (Malloy imports are not
 * transitive), lost the base's block, and answered live through `ext.malloy`
 * (`fields_not_carried`). The name is now read in the declaring file's own
 * scope, and the declaration taken from the package model that holds it.
 *
 * The four-file shape chains two such bases across files, which was answered
 * live even in the declaring file, and adds an extension in the importing
 * file that reads a base measure (which failed that file's copy,
 * `compile_failed`).
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
   "4,2015-11-01,b,11\n5,2016-01-01,a,2\n";

const BIKES = `source: trips is duckdb.table('trips.csv') extend {
  measure: trip_count is count()
  measure: total_duration is duration.sum()
}
`;

const MATERIALIZE = `import "bikes.malloy"

#(materialize) name="trips_2015"
source: trips_2015 is trips extend { where: start_date ? @2015 }
`;

const QUERY =
   "run: trips_2015 -> { group_by: station; aggregate: trip_count, total_duration; order_by: station }";

const SHAPES: {
   label: string;
   files: Record<string, string>;
   via: string[];
   query?: string;
}[] = [
   {
      label: "three files",
      files: {
         "bikes.malloy": BIKES,
         "materialize.malloy": MATERIALIZE,
         "ext.malloy": `import "materialize.malloy"\n`,
      },
      via: ["materialize.malloy", "ext.malloy"],
   },
   {
      label: "four files, the base two imports from the declaring file",
      files: {
         "bikes.malloy": BIKES,
         "mid.malloy": `import "bikes.malloy"

source: trips_mid is trips extend {
  measure: longest is duration.max()
}
`,
         "materialize.malloy": `import "mid.malloy"

#(materialize) name="trips_2015"
source: trips_2015 is trips_mid extend { where: start_date ? @2015 }
`,
         "ext.malloy": `import "materialize.malloy"

source: trips_ext is trips_2015 extend {
  measure: half_trips is trip_count / 2
}
`,
      },
      via: ["materialize.malloy", "ext.malloy"],
      query: QUERY.replace("total_duration;", "total_duration, longest;"),
   },
];

/** Where an answer came from, and its rows as text. */
function answer(o: Outcome | undefined): { servedBy: string; rows: string } {
   if (!o) return { servedBy: "missing", rows: "" };
   if ("error" in o) return { servedBy: "error", rows: o.error };
   return {
      servedBy: o.answer.via ?? o.answer.servedBy,
      rows: JSON.stringify(o.answer.rows, (_k, v: unknown) =>
         typeof v === "bigint" ? v.toString() : v,
      ),
   };
}

for (const shape of SHAPES) {
   describe(`N-06: a #(materialize) source over a measured base in an imported file (${shape.label})`, () => {
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
         loader = await loadFixture({ ...shape.files, "trips.csv": TRIPS });
         for (const model of shape.via) {
            live[model] = await loader.run(model, shape.query ?? QUERY);
         }
         fixture = await loader.bind();
         for (const model of shape.via) {
            served[model] = await fixture.run(model, shape.query ?? QUERY);
         }
      }, 600_000);

      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      it("built the table", () => {
         expect(fixture?.buildErrors).toEqual({});
         expect(fixture?.built.map((b) => b.name)).toEqual(["trips_2015"]);
      });

      for (const model of shape.via) {
         it(`is served from its table through ${model}, equal to live`, () => {
            const before = answer(live[model]);
            expect(before.servedBy).toBe("live");
            expect(answer(served[model])).toEqual({
               servedBy: "served_copy",
               rows: before.rows,
            });
         });
      }

      it("raises no served-copy problem in any model", () => {
         const warnings = (
            fixture?.pkg.getPackageMetadata().warnings ?? []
         ).filter((w) => w.code === "served-copy-not-served");
         expect(warnings).toEqual([]);
         for (const model of shape.via) {
            expect([
               model,
               fixture?.pkg.getModel(model)?.servedCopyProblems(),
            ]).toEqual([model, []]);
         }
      });
   });
}
