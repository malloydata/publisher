// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one placement rule (plan/declaration.ts), route by route, through the
 * real load, plan, build, bind and query path (`package_case.ts`):
 *
 *  - `storage=` written beside a route's key or inside it builds into the
 *    destination, and a query is served from it, equal to live;
 *  - a `storage=` that names nothing (empty, or not a name) is an invalid
 *    annotation on every route: the package fails to load, with the route's
 *    `*-invalid` code, and never builds the source in its own warehouse.
 */

import { describe, expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "../r4/conformance/data";
import { runCase, type GoldenCase } from "./package_case";

interface Served {
   query: string;
   switch: string;
   servedFrom: string | null;
   servedBy: string;
   equalToLive?: boolean;
}

interface Planned {
   wirePlan?: {
      sources: Record<string, { annotationFields?: Record<string, string> }>;
      refusedSources?: unknown;
   };
   loadWarnings: { code?: string; message?: string }[];
   loadError?: string;
}

const FLAGS = "##! experimental { persistence composite_sources }";

function packageOf(model: string): Record<string, string> {
   return {
      "m.malloy": `${FLAGS}\nimport "raw.malloy"\n\n${model}\n`,
      "raw.malloy": RAW_MODEL,
      ...DATA_FILES,
   };
}

async function run(
   name: string,
   model: string,
   queries: { name: string; malloy: string }[],
): Promise<{ plan: Planned; served: Served[] }> {
   const c: GoldenCase = {
      stem: `placement/${name}`,
      title: name,
      files: packageOf(model),
      entry: "m.malloy",
      queries,
   };
   const out = await runCase(c);
   const plan = JSON.parse(out["plan.json"] ?? "{}") as Planned;
   const serve = out["serve.json"]
      ? (JSON.parse(out["serve.json"]) as { queries: Served[] })
      : { queries: [] };
   return { plan, served: serve.queries.filter((q) => q.switch === "on") };
}

/** Each route, its flat and nested placements, and a query it serves. */
const ROUTES: {
   route: string;
   flat: string;
   nested: string;
   query: (source: string) => string;
}[] = [
   {
      route: "#@ persist",
      flat: `#@ persist storage="lake"
source: SRC is orders_raw -> { group_by: region; aggregate: n is count() }`,
      nested: `#@ persist { storage="lake" }
source: SRC is orders_raw -> { group_by: region; aggregate: n is count() }`,
      query: (s) => `run: ${s} -> { select: * }`,
   },
   {
      route: "a persisted view",
      flat: `source: SRC is orders_raw extend {
  measure: n is count()
  #@ persist storage="lake"
  view: v is { group_by: region; aggregate: n }
}`,
      nested: `source: SRC is orders_raw extend {
  measure: n is count()
  #@ persist { storage="lake" }
  view: v is { group_by: region; aggregate: n }
}`,
      query: (s) => `run: ${s} -> v`,
   },
   {
      route: "#@ preaggregate",
      flat: `source: SRC is orders_raw extend {
  #@ preaggregate grain="region" storage="lake"
  measure: total is amount.sum()
}`,
      nested: `source: SRC is orders_raw extend {
  #@ preaggregate { grain="region" storage="lake" }
  measure: total is amount.sum()
}`,
      query: (s) => `run: ${s} -> { group_by: region; aggregate: total }`,
   },
];

describe("one placement rule", () => {
   for (const r of ROUTES) {
      for (const form of ["flat", "nested"] as const) {
         it(`${r.route}, ${form}: builds into the destination and serves, equal to live`, async () => {
            const { served } = await run(
               `${r.route}-${form}`,
               r[form].replace("SRC", "src"),
               [{ name: "q", malloy: r.query("src") }],
            );
            expect(served).toHaveLength(1);
            expect({
               servedFrom: served[0].servedFrom,
               servedBy: served[0].servedBy,
               equalToLive: served[0].equalToLive,
            }).toEqual({
               servedFrom: "storage",
               servedBy: expect.not.stringMatching(/^live$/),
               equalToLive: true,
            });
         }, 120_000);
      }
   }

   it("#(materialize) and #(rollup) build into the destination and serve", async () => {
      const { served } = await run(
         "own-routes",
         `#(materialize) storage="lake"
source: mat is orders_raw extend { measure: n is count() }

source: agg is orders_raw extend {
  #(rollup) grain=["region"] storage="lake"
  measure: total is amount.sum()
}`,
         [
            {
               name: "mat",
               malloy: "run: mat -> { group_by: region; aggregate: n }",
            },
            {
               name: "agg",
               malloy: "run: agg -> { group_by: region; aggregate: total }",
            },
         ],
      );
      for (const q of served) {
         expect({ q: q.query, from: q.servedFrom, eq: q.equalToLive }).toEqual({
            q: q.query,
            from: "storage",
            eq: true,
         });
      }
   }, 120_000);

   /** An empty and a non-name destination, per route, beside a sound source. */
   const EMPTY: {
      route: string;
      code: string;
      declaration: (storage: string) => string;
   }[] = [
      {
         route: "#(materialize)",
         code: "materialize-invalid",
         declaration: (st) => `#(materialize) storage=${st}
source: bad is orders_raw extend { measure: n is count() }`,
      },
      {
         route: "a persisted view",
         code: "persist-view-invalid",
         declaration: (st) => `source: bad is orders_raw extend {
  measure: n is count()
  #@ persist storage=${st}
  view: v is { group_by: region; aggregate: n }
}`,
      },
      {
         route: "#@ preaggregate",
         code: "rollup-invalid",
         declaration: (st) => `source: bad is orders_raw extend {
  #@ preaggregate grain="region" storage=${st}
  measure: total is amount.sum()
}`,
      },
      {
         route: "#(rollup)",
         code: "rollup-invalid",
         declaration: (st) => `source: bad is orders_raw extend {
  #(rollup) grain=["region"] storage=${st}
  measure: total is amount.sum()
}`,
      },
   ];
   const SOUND = `#(materialize) storage="lake"
source: good is orders_raw extend { measure: n is count() }`;
   const GOOD_QUERY = [
      {
         name: "good",
         malloy: "run: good -> { group_by: region; aggregate: n }",
      },
   ];

   // A nested `storage=` wins whenever it is written: one that names nothing
   // is invalid, never falling through to a flat destination beside it.
   for (const [label, persist] of [
      ["empty", '#@ persist storage=""'],
      ["whitespace", '#@ persist storage="  "'],
      ["a list", '#@ persist storage=["lake"]'],
      ["a block", '#@ persist storage { at="lake" }'],
      [
         "a nested list over a flat name",
         '#@ persist { storage=["x"] } storage="lake"',
      ],
      [
         "a nested block over a flat name",
         '#@ persist { storage { at="lake" } } storage="lake"',
      ],
   ] as const) {
      it(`#@ persist, storage ${label}: the load fails, persist-invalid`, async () => {
         const { plan } = await run(
            `persist-${label}`,
            `${persist}
source: bad is orders_raw -> { group_by: region; aggregate: n is count() }

${SOUND}`,
            GOOD_QUERY,
         );
         expect(plan.loadError).toContain('#@ persist source "bad"');
         expect(plan.loadError).toContain("[persist-invalid]");
      }, 120_000);
   }

   for (const e of EMPTY) {
      for (const [label, storage] of [
         ["empty", '""'],
         ["not a name", '["lake"]'],
      ] as const) {
         it(`${e.route}, storage ${label}: the load fails, ${e.code}`, async () => {
            const { plan } = await run(
               `${e.route}-${label}`,
               `${e.declaration(storage)}\n\n${SOUND}`,
               GOOD_QUERY,
            );
            expect(plan.loadError).toContain(`[${e.code}]`);
         }, 120_000);
      }
   }
});
