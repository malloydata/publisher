// Review probes (PR #1703, second review): v1 colocated tables under
// PUBLISHER_V1_SERVED_COPY=on (and today's `off` for comparison). Each query is
// answered live before any table is bound, then served, callers interleaved
// and repeated; the served answer must equal live per caller by the shadow's
// exact rule, and a refusal must stay a refusal.

import { afterAll, describe, expect, it } from "bun:test";
import { shadowRowDifference } from "../../../src/service/materialization/serve/v1_shadow";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   type Outcome,
   type Switch,
   withSwitch,
} from "./conformance/package_harness";

type Q = {
   name: string;
   model: string;
   malloy: string;
   givens?: Record<string, unknown>;
   /** The served run must read a table (default true unless live refuses). */
   stored?: boolean;
};

async function probe(
   sw: Switch,
   files: Record<string, string>,
   qs: Q[],
): Promise<{ live: Map<string, Outcome>; served: Map<string, Outcome> }> {
   const live = new Map<string, Outcome>();
   const served = new Map<string, Outcome>();
   await freshLoadPool();
   const restore = withSwitch(sw);
   const loader = await loadFixture({
      ...DATA_FILES,
      "raw.malloy": RAW_MODEL,
      ...files,
   });
   try {
      for (const q of qs)
         live.set(
            q.name,
            await loader.run(q.model, q.malloy, q.givens as never),
         );
      const f = await loader.bind();
      for (const round of [0, 1, 2]) {
         for (const q of round === 1 ? [...qs].reverse() : qs) {
            const out = await f.run(q.model, q.malloy, q.givens as never);
            const prev = served.get(q.name);
            if (!prev) served.set(q.name, out);
            else if ("answer" in prev && "answer" in out) {
               expect(
                  shadowRowDifference(prev.answer.rows, out.answer.rows),
               ).toBeUndefined();
            } else expect("error" in prev).toBe("error" in out);
         }
      }
      return { live, served };
   } finally {
      restore();
      await loader.dispose();
      await freshLoadPool();
   }
}

function check(
   r: { live: Map<string, Outcome>; served: Map<string, Outcome> },
   q: Q,
) {
   const l = r.live.get(q.name)!;
   const s = r.served.get(q.name)!;
   if ("error" in l) {
      expect("error" in s).toBe(true);
      return;
   }
   if ("error" in s) throw new Error(`served errored: ${s.error}`);
   expect(shadowRowDifference(l.answer.rows, s.answer.rows)).toBeUndefined();
   if (q.stored !== false) expect(s.answer.servedBy).not.toBe("live");
}

const GOVERNED = `##! experimental { persistence givens access_modifiers }
import "raw.malloy"

#(secure)
given: TENANTS :: string[]
given: ROLE :: string

// A row-scoping given term on a colocated persisted source's extend block.
#@ persist name="scoped"
source: scoped is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  where: region in $TENANTS
  measure: total is amount.sum()
}

// A row-level gate and a whole-source gate, over a table with hidden columns.
#(authorize) 'analyst' = $ROLE
#(access_filter) home_region in $TENANTS
#@ persist name="cust"
source: cust is customers_raw -> {
  select: id, segment, home_region, name
} include { internal: home_region; private: name; public: * } extend {
  primary_key: id
  measure: n is count()
  measure: named is count() { where: name != null }
  view: by_segment is { group_by: segment; aggregate: n }
}

source: cust_ext is cust extend {
  dimension: region_copy is home_region
}

#@ persist name="facts"
source: facts is orders_raw -> {
  select: id, customer_id, amount
} extend {
  join_one: c is cust on customer_id = c.id
  measure: total is amount.sum()
}
`;

const CALLERS = [
   { ROLE: "analyst", TENANTS: ["north"] },
   { ROLE: "analyst", TENANTS: ["south", "east"] },
];
const DENIED = { ROLE: "intern", TENANTS: ["north"] };

const GOVERNED_QS: Q[] = [
   ...CALLERS.map((g, i) => ({
      name: `given term, caller ${i}`,
      model: "g.malloy",
      malloy: "run: scoped -> { group_by: region; aggregate: total }",
      givens: g,
   })),
   ...CALLERS.map((g, i) => ({
      name: `gated named view, caller ${i}`,
      model: "g.malloy",
      malloy: "run: cust -> by_segment",
      givens: g,
   })),
   {
      name: "gated named view, denied role",
      model: "g.malloy",
      malloy: "run: cust -> by_segment",
      givens: DENIED,
   },
   {
      name: "gated, no givens at all",
      model: "g.malloy",
      malloy: "run: cust -> by_segment",
   },
   ...CALLERS.map((g, i) => ({
      name: `gated join through a persisted fact, caller ${i}`,
      model: "g.malloy",
      malloy:
         "run: facts -> { group_by: c.segment; aggregate: total, k is c.n }",
      givens: g,
   })),
   {
      name: "gated join, denied role",
      model: "g.malloy",
      malloy: "run: facts -> { group_by: c.segment; aggregate: total }",
      givens: DENIED,
   },
   // Hidden columns, every caller path: each must be refused as live refuses.
   ...[
      ["ad-hoc group_by internal", "run: cust -> { group_by: home_region }"],
      ["ad-hoc private measure", "run: cust -> { aggregate: named }"],
      ["ad-hoc select private", "run: cust -> { select: name }"],
      [
         "nest over internal",
         "run: cust -> { group_by: segment; nest: x is { group_by: home_region } }",
      ],
      [
         "refinement of a view with internal",
         "run: cust -> by_segment + { group_by: home_region }",
      ],
      ["join path to internal", "run: facts -> { group_by: c.home_region }"],
      ["join path to private", "run: facts -> { group_by: c.name }"],
      [
         "filter on internal",
         "run: cust -> { where: home_region = 'north'; aggregate: n }",
      ],
      [
         "extension re-exposing internal as a public dimension",
         "run: cust_ext -> { group_by: region_copy; aggregate: n }",
      ],
      ["select * hides internal", "run: cust -> { select: * }"],
   ].map(([name, malloy]) => ({
      name,
      model: "g.malloy",
      malloy,
      givens: CALLERS[0],
   })),
];

for (const sw of ["cutover", "off"] as const) {
   describe(`[${sw}] colocated gates, givens and hidden columns`, () => {
      let r: Awaited<ReturnType<typeof probe>>;
      it("loads", async () => {
         r = await probe(sw, { "g.malloy": GOVERNED }, GOVERNED_QS);
      }, 300_000);
      for (const q of GOVERNED_QS) {
         // Today a query joining the gated source through the persisted
         // fact is answered live (rows equal); after the cutover, served.
         const asked =
            sw === "off" && q.name.startsWith("gated join")
               ? { ...q, stored: false }
               : q;
         it(q.name, () => check(r, asked));
      }
   });
}

// A BIGINT beyond 2^53 and a TIMESTAMPTZ column on a colocated table: today's
// manifest path keeps Malloy's own definition; the served copy declares them.
const WIDE = `##! experimental { persistence }
source: wide_raw is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 9007199254740993::BIGINT, 12345678901234567.123456789::DECIMAL(38,9), TIMESTAMPTZ '2026-01-15 23:30:00+00'),
    (2, 42::BIGINT, 0.5::DECIMAL(38,9), TIMESTAMPTZ '2026-02-03 01:00:00+00')
  ) AS t(id, big, amt, tz)
""")

#@ persist name="wide"
source: wide is wide_raw -> { select: id, big, amt }

#@ persist name="wide_tz"
source: wide_tz is wide_raw -> { select: id, tz }
`;
// A BIGINT beyond 2^53 reads back as a double after the cutover (the type:
// declares a plain number, limitation 12): a decision, left out here (A1).
const WIDE_QS: Q[] = [
   {
      name: "a decimal(38,9)",
      model: "w.malloy",
      malloy: "run: wide -> { group_by: id, amt; order_by: id }",
   },
   {
      name: "a timestamptz by day under a zone",
      model: "w.malloy",
      malloy:
         "run: wide_tz -> { timezone: 'America/Los_Angeles'; group_by: d is tz.day; aggregate: n is count() }",
   },
];
for (const sw of ["cutover", "off"] as const) {
   describe(`[${sw}] colocated wide types`, () => {
      let r: Awaited<ReturnType<typeof probe>>;
      it("loads", async () => {
         r = await probe(sw, { "w.malloy": WIDE }, WIDE_QS);
      }, 300_000);
      for (const q of WIDE_QS) {
         it(q.name, () => check(r, q));
         it(`${q.name}: result types`, () => {
            const l = r.live.get(q.name)!;
            const s = r.served.get(q.name)!;
            if (!("answer" in l) || !("answer" in s)) return;
            const t = (f: { name: string; type: unknown }[]) =>
               f.map((x) => [x.name, JSON.stringify(x.type)]);
            expect(
               t(s.answer.fields).map(([n, x]) => [
                  n,
                  x.replace(/,"subtype":"[a-z]+"/g, ""),
               ]),
            ).toEqual(
               t(l.answer.fields).map(([n, x]) => [
                  n,
                  x.replace(/,"subtype":"[a-z]+"/g, ""),
               ]),
            );
         });
      }
   });
}

afterAll(async () => {
   await freshLoadPool();
});
