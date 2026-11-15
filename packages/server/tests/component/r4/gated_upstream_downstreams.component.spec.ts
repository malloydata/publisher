// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored source downstream of a gated upstream. A table built from a
 * source whose upstream carries `#(access_filter)`, `#(authorize)` or a
 * given-reading `where:` must never answer a caller with rows that caller's
 * gate excludes, nor with fewer rows than live.
 *
 * The upstream is gated three ways (`#(access_filter)` in the same file and in
 * an imported one, `#(authorize)`, and an extend-block `where:` over a
 * given), and each downstream shape is stored four ways (v1 `#@ persist`
 * colocated and `storage=`, `#(materialize)` colocated and `storage=`):
 *
 *  - a query over it (`a -> { group_by … }`, `a -> { aggregate … }`,
 *    `a -> { select: * }`), an `extend` of it, and a query two hops away;
 *  - a source that joins it, plainly and through an annotated join;
 *  - `#@ preaggregate` and `#(preaggregate)` rollups over a derivation of it.
 *
 * With `PUBLISHER_MATERIALIZE_GATED_SOURCES` off and on, each is queried as
 * three callers whose gates admit different rows, and each served answer
 * must equal live. A downstream that reads an `#(access_filter)` from the
 * source it derives from or extends (a query over it, an `extend`, two hops,
 * across files) is refused at plan in every form, with the switch off or on:
 * its table would hold every caller's rows, and the served copy cannot graft
 * an inherited row filter onto it, so it would never be read. Readers of
 * those sources that are not stored themselves (an author join, an
 * extension, a caller join, a caller derivation) are queried too.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "./conformance/package_harness";
import { rowDifference } from "./harness";

/** The gated upstreams, by prefix. `xf` is declared in base.malloy. */
const GATES: Record<string, string | undefined> = {
   af: `#(access_filter) org_id = $ORG_ID
source: af_a is raw -> { where: amount > 0; select: * }`,
   az: `#(authorize) $ROLE = 'admin'
source: az_a is raw -> { where: amount > -1; select: * }`,
   gv: `source: gv_a is raw -> { where: amount > -2; select: * } extend { where: org_id = $ORG_ID }`,
   xf: undefined,
};

/** How each downstream is stored, by prefix. */
const STORED: Record<string, (name: string) => string> = {
   pc: (n) => `#@ persist name="${n}"`,
   ps: (n) => `#@ persist name="${n}" storage="lake"`,
   mc: (n) => `#(materialize) name="${n}_t"`,
   ms: () => `#(materialize) storage="lake"`,
};

const SHAPES = ["q", "agg", "sel", "ext", "join", "ajoin", "hop2"] as const;

function downstreams(g: string, v: string): string {
   const a = STORED[v];
   const p = `${g}_${v}`;
   return `
${a(`${p}_q`)}
source: ${p}_q is ${g}_a -> { group_by: org_id; aggregate: total is amount.sum() }
${a(`${p}_agg`)}
source: ${p}_agg is ${g}_a -> { aggregate: total is amount.sum() }
${a(`${p}_sel`)}
source: ${p}_sel is ${g}_a -> { select: * }
${a(`${p}_ext`)}
source: ${p}_ext is ${g}_a extend { dimension: k is opp_id + 1 }
${a(`${p}_join`)}
source: ${p}_join is orgs extend {
  join_many: a is ${g}_a on org_id = a.org_id
} -> { group_by: org_id; aggregate: total is a.amount.sum() }
${a(`${p}_ajoin`)}
source: ${p}_ajoin is orgs extend {
  # label="annotated"
  join_many: a is ${g}_a on org_id = a.org_id
} -> { group_by: org_id; aggregate: total is a.amount.sum() }
source: ${p}_mid is ${g}_a -> { select: * }
${a(`${p}_hop2`)}
source: ${p}_hop2 is ${p}_mid -> { group_by: org_id; aggregate: total is amount.sum() }
`;
}

const ROLLUPS = ["pre1_q", "pre2_q", "pre2s_q", "pre2_ext"] as const;

function rollups(g: string): string {
   return `
source: ${g}_pre1_q is ${g}_a -> { select: * } extend {
  #@ preaggregate grain="org_id"
  measure: total is amount.sum()
}
source: ${g}_pre2_q is ${g}_a -> { select: * } extend {
  #(preaggregate) grain=["org_id"]
  measure: total is amount.sum()
}
source: ${g}_pre2s_q is ${g}_a -> { select: * } extend {
  #(preaggregate) grain=["org_id"] storage="lake"
  measure: total is amount.sum()
}
source: ${g}_pre2_ext is ${g}_a extend {
  #(preaggregate) grain=["org_id"]
  measure: total is amount.sum()
}
`;
}

const BASE = `##! experimental { persistence givens access_modifiers }
given:
  ORG_ID :: number
  ROLE :: string
source: xraw is duckdb.sql("""SELECT * FROM (VALUES
  (1, 10, 50.0), (1, 11, 70.0), (2, 20, 30.0), (2, 21, 40.0)
) AS t(org_id, opp_id, amount)""")
#(access_filter) org_id = $ORG_ID
source: xf_a is xraw -> { select: * }
`;

const MODEL = `##! experimental { persistence givens access_modifiers }
import { ORG_ID, ROLE, xf_a } from "base.malloy"

source: raw is duckdb.sql("""SELECT * FROM (VALUES
  (1, 10, 5.0), (1, 11, 7.0), (2, 20, 3.0), (2, 21, 4.0)
) AS t(org_id, opp_id, amount)""")
source: orgs is duckdb.sql("""SELECT * FROM (VALUES (1), (2)) AS t(org_id)""")
${Object.values(GATES)
   .filter((gate) => gate !== undefined)
   .join("\n")}
${Object.keys(GATES)
   .flatMap((g) => Object.keys(STORED).map((v) => downstreams(g, v)))
   .join("\n")}
${Object.keys(GATES).map(rollups).join("\n")}
source: rd_author is orgs extend { join_many: s is af_ms_sel on org_id = s.org_id }
source: rd_author_pc is orgs extend { join_many: s is af_pc_sel on org_id = s.org_id }
source: rd_ext is af_ms_sel extend { dimension: z is 1 }
source: rd_ext_pc is af_pc_sel extend { dimension: z is 1 }
source: rd_xf is xf_ms_sel extend { dimension: z is 1 }
`;

/** Readers of a stored gated downstream that are not stored themselves. */
const READERS: [string, string][] = [
   [
      "an author join to it",
      "run: rd_author -> { group_by: org_id; aggregate: t is s.amount.sum() }",
   ],
   [
      "an author join to its v1 colocated form",
      "run: rd_author_pc -> { group_by: org_id; aggregate: t is s.amount.sum() }",
   ],
   ["an extension of it", "run: rd_ext -> { select: * }"],
   ["an extension of its v1 colocated form", "run: rd_ext_pc -> { select: * }"],
   ["an extension, cross-file upstream", "run: rd_xf -> { select: * }"],
   [
      "a caller join to it",
      "run: orgs extend { join_many: s is af_ms_sel on org_id = s.org_id } -> { group_by: org_id; aggregate: t is s.amount.sum() }",
   ],
   [
      "a caller join to its v1 colocated form",
      "run: orgs extend { join_many: s is af_pc_sel on org_id = s.org_id } -> { group_by: org_id; aggregate: t is s.amount.sum() }",
   ],
   [
      "a caller derivation of it",
      "run: af_ms_sel -> { group_by: org_id; aggregate: t is amount.sum() }",
   ],
   [
      "a caller derivation, cross-file upstream",
      "run: xf_ms_q -> { group_by: org_id; aggregate: t is total.sum() }",
   ],
];

/** Each gate admits different rows to each: org 1, org 2, and a non-admin. */
const CALLERS = [
   { ORG_ID: 1, ROLE: "admin" },
   { ORG_ID: 2, ROLE: "admin" },
   { ORG_ID: 2, ROLE: "viewer" },
];

const CASES: [string, string][] = [
   ...Object.keys(GATES).flatMap((g) =>
      Object.keys(STORED).flatMap((v) =>
         SHAPES.map((s): [string, string] => [
            `${g}_${v}_${s}`,
            `run: ${g}_${v}_${s} -> { select: * }`,
         ]),
      ),
   ),
   ...Object.keys(GATES).flatMap((g) =>
      ROLLUPS.map((s): [string, string] => [
         `${g}_${s}`,
         `run: ${g}_${s} -> { group_by: org_id; aggregate: total }`,
      ]),
   ),
   ...READERS,
];

const sameOutcome = (live: Outcome, served: Outcome): string | undefined => {
   if ("error" in live) {
      return "error" in served ? undefined : "live refused, served answered";
   }
   if ("error" in served) return `served failed: ${served.error}`;
   return rowDifference(live.answer.rows, served.answer.rows);
};

/**
 * The refusals each setting must report for the downstreams of the
 * `#(access_filter)` and `#(authorize)` upstreams, by `name:tier:reason`.
 * A v1 `storage=` downstream is refused in every shape but the annotated
 * join (the deep scan's documented gap; a join does not carry the gate live
 * either, so the table equals live), and the v1 colocated one, whose table is
 * the refused plain join's. A downstream that inherits a row filter
 * (`af`, `xf`) is refused in every form, so no table holds every caller's
 * rows that nothing reads.
 */
function expectedGateRefusals(gated: boolean): string[] {
   const out: string[] = [];
   for (const g of ["af", "az", "xf"]) {
      for (const s of SHAPES) {
         if (s === "ajoin") {
            // The v1 colocated annotated join shares its table (one address:
            // the same SQL) with the plain join, which is refused, so it is
            // refused too: the bind serves no source on that address.
            out.push(`${g}_pc_${s}:colocated:authorize`);
            continue;
         }
         out.push(`${g}_ps_${s}:storage:authorize`);
         // A gate reached only through a join is never the entry point's own.
         if (s === "join") {
            out.push(`${g}_pc_${s}:colocated:authorize`);
            out.push(`${g}_mc_${s}:colocated:authorize`);
            out.push(`${g}_ms_${s}:storage:authorize`);
            continue;
         }
         // The authorize-route lock is admitted colocated (it is decided
         // before any row is read); a row filter over a dropped gate column
         // classifies rejected.
         if (g !== "az" && s === "agg") {
            out.push(`${g}_pc_${s}:colocated:authorize`);
            out.push(`${g}_mc_${s}:colocated:authorize`);
            out.push(`${g}_ms_${s}:storage:authorize`);
            continue;
         }
         // An inherited row filter: refused colocated and stored, either way.
         if (g !== "az") {
            out.push(`${g}_pc_${s}:colocated:authorize`);
            out.push(`${g}_mc_${s}:colocated:authorize`);
            out.push(`${g}_ms_${s}:storage:authorize`);
            continue;
         }
         if (!gated) {
            // Off, a `storage=` `#(materialize)` table is read by nothing
            // that keeps the gate. A colocated one keeps it under G3, as the
            // v1 colocated table beside it does.
            out.push(`${g}_ms_${s}:storage:authorize`);
         }
      }
   }
   return out.sort();
}

for (const gated of [false, true]) {
   describe(`a stored downstream of a gated upstream (gated sources ${gated ? "on" : "off"})`, () => {
      let restore: () => void = () => {};
      let loader: Awaited<ReturnType<typeof loadFixture>>;
      let fixture: LoadedFixture;
      let refused: string[] = [];
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome>();

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: gated ? "true" : "false",
            },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({ "m.malloy": MODEL, "base.malloy": BASE });
         for (const [name, q] of CASES) {
            for (const [i, c] of CALLERS.entries()) {
               live.set(`${name}#${i}`, await loader.run("m.malloy", q, c));
            }
         }
         refused = Object.values(
            (
               loader.pkg.getBuildPlan() as unknown as {
                  refusedSources?: Record<
                     string,
                     { name: string; tier: string; reason: string }
                  >;
               }
            )?.refusedSources ?? {},
         ).map((r) => `${r.name}:${r.tier}:${r.reason}`);
         fixture = await loader.bind({ includeRollups: true });
         for (const [name, q] of CASES) {
            for (const [i, c] of CALLERS.entries()) {
               served.set(`${name}#${i}`, await fixture.run("m.malloy", q, c));
            }
         }
      }, 600_000);
      afterAll(async () => {
         restore();
         await loader?.dispose();
         await freshLoadPool();
      });

      it("builds every source the plan admits", () => {
         expect(fixture.buildErrors).toEqual({});
      });

      it("refuses the gated downstreams it cannot prove at read", () => {
         const gateRefusals = refused
            .filter((r) => /^(af|az|xf)_(pc|ps|mc|ms)_/.test(r))
            .sort();
         expect(gateRefusals).toEqual(expectedGateRefusals(gated));
         // A downstream that bakes the upstream's given into its build is
         // refused whatever the switch. An extension inherits the upstream's
         // given-reading `where:`, which only the upstream's own declaration
         // re-applies at read: a v1 one is refused as
         // `inherited_where_with_given`, a `#(materialize)` one at plan
         // (`materialize-not-built`, no refusedSources entry); none is built.
         const built = fixture.built.map((b) => b.name);
         for (const v of Object.keys(STORED)) {
            for (const s of SHAPES) {
               const name = `gv_${v}_${s}`;
               const hit = refused.find((r) => r.startsWith(`${name}:`));
               if (s === "ext") {
                  expect([name, hit]).toEqual([
                     name,
                     v.startsWith("p")
                        ? `${name}:${v === "pc" ? "colocated" : "storage"}:inherited_where_with_given`
                        : undefined,
                  ]);
                  expect([name, built.includes(name)]).toEqual([name, false]);
               } else expect(hit).toEndWith(":given_in_persisted_query");
            }
         }
      });

      for (const [name] of CASES) {
         it(`${name}: each caller as live`, () => {
            for (const [i, c] of CALLERS.entries()) {
               const l = live.get(`${name}#${i}`)!;
               const s = served.get(`${name}#${i}`)!;
               expect({ caller: c, difference: sameOutcome(l, s) }).toEqual({
                  caller: c,
                  difference: undefined,
               });
            }
         });
      }

      it("the callers' live answers differ, so one caller's rows could not pass for another's", () => {
         for (const name of ["af_pc_sel", "xf_ms_q", "gv_ms_ext"]) {
            const a = live.get(`${name}#0`)!;
            const b = live.get(`${name}#1`)!;
            if ("error" in a || "error" in b) throw new Error(`${name} failed`);
            expect(rowDifference(a.answer.rows, b.answer.rows)).toBeDefined();
         }
         const viewer = live.get("az_pc_sel#2")!;
         expect("error" in viewer).toBe(true);
      });

      it("builds no table of an inherited row filter, and answers its callers live", () => {
         // Refused at plan (above): nothing holds every caller's rows.
         const inherited = [
            "af_pc_q",
            "af_pc_sel",
            "af_pc_ext",
            "af_pc_hop2",
            "xf_pc_sel",
            "af_ms_q",
            "af_ms_sel",
            "af_ms_ext",
            "af_ms_hop2",
            "xf_ms_sel",
         ];
         const built = fixture.built.map((b) => b.name);
         for (const name of inherited) {
            expect([name, built.includes(name)]).toEqual([name, false]);
            for (const i of CALLERS.keys()) {
               const out = served.get(`${name}#${i}`)!;
               if ("error" in out) throw new Error(`${name}: ${out.error}`);
               expect([name, out.answer.servedBy]).toEqual([name, "live"]);
            }
         }
         // The refusal names the inherited gate.
         const plan = loader.pkg.getBuildPlan() as unknown as {
            refusedSources?: Record<string, { name: string; message: string }>;
         };
         const message = Object.values(plan.refusedSources ?? {}).find(
            (r) => r.name === "af_pc_sel",
         )?.message;
         expect(message).toContain("reads an #(access_filter) row-level gate");
      });

      it("builds no v1 colocated table it shares with a refused source, and answers its callers live", () => {
         const built = fixture.built.map((b) => b.name);
         const plan = loader.pkg.getBuildPlan() as unknown as {
            refusedSources?: Record<string, { name: string; message: string }>;
         };
         for (const g of ["af", "az", "xf"]) {
            const name = `${g}_pc_ajoin`;
            expect([name, built.includes(name)]).toEqual([name, false]);
            const message = Object.values(plan.refusedSources ?? {}).find(
               (r) => r.name === name,
            )?.message;
            expect(message).toContain(`shares its table with '${g}_`);
            for (const i of CALLERS.keys()) {
               const out = served.get(`${name}#${i}`)!;
               if ("error" in out) throw new Error(`${name}: ${out.error}`);
               expect([name, out.answer.servedBy]).toEqual([name, "live"]);
            }
         }
      });

      it("still builds and serves the gated downstreams it can read: an annotated join, an authorize lock", () => {
         const built = fixture.built.map((b) => b.name);
         for (const name of ["af_ms_ajoin", "az_pc_sel"]) {
            expect([name, built.includes(name)]).toEqual([name, true]);
            const out = served.get(`${name}#0`)!;
            if ("error" in out) throw new Error(`${name}: ${out.error}`);
            expect([name, out.answer.servedBy]).toEqual([name, "materialized"]);
         }
      });

      if (gated) {
         it("a rollup over a row-gated derivation is read, with the gate grafted on it", () => {
            for (const name of [
               "af_pre1_q",
               "af_pre2_q",
               "af_pre2_ext",
               "xf_pre2_q",
            ]) {
               for (const i of CALLERS.keys()) {
                  const out = served.get(`${name}#${i}`)!;
                  if ("error" in out) throw new Error(`${name}: ${out.error}`);
                  expect([name, out.answer.servedBy]).toEqual([name, "rollup"]);
               }
            }
         });
      }
   });
}
