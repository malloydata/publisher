// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a stored table keeps its source's gates when read, pinned at each of
 * the three places that decide it: plan (is a table over a gated source built
 * at all), the serve verdict (does a gated entry point send it live), and the
 * router (does a leg get past an entry-point gate). Every combination of kind,
 * tier, v1 or not, and the switches is a row, written against the code as it
 * was before the gate-keeping policy became one table, so that change must
 * leave every row as it is. Rows changed since, by maintainer decision:
 * colocated `#(materialize)` keeps its own row-level gate under G3 with the
 * gated-sources switch off, as v1 colocated does, and so does a `storage=`
 * table, v1 or `#(materialize)`, on the same conditions (a maintainer-approved
 * exception made ahead of the question-14 review, for consistency). A
 * `storage=` table of an ungated source reached only through a gated entry
 * point still answers live (`row_level_gate`), unless a source admitted past
 * its own gate stores into the same table (a v1 extension under its own
 * lock, `secure_persist_sto`, shares `persist_sto`'s). A wrong row lets a
 * leg past a veto; `govern()`'s re-proof stays behind every row, and is
 * tested by the routed gate suites.
 *
 * Plan and the serve verdict are read off a real package load (the load
 * worker plans), for two gate shapes: a source's own row-level gate
 * (`#(access_filter)` on the declared source) and a gate above it (an
 * `#(authorize)` entry point over an ungated declared source, the only one
 * published). The storage-rollup review flag is a main-thread test seam the
 * load worker never sees, so it is pinned at the plan's own predicate. The
 * router is driven with a fake host whose legs answer with tables of a given
 * tier, under an entry-point veto.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../package_load/package_load_pool";
import { installSettings } from "../../test_helpers/settings_env";
import { writeTempPackage } from "../../test_helpers/temp_package";
import { Package } from "../package";
import { variantsOf } from "../query_serving";
import { type LegContext, makeLegs } from "./serve/leg";
import { routeQuery } from "../query_router";
import { EMPTY_SNAPSHOT, pin } from "./domain/binding";
import { gateEnforcedAtRead } from "./plan/materialization_eligibility";
import { allowGatedStorageRollupsForTests } from "../../config";
import { recordConnectionsAsForTests } from "../connection_type";

// The package sandbox stands in for a warehouse in these fixtures: a
// `storage=` build cannot read the sandbox, and the plan refuses a `storage=`
// source on it (storage_source_unsupported).
let restoreConnectionTypes: () => void;
beforeAll(() => {
   restoreConnectionTypes = recordConnectionsAsForTests({ duckdb: "postgres" });
});
afterAll(() => restoreConnectionTypes());

const DUMP = process.env.GATE_KEEPING_DUMP;

const ROWS = `duckdb.sql("select * from (values (1,'east','A',10.0),(2,'west','B',20.0)) as t(id, region, category, amount)")`;
const KIDS = `duckdb.sql("select * from (values (1, 1, 5.0),(2, 2, 6.0)) as t(kid_id, parent_id, qty)")`;

/**
 * One declaration per kind and tier. `GATE` stands where a source's own gate
 * goes, and is blank for the gate-above shape.
 */
const DECLARATIONS = `
source: kids is ${KIDS} extend { primary_key: kid_id }

GATE
#@ persist name="persist_col_t"
source: persist_col is ${ROWS} -> { select: * }

GATE
#@ persist name="persist_sto_t" storage="lake"
source: persist_sto is ${ROWS} -> { select: * }

GATE
#(materialize) name="mat_col_t"
source: mat_col is ${ROWS} extend { measure: total is amount.sum() }

GATE
#(materialize) storage="lake"
source: mat_sto is ${ROWS} extend { measure: total is amount.sum() }

GATE
#(materialize) name="pair_col_t" inline=["k"]
source: pair_col is ${ROWS} extend {
  primary_key: id
  join_many: k is kids on k.parent_id = id
  measure: kq is k.qty.sum()
}

GATE
#(materialize) storage="lake" inline=["k"]
source: pair_sto is ${ROWS} extend {
  primary_key: id
  join_many: k is kids on k.parent_id = id
  measure: kq is k.qty.sum()
}

GATE
source: roll is ${ROWS} extend {
  #(preaggregate) grain=["category", "region"]
  measure: total_col is amount.sum()
  #(preaggregate) grain=["category", "region"] storage="lake"
  measure: total_sto is amount.sum()
}

GATE
source: roll_v1 is ${ROWS} extend {
  #@ preaggregate grain="region"
  measure: v1_total is amount.sum()
}

GATE
source: views is ${ROWS} extend {
  measure: n is count()
  #@ persist name="views_col_t"
  view: by_region_col is { group_by: region; aggregate: n }
  #@ persist name="views_sto_t" storage="lake"
  view: by_region_sto is { group_by: region; aggregate: n }
}
`;

const DECLARED = [
   "persist_col",
   "persist_sto",
   "mat_col",
   "mat_sto",
   "pair_col",
   "pair_sto",
   "roll",
   "roll_v1",
   "views",
];

const SHAPES: Record<"own" | "above", Record<string, string>> = {
   own: {
      "m.malloy":
         "##! experimental { persistence givens access_modifiers }\n" +
         "given: REGION :: string\n" +
         DECLARATIONS.replaceAll("GATE", "#(access_filter) region = $REGION"),
      "index.malloy": `import "m.malloy"\nexport { ${DECLARED.join(", ")} }\n`,
   },
   above: {
      "m.malloy":
         "##! experimental { persistence givens access_modifiers }\n" +
         "given: ROLE :: string\n" +
         DECLARATIONS.replaceAll("GATE", "") +
         DECLARED.map(
            (name) =>
               `\n#(authorize) $ROLE = 'admin'\nsource: secure_${name} is ${name} extend {}\n`,
         ).join(""),
      "index.malloy": `import "m.malloy"\nexport { ${DECLARED.map((n) => `secure_${n}`).join(", ")} }\n`,
   },
};

const SWITCHES = [
   { materialize: true, gatedSources: true },
   { materialize: true, gatedSources: false },
   { materialize: false, gatedSources: true },
   { materialize: false, gatedSources: false },
] as const;

function switchLabel(s: { materialize: boolean; gatedSources: boolean }) {
   return `materialize ${s.materialize ? "on" : "off"}, gated sources ${s.gatedSources ? "on" : "off"}`;
}

/**
 * A planned table's row: `planned` or `refused <reason>`, its tier, and its
 * serve verdict with the reason, keyed by the table's source (a rollup's and
 * a view's by its generated name, which is content-addressed).
 */
type Row = string;

async function loadRows(
   files: Record<string, string>,
   s: { materialize: boolean; gatedSources: boolean },
): Promise<Record<string, Row>> {
   const restore = installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE: String(s.materialize),
         PUBLISHER_MATERIALIZE_GATED_SOURCES: String(s.gatedSources),
      },
   }).restore;
   // A load worker reads its settings once, at spawn, and the plan is
   // computed there: one per row's switches, so the plan reads them too.
   await __setPackageLoadPoolForTests(new PackageLoadPool(1));
   const { dir } = writeTempPackage(
      {},
      { prefix: "gate-keeping-", manifest: { name: "pkg" } },
   );
   for (const [name, text] of Object.entries(files)) {
      fs.writeFileSync(path.join(dir, name), text);
   }
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const config = new MalloyConfig({ connections: {} });
   config.wrapConnections(
      () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
   );
   try {
      const pkg = await Package.create("env", "pkg", dir, config);
      const plan = pkg.getBuildPlan();
      const rows: Record<string, string[]> = {};
      for (const s of Object.values(plan?.sources ?? {})) {
         rows[s.name] = ["planned"];
      }
      for (const s of Object.values(plan?.refusedSources ?? {})) {
         rows[s.name] = [`refused ${s.reason}`];
      }
      const verdicts = (
         pkg.materialization() as unknown as {
            serveVerdicts(): {
               sourceName: string;
               tier: string;
               verdict: string;
               reason?: string;
            }[];
         }
      ).serveVerdicts();
      for (const v of verdicts) {
         const key = v.sourceName;
         if (!rows[key]) continue;
         rows[key].push(
            v.tier,
            v.reason ? `${v.verdict} ${v.reason}` : v.verdict,
         );
      }
      return Object.fromEntries(
         Object.entries(rows)
            .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
            .map(([k, v]) => [k, v.join(" | ")]),
      );
   } finally {
      restore();
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

const dumped: Record<string, unknown> = {};

afterAll(() => {
   if (DUMP) fs.writeFileSync(DUMP, JSON.stringify(dumped, null, 3));
});

/** Plan and serve verdict, per gate shape and switches: see the header. */
const PLAN_AND_VERDICT: Record<string, Record<string, Row>> = {
   "a gate on the declared source, materialize on, gated sources on": {
      mat_col: "planned | database | not_built",
      mat_sto: "planned | storage | not_built",
      pair_col: "planned | database | not_built",
      pair_sto: "planned | storage | not_built",
      persist_col: "planned | database | not_built",
      persist_sto: "planned | storage | not_built",
      "roll.total_sto": "refused access_filter | storage | live refused",
      roll__rollup__category_region__7617040b: "planned | database | not_built",
      roll_v1__rollup__region__b3ca2623: "planned | database | not_built",
      views__by_region_col__view: "planned | database | not_built",
      views__by_region_sto__view:
         "refused access_filter | storage | live refused",
   },
   "a gate on the declared source, materialize on, gated sources off": {
      mat_col: "planned | database | not_built",
      mat_sto: "planned | storage | not_built",
      pair_col: "planned | database | not_built",
      pair_sto: "planned | storage | not_built",
      persist_col: "planned | database | not_built",
      persist_sto: "planned | storage | not_built",
      // G3 for a colocated rollup: the base's own gate, proven on its grain.
      roll__rollup__category_region__7617040b: "planned | database | not_built",
      "roll.total_sto": "refused access_filter | storage | live refused",
      roll_v1__rollup__region__b3ca2623: "planned | database | not_built",
      views__by_region_col__view:
         "refused access_filter | database | live refused",
      views__by_region_sto__view:
         "refused access_filter | storage | live refused",
   },
   "a gate on the declared source, materialize off, gated sources on": {
      persist_col: "planned | database | not_built",
      persist_sto: "planned | storage | not_built",
      roll_v1__rollup__region__b3ca2623: "planned | database | not_built",
      views__by_region_col__view:
         "refused materialization_off | database | live refused",
      views__by_region_sto__view:
         "refused materialization_off | storage | live refused",
   },
   "a gate on the declared source, materialize off, gated sources off": {
      persist_col: "planned | database | not_built",
      persist_sto: "planned | storage | not_built",
      roll_v1__rollup__region__b3ca2623: "planned | database | not_built",
      views__by_region_col__view:
         "refused materialization_off | database | live refused",
      views__by_region_sto__view:
         "refused materialization_off | storage | live refused",
   },
   "a gate above it, materialize on, gated sources on": {
      mat_col: "planned | database | not_built",
      mat_sto: "planned | storage | not_built",
      pair_col: "planned | database | not_built",
      pair_sto: "planned | storage | not_built",
      persist_col: "planned | database | not_built",
      persist_sto: "planned | storage | not_built",
      roll__rollup__category_region__7617040b:
         "planned | database | live row_level_gate",
      roll__rollup__category_region__b1b70c25:
         "planned | storage | live row_level_gate",
      roll_v1__rollup__region__b3ca2623:
         "planned | database | live row_level_gate",
      secure_persist_col: "planned | database | not_built",
      secure_persist_sto: "planned | storage | not_built",
      views__by_region_col__view: "planned | database | not_built",
      views__by_region_sto__view: "planned | storage | not_built",
   },
   "a gate above it, materialize on, gated sources off": {
      mat_col: "planned | database | not_built",
      mat_sto: "planned | storage | live row_level_gate",
      pair_col: "planned | database | not_built",
      pair_sto: "planned | storage | live row_level_gate",
      persist_col: "planned | database | not_built",
      persist_sto: "planned | storage | not_built",
      roll__rollup__category_region__7617040b:
         "planned | database | live row_level_gate",
      roll__rollup__category_region__b1b70c25:
         "planned | storage | live row_level_gate",
      roll_v1__rollup__region__b3ca2623:
         "planned | database | live row_level_gate",
      secure_persist_col: "planned | database | not_built",
      secure_persist_sto: "planned | storage | not_built",
      views__by_region_col__view: "planned | database | not_built",
      views__by_region_sto__view: "planned | storage | live row_level_gate",
   },
   "a gate above it, materialize off, gated sources on": {
      persist_col: "planned | database | not_built",
      persist_sto: "planned | storage | not_built",
      roll_v1__rollup__region__b3ca2623:
         "planned | database | live row_level_gate",
      secure_persist_col: "planned | database | not_built",
      secure_persist_sto: "planned | storage | not_built",
      views__by_region_col__view:
         "refused materialization_off | database | live refused",
      views__by_region_sto__view:
         "refused materialization_off | storage | live refused",
   },
   "a gate above it, materialize off, gated sources off": {
      persist_col: "planned | database | not_built",
      persist_sto: "planned | storage | not_built",
      roll_v1__rollup__region__b3ca2623:
         "planned | database | live row_level_gate",
      secure_persist_col: "planned | database | not_built",
      secure_persist_sto: "planned | storage | not_built",
      views__by_region_col__view:
         "refused materialization_off | database | live refused",
      views__by_region_sto__view:
         "refused materialization_off | storage | live refused",
   },
};

describe("gate keeping at plan and at the serve verdict, on a package load", () => {
   let pool: PackageLoadPool;
   beforeAll(async () => {
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });
   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
   });

   for (const shape of ["own", "above"] as const) {
      for (const s of SWITCHES) {
         const label = `a gate ${shape === "own" ? "on the declared source" : "above it"}, ${switchLabel(s)}`;
         it(
            label,
            async () => {
               const rows = await loadRows(SHAPES[shape], s);
               if (DUMP) dumped[`plan ${label}`] = rows;
               else expect(rows).toEqual(PLAN_AND_VERDICT[label]);
            },
            60_000,
         );
      }
   }
});

/** The plan's own predicates, per kind, tier, v1 or not, and switches. */
const PLAN_PREDICATES: Record<string, Record<string, boolean>> = {
   "materialize on, gated sources on, storage rollups not reviewed": {
      "persist (v1), colocated": false,
      "persist (v1), storage": true,
      "persist (v1), storage, a gate not its own": false,
      "persist (v1), storage, an inherited row filter": false,
      "materialize, colocated": true,
      "materialize, colocated, an inherited row filter": false,
      "materialize, colocated, a gate not row-level": false,
      "materialize, colocated, a gate not its own": false,
      "materialize, storage": true,
      "materialize, storage, an inherited row filter": false,
      "materialize, storage, a gate not row-level": false,
      "rollup, colocated": true,
      "rollup, storage": false,
      "rollup, colocated, ungated base": false,
      "rollup (v1), colocated": false,
   },
   "materialize on, gated sources on, storage rollups reviewed": {
      "persist (v1), colocated": false,
      "persist (v1), storage": true,
      "persist (v1), storage, a gate not its own": false,
      "persist (v1), storage, an inherited row filter": false,
      "materialize, colocated": true,
      "materialize, colocated, an inherited row filter": false,
      "materialize, colocated, a gate not row-level": false,
      "materialize, colocated, a gate not its own": false,
      "materialize, storage": true,
      "materialize, storage, an inherited row filter": false,
      "materialize, storage, a gate not row-level": false,
      "rollup, colocated": true,
      "rollup, storage": true,
      "rollup, colocated, ungated base": false,
      "rollup (v1), colocated": false,
   },
   "materialize on, gated sources off, storage rollups not reviewed": {
      "persist (v1), colocated": false,
      "persist (v1), storage": true,
      "persist (v1), storage, a gate not its own": false,
      "persist (v1), storage, an inherited row filter": false,
      "materialize, colocated": true,
      "materialize, colocated, an inherited row filter": false,
      "materialize, colocated, a gate not row-level": false,
      "materialize, colocated, a gate not its own": false,
      "materialize, storage": true,
      "materialize, storage, an inherited row filter": false,
      "materialize, storage, a gate not row-level": false,
      "rollup, colocated": false,
      "rollup, storage": false,
      "rollup, colocated, ungated base": false,
      "rollup (v1), colocated": false,
   },
   "materialize on, gated sources off, storage rollups reviewed": {
      "persist (v1), colocated": false,
      "persist (v1), storage": true,
      "persist (v1), storage, a gate not its own": false,
      "persist (v1), storage, an inherited row filter": false,
      "materialize, colocated": true,
      "materialize, colocated, an inherited row filter": false,
      "materialize, colocated, a gate not row-level": false,
      "materialize, colocated, a gate not its own": false,
      "materialize, storage": true,
      "materialize, storage, an inherited row filter": false,
      "materialize, storage, a gate not row-level": false,
      "rollup, colocated": false,
      "rollup, storage": false,
      "rollup, colocated, ungated base": false,
      "rollup (v1), colocated": false,
   },
   "materialize off, gated sources on, storage rollups not reviewed": {
      "persist (v1), colocated": false,
      "persist (v1), storage": true,
      "persist (v1), storage, a gate not its own": false,
      "persist (v1), storage, an inherited row filter": false,
      "materialize, colocated": false,
      "materialize, colocated, an inherited row filter": false,
      "materialize, colocated, a gate not row-level": false,
      "materialize, colocated, a gate not its own": false,
      "materialize, storage": false,
      "materialize, storage, an inherited row filter": false,
      "materialize, storage, a gate not row-level": false,
      "rollup, colocated": true,
      "rollup, storage": false,
      "rollup, colocated, ungated base": false,
      "rollup (v1), colocated": false,
   },
   "materialize off, gated sources on, storage rollups reviewed": {
      "persist (v1), colocated": false,
      "persist (v1), storage": true,
      "persist (v1), storage, a gate not its own": false,
      "persist (v1), storage, an inherited row filter": false,
      "materialize, colocated": false,
      "materialize, colocated, an inherited row filter": false,
      "materialize, colocated, a gate not row-level": false,
      "materialize, colocated, a gate not its own": false,
      "materialize, storage": false,
      "materialize, storage, an inherited row filter": false,
      "materialize, storage, a gate not row-level": false,
      "rollup, colocated": true,
      "rollup, storage": true,
      "rollup, colocated, ungated base": false,
      "rollup (v1), colocated": false,
   },
   "materialize off, gated sources off, storage rollups not reviewed": {
      "persist (v1), colocated": false,
      "persist (v1), storage": true,
      "persist (v1), storage, a gate not its own": false,
      "persist (v1), storage, an inherited row filter": false,
      "materialize, colocated": false,
      "materialize, colocated, an inherited row filter": false,
      "materialize, colocated, a gate not row-level": false,
      "materialize, colocated, a gate not its own": false,
      "materialize, storage": false,
      "materialize, storage, an inherited row filter": false,
      "materialize, storage, a gate not row-level": false,
      "rollup, colocated": false,
      "rollup, storage": false,
      "rollup, colocated, ungated base": false,
      "rollup (v1), colocated": false,
   },
   "materialize off, gated sources off, storage rollups reviewed": {
      "persist (v1), colocated": false,
      "persist (v1), storage": true,
      "persist (v1), storage, a gate not its own": false,
      "persist (v1), storage, an inherited row filter": false,
      "materialize, colocated": false,
      "materialize, colocated, an inherited row filter": false,
      "materialize, colocated, a gate not row-level": false,
      "materialize, colocated, a gate not its own": false,
      "materialize, storage": false,
      "materialize, storage, an inherited row filter": false,
      "materialize, storage, a gate not row-level": false,
      "rollup, colocated": false,
      "rollup, storage": false,
      "rollup, colocated, ungated base": false,
      "rollup (v1), colocated": false,
   },
};

describe("gate keeping at plan, per kind and tier, with the storage-rollup review flag", () => {
   const ID = "s@m.malloy";
   const gate = {
      classification: "row_level" as const,
      attributed: true,
   };
   // The one table `ID` the gate is asked about, as the plan records it.
   const table = (plan: Record<string, unknown>) => ({
      tables: { [ID]: plan },
   });
   const inputs: Record<string, Parameters<typeof gateEnforcedAtRead>[1]> = {
      "persist (v1), colocated": table({ kind: "persist", gate }),
      "persist (v1), storage": table({ kind: "persist", gate }),
      "persist (v1), storage, an inherited row filter": table({
         kind: "persist",
         gate: { ...gate, inheritedRowFilter: true },
      }),
      "persist (v1), storage, a gate not its own": table({
         kind: "persist",
         gate: { ...gate, attributed: false },
      }),
      "materialize, colocated": table({
         kind: "materialize",
         materialize: {},
         gate,
      }),
      "materialize, colocated, an inherited row filter": table({
         kind: "materialize",
         materialize: {},
         gate: { ...gate, inheritedRowFilter: true },
      }),
      "materialize, colocated, a gate not row-level": table({
         kind: "materialize",
         materialize: {},
         gate: { classification: "unclassified", attributed: true },
      }),
      "materialize, colocated, a gate not its own": table({
         kind: "materialize",
         materialize: {},
         gate: { ...gate, attributed: false },
      }),
      "materialize, storage": table({
         kind: "materialize",
         materialize: { persist: { storage: "lake" } },
         gate,
      }),
      "materialize, storage, an inherited row filter": table({
         kind: "materialize",
         materialize: { persist: { storage: "lake" } },
         gate: { ...gate, inheritedRowFilter: true },
      }),
      "materialize, storage, a gate not row-level": table({
         kind: "materialize",
         materialize: { persist: { storage: "lake" } },
         gate: { classification: "unclassified", attributed: true },
      }),
      "rollup, colocated": table({
         kind: "rollup",
         rollup: { variant: true, baseGated: true },
      }),
      "rollup, storage": table({
         kind: "rollup",
         rollup: { variant: true, baseGated: true, storage: "lake" },
      }),
      "rollup, colocated, ungated base": table({
         kind: "rollup",
         rollup: { variant: true, baseGated: false },
      }),
      "rollup (v1), colocated": table({
         kind: "rollup",
         rollup: { baseGated: true },
      }),
   } as unknown as Record<string, Parameters<typeof gateEnforcedAtRead>[1]>;

   for (const s of SWITCHES) {
      for (const reviewed of [false, true]) {
         const label = `${switchLabel(s)}, storage rollups ${reviewed ? "reviewed" : "not reviewed"}`;
         it(label, () => {
            const restore = installSettings({
               env: {
                  PUBLISHER_MATERIALIZE: String(s.materialize),
                  PUBLISHER_MATERIALIZE_GATED_SOURCES: String(s.gatedSources),
               },
            }).restore;
            const unreview = allowGatedStorageRollupsForTests(reviewed);
            try {
               const rows = Object.fromEntries(
                  // A storage row is asked as stored, as the storage
                  // eligibility and bind ask it.
                  Object.entries(inputs).map(([row, plan]) => [
                     row,
                     gateEnforcedAtRead(ID, plan, row.includes(", storage")),
                  ]),
               );
               if (DUMP) dumped[`predicate ${label}`] = rows;
               else expect(rows).toEqual(PLAN_PREDICATES[label]);
            } finally {
               unreview();
               restore();
            }
         });
      }
   }
});

/** The router under an entry-point veto, per leg, tier read, and switches. */
const ROUTER: Record<string, Record<string, string>> = {
   "materialize on, gated sources on, the colocated rule on (as every model passes it)":
      {
         "view, colocated tables": "used, past the gate",
         "view, storage tables": "used, past the gate",
         "view, colocated and storage tables": "used, past the gate",
         "view, storage tables kept past their own gate": "used, past the gate",
         "view, a kept and another storage table": "used, past the gate",
         "view, no table": "used, past the gate",
         "rollup, colocated tables": "used, past the gate",
         "rollup, storage tables": "used, past the gate",
         "rollup, colocated and storage tables": "used, past the gate",
         "rollup, storage tables kept past their own gate":
            "used, past the gate",
         "rollup, a kept and another storage table": "used, past the gate",
         "rollup, no table": "used, past the gate",
         "served, colocated tables": "used, past the gate",
         "served, storage tables": "used, past the gate",
         "served, colocated and storage tables": "used, past the gate",
         "served, storage tables kept past their own gate":
            "used, past the gate",
         "served, a kept and another storage table": "used, past the gate",
         "served, no table": "used, past the gate",
      },
   "materialize on, gated sources off, the colocated rule on (as every model passes it)":
      {
         "view, colocated tables": "skipped row_level_gate",
         "view, storage tables": "skipped row_level_gate",
         "view, colocated and storage tables": "skipped row_level_gate",
         "view, storage tables kept past their own gate":
            "skipped row_level_gate",
         "view, a kept and another storage table": "skipped row_level_gate",
         "view, no table": "skipped row_level_gate",
         "rollup, colocated tables": "used, past the gate",
         "rollup, storage tables": "skipped row_level_gate",
         "rollup, colocated and storage tables": "used, past the gate",
         "rollup, storage tables kept past their own gate":
            "skipped row_level_gate",
         "rollup, a kept and another storage table": "skipped row_level_gate",
         "rollup, no table": "used, past the gate",
         "served, colocated tables": "used, past the gate",
         "served, storage tables": "skipped row_level_gate",
         "served, colocated and storage tables": "skipped row_level_gate",
         "served, storage tables kept past their own gate":
            "used, past the gate",
         "served, a kept and another storage table": "skipped row_level_gate",
         "served, no table": "skipped row_level_gate",
      },
   "materialize off, gated sources on, the colocated rule on (as every model passes it)":
      {
         "view, colocated tables": "used, past the gate",
         "view, storage tables": "used, past the gate",
         "view, colocated and storage tables": "used, past the gate",
         "view, storage tables kept past their own gate": "used, past the gate",
         "view, a kept and another storage table": "used, past the gate",
         "view, no table": "used, past the gate",
         "rollup, colocated tables": "used, past the gate",
         "rollup, storage tables": "used, past the gate",
         "rollup, colocated and storage tables": "used, past the gate",
         "rollup, storage tables kept past their own gate":
            "used, past the gate",
         "rollup, a kept and another storage table": "used, past the gate",
         "rollup, no table": "used, past the gate",
         "served, colocated tables": "used, past the gate",
         "served, storage tables": "used, past the gate",
         "served, colocated and storage tables": "used, past the gate",
         "served, storage tables kept past their own gate":
            "used, past the gate",
         "served, a kept and another storage table": "used, past the gate",
         "served, no table": "used, past the gate",
      },
   "materialize off, gated sources off, the colocated rule on (as every model passes it)":
      {
         "view, colocated tables": "skipped row_level_gate",
         "view, storage tables": "skipped row_level_gate",
         "view, colocated and storage tables": "skipped row_level_gate",
         "view, storage tables kept past their own gate":
            "skipped row_level_gate",
         "view, a kept and another storage table": "skipped row_level_gate",
         "view, no table": "skipped row_level_gate",
         "rollup, colocated tables": "used, past the gate",
         "rollup, storage tables": "skipped row_level_gate",
         "rollup, colocated and storage tables": "used, past the gate",
         "rollup, storage tables kept past their own gate":
            "skipped row_level_gate",
         "rollup, a kept and another storage table": "skipped row_level_gate",
         "rollup, no table": "used, past the gate",
         "served, colocated tables": "used, past the gate",
         "served, storage tables": "skipped row_level_gate",
         "served, colocated and storage tables": "skipped row_level_gate",
         "served, storage tables kept past their own gate":
            "used, past the gate",
         "served, a kept and another storage table": "skipped row_level_gate",
         "served, no table": "skipped row_level_gate",
      },
};

describe("gate keeping at the router, under an entry-point veto", () => {
   const table = (
      tier: "database" | "storage",
   ): {
      kind: string;
      tier: "database" | "storage";
      sourceName: string;
      tablePath: string;
      ownGateKept?: boolean;
   } => ({
      kind: "materialize",
      tier,
      sourceName: `t_${tier}`,
      tablePath: `t_${tier}`,
   });
   const reads: Record<string, ReturnType<typeof table>[]> = {
      "colocated tables": [table("database")],
      "storage tables": [table("storage")],
      "colocated and storage tables": [table("database"), table("storage")],
      // G3 on the storage tier: a table plan admitted past its own gate,
      // alone and beside one it did not admit.
      "storage tables kept past their own gate": [
         { ...table("storage"), ownGateKept: true },
      ],
      "a kept and another storage table": [
         { ...table("storage"), ownGateKept: true },
         { ...table("storage"), sourceName: "t_other" },
      ],
      "no table": [],
   };
   const runnable = {} as never;

   function host(tables: ReturnType<typeof table>[]): Required<LegContext> {
      return {
         servedCopy: {
            hasBindings: () => true,
            loadQuery: async () => ({
               runnable,
               virtualMap: new Map(),
               bindings: tables,
               tablesRead: tables,
            }),
         },
         rollupVariants: {
            attempt: async () => ({
               runnable,
               rollupSourceName: "r",
               ...(tables[0] ? { binding: tables[0] } : {}),
               bindings: tables,
            }),
         },
         persistedViews: {
            calledView: () => "v",
            attempt: async () => ({
               runnable,
               table: { sourceName: "v" },
               graftScope: {},
               ...(tables[0] ? { binding: tables[0] } : {}),
            }),
         },
      } as unknown as Required<LegContext>;
   }

   for (const s of SWITCHES) {
      const label = `${switchLabel(s)}, the colocated rule on (as every model passes it)`;
      it(label, async () => {
         const restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: String(s.materialize),
               PUBLISHER_MATERIALIZE_GATED_SOURCES: String(s.gatedSources),
            },
         }).restore;
         try {
            const rows: Record<string, string> = {};
            for (const leg of ["view", "rollup", "served"] as const) {
               for (const [read, tables] of Object.entries(reads)) {
                  const pinned = pin(EMPTY_SNAPSHOT, new Date(0));
                  const legs = host(tables);
                  const candidates = variantsOf(
                     makeLegs(pinned, {
                        ...(leg === "served"
                           ? { servedCopy: legs.servedCopy }
                           : {}),
                        ...(leg === "rollup"
                           ? { rollupVariants: legs.rollupVariants }
                           : {}),
                        ...(leg === "view"
                           ? { persistedViews: legs.persistedViews }
                           : {}),
                     }),
                     {
                        queryString: "run: s -> v",
                        givens: undefined,
                        authorRunnable: runnable,
                     },
                     pinned,
                     { modelPath: "m.malloy" },
                  );
                  const decision = await routeQuery(
                     candidates,
                     async () => "entry_point",
                     { recordMetrics: false },
                  );
                  const c = decision.considered[0];
                  // A use under the veto is a use past the gate, which is
                  // what sends the answer to `govern()` to graft and prove.
                  expect(
                     decision.served === "variant" &&
                        decision.pastEntryPointGate === true,
                  ).toBe(c.outcome === "used");
                  rows[`${leg}, ${read}`] =
                     c.outcome === "used"
                        ? "used, past the gate"
                        : `skipped ${c.reason}`;
               }
            }
            if (DUMP) dumped[`router ${label}`] = rows;
            else expect(rows).toEqual(ROUTER[label]);
         } finally {
            restore();
         }
      });
   }
});
