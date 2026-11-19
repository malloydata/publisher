// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of read admission: whether a routed answer that read
 * stored tables is used, or declined and answered live, and why, for each
 * leg x table read x gate shape x switch. It drives the router's own
 * candidates (`variantsOf` and `routeQuery`) over a real author compile, so
 * every rule an answer passes today is in it at once: the gate policy
 * (`domain/gate_policy.ts`, legMayPassGate / answerKeepsGates), the bind's
 * marks (`ownGateKept`, `holdsOnlyUngatedRows`), and query_serving's
 * exactness, zone, collation, composite and row-scope checks.
 *
 * This table is what a single pure `admitRead` must reproduce exactly. The
 * bind's marks are pinned at their own rule below it
 * (`keptTableAddresses`, OR over sharers; `ungatedTableAddresses`, AND over
 * every source at an address). The serve verdict's narrower question is
 * asked of the same rows in the last block, so where it and the router
 * disagree today is visible.
 *
 * `ADMISSION_DUMP=<file>` writes the observed table instead of comparing it.
 */

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   type QueryMaterializer,
   SingleConnectionRuntime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { installSettings } from "../../test_helpers/settings_env";
import { routeQuery } from "../query_router";
import { variantsOf } from "../query_serving";
import { type Binding, EMPTY_SNAPSHOT, pin } from "./domain/binding";
import { admitRead, type ReadFacts } from "./domain/admission";
import {
   answerKeepsGates,
   SERVING_GATE_CONTEXT,
   type TableRead,
} from "./domain/gate_policy";
import { keptTableAddresses, ungatedTableAddresses } from "./serve/bind";
import * as gateReads from "./serve/gate_reads";
import { type LegContext, makeLegs } from "./serve/leg";

const DUMP = process.env.ADMISSION_DUMP;
const dumped: Record<string, unknown> = {};

// ---------------------------------------------------------------------------
// The author's model: one source per gate shape, over the same columns.
// ---------------------------------------------------------------------------

const COLUMNS = `duckdb.sql("SELECT 1 AS id, 'east' AS region, CAST(1.5 AS DOUBLE) AS amount, TIMESTAMPTZ '2026-01-01 00:00:00+00' AS at")`;
const MODEL = `##! experimental { givens composite_sources }
given: N :: number
given: S :: string
given: FLAG :: boolean
given: ROLE :: string
source: base is ${COLUMNS} extend { dimension: late is region > 'M' }
source: open is base
source: scoped is base extend { where: amount > 1 }
source: scoped_str is base extend { where: region > 'M' }
#(access_filter) id = $N
source: num_gate is base
#(access_filter) region = $S
source: str_eq_gate is base
#(access_filter) region > $S
source: str_order_gate is base
#(access_filter) late = $FLAG
source: bool_over_str_gate is base
#(authorize) 'analyst' = $ROLE
source: lock_gate is base
#(access_filter) amount = $N
source: float_gate is base
#(access_filter) at > @2020-01-01
source: zoned_gate is base
#(access_filter) id = $N
source: composite_gate is compose(base, base)
`;

/** The gate shapes, by the author source that has each. */
const SHAPES = {
   "no gate": "open",
   "no gate, a where: on a float column": "scoped",
   "no gate, a where: ordering a string": "scoped_str",
   "row filter, number": "num_gate",
   "row filter, string equality": "str_eq_gate",
   "row filter, string ordering": "str_order_gate",
   "row filter, boolean over a string": "bool_over_str_gate",
   "lock only": "lock_gate",
   "row filter, float column": "float_gate",
   "row filter, zoned timestamp column": "zoned_gate",
   "row filter on a composite": "composite_gate",
} as const;
type Shape = keyof typeof SHAPES;
const GATED = (shape: Shape) => !shape.startsWith("no gate");

/** An author compile per collation of the author's instance. */
const authors: Record<"binary" | "nocase", SingleConnectionRuntime> =
   {} as never;
const connections: DuckDBConnection[] = [];
// Each author instance in a database file of its own: `:memory:` instances
// are shared across the process, so a `default_collation` set on one would
// reach every other spec's.
const dir = mkdtempSync(join(tmpdir(), "admission-"));
beforeAll(async () => {
   for (const collation of ["binary", "nocase"] as const) {
      const connection = new DuckDBConnection(
         "duckdb",
         join(dir, `${collation}.duckdb`),
      );
      connections.push(connection);
      if (collation === "nocase") {
         await connection.runSQL("SET default_collation = 'nocase'");
      }
      authors[collation] = new SingleConnectionRuntime({ connection });
   }
});
afterAll(async () => {
   for (const c of connections) await c.close();
   rmSync(dir, { recursive: true, force: true });
   if (DUMP) writeFileSync(DUMP, JSON.stringify(dumped, null, 3));
});

const authorOf = (
   source: string,
   collation: "binary" | "nocase" = "binary",
): QueryMaterializer =>
   authors[collation]
      .loadModel(MODEL)
      .loadQuery(`run: ${source} -> { select: id }`);

// ---------------------------------------------------------------------------
// The tables an answer read, and the stored side's collation.
// ---------------------------------------------------------------------------

/** The stored table's columns: the zone and the float are stored as such. */
const SCHEMA = [
   { name: "id", type: "BIGINT" },
   { name: "region", type: "VARCHAR" },
   { name: "amount", type: "DOUBLE" },
   { name: "at", type: "TIMESTAMP WITH TIME ZONE" },
];

const table = (tier: "database" | "storage", extra: Partial<Binding> = {}) =>
   ({
      kind: "persist",
      tier,
      sourceName: "base",
      destinationName: tier === "storage" ? "lake" : "duckdb",
      virtualHandle: `addr-${tier}`,
      tablePath: `t_${tier}`,
      schema: SCHEMA,
      ...extra,
   }) as unknown as Binding;

const READS: Record<string, Binding[]> = {
   database: [table("database")],
   storage: [table("storage")],
   "storage, kept past its own gate": [table("storage", { ownGateKept: true })],
   "storage, holding only ungated rows": [
      table("storage", { holdsOnlyUngatedRows: true }),
   ],
};

/** An answer's runtime whose every stored table compares as `collation`. */
function storedRuntime(collation: "binary" | "nocase"): unknown {
   const connection = {
      runSQL: async (sql: string) => ({
         rows: /default_collation/.test(sql)
            ? [{ c: collation === "nocase" ? "nocase" : "" }]
            : /duckdb_tables\(\)/.test(sql)
              ? [{ sql: "CREATE TABLE t(c VARCHAR);", n: 0 }]
              : [],
      }),
   };
   return { connections: { lookupConnection: async () => connection } };
}

// ---------------------------------------------------------------------------
// One routed answer.
// ---------------------------------------------------------------------------

const LEGS = ["served", "rollup", "view", "query"] as const;
type Leg = (typeof LEGS)[number];

/** A leg host whose one leg answers with `tables` read. */
function legHost(
   leg: Leg,
   tables: Binding[],
   runnable: QueryMaterializer,
): LegContext {
   const answer = {
      runnable,
      graftScope: {},
      ...(tables[0] ? { binding: tables[0] } : {}),
   };
   switch (leg) {
      case "served":
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
         } as unknown as LegContext;
      case "rollup":
         return {
            rollupVariants: {
               attempt: async () => ({
                  ...answer,
                  rollupSourceName: "r",
                  bindings: tables,
                  ...(tables.some((t) => t.tier === "storage")
                     ? { virtualMap: new Map() }
                     : {}),
               }),
            },
         } as unknown as LegContext;
      case "view":
         return {
            persistedViews: {
               calledView: () => "v",
               attempt: async () => ({ ...answer, table: { sourceName: "v" } }),
            },
         } as unknown as LegContext;
      case "query":
         return {
            persistedQueries: {
               calledQuery: () => "q",
               attempt: async () => ({ ...answer, table: { sourceName: "q" } }),
            },
         } as unknown as LegContext;
   }
}

/** The router's outcome for one leg's answer, as a short string. */
async function routed(args: {
   leg: Leg;
   shape: Shape;
   tables: Binding[];
   authorCollation?: "binary" | "nocase";
   storedCollation?: "binary" | "nocase";
   /** The router cannot read what the gates and row scope read. */
   unreadable?: boolean;
}): Promise<string> {
   const pinned = pin(EMPTY_SNAPSHOT, new Date(0));
   const authorRunnable = authorOf(SHAPES[args.shape], args.authorCollation);
   // The routed compile: the author's own query (so a composite resolves to
   // the same member), read through a runtime whose stored tables compare
   // as `storedCollation`.
   const runnable = {
      runtime: storedRuntime(args.storedCollation ?? "binary"),
      getPreparedQuery: () => authorRunnable.getPreparedQuery(),
   } as unknown as QueryMaterializer;
   const candidates = variantsOf(
      makeLegs(pinned, legHost(args.leg, args.tables, runnable)),
      {
         queryString: `run: ${SHAPES[args.shape]} -> v`,
         givens: undefined,
         authorRunnable,
      },
      pinned,
      { modelPath: "m.malloy" },
   );
   const unreadable = args.unreadable
      ? spyOn(gateReads, "gateReadsOfQuery").mockResolvedValue(undefined)
      : undefined;
   const decision = await routeQuery(
      candidates,
      async () => (GATED(args.shape) ? "entry_point" : false),
      { recordMetrics: false },
   ).finally(() => unreadable?.mockRestore());
   const c = decision.considered[0];
   if (c.outcome === "used") {
      return decision.served === "variant" && decision.pastEntryPointGate
         ? "used, past the gate"
         : "used";
   }
   return `skipped ${c.reason}${c.code ? ` (${c.code})` : ""}`;
}

/** Run `fn` with the switches set, restoring them after. */
async function withSwitches<T>(
   gatedSources: boolean,
   fn: () => Promise<T>,
): Promise<T> {
   const { restore } = installSettings({
      env: {
         PUBLISHER_MATERIALIZE: "true",
         PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
      },
   });
   try {
      return await fn();
   } finally {
      restore();
   }
}

// ---------------------------------------------------------------------------
// The table: leg x table read, per gate shape and switch.
// ---------------------------------------------------------------------------

// "gated sources off" is the gate policy's G3 row (gate_policy.ts): past a
// gate, only a database table, or a `storage=` table kept past its own gate
// or holding only ungated rows, keeps the gates.
const SWITCHES = [
   ["gated sources off", false],
   ["gated sources on", true],
] as const;

/** shape -> switch -> "leg, table read" -> outcome. */
const ADMISSION: Record<string, Record<string, Record<string, string>>> = {
   "no gate": {
      "gated sources off": {
         "served, database": "used",
         "served, storage": "used",
         "served, storage, kept past its own gate": "used",
         "served, storage, holding only ungated rows": "used",
         "rollup, database": "used",
         "rollup, storage": "used",
         "rollup, storage, kept past its own gate": "used",
         "rollup, storage, holding only ungated rows": "used",
         "view, database": "used",
         "view, storage": "used",
         "view, storage, kept past its own gate": "used",
         "view, storage, holding only ungated rows": "used",
         "query, database": "used",
         "query, storage": "used",
         "query, storage, kept past its own gate": "used",
         "query, storage, holding only ungated rows": "used",
      },
      "gated sources on": {
         "served, database": "used",
         "served, storage": "used",
         "served, storage, kept past its own gate": "used",
         "served, storage, holding only ungated rows": "used",
         "rollup, database": "used",
         "rollup, storage": "used",
         "rollup, storage, kept past its own gate": "used",
         "rollup, storage, holding only ungated rows": "used",
         "view, database": "used",
         "view, storage": "used",
         "view, storage, kept past its own gate": "used",
         "view, storage, holding only ungated rows": "used",
         "query, database": "used",
         "query, storage": "used",
         "query, storage, kept past its own gate": "used",
         "query, storage, holding only ungated rows": "used",
      },
   },
   "no gate, a where: on a float column": {
      "gated sources off": {
         "served, database": "used",
         "served, storage": "skipped does_not_answer (served_copy_not_served)",
         "served, storage, kept past its own gate":
            "skipped does_not_answer (served_copy_not_served)",
         "served, storage, holding only ungated rows":
            "skipped does_not_answer (served_copy_not_served)",
         "rollup, database": "used",
         "rollup, storage": "used",
         "rollup, storage, kept past its own gate": "used",
         "rollup, storage, holding only ungated rows": "used",
         "view, database": "used",
         "view, storage": "used",
         "view, storage, kept past its own gate": "used",
         "view, storage, holding only ungated rows": "used",
         "query, database": "used",
         "query, storage": "used",
         "query, storage, kept past its own gate": "used",
         "query, storage, holding only ungated rows": "used",
      },
      "gated sources on": {
         "served, database": "used",
         "served, storage": "skipped does_not_answer (served_copy_not_served)",
         "served, storage, kept past its own gate":
            "skipped does_not_answer (served_copy_not_served)",
         "served, storage, holding only ungated rows":
            "skipped does_not_answer (served_copy_not_served)",
         "rollup, database": "used",
         "rollup, storage": "used",
         "rollup, storage, kept past its own gate": "used",
         "rollup, storage, holding only ungated rows": "used",
         "view, database": "used",
         "view, storage": "used",
         "view, storage, kept past its own gate": "used",
         "view, storage, holding only ungated rows": "used",
         "query, database": "used",
         "query, storage": "used",
         "query, storage, kept past its own gate": "used",
         "query, storage, holding only ungated rows": "used",
      },
   },
   "no gate, a where: ordering a string": {
      "gated sources off": {
         "served, database": "used",
         "served, storage": "used",
         "served, storage, kept past its own gate": "used",
         "served, storage, holding only ungated rows": "used",
         "rollup, database": "used",
         "rollup, storage": "used",
         "rollup, storage, kept past its own gate": "used",
         "rollup, storage, holding only ungated rows": "used",
         "view, database": "used",
         "view, storage": "used",
         "view, storage, kept past its own gate": "used",
         "view, storage, holding only ungated rows": "used",
         "query, database": "used",
         "query, storage": "used",
         "query, storage, kept past its own gate": "used",
         "query, storage, holding only ungated rows": "used",
      },
      "gated sources on": {
         "served, database": "used",
         "served, storage": "used",
         "served, storage, kept past its own gate": "used",
         "served, storage, holding only ungated rows": "used",
         "rollup, database": "used",
         "rollup, storage": "used",
         "rollup, storage, kept past its own gate": "used",
         "rollup, storage, holding only ungated rows": "used",
         "view, database": "used",
         "view, storage": "used",
         "view, storage, kept past its own gate": "used",
         "view, storage, holding only ungated rows": "used",
         "query, database": "used",
         "query, storage": "used",
         "query, storage, kept past its own gate": "used",
         "query, storage, holding only ungated rows": "used",
      },
   },
   "row filter, number": {
      "gated sources off": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
      "gated sources on": {
         "served, database": "used, past the gate",
         "served, storage": "used, past the gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "used, past the gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "used, past the gate",
         "view, storage": "used, past the gate",
         "view, storage, kept past its own gate": "used, past the gate",
         "view, storage, holding only ungated rows": "used, past the gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
   },
   "row filter, string equality": {
      "gated sources off": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
      "gated sources on": {
         "served, database": "used, past the gate",
         "served, storage": "used, past the gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "used, past the gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "used, past the gate",
         "view, storage": "used, past the gate",
         "view, storage, kept past its own gate": "used, past the gate",
         "view, storage, holding only ungated rows": "used, past the gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
   },
   "row filter, string ordering": {
      "gated sources off": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
      "gated sources on": {
         "served, database": "used, past the gate",
         "served, storage": "used, past the gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "used, past the gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "used, past the gate",
         "view, storage": "used, past the gate",
         "view, storage, kept past its own gate": "used, past the gate",
         "view, storage, holding only ungated rows": "used, past the gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
   },
   "row filter, boolean over a string": {
      "gated sources off": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
      "gated sources on": {
         "served, database": "used, past the gate",
         "served, storage": "used, past the gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "used, past the gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "used, past the gate",
         "view, storage": "used, past the gate",
         "view, storage, kept past its own gate": "used, past the gate",
         "view, storage, holding only ungated rows": "used, past the gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
   },
   "lock only": {
      "gated sources off": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "used, past the gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
      "gated sources on": {
         "served, database": "used, past the gate",
         "served, storage": "used, past the gate",
         "served, storage, kept past its own gate": "used, past the gate",
         "served, storage, holding only ungated rows": "used, past the gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "used, past the gate",
         "rollup, storage, kept past its own gate": "used, past the gate",
         "rollup, storage, holding only ungated rows": "used, past the gate",
         "view, database": "used, past the gate",
         "view, storage": "used, past the gate",
         "view, storage, kept past its own gate": "used, past the gate",
         "view, storage, holding only ungated rows": "used, past the gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
   },
   "row filter, float column": {
      "gated sources off": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "skipped row_level_gate",
         "served, storage, holding only ungated rows": "skipped row_level_gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "skipped row_level_gate",
         "rollup, storage, holding only ungated rows": "skipped row_level_gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
      "gated sources on": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "skipped row_level_gate",
         "served, storage, holding only ungated rows": "skipped row_level_gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "skipped row_level_gate",
         "rollup, storage, holding only ungated rows": "skipped row_level_gate",
         "view, database": "used, past the gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
   },
   "row filter, zoned timestamp column": {
      "gated sources off": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "skipped row_level_gate",
         "served, storage, holding only ungated rows": "skipped row_level_gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "skipped row_level_gate",
         "rollup, storage, holding only ungated rows": "skipped row_level_gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
      "gated sources on": {
         "served, database": "used, past the gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "skipped row_level_gate",
         "served, storage, holding only ungated rows": "skipped row_level_gate",
         "rollup, database": "used, past the gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "skipped row_level_gate",
         "rollup, storage, holding only ungated rows": "skipped row_level_gate",
         "view, database": "used, past the gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
   },
   "row filter on a composite": {
      "gated sources off": {
         "served, database": "skipped row_level_gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "skipped row_level_gate",
         "served, storage, holding only ungated rows": "skipped row_level_gate",
         "rollup, database": "skipped row_level_gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "skipped row_level_gate",
         "rollup, storage, holding only ungated rows": "skipped row_level_gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
      "gated sources on": {
         "served, database": "skipped row_level_gate",
         "served, storage": "skipped row_level_gate",
         "served, storage, kept past its own gate": "skipped row_level_gate",
         "served, storage, holding only ungated rows": "skipped row_level_gate",
         "rollup, database": "skipped row_level_gate",
         "rollup, storage": "skipped row_level_gate",
         "rollup, storage, kept past its own gate": "skipped row_level_gate",
         "rollup, storage, holding only ungated rows": "skipped row_level_gate",
         "view, database": "skipped row_level_gate",
         "view, storage": "skipped row_level_gate",
         "view, storage, kept past its own gate": "skipped row_level_gate",
         "view, storage, holding only ungated rows": "skipped row_level_gate",
         "query, database": "skipped row_level_gate",
         "query, storage": "skipped row_level_gate",
         "query, storage, kept past its own gate": "skipped row_level_gate",
         "query, storage, holding only ungated rows": "skipped row_level_gate",
      },
   },
};

describe("read admission at the router, per leg, table read, gate shape and switch", () => {
   for (const shape of Object.keys(SHAPES) as Shape[]) {
      for (const [switchLabel, gatedSources] of SWITCHES) {
         it(`${shape}, ${switchLabel}`, async () => {
            const rows: Record<string, string> = {};
            await withSwitches(gatedSources, async () => {
               for (const leg of LEGS) {
                  for (const [read, tables] of Object.entries(READS)) {
                     rows[`${leg}, ${read}`] = await routed({
                        leg,
                        shape,
                        tables,
                     });
                  }
               }
            });
            if (DUMP) {
               ((dumped.admission ??= {}) as Record<string, unknown>)[
                  `${shape} | ${switchLabel}`
               ] = rows;
            } else {
               expect(rows).toEqual(ADMISSION[shape]?.[switchLabel] ?? {});
            }
         });
      }
   }
});

/** shape -> "where the collation is not binary, table read" -> outcome, served leg, gated sources off. */
const COLLATION: Record<string, Record<string, string>> = {
   "row filter, number": {
      "author instance nocase, database": "used, past the gate",
      "author instance nocase, storage, kept past its own gate":
         "used, past the gate",
      "stored table nocase, database": "used, past the gate",
      "stored table nocase, storage, kept past its own gate":
         "used, past the gate",
   },
   "row filter, string equality": {
      "author instance nocase, database": "used, past the gate",
      "author instance nocase, storage, kept past its own gate":
         "skipped row_level_gate",
      "stored table nocase, database": "skipped row_level_gate",
      "stored table nocase, storage, kept past its own gate":
         "skipped row_level_gate",
   },
   "row filter, string ordering": {
      "author instance nocase, database": "used, past the gate",
      "author instance nocase, storage, kept past its own gate":
         "skipped row_level_gate",
      "stored table nocase, database": "skipped row_level_gate",
      "stored table nocase, storage, kept past its own gate":
         "skipped row_level_gate",
   },
   // A gate on a boolean computed by ordering a string is judged as the
   // string gates above are: both collation probes run.
   "row filter, boolean over a string": {
      "author instance nocase, database": "used, past the gate",
      "author instance nocase, storage, kept past its own gate":
         "skipped row_level_gate",
      "stored table nocase, database": "skipped row_level_gate",
      "stored table nocase, storage, kept past its own gate":
         "skipped row_level_gate",
   },
   "no gate, a where: on a float column": {
      "author instance nocase, database": "used",
      "author instance nocase, storage, kept past its own gate":
         "skipped does_not_answer (served_copy_not_served)",
      "stored table nocase, database": "used",
      "stored table nocase, storage, kept past its own gate":
         "skipped does_not_answer (served_copy_not_served)",
   },
   // An ungated scope on a database table is judged by the stored side's
   // collation too, whatever its columns' types.
   "no gate, a where: ordering a string": {
      "author instance nocase, database": "used",
      "author instance nocase, storage, kept past its own gate":
         "skipped does_not_answer (served_copy_not_served)",
      "stored table nocase, database":
         "skipped does_not_answer (served_copy_not_served)",
      "stored table nocase, storage, kept past its own gate":
         "skipped does_not_answer (served_copy_not_served)",
   },
};

describe("read admission under a collation that is not binary (served leg, gated sources off)", () => {
   const shapes: Shape[] = [
      "row filter, number",
      "row filter, string equality",
      "row filter, string ordering",
      "row filter, boolean over a string",
      "no gate, a where: on a float column",
      "no gate, a where: ordering a string",
   ];
   for (const shape of shapes) {
      it(shape, async () => {
         const rows: Record<string, string> = {};
         await withSwitches(false, async () => {
            for (const [where, collations] of [
               ["author instance nocase", { authorCollation: "nocase" }],
               ["stored table nocase", { storedCollation: "nocase" }],
            ] as const) {
               for (const read of [
                  "database",
                  "storage, kept past its own gate",
               ]) {
                  rows[`${where}, ${read}`] = await routed({
                     leg: "served",
                     shape,
                     tables: READS[read],
                     ...collations,
                  });
               }
            }
         });
         if (DUMP) {
            ((dumped.collation ??= {}) as Record<string, unknown>)[shape] =
               rows;
         } else {
            expect(rows).toEqual(COLLATION[shape] ?? {});
         }
      });
   }
});

/**
 * The row scope's and the gate's two fail-closed inputs, on no gate, a
 * `where:` on `amount`, and a row filter on `id` (past the gate): a database
 * table whose columns moved since its build (`retypedColumns`, so their
 * stored types are listed and judged, admitRead's `columnsListed`), and a
 * compile whose gate and scope reads cannot be listed (admitRead's
 * `reads === undefined`).
 */
const MOVED_READS: Record<string, Binding[]> = {
   database: [table("database")],
   "database, amount moved to a float": [
      table("database", {
         retypedColumns: [{ name: "amount", type: "DOUBLE" }],
      }),
   ],
   "database, id moved to a float": [
      table("database", { retypedColumns: [{ name: "id", type: "DOUBLE" }] }),
   ],
   storage: [table("storage")],
};

/** The columns each shape's gates and row scope read, by name. */
const READ_COLUMNS: Partial<Record<Shape, string[]>> = {
   "no gate": [],
   "no gate, a where: on a float column": ["amount"],
   "row filter, number": ["id"],
};

/** shape -> "reads, leg, table read" -> outcome, gated sources off. */
const ROW_SCOPE: Record<string, Record<string, string>> = {
   "no gate": {
      "readable, served, database": "used",
      "readable, served, database, amount moved to a float": "used",
      "readable, served, database, id moved to a float": "used",
      "readable, served, storage": "used",
      "readable, rollup, database": "used",
      "readable, rollup, database, amount moved to a float": "used",
      "readable, rollup, database, id moved to a float": "used",
      "readable, rollup, storage": "used",
      "unreadable, served, database": "used",
      "unreadable, served, database, amount moved to a float":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, served, database, id moved to a float":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, served, storage":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, rollup, database": "used",
      "unreadable, rollup, database, amount moved to a float": "used",
      "unreadable, rollup, database, id moved to a float": "used",
      "unreadable, rollup, storage":
         "skipped does_not_answer (rollup_row_level_gate)",
   },
   "no gate, a where: on a float column": {
      "readable, served, database": "used",
      "readable, served, database, amount moved to a float":
         "skipped does_not_answer (served_copy_not_served)",
      "readable, served, database, id moved to a float": "used",
      "readable, served, storage":
         "skipped does_not_answer (served_copy_not_served)",
      "readable, rollup, database": "used",
      "readable, rollup, database, amount moved to a float": "used",
      "readable, rollup, database, id moved to a float": "used",
      "readable, rollup, storage": "used",
      "unreadable, served, database": "used",
      "unreadable, served, database, amount moved to a float":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, served, database, id moved to a float":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, served, storage":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, rollup, database": "used",
      "unreadable, rollup, database, amount moved to a float": "used",
      "unreadable, rollup, database, id moved to a float": "used",
      "unreadable, rollup, storage":
         "skipped does_not_answer (rollup_row_level_gate)",
   },
   "row filter, number": {
      "readable, served, database": "used, past the gate",
      "readable, served, database, amount moved to a float":
         "used, past the gate",
      "readable, served, database, id moved to a float":
         "skipped row_level_gate",
      "readable, served, storage": "skipped row_level_gate",
      "readable, rollup, database": "used, past the gate",
      "readable, rollup, database, amount moved to a float":
         "used, past the gate",
      "readable, rollup, database, id moved to a float":
         "skipped row_level_gate",
      "readable, rollup, storage": "skipped row_level_gate",
      "unreadable, served, database": "used, past the gate",
      "unreadable, served, database, amount moved to a float":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, served, database, id moved to a float":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, served, storage":
         "skipped does_not_answer (served_copy_not_served)",
      "unreadable, rollup, database": "used, past the gate",
      "unreadable, rollup, database, amount moved to a float":
         "skipped row_level_gate",
      "unreadable, rollup, database, id moved to a float":
         "skipped row_level_gate",
      "unreadable, rollup, storage":
         "skipped does_not_answer (rollup_row_level_gate)",
   },
};

describe("read admission over moved columns and unreadable reads, gated sources off", () => {
   for (const shape of Object.keys(READ_COLUMNS) as Shape[]) {
      it(shape, async () => {
         const rows: Record<string, string> = {};
         await withSwitches(false, async () => {
            for (const unreadable of [false, true]) {
               for (const leg of ["served", "rollup"] as const) {
                  for (const [read, tables] of Object.entries(MOVED_READS)) {
                     rows[
                        `${unreadable ? "unreadable" : "readable"}, ${leg}, ${read}`
                     ] = await routed({ leg, shape, tables, unreadable });
                  }
               }
            }
         });
         if (DUMP) {
            ((dumped.rowScope ??= {}) as Record<string, unknown>)[shape] = rows;
         } else {
            expect(rows).toEqual(ROW_SCOPE[shape] ?? {});
         }
      });
   }
});

describe("the bind's admission marks", () => {
   it("marks a table kept past its own gate when ANY source stored at its address is (OR over sharers)", () => {
      const address = { a: "x", b: "x", c: "y" } as Record<string, string>;
      expect([...keptTableAddresses(["a"], (id) => address[id])]).toEqual([
         "x",
      ]);
      expect(
         [...keptTableAddresses(["a", "c"], (id) => address[id])].sort(),
      ).toEqual(["x", "y"]);
      expect([...keptTableAddresses(undefined, (id) => address[id])]).toEqual(
         [],
      );
      // A source with no address marks nothing.
      expect([...keptTableAddresses(["z"], (id) => address[id])]).toEqual([]);
   });

   it("marks a table as holding only ungated rows when EVERY source the plan stores at its address is (AND over sources)", () => {
      const sources = {
         a: { sourceEntityId: "x" },
         b: { sourceEntityId: "x" },
         c: { sourceEntityId: "y" },
         d: {},
      };
      expect(
         [...ungatedTableAddresses(["a", "b", "c"], sources)].sort(),
      ).toEqual(["x", "y"]);
      // One gated (or unread) sharer keeps the address out.
      expect([...ungatedTableAddresses(["a", "c"], sources)]).toEqual(["y"]);
      expect([...ungatedTableAddresses(undefined, sources)]).toEqual([]);
   });
});

/**
 * The serve verdict's question (`serve_verdicts.ts` `keepsGate`), asked of
 * one source's own table as it asks it: the gate policy over the table's
 * tier, marks and gate column types, with none of the router's comparison,
 * composite or row-scope checks. So it says `true` for a `storage=` table
 * whose gate column is zoned, where the router above declines the same read
 * (`row filter, zoned timestamp column`): the drift `admitRead` removes.
 */
const VERDICT: Record<string, Record<string, boolean>> = {
   "gated sources off": {
      "database, number gate column": true,
      "database, float gate column": true,
      "database, zoned gate column": true,
      "database, string gate column": true,
      "storage, number gate column": false,
      "storage, float gate column": false,
      "storage, zoned gate column": false,
      "storage, string gate column": false,
      "storage, kept past its own gate, number gate column": true,
      "storage, kept past its own gate, float gate column": false,
      "storage, kept past its own gate, zoned gate column": true,
      "storage, kept past its own gate, string gate column": true,
      "storage, holding only ungated rows, number gate column": true,
      "storage, holding only ungated rows, float gate column": false,
      "storage, holding only ungated rows, zoned gate column": true,
      "storage, holding only ungated rows, string gate column": true,
   },
   "gated sources on": {
      "database, number gate column": true,
      "database, float gate column": true,
      "database, zoned gate column": true,
      "database, string gate column": true,
      "storage, number gate column": true,
      "storage, float gate column": false,
      "storage, zoned gate column": true,
      "storage, string gate column": true,
      "storage, kept past its own gate, number gate column": true,
      "storage, kept past its own gate, float gate column": false,
      "storage, kept past its own gate, zoned gate column": true,
      "storage, kept past its own gate, string gate column": true,
      "storage, holding only ungated rows, number gate column": true,
      "storage, holding only ungated rows, float gate column": false,
      "storage, holding only ungated rows, zoned gate column": true,
      "storage, holding only ungated rows, string gate column": true,
   },
};

describe("the serve verdict's narrower question, for the same tables", () => {
   for (const [switchLabel, gatedSources] of SWITCHES) {
      it(switchLabel, () => {
         const rows: Record<string, boolean> = {};
         for (const [read, [t]] of Object.entries(READS)) {
            for (const [columns, types] of [
               ["number gate column", ["BIGINT"]],
               ["float gate column", ["DOUBLE"]],
               ["zoned gate column", ["TIMESTAMP WITH TIME ZONE"]],
               ["string gate column", ["VARCHAR"]],
            ] as const) {
               rows[`${read}, ${columns}`] = answerKeepsGates(
                  "served",
                  [
                     {
                        tier: t.tier,
                        ownGateKept:
                           t.tier === "storage" && t.ownGateKept === true,
                        holdsOnlyUngatedRows:
                           t.tier === "storage" &&
                           t.holdsOnlyUngatedRows === true,
                        ...(t.tier === "storage"
                           ? { gateColumnTypes: [...types] }
                           : {}),
                     },
                  ],
                  { materialize: true, gatedSources },
                  SERVING_GATE_CONTEXT,
               );
            }
         }
         if (DUMP) {
            ((dumped.verdict ??= {}) as Record<string, unknown>)[switchLabel] =
               rows;
         } else {
            expect(rows).toEqual(VERDICT[switchLabel] ?? {});
         }
      });
   }
});

// ---------------------------------------------------------------------------
// The pure core: admitRead reproduces both tables above from the facts.
// ---------------------------------------------------------------------------

/**
 * What the router gathers for each gate shape, the probes' answers aside:
 * whether the entry point is gated, a composite row filter, a row scope,
 * the stored types of the columns its gates and scope read (from SCHEMA),
 * whether they read no column at all (a lock), and whether they compare a
 * string (which a collation that is not binary refuses).
 */
const SHAPE_FACTS: Record<
   Shape,
   {
      composite?: true;
      scoped?: true;
      types: string[];
      readsNoColumn?: true;
      comparesString?: true;
   }
> = {
   "no gate": { types: [], readsNoColumn: true },
   "no gate, a where: on a float column": { scoped: true, types: ["DOUBLE"] },
   "no gate, a where: ordering a string": {
      scoped: true,
      types: ["VARCHAR"],
      comparesString: true,
   },
   "row filter, number": { types: ["BIGINT"] },
   "row filter, string equality": { types: ["VARCHAR"], comparesString: true },
   "row filter, string ordering": { types: ["VARCHAR"], comparesString: true },
   "row filter, boolean over a string": {
      types: ["VARCHAR"],
      comparesString: true,
   },
   "lock only": { types: [], readsNoColumn: true },
   "row filter, float column": { types: ["DOUBLE"] },
   "row filter, zoned timestamp column": {
      types: ["TIMESTAMP WITH TIME ZONE"],
   },
   "row filter on a composite": { composite: true, types: ["BIGINT"] },
};

/** admitRead's verdict for one row, written as the router's outcome reads. */
function admitted(args: {
   leg: Leg;
   shape: Shape;
   tables: Binding[];
   gatedSources: boolean;
   authorCollation?: "binary" | "nocase";
   storedCollation?: "binary" | "nocase";
}): string {
   const f = SHAPE_FACTS[args.shape];
   const gated = GATED(args.shape);
   const tables: TableRead[] = args.tables.map((t) => ({
      tier: t.tier,
      ...(t.ownGateKept ? { ownGateKept: true } : {}),
      ...(t.holdsOnlyUngatedRows ? { holdsOnlyUngatedRows: true } : {}),
      ...(t.tier === "storage"
         ? {
              gateColumnTypes: f.types,
              ...(f.readsNoColumn ? { gatesReadNoColumn: true } : {}),
           }
         : {}),
   }));
   const refusal = (collation: "binary" | "nocase" | undefined) =>
      collation === "nocase" && f.comparesString
         ? "compares under a collation that is not binary"
         : undefined;
   const facts: ReadFacts = {
      pastEntryPointGate: gated,
      compositeRowFilter: f.composite === true,
      reads: { scoped: f.scoped === true, gated },
      columnsListed: tables.some((t) => t.tier === "storage"),
      sourceCollationRefusal: refusal(args.authorCollation),
      storedCollationRefusal: refusal(args.storedCollation),
   };
   const verdict = admitRead(args.leg, tables, facts, {
      materialize: true,
      gatedSources: args.gatedSources,
   });
   if (verdict.admit) {
      return verdict.pastEntryPointGate ? "used, past the gate" : "used";
   }
   if (verdict.reason === "row_level_gate") return "skipped row_level_gate";
   return args.leg === "served"
      ? "skipped does_not_answer (served_copy_not_served)"
      : "skipped does_not_answer (rollup_row_level_gate)";
}

/**
 * admitRead's verdict for one row of {@link ROW_SCOPE}, from what the router
 * gathers there: a moved database column is judged by the type it holds now
 * (only a scoped one while the reads can be listed, every one when they
 * cannot), and so is listed (`columnsListed`); unreadable reads leave a
 * `storage=` table's scope columns unproven, and the source side's collation
 * probe, which cannot tell what is compared, refuses.
 */
function admittedOverMoves(args: {
   leg: "served" | "rollup";
   shape: Shape;
   tables: Binding[];
   unreadable: boolean;
}): string {
   const read = READ_COLUMNS[args.shape] ?? [];
   const gated = GATED(args.shape);
   const typeOf = (name: string) =>
      SCHEMA.find((c) => c.name === name)?.type ?? "";
   const tables: TableRead[] = args.tables.map((t) => {
      if (t.tier === "storage") {
         return args.unreadable
            ? { tier: "storage" }
            : {
                 tier: "storage",
                 gateColumnTypes: read.map(typeOf),
                 ...(read.length === 0 ? { gatesReadNoColumn: true } : {}),
              };
      }
      const moved = t.retypedColumns ?? [];
      if (moved.length === 0) return { tier: "database" };
      return {
         tier: "database",
         gateColumnTypes: moved
            .filter((c) => args.unreadable || read.includes(c.name))
            .map((c) => c.type),
      };
   });
   const facts: ReadFacts = {
      pastEntryPointGate: gated,
      compositeRowFilter: false,
      reads: args.unreadable
         ? undefined
         : { scoped: !gated && read.length > 0, gated },
      columnsListed: tables.some(
         (t) => t.tier === "storage" || t.gateColumnTypes !== undefined,
      ),
      sourceCollationRefusal: args.unreadable
         ? "what the scope compares cannot be read"
         : undefined,
   };
   const verdict = admitRead(args.leg, tables, facts, {
      materialize: true,
      gatedSources: false,
   });
   if (verdict.admit) {
      return verdict.pastEntryPointGate ? "used, past the gate" : "used";
   }
   if (verdict.reason === "row_level_gate") return "skipped row_level_gate";
   return args.leg === "served"
      ? "skipped does_not_answer (served_copy_not_served)"
      : "skipped does_not_answer (rollup_row_level_gate)";
}

describe("admitRead reproduces the router's admission", () => {
   it("for every leg, table read, gate shape and switch", () => {
      for (const shape of Object.keys(SHAPES) as Shape[]) {
         for (const [switchLabel, gatedSources] of SWITCHES) {
            const rows: Record<string, string> = {};
            for (const leg of LEGS) {
               for (const [read, tables] of Object.entries(READS)) {
                  rows[`${leg}, ${read}`] = admitted({
                     leg,
                     shape,
                     tables,
                     gatedSources,
                  });
               }
            }
            expect({ shape, switchLabel, rows }).toEqual({
               shape,
               switchLabel,
               rows: ADMISSION[shape][switchLabel],
            });
         }
      }
   });

   it("under a collation that is not binary", () => {
      for (const [shape, expected] of Object.entries(COLLATION)) {
         const rows: Record<string, string> = {};
         for (const [where, collations] of [
            ["author instance nocase", { authorCollation: "nocase" }],
            ["stored table nocase", { storedCollation: "nocase" }],
         ] as const) {
            for (const read of [
               "database",
               "storage, kept past its own gate",
            ]) {
               rows[`${where}, ${read}`] = admitted({
                  leg: "served",
                  shape: shape as Shape,
                  tables: READS[read],
                  gatedSources: false,
                  ...collations,
               });
            }
         }
         expect({ shape, rows }).toEqual({ shape, rows: expected });
      }
   });

   it("over moved columns and unreadable reads", () => {
      for (const [shape, expected] of Object.entries(ROW_SCOPE)) {
         const rows: Record<string, string> = {};
         for (const unreadable of [false, true]) {
            for (const leg of ["served", "rollup"] as const) {
               for (const [read, tables] of Object.entries(MOVED_READS)) {
                  rows[
                     `${unreadable ? "unreadable" : "readable"}, ${leg}, ${read}`
                  ] = admittedOverMoves({
                     leg,
                     shape: shape as Shape,
                     tables,
                     unreadable,
                  });
               }
            }
         }
         expect({ shape, rows }).toEqual({ shape, rows: expected });
      }
   });
});
