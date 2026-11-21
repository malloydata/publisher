// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A built table the served copy does not read is not served, and the serve
 * verdict says so, with the served copy's own reason and the models it is not
 * served in: the gtm-funnel report, where every table built READY and the
 * build reported success while every query answered live, and the only
 * signal was a package warning and each query's path report.
 *
 * Through a real package (plan, build, bind, the served copy's compile, the
 * router), on DuckDB with a DuckDB `lake` destination:
 * - a join across connections the leave-out handles: only the source whose
 *   join could not be left out alone is not served, and one whose join is
 *   left out still serves (in part);
 * - a model whose whole served copy is off (`cross_connection_unbuilt`):
 *   every stored table it holds is not served, not only the joining one;
 * - a package that serves every table, and a stale table, which keep their
 *   verdicts.
 *
 * Every verdict agrees with the `served-copy-not-served` warnings (one
 * compile of record) and with where a query is answered from; a served
 * answer equals live's, compared as text.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../src/test_helpers/conformance/package_runner";
import type { components } from "../../src/api";

type ServeVerdict = components["schemas"]["ServeVerdict"];
type PackageWarning = {
   model?: string;
   subject?: string;
   code?: string;
   reason?: string;
};

const DATA = {
   "accounts.csv":
      "id,account_name,region\n1,acme,east\n2,globex,west\n3,initech,east\n",
   "deals.csv":
      "deal_id,account_id,amount\n10,1,100.0\n11,1,5.0\n12,2,300.0\n13,3,50.0\n14,9,75.0\n",
   "carriers.csv": "code,region\nAA,east\nUA,west\nDL,east\n",
};

// gtm-funnel finding 2, the shape the leave-out handles: `d_stored` stays in
// its own database and joins `crm_current`, which the served copy moves to
// `lake`. Its join is read by a `rename:`, so it cannot be left out alone and
// the whole source is left out.
const LEAVE_OUT = `##! experimental { persistence }
#@ persist name="crm_current" storage="${LAKE}"
source: crm_current is duckdb.table('accounts.csv') -> { select: * } extend {
  primary_key: id
}

#@ persist name="d_stored"
source: d_stored is duckdb.table('deals.csv') -> { select: * } extend {
  primary_key: deal_id
  join_one: cur is crm_current with account_id
  dimension: cur_name is cur.account_name
  rename: account_name is cur_name
  measure: deal_total is amount.sum()
}

#@ persist name="carriers_copy" storage="${LAKE}"
source: carriers_copy is duckdb.table('carriers.csv') -> { select: * } extend {
  measure: carrier_count is count()
}
`;

// The shape that turns the whole copy off: a model-level query reads a join
// the re-root moves across connections, which cannot be left out, so the copy
// does not compile and nothing in the model is served, `carriers_s` included.
const WHOLE_COPY_OFF = `##! experimental { persistence }
source: accts is duckdb.table('accounts.csv') extend {
  primary_key: id
  measure: acct_count is count()
}

#@ persist name="deals_s" storage="${LAKE}"
source: deals_s is duckdb.table('deals.csv') -> { select: * } extend {
  primary_key: deal_id
  join_one: accts with account_id
  measure: deal_count is count()
}

#@ persist name="carriers_s" storage="${LAKE}"
source: carriers_s is duckdb.table('carriers.csv') -> { select: * } extend {
  measure: carrier_count is count()
}

run: deals_s -> { aggregate: accts.acct_count }
`;

const SERVED = `##! experimental { persistence }
#@ persist name="acc_db"
source: acc_db is duckdb.table('accounts.csv') -> { select: * } extend {
  measure: n is count()
}

#@ persist name="acc_lake" storage="${LAKE}"
source: acc_lake is duckdb.table('accounts.csv') -> {
  group_by: region
  aggregate: c is count()
}
`;

/** A package loaded, built and bound, with what was answered before the bind. */
function scenario(model: string, queries: Record<string, string>) {
   const state: {
      fixture?: LoadedFixture;
      live: Record<string, Outcome>;
      served: Record<string, Outcome>;
   } = { live: {}, served: {} };
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ "m.malloy": model, ...DATA });
      for (const [name, query] of Object.entries(queries)) {
         state.live[name] = await loader.run("m.malloy", query);
      }
      state.fixture = await loader.bind();
      for (const [name, query] of Object.entries(queries)) {
         state.served[name] = await state.fixture.run("m.malloy", query);
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

function verdictsOf(fixture: LoadedFixture | undefined): ServeVerdict[] {
   return fixture?.pkg.materialization().serveVerdicts() ?? [];
}

function verdict(
   fixture: LoadedFixture | undefined,
   name: string,
): ServeVerdict {
   const found = verdictsOf(fixture).filter((v) => v.sourceName === name);
   expect(found).toHaveLength(1);
   return found[0];
}

function servedCopyWarnings(
   fixture: LoadedFixture | undefined,
): PackageWarning[] {
   return (
      (fixture?.pkg.getPackageMetadata().warnings ?? []) as PackageWarning[]
   ).filter((w) => w.code === "served-copy-not-served");
}

/** Where an answer came from, and its rows as text. */
function answer(o: Outcome | undefined): { servedBy: string; rows: string } {
   if (!o) return { servedBy: "missing", rows: "" };
   if ("error" in o) return { servedBy: "error", rows: o.error };
   return {
      servedBy: o.answer.servedBy,
      // Every value as text: a count arrives as a BigInt.
      rows: JSON.stringify(o.answer.rows, (_k, v: unknown) =>
         typeof v === "bigint" ? v.toString() : v,
      ),
   };
}

/**
 * The verdicts and the warnings agree, both ways: each verdict's served-copy
 * reason is a warning on the first model it names (on the source, or on the
 * model's whole copy), and each warning that leaves a stored source out marks
 * that source not served in its model.
 */
function expectVerdictsAgreeWithWarnings(fixture: LoadedFixture | undefined) {
   const warnings = servedCopyWarnings(fixture);
   const verdicts = verdictsOf(fixture);
   for (const v of verdicts) {
      if (!v.notServedIn) continue;
      expect(v.servedCopyReason).toBeDefined();
      if (v.servedCopyReason === "not_rerooted") continue;
      expect(
         warnings.some(
            (w) =>
               w.model === v.notServedIn?.[0] &&
               w.reason === v.servedCopyReason &&
               (w.subject === undefined || w.subject === v.sourceName),
         ),
      ).toBe(true);
   }
   for (const w of warnings) {
      const marked = verdicts.filter((v) =>
         w.subject === undefined
            ? v.verdict !== "awaiting_build"
            : v.sourceName === w.subject,
      );
      for (const v of marked) {
         // A join left out alone leaves its source served, and unmarked.
         if (v.verdict === "serves" && w.reason === "cross_connection_join") {
            continue;
         }
         expect(v.notServedIn ?? []).toContain(w.model ?? "");
      }
   }
}

const LEAVE_OUT_QUERIES = {
   d_stored:
      "run: d_stored -> { group_by: account_id; aggregate: deal_total; order_by: account_id }",
   carriers_copy:
      "run: carriers_copy -> { group_by: region; aggregate: carrier_count; order_by: region }",
   crm_current:
      "run: crm_current -> { group_by: region; aggregate: n is count(); order_by: region }",
};

describe("a join across connections the leave-out handles", () => {
   const s = scenario(LEAVE_OUT, LEAVE_OUT_QUERIES);

   it("built every table", () => {
      expect(s.fixture?.buildErrors).toEqual({});
   });

   it("marks only the source left out whole not served, with its warning's reason", () => {
      const left = verdict(s.fixture, "d_stored");
      const reasons = servedCopyWarnings(s.fixture)
         .filter((w) => w.subject === "d_stored")
         .map((w) => w.reason);
      expect(reasons).toContain("cross_connection_join");
      expect(left).toMatchObject({
         verdict: "live",
         reason: "served_copy",
         tier: "database",
         servedCopyReason: reasons[0],
         notServedIn: ["m.malloy"],
      });
      for (const name of ["crm_current", "carriers_copy"]) {
         const v = verdict(s.fixture, name);
         expect(v.verdict).toBe("serves");
         expect(v.notServedIn).toBeUndefined();
         expect(v.servedCopyReason).toBeUndefined();
      }
   });

   it("agrees with the warnings", () => {
      expectVerdictsAgreeWithWarnings(s.fixture);
   });

   it("agrees with where each query is answered from, served equal to live", () => {
      expect(answer(s.served.d_stored).servedBy).toBe("live");
      for (const name of ["carriers_copy", "crm_current"] as const) {
         expect(answer(s.served[name]).servedBy).toBe("materialized");
      }
      for (const name of Object.keys(LEAVE_OUT_QUERIES)) {
         expect(answer(s.served[name]).rows).toBe(answer(s.live[name]).rows);
      }
   });
});

// The join left out alone: the source still serves the queries that do not
// use it, so its table is served, and its warnings say which joins are not.
const LEFT_OUT_IN_PART = `##! experimental { persistence }
#@ persist name="crm_current" storage="${LAKE}"
source: crm_current is duckdb.table('accounts.csv') -> { select: * } extend {
  primary_key: id
}

#@ persist name="d_partial"
source: d_partial is duckdb.table('deals.csv') -> { select: * } extend {
  primary_key: deal_id
  join_one: cur is crm_current with account_id, prev is crm_current with account_id
  measure: deal_total is amount.sum()
}
`;

const IN_PART_QUERIES = {
   d_partial:
      "run: d_partial -> { group_by: account_id; aggregate: deal_total; order_by: account_id }",
};

describe("a join across connections left out alone", () => {
   const s = scenario(LEFT_OUT_IN_PART, IN_PART_QUERIES);

   it("keeps its source served, as its queries are", () => {
      expect(
         servedCopyWarnings(s.fixture).map((w) => [w.subject, w.reason]),
      ).toEqual([
         ["d_partial", "cross_connection_join"],
         ["d_partial", "cross_connection_join"],
      ]);
      const v = verdict(s.fixture, "d_partial");
      expect(v.verdict).toBe("serves");
      expect(v.notServedIn).toBeUndefined();
      expectVerdictsAgreeWithWarnings(s.fixture);
      expect(answer(s.served.d_partial).servedBy).toBe("materialized");
      expect(answer(s.served.d_partial).rows).toBe(
         answer(s.live.d_partial).rows,
      );
   });
});

const WHOLE_QUERIES = {
   deals_s:
      "run: deals_s -> { group_by: account_id; aggregate: deal_count; order_by: account_id }",
   carriers_s:
      "run: carriers_s -> { group_by: region; aggregate: carrier_count; order_by: region }",
};

describe("a model whose whole served copy is off", () => {
   const s = scenario(WHOLE_COPY_OFF, WHOLE_QUERIES);

   it("built every table, as the build reports", () => {
      expect(s.fixture?.buildErrors).toEqual({});
      expect(s.fixture?.built.map((b) => b.name).sort()).toEqual([
         "carriers_s",
         "deals_s",
      ]);
   });

   it("marks every table the model holds not served, for cross_connection_unbuilt", () => {
      expect(servedCopyWarnings(s.fixture)).toMatchObject([
         { model: "m.malloy", reason: "cross_connection_unbuilt" },
      ]);
      for (const name of ["deals_s", "carriers_s"]) {
         expect(verdict(s.fixture, name)).toMatchObject({
            verdict: "live",
            reason: "served_copy",
            servedCopyReason: "cross_connection_unbuilt",
            notServedIn: ["m.malloy"],
         });
      }
   });

   it("agrees with the warnings", () => {
      expectVerdictsAgreeWithWarnings(s.fixture);
   });

   it("agrees with every query answering live, equal to live", () => {
      for (const name of Object.keys(WHOLE_QUERIES)) {
         expect(answer(s.served[name]).servedBy).toBe("live");
         expect(answer(s.served[name]).rows).toBe(answer(s.live[name]).rows);
      }
   });
});

const SERVED_QUERIES = {
   acc_db:
      "run: acc_db -> { group_by: region; aggregate: n; order_by: region }",
   acc_lake:
      "run: acc_lake -> { group_by: region; aggregate: t is c.sum(); order_by: region }",
};

describe("a package that serves every table, then one gone stale", () => {
   const s = scenario(SERVED, SERVED_QUERIES);

   it("marks every table served, with no served-copy reason", () => {
      expect(servedCopyWarnings(s.fixture)).toEqual([]);
      for (const name of ["acc_db", "acc_lake"]) {
         const v = verdict(s.fixture, name);
         expect(v.verdict).toBe("serves");
         expect(v.notServedIn).toBeUndefined();
         expect(v.servedCopyReason).toBeUndefined();
         expect(answer(s.served[name]).servedBy).toBe("materialized");
         expect(answer(s.served[name]).rows).toBe(answer(s.live[name]).rows);
      }
   });

   it("says stale for a table past its window, not a served-copy reason", async () => {
      const pkg = s.fixture?.pkg;
      if (!pkg) throw new Error("not loaded");
      const entry = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
         (e) => e.name === "acc_db",
      );
      if (!entry) throw new Error("acc_db is not planned");
      const tableName = s.fixture?.tables[entry.sourceEntityId];
      pkg.materialization().bind({
         database: {
            [entry.sourceEntityId]: {
               tableName,
               connectionName: "duckdb",
               dataAsOf: "2020-01-01T00:00:00Z",
               freshnessWindowSeconds: 60,
               freshnessFallback: "live",
            },
         } as never,
      });
      await pkg.getModel("m.malloy")?.prepareServedCopy();
      const v = verdict(s.fixture, "acc_db");
      expect(v).toMatchObject({ verdict: "live", reason: "stale" });
      expect(v.notServedIn).toBeUndefined();
      expect(v.servedCopyReason).toBeUndefined();
      const stale = await s.fixture?.run("m.malloy", SERVED_QUERIES.acc_db);
      expect(answer(stale).servedBy).toBe("live");
      expect(answer(stale).rows).toBe(answer(s.live.acc_db).rows);
   });
});
