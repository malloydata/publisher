// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A built table the served copy does not read is not served, and the serve
 * verdict says so in `notServedIn`, each model with every reason its
 * `served-copy-not-served` warnings give: the gtm-funnel report, where every
 * table built READY and the build reported success while every query
 * answered live, and the only signal was a package warning and each query's
 * path report. A `#(materialize)` source and a `#@ persist` one read the
 * same.
 *
 * Through a real package (plan, build, bind, the served copy's compile, the
 * router), on DuckDB with a DuckDB `lake` destination:
 * - a join across connections the leave-out handles: only the source whose
 *   join could not be left out alone is not served;
 * - a model whose whole served copy is off (`cross_connection_unbuilt`):
 *   every stored table it holds is not served, `#(materialize)` and
 *   `#@ persist` alike, while a persisted view and a rollup, which the served
 *   copy does not read, carry no served-copy judgement;
 * - a source two models hold, one of whose copies is off: `serves`, with
 *   that model in `notServedIn`;
 * - a package that serves every table, a stale table, and
 *   `PERSIST_STORAGE_MODE` write-only and off.
 *
 * Every verdict agrees with the `served-copy-not-served` warnings (one
 * compile of record) and with where each query is answered from; a served
 * answer equals live's, compared as text.
 */

import { contractFor } from "../../src/test_helpers/contract_fixture";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { installSettings } from "../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../src/test_helpers/conformance/package_runner";
import type { components } from "../../src/api";
import { setEventLoopYieldBudgetMsForTesting } from "../../src/service/event_loop_yield";
import { scopeFactsOf } from "../../src/service/materialization/serve/scope_facts";

type ServeVerdict = components["schemas"]["ServeVerdict"];
type PackageWarning = components["schemas"]["PackageWarning"];

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
// does not compile and nothing in the model is served: `carriers_s`
// (`#@ persist`) and `carriers_m` (`#(materialize)`) alike. The persisted
// view and the rollup are read by legs of their own.
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

#(materialize) name="carriers_m" storage="${LAKE}"
source: carriers_m is duckdb.table('carriers.csv') extend {
  measure: carrier_count is count()
}

source: deals_raw is duckdb.table('deals.csv') extend {
  #@ persist
  view: by_account is { group_by: account_id; aggregate: total is amount.sum() }
  #(rollup) grain=["account_id"]
  measure: deal_rows is count()
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

// One source, two models: `a.malloy` declares and serves it; `b.malloy`
// imports it and its own copy is off.
const DECLARING = `##! experimental { persistence }
#@ persist name="x_db"
source: x_db is duckdb.table('accounts.csv') -> { select: * } extend {
  measure: n is count()
}
`;
const IMPORTING_COPY_OFF = `##! experimental { persistence }
import "a.malloy"
source: accts is duckdb.table('accounts.csv') extend {
  primary_key: id
  measure: acct_count is count()
}
#@ persist name="deals_b" storage="${LAKE}"
source: deals_b is duckdb.table('deals.csv') -> { select: * } extend {
  primary_key: deal_id
  join_one: accts with account_id
  measure: deal_count is count()
}
run: deals_b -> { aggregate: accts.acct_count }
`;

type Query = { model: string; query: string };

/** A package loaded, built and bound, with what was answered before the bind. */
function scenario(
   files: Record<string, string>,
   queries: Record<string, Query>,
   storageMode = "on",
) {
   const state: {
      fixture?: LoadedFixture;
      live: Record<string, Outcome>;
      served: Record<string, Outcome>;
   } = { live: {}, served: {} };
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: storageMode,
            PUBLISHER_MATERIALIZE: "true",
         },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ ...files, ...DATA });
      for (const [name, q] of Object.entries(queries)) {
         state.live[name] = await loader.run(q.model, q.query);
      }
      state.fixture = await loader.bind({ includeRollups: true });
      for (const [name, q] of Object.entries(queries)) {
         state.served[name] = await state.fixture.run(q.model, q.query);
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

function inM(query: string): Query {
   return { model: "m.malloy", query };
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
   return (fixture?.pkg.getPackageMetadata().warnings ?? []).filter(
      (w) => w.code === "served-copy-not-served",
   );
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
 * The verdicts, the warnings and the answers agree:
 * - each source queried in its declaring model answers live exactly when
 *   its verdict is `live`;
 * - each reason `notServedIn` lists for a model (but `not_rerooted`) is a
 *   warning on that model naming the source or the model's whole copy;
 * - each source a warning leaves unserved (its query answered live) lists
 *   that model with that reason.
 */
function expectAgreement(
   s: {
      fixture?: LoadedFixture;
      served: Record<string, Outcome>;
   },
   queries: Record<string, Query>,
) {
   const warnings = servedCopyWarnings(s.fixture);
   const verdicts = verdictsOf(s.fixture);
   for (const [name, q] of Object.entries(queries)) {
      const v = verdicts.find((x) => x.sourceName === name);
      if (!v || v.modelPath !== q.model) continue;
      expect([name, answer(s.served[name]).servedBy === "live"]).toEqual([
         name,
         v.verdict === "live",
      ]);
   }
   for (const v of verdicts) {
      for (const { model, reasons } of v.notServedIn ?? []) {
         expect(reasons.length).toBeGreaterThan(0);
         for (const reason of reasons) {
            if (reason === "not_rerooted") continue;
            expect([
               v.sourceName,
               model,
               reason,
               warnings.some(
                  (w) =>
                     w.model === model &&
                     w.reason === reason &&
                     (w.subject === undefined || w.subject === v.sourceName),
               ),
            ]).toEqual([v.sourceName, model, reason, true]);
         }
      }
   }
   for (const [name, q] of Object.entries(queries)) {
      if (answer(s.served[name]).servedBy !== "live") continue;
      const v = verdicts.find((x) => x.sourceName === name);
      if (!v || v.verdict !== "live" || v.reason !== "served_copy") continue;
      for (const w of warnings) {
         if (w.model !== q.model) continue;
         if (w.subject !== undefined && w.subject !== name) continue;
         expect(
            v.notServedIn?.find((n) => n.model === q.model)?.reasons,
         ).toContain(w.reason);
      }
   }
}

const LEAVE_OUT_QUERIES = {
   d_stored: inM(
      "run: d_stored -> { group_by: account_id; aggregate: deal_total; order_by: account_id }",
   ),
   carriers_copy: inM(
      "run: carriers_copy -> { group_by: region; aggregate: carrier_count; order_by: region }",
   ),
   crm_current: inM(
      "run: crm_current -> { group_by: region; aggregate: n is count(); order_by: region }",
   ),
};

describe("a join across connections the leave-out handles", () => {
   const s = scenario({ "m.malloy": LEAVE_OUT }, LEAVE_OUT_QUERIES);

   it("built every table", () => {
      expect(s.fixture?.buildErrors).toEqual({});
   });

   it("marks only the source left out whole not served, with every reason, most actionable first", () => {
      const reasons = servedCopyWarnings(s.fixture)
         .filter((w) => w.subject === "d_stored")
         .map((w) => w.reason);
      expect(reasons).toContain("cross_connection_join");
      const left = verdict(s.fixture, "d_stored");
      expect(left).toMatchObject({
         verdict: "live",
         reason: "served_copy",
         tier: "database",
      });
      expect(left.notServedIn).toHaveLength(1);
      expect(left.notServedIn?.[0].model).toBe("m.malloy");
      expect(left.notServedIn?.[0].reasons[0]).toBe("cross_connection_join");
      expect([...(left.notServedIn?.[0].reasons ?? [])].sort()).toEqual(
         [...new Set(reasons.map(String))].sort(),
      );
      for (const name of ["crm_current", "carriers_copy"]) {
         const v = verdict(s.fixture, name);
         expect(v.verdict).toBe("serves");
         expect(v.notServedIn).toBeUndefined();
      }
   });

   it("agrees with the warnings and the answers", () => {
      expectAgreement(s, LEAVE_OUT_QUERIES);
   });

   it("serves equal to live", () => {
      for (const name of ["carriers_copy", "crm_current"] as const) {
         expect(answer(s.served[name]).servedBy).toBe("materialized");
      }
      for (const name of Object.keys(LEAVE_OUT_QUERIES)) {
         expect(answer(s.served[name]).rows).toBe(answer(s.live[name]).rows);
      }
   });
});

const WHOLE_QUERIES = {
   deals_s: inM(
      "run: deals_s -> { group_by: account_id; aggregate: deal_count; order_by: account_id }",
   ),
   carriers_s: inM(
      "run: carriers_s -> { group_by: region; aggregate: carrier_count; order_by: region }",
   ),
   carriers_m: inM(
      "run: carriers_m -> { group_by: region; aggregate: carrier_count; order_by: region }",
   ),
};

describe("a model whose whole served copy is off", () => {
   const s = scenario({ "m.malloy": WHOLE_COPY_OFF }, WHOLE_QUERIES);

   it("built every table, as the build reports", () => {
      expect(s.fixture?.buildErrors).toEqual({});
      for (const name of ["carriers_m", "carriers_s", "deals_s"]) {
         expect(s.fixture?.built.map((b) => b.name)).toContain(name);
      }
   });

   it("marks every table the model holds not served, for cross_connection_unbuilt", () => {
      expect(servedCopyWarnings(s.fixture)).toMatchObject([
         { model: "m.malloy", reason: "cross_connection_unbuilt" },
      ]);
      for (const name of ["deals_s", "carriers_s", "carriers_m"]) {
         expect(verdict(s.fixture, name)).toMatchObject({
            verdict: "live",
            reason: "served_copy",
            notServedIn: [
               { model: "m.malloy", reasons: ["cross_connection_unbuilt"] },
            ],
         });
      }
   });

   it("reads a #(materialize) source and a #@ persist source the same", () => {
      const shape = (v: ServeVerdict) => ({
         tier: v.tier,
         verdict: v.verdict,
         reason: v.reason,
         notServedIn: v.notServedIn,
      });
      expect(verdict(s.fixture, "carriers_m").origin).toBe("materialize");
      expect(verdict(s.fixture, "carriers_s").origin).toBe("persist");
      expect(shape(verdict(s.fixture, "carriers_m"))).toEqual(
         shape(verdict(s.fixture, "carriers_s")),
      );
   });

   it("leaves a persisted view and a rollup, which the served copy does not read, unjudged by it", () => {
      const others = verdictsOf(s.fixture).filter(
         (v) => v.origin === "view" || v.origin === "rollup",
      );
      expect(others.map((v) => v.origin).sort()).toEqual(["rollup", "view"]);
      for (const v of others) {
         expect(v.notServedIn).toBeUndefined();
         expect(v.reason).not.toBe("served_copy");
      }
   });

   it("agrees with the warnings and every query answering live, equal to live", () => {
      expectAgreement(s, WHOLE_QUERIES);
      for (const name of Object.keys(WHOLE_QUERIES)) {
         expect(answer(s.served[name]).servedBy).toBe("live");
         expect(answer(s.served[name]).rows).toBe(answer(s.live[name]).rows);
      }
   });
});

const TWO_MODEL_QUERIES = {
   via_a: { model: "a.malloy", query: "run: x_db -> { aggregate: n }" },
   via_b: { model: "b.malloy", query: "run: x_db -> { aggregate: n }" },
};

describe("a source two models hold, one of whose copies is off", () => {
   const s = scenario(
      { "a.malloy": DECLARING, "b.malloy": IMPORTING_COPY_OFF },
      TWO_MODEL_QUERIES,
   );

   it("serves, naming the model that answers it live", () => {
      expect(verdict(s.fixture, "x_db")).toMatchObject({
         verdict: "serves",
         notServedIn: [
            { model: "b.malloy", reasons: ["cross_connection_unbuilt"] },
         ],
      });
   });

   it("is answered from its table in one model and live in the other, equal to live", () => {
      expect(answer(s.served.via_a).servedBy).toBe("materialized");
      expect(answer(s.served.via_b).servedBy).toBe("live");
      for (const name of Object.keys(TWO_MODEL_QUERIES)) {
         expect(answer(s.served[name]).rows).toBe(answer(s.live[name]).rows);
      }
   });
});

const SERVED_QUERIES = {
   acc_db: inM(
      "run: acc_db -> { group_by: region; aggregate: n; order_by: region }",
   ),
   acc_lake: inM(
      "run: acc_lake -> { group_by: region; aggregate: t is c.sum(); order_by: region }",
   ),
};

describe("a package that serves every table, then one gone stale", () => {
   const s = scenario({ "m.malloy": SERVED }, SERVED_QUERIES);

   it("marks every table served, with no models listed", () => {
      expect(servedCopyWarnings(s.fixture)).toEqual([]);
      for (const name of ["acc_db", "acc_lake"]) {
         const v = verdict(s.fixture, name);
         expect(v.verdict).toBe("serves");
         expect(v.notServedIn).toBeUndefined();
         expect(answer(s.served[name]).servedBy).toBe("materialized");
         expect(answer(s.served[name]).rows).toBe(answer(s.live[name]).rows);
      }
   });

   it("says stale for a table past its window, with no models listed", async () => {
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
               contract: contractFor(),
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
      const stale = await s.fixture?.run(
         "m.malloy",
         SERVED_QUERIES.acc_db.query,
      );
      expect(answer(stale).servedBy).toBe("live");
      expect(answer(stale).rows).toBe(answer(s.live.acc_db).rows);
   });
});

for (const mode of ["write-only", "off"] as const) {
   describe(`PERSIST_STORAGE_MODE=${mode}`, () => {
      const s = scenario({ "m.malloy": SERVED }, SERVED_QUERIES, mode);

      it("says storage_mode for the storage table, and serves the database one", () => {
         expect(verdict(s.fixture, "acc_lake")).toMatchObject({
            verdict: "live",
            reason: "storage_mode",
         });
         expect(verdict(s.fixture, "acc_lake").notServedIn).toBeUndefined();
         expect(verdict(s.fixture, "acc_db")).toMatchObject({
            verdict: "serves",
         });
         expect(verdict(s.fixture, "acc_db").notServedIn).toBeUndefined();
         expect(answer(s.served.acc_db).servedBy).toBe("materialized");
         expect(answer(s.served.acc_lake).servedBy).toBe("live");
         for (const name of Object.keys(SERVED_QUERIES)) {
            expect(answer(s.served[name]).rows).toBe(answer(s.live[name]).rows);
         }
      });
   });
}

// A database table, a storage table and a `#(materialize)` one, rebound by
// hand so the verdicts can be read the moment a bind's commit returns.
const AFTER_BIND = `${SERVED}
#(materialize) name="carriers_m" storage="${LAKE}"
source: carriers_m is duckdb.table('carriers.csv') extend {
  measure: carrier_count is count()
}
`;
const AFTER_BIND_QUERIES = {
   ...SERVED_QUERIES,
   carriers_m: inM(
      "run: carriers_m -> { group_by: region; aggregate: carrier_count; order_by: region }",
   ),
};
const AFTER_BIND_TABLES = ["acc_db", "acc_lake", "carriers_m"];

describe("verdicts read the moment a bind's commit returns", () => {
   const s = scenario({ "m.malloy": AFTER_BIND }, AFTER_BIND_QUERIES);

   function bound() {
      const pkg = s.fixture?.pkg;
      if (!pkg) throw new Error("not loaded");
      const mz = pkg.materialization();
      const database = mz.boundDatabaseEntries();
      if (!database) throw new Error("no database tables bound");
      const model = pkg.getModel("m.malloy");
      if (!model) throw new Error("no model");
      return { pkg, mz, database, stored: s.fixture!.stored, model };
   }

   /** Unbind every table and let the copy compile over none. */
   async function unbindAll() {
      const { mz } = bound();
      await mz.bindCompiled({ database: {} });
      await mz.bindCompiled({ storage: {} });
   }

   /** Each table's verdict, `verdict[:reason]`. */
   function shapes(): Record<string, string> {
      return Object.fromEntries(
         AFTER_BIND_TABLES.map((name) => {
            const v = verdict(s.fixture, name);
            return [name, `${v.verdict}${v.reason ? `:${v.reason}` : ""}`];
         }),
      );
   }

   const SERVING = {
      acc_db: "serves",
      acc_lake: "serves",
      carriers_m: "serves",
   };

   async function expectServedEqualLive() {
      for (const [name, q] of Object.entries(AFTER_BIND_QUERIES)) {
         const served = await s.fixture?.run(q.model, q.query);
         expect([name, answer(served).servedBy]).toEqual([
            name,
            "materialized",
         ]);
         expect(answer(served).rows).toBe(answer(s.live[name]).rows);
      }
   }

   it("serves every table once settled, equal to live", async () => {
      expect(s.fixture?.buildErrors).toEqual({});
      expect(shapes()).toEqual(SERVING);
      await expectServedEqualLive();
   });

   it("a bind's commit compiles the served copy before the swap: correct the moment it returns", async () => {
      const { mz, database, stored, model } = bound();
      await unbindAll();
      // The models read the tables from before the bind until every copy
      // compiled over the new ones, so the verdicts are final when it returns.
      await mz.bindCompiled({ database: database as never });
      await mz.bindCompiled({ storage: stored as never });
      expect(model.servedCopySourceIDs()).not.toBe("uncompiled");
      expect(shapes()).toEqual(SERVING);
      for (const v of verdictsOf(s.fixture)) {
         expect(v).not.toHaveProperty("servedCopyPending");
      }
      await expectServedEqualLive();
   });

   it("a publish while a bind's commit records leaves the commit's scope facts", async () => {
      const { mz, database, stored, model } = bound();
      await unbindAll();
      await mz.bindCompiled({ database: database as never });
      // A publish that fires while the commit records (a plan refresh, a
      // storage-read retry): its copy is prepared over the snapshot the
      // models read until the swap, the one from before the bind. Its
      // scope facts must not land over the commit's. Its string-capability
      // read answers at once here, so it reads that snapshot while the
      // commit is still recording, as a quick catalog read does.
      const refresh = spyOn(
         model,
         "refreshStringCapabilities",
      ).mockResolvedValue(undefined);
      const record = model.recordScopeFactsOver.bind(model);
      // Every recording, the commit's and the publish's: the publish's is
      // awaited by nothing, so the check waits for both to land.
      const recordings: Promise<void>[] = [];
      let published = false;
      const spy = spyOn(model, "recordScopeFactsOver").mockImplementation(
         async (snapshot) => {
            if (!published) {
               published = true;
               (
                  mz as unknown as { publish(models?: "installed"): void }
               ).publish("installed");
            }
            const recording = record(snapshot);
            recordings.push(recording);
            return recording;
         },
      );
      const previous = setEventLoopYieldBudgetMsForTesting(0);
      try {
         await mz.bindCompiled({ storage: stored as never });
         await (mz as unknown as { commitChain: Promise<void> }).commitChain;
         const deadline = Date.now() + 60_000;
         while (recordings.length < 2 && Date.now() < deadline) {
            await Bun.sleep(10);
         }
         await Promise.all(recordings);
      } finally {
         setEventLoopYieldBudgetMsForTesting(previous);
         spy.mockRestore();
         refresh.mockRestore();
      }
      expect(published).toBe(true);
      expect(recordings).toHaveLength(2);
      const serving = (
         model as unknown as { servingSnapshot(): object }
      ).servingSnapshot();
      expect(
         scopeFactsOf(model.stringCapabilityRuntime(), serving as never),
      ).toBeDefined();
      await expectServedEqualLive();
   });

   it("a rebind that adds a table: correct the moment it returns", async () => {
      const { mz, database, stored } = bound();
      await unbindAll();
      await mz.bindCompiled({ storage: stored as never });
      expect(shapes()).toMatchObject({
         acc_db: "awaiting_build",
         acc_lake: "serves",
      });
      await mz.bindCompiled({ database: database as never });
      expect(shapes()).toEqual(SERVING);
      await expectServedEqualLive();
   });

   it("two binds back to back: the later bind's verdicts once both return", async () => {
      const { mz, database, stored } = bound();
      await unbindAll();
      // Neither awaited before the other starts: the commits run in order.
      const first = mz.bindCompiled({ database: database as never });
      const second = mz.bindCompiled({ storage: stored as never });
      await Promise.all([first, second]);
      expect(shapes()).toEqual(SERVING);
      await expectServedEqualLive();
   });
});

describe("a package's warnings read the moment a bind's commit returns", () => {
   const s = scenario({ "m.malloy": WHOLE_COPY_OFF }, WHOLE_QUERIES);

   it("carry the new copy's served-copy-not-served warnings, never the previous one's", async () => {
      const pkg = s.fixture?.pkg;
      if (!pkg) throw new Error("not loaded");
      const mz = pkg.materialization();
      const stored = s.fixture!.stored;
      expect(servedCopyWarnings(s.fixture)).toMatchObject([
         { model: "m.malloy", reason: "cross_connection_unbuilt" },
      ]);
      // Unbind the tables the copy failed over: what a bind response reads
      // at once is the package's warnings.
      await mz.bindCompiled({ storage: {} });
      expect(servedCopyWarnings(s.fixture)).toEqual([]);
      // Bound again: the new copy's own, at once.
      await mz.bindCompiled({ storage: stored as never });
      expect(servedCopyWarnings(s.fixture)).toMatchObject([
         { model: "m.malloy", reason: "cross_connection_unbuilt" },
      ]);
      expectAgreement(s, WHOLE_QUERIES);
   });
});

describe("a rebind whose served copy ends up off", () => {
   const s = scenario({ "m.malloy": WHOLE_COPY_OFF }, WHOLE_QUERIES);
   const STORED = ["carriers_m", "carriers_s", "deals_s"];

   it("judges every table it holds live/served_copy the moment the commit returns", async () => {
      const pkg = s.fixture?.pkg;
      if (!pkg) throw new Error("not loaded");
      const mz = pkg.materialization();
      const database = mz.boundDatabaseEntries() ?? {};
      const stored = s.fixture!.stored;
      await mz.bindCompiled({ database: {} });
      await mz.bindCompiled({ storage: {} });
      await mz.bindCompiled({ database: database as never });
      await mz.bindCompiled({ storage: stored as never });
      for (const name of STORED) {
         expect(verdict(s.fixture, name)).toMatchObject({
            verdict: "live",
            reason: "served_copy",
         });
      }
      expectAgreement(s, WHOLE_QUERIES);
      for (const [name, q] of Object.entries(WHOLE_QUERIES)) {
         const again = await s.fixture?.run(q.model, q.query);
         expect([name, answer(again).servedBy]).toEqual([name, "live"]);
         expect(answer(again).rows).toBe(answer(s.live[name]).rows);
      }
   });
});

describe("a freshness window that lapses is a commit: the served copy compiles at the edge", () => {
   const s = scenario({ "m.malloy": SERVED }, SERVED_QUERIES);

   function parts() {
      const pkg = s.fixture?.pkg;
      if (!pkg) throw new Error("not loaded");
      const mz = pkg.materialization();
      const model = pkg.getModel("m.malloy");
      if (!model) throw new Error("no model");
      const entry = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
         (e) => e.name === "acc_db",
      );
      if (!entry) throw new Error("acc_db is not planned");
      const database = mz.boundDatabaseEntries() ?? {};
      // The model's served copy, whose compile a spec may watch or fail.
      const copy = (model as unknown as { servedCopy: object }).servedCopy as {
         compileWithFallbacks(...args: unknown[]): Promise<unknown>;
         isCurrent(): boolean;
      };
      return { pkg, mz, model, entry, database, copy };
   }

   /** `database` with acc_db's table given a window of `seconds`, anchored now. */
   function lapsing(
      database: object,
      sourceEntityId: string,
      seconds: number,
      fallback: "live" | "stale_ok",
   ) {
      return {
         ...database,
         [sourceEntityId]: {
            ...(database as Record<string, object>)[sourceEntityId],
            dataAsOf: new Date().toISOString(),
            freshnessWindowSeconds: seconds,
            freshnessFallback: fallback,
         },
      } as never;
   }

   it("compiles the copy over the tables read after the edge, with no query, read or bind", async () => {
      const { mz, entry, database, copy } = parts();
      await mz.bindCompiled({
         database: lapsing(database, entry.sourceEntityId, 1, "live"),
      });
      expect(verdict(s.fixture, "acc_db").verdict).toBe("serves");
      expect(copy.isCurrent()).toBe(true);
      const compiles = spyOn(copy, "compileWithFallbacks");
      try {
         // Nothing reads the package across the edge: the commit armed at
         // it is what compiles the copy over the tables read after it.
         await new Promise((r) => setTimeout(r, 2_000));
         expect(compiles).toHaveBeenCalledTimes(1);
         expect(copy.isCurrent()).toBe(true);
      } finally {
         compiles.mockRestore();
      }
      expect(verdict(s.fixture, "acc_db")).toMatchObject({
         verdict: "live",
         reason: "stale",
      });
      expect(verdict(s.fixture, "acc_lake").verdict).toBe("serves");
      for (const [name, q] of Object.entries(SERVED_QUERIES)) {
         const o = await s.fixture?.run(q.model, q.query);
         expect(answer(o).rows).toBe(answer(s.live[name]).rows);
      }
      // Rebound fresh for the next case.
      await mz.bindCompiled({ database: database as never });
   }, 30_000);

   it("arms nothing for a window whose fallback serves stale: no edge to compile at", async () => {
      const { mz, entry, database, copy } = parts();
      await mz.bindCompiled({
         database: lapsing(database, entry.sourceEntityId, 1, "stale_ok"),
      });
      const compiles = spyOn(copy, "compileWithFallbacks");
      try {
         await new Promise((r) => setTimeout(r, 1_500));
         expect(compiles).toHaveBeenCalledTimes(0);
      } finally {
         compiles.mockRestore();
      }
      expect(verdict(s.fixture, "acc_db").verdict).toBe("serves");
      await mz.bindCompiled({ database: database as never });
   }, 30_000);

   it("reading the verdicts never compiles", async () => {
      const { mz, database, copy } = parts();
      await mz.bindCompiled({ database: {} });
      await mz.bindCompiled({ database: database as never });
      const compiles = spyOn(copy, "compileWithFallbacks");
      try {
         for (let i = 0; i < 25; i++) verdictsOf(s.fixture);
         expect(compiles).toHaveBeenCalledTimes(0);
      } finally {
         compiles.mockRestore();
      }
   }, 30_000);

   it("a compile that throws is compile_failed the moment the commit returns, answers live, and serves again on a later query with no bind", async () => {
      const { mz, model, database, copy } = parts();
      await mz.bindCompiled({ database: {} });
      const thrown = spyOn(copy, "compileWithFallbacks").mockImplementation(
         () => Promise.reject(new Error("an escaped failure")),
      );
      try {
         await mz.bindCompiled({ database: database as never });
         expect(model.servedCopyProblems().map((p) => p.reason)).toEqual([
            "compile_failed",
         ]);
         for (const name of ["acc_db", "acc_lake"]) {
            expect(verdict(s.fixture, name)).toMatchObject({
               verdict: "live",
               reason: "served_copy",
               notServedIn: [
                  { model: "m.malloy", reasons: ["compile_failed"] },
               ],
            });
         }
         for (const [name, q] of Object.entries(SERVED_QUERIES)) {
            const o = await s.fixture?.run(q.model, q.query);
            expect([name, answer(o).servedBy]).toEqual([name, "live"]);
            expect(answer(o).rows).toBe(answer(s.live[name]).rows);
         }
      } finally {
         thrown.mockRestore();
      }
      // The failure was transient: the next query compiles again, with no
      // bind, and serves; the verdict follows.
      for (const [name, q] of Object.entries(SERVED_QUERIES)) {
         const o = await s.fixture?.run(q.model, q.query);
         expect([name, answer(o).servedBy]).toEqual([name, "materialized"]);
         expect(answer(o).rows).toBe(answer(s.live[name]).rows);
      }
      expect(model.servedCopyProblems()).toEqual([]);
      for (const name of ["acc_db", "acc_lake"]) {
         const v = verdict(s.fixture, name);
         expect(v.verdict).toBe("serves");
         expect(v.notServedIn).toBeUndefined();
      }
   }, 30_000);
});
