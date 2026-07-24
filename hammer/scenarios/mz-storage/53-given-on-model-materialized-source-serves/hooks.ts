// The ONE thing markdown can't do here: supply a runtime given on a query. This
// hook queries the materialized (given-free) source `daily` with the model-level
// given `REGION` supplied, and asserts it serves from the lake. Regression guard
// for code-review finding #3: the storage serve path used to forward
// model-surface givens to the gate-free serve shape, whose transient model declares
// no `given:`, so prepare threw "unknown given" → spurious 400, no live fallback.

import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

export async function queryMaterializedWithGiven(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const on = await api.use("on");
   const res = await on.tryQuery("gx", api.modelPath("gx"), {
      query:
         "run: daily -> { select: order_date, total; order_by: order_date asc }",
      // A model-level given the query doesn't use. The live model tolerates it;
      // the fix drops it before the shape, so the value is irrelevant.
      givens: { REGION: "US" },
   });
   assert.ok(
      "a query carrying a model-level given serves the given-free materialized source",
      res.ok,
      res.ok ? "" : res.error,
   );
   if (res.ok) {
      // It should serve the materialized snapshot, not error. Dates come back
      // as full ISO timestamps over the API (the markdown `Expect:` path
      // truncates them to YYYY-MM-DD in normalizeCell); do the same here so the
      // assertion reads like a scenario table.
      const rows = (res.outcome.rows as Record<string, unknown>[]).map((r) => ({
         order_date: String(r.order_date).slice(0, 10),
         total: r.total,
      }));
      assert.eq("serves the materialized rows", rows, [
         { order_date: "2026-01-01", total: 150 },
         { order_date: "2026-01-02", total: 200 },
      ]);
   }
}
