// The ONE thing markdown can't do here: supply a runtime given on a query. Queries
// the materialized (given-free) source `daily` with the model-level given `REGION`
// supplied, and asserts it still serves from the lake.

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
      // A model-level given the query doesn't use; the value is irrelevant.
      givens: { REGION: "US" },
   });
   assert.ok(
      "a query carrying a model-level given serves the given-free materialized source",
      res.ok,
      res.ok ? "" : res.error,
   );
   if (res.ok) {
      // Dates come back as full ISO timestamps over the API; truncate as the
      // markdown `Expect:` path does (normalizeCell).
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
