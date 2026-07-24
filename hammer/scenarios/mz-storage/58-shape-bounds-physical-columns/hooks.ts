// Probe whether a physical column absent from the declared serve shape is
// reachable through the virtual source. Three shapes of reference, so a refusal
// isn't just one syntax being rejected.

import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

const PROBES: { label: string; query: string }[] = [
   {
      label: "direct select",
      query: "run: daily -> { select: order_date, secret_col }",
   },
   {
      label: "group_by",
      query: "run: daily -> { group_by: secret_col }",
   },
   {
      label: "select: * expansion",
      query: "run: daily -> { select: * }",
   },
];

export async function probeSmuggledColumn(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const on = await api.use("on");
   const leaked: string[] = [];

   for (const probe of PROBES) {
      const res = await on.tryQuery("shp", api.modelPath("shp"), {
         query: probe.query,
      });
      const body = res.ok ? JSON.stringify(res.outcome.rows) : "";
      const sawValue = body.includes("LEAKED");
      if (sawValue) leaked.push(probe.label);
      // Report, don't judge: the point of the run is to learn which way it goes.
      assert.ok(
         `probe (${probe.label}): ${
            res.ok
               ? sawValue
                  ? "RETURNED the smuggled value"
                  : `served without it — ${body.slice(0, 160)}`
               : `refused — ${res.error.slice(0, 160)}`
         }`,
         true,
      );
   }

   assert.eq(
      "no probe reaches a physical column absent from the declared shape",
      leaked,
      [],
   );

   // `select: *` is the discriminating probe: it must SUCCEED (so it is really
   // being served, not refused) while expanding to the declared columns only. A
   // stale total proves it came from the snapshot — see the following step.
   const star = await on.tryQuery("shp", api.modelPath("shp"), {
      query: "run: daily -> { select: * }",
   });
   assert.ok(
      "select: * is served (not refused)",
      star.ok,
      star.ok ? "" : star.error,
   );
   if (star.ok) {
      const cols = Object.keys(
         (star.outcome.rows as Record<string, unknown>[])[0] ?? {},
      ).sort();
      assert.eq("select: * expands to the DECLARED columns only", cols, [
         "order_date",
         "total",
      ]);
      const totals = (star.outcome.rows as { total?: unknown }[])
         .map((r) => Number(r.total))
         .sort((a, b) => a - b);
      // 150 (not 1150) ⇒ the stored snapshot, so the probes above really did
      // exercise the storage path against the widened physical table.
      assert.eq("served from the snapshot, not recomputed live", totals, [
         150, 200,
      ]);
   }
}
