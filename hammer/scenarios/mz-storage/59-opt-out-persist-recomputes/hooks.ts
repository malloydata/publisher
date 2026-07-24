import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

// `#@ -persist` must not make the opting-out source a build target: only the base
// `daily` may write nop_daily, and no second table may appear.
export async function assertOptOutIsNotABuildTarget(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const on = await api.use("on");
   const pkg = (await on.getPackage("nop")) as {
      buildPlan?: {
         sources?: Record<
            string,
            { name?: string; annotationFields?: { name?: string } }
         >;
      };
   };
   const sources = pkg.buildPlan?.sources
      ? Object.values(pkg.buildPlan.sources)
      : [];
   const names = [...new Set(sources.map((s) => s.name ?? ""))].sort();
   assert.eq("only `daily` is a build target", names, ["daily"]);

   const targets = [
      ...new Set(sources.map((s) => s.annotationFields?.name ?? s.name ?? "")),
   ].sort();
   assert.eq("only nop_daily is written", targets, ["nop_daily"]);
}

// Stale (150) ⇒ the opt-out was ignored and the reader was served from the stored
// snapshot. Fresh (1150) ⇒ it was recomputed, as the documentation describes.
export async function probeOptOutFreshness(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const on = await api.use("on");
   const res = await on.tryQuery("nop", api.modelPath("nop"), {
      query:
         "run: daily_optout -> { select: order_date, total, doubled; " +
         "order_by: order_date asc }",
   });
   assert.ok(
      "daily_optout is servable",
      res.ok,
      res.ok ? "" : res.error.slice(0, 300),
   );
   if (!res.ok) return;

   const rows = res.outcome.rows as { order_date?: unknown; total?: unknown }[];
   const jan1 = rows.find((r) => String(r.order_date).startsWith("2026-01-01"));
   const total = Number(jan1?.total);
   assert.eq("`#@ -persist` recomputes rather than reading the stored table", total, 1150);
}
