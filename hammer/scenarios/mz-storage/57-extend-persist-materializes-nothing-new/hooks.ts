import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

// Proof that persisting an `extend` of a persisted source materializes nothing new:
// (1) the base and the extension share one sourceEntityId (identical content
// address), and (2) only ONE of the two distinctly-named tables is physically
// materialized — the other is deduped away.
export async function assertExtendPersistIsNoOp(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const on = await api.use("on");
   const pkg = (await on.getPackage("xpn")) as {
      buildPlan?: {
         sources?: Record<string, { name?: string; sourceEntityId?: string }>;
      };
   };
   const byName = new Map<string, string>();
   for (const s of Object.values(pkg.buildPlan?.sources ?? {})) {
      if (s.name && s.sourceEntityId) byName.set(s.name, s.sourceEntityId);
   }
   // (1) identical content address.
   assert.ok(
      "daily and daily_with_avg (extend) share one sourceEntityId (content-identical)",
      !!byName.get("daily") && byName.get("daily") === byName.get("daily_with_avg"),
      `daily=${byName.get("daily")} daily_with_avg=${byName.get("daily_with_avg")}`,
   );

   // (2) exactly ONE physical table exists (dedup) — order-independent: whichever
   // of the two names won, only one is materialized, never both.
   const res = (await on.connectionSql(
      "lake",
      "SELECT table_name FROM information_schema.tables " +
         "WHERE table_name IN ('daily_tbl','daily_with_avg_tbl')",
   )) as { data?: string };
   const parsed = JSON.parse(res.data ?? '{"rows":[]}') as {
      rows?: { table_name?: string }[];
   };
   const tables = (parsed.rows ?? [])
      .map((r) => r.table_name)
      .filter(Boolean)
      .sort();
   assert.eq(
      "exactly one physical table is materialized (the extend persist is a no-op)",
      tables.length,
      1,
   );
}
