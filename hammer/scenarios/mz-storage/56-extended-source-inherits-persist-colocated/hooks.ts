import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

// KNOWN-RED (malloy PR 3012), colocated twin: extending a persisted source inherits
// its `#@ persist name=`, so the extended reader becomes a duplicate build target
// writing the same (colocated) warehouse table. Assert exactly one build target
// for `esc_daily` (the base `daily`). RED today; GREEN once malloy stops
// propagating `#@ persist` through `extend`.
export async function assertNoDuplicateInheritedTarget(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const on = await api.use("on");
   const pkg = (await on.getPackage("esc")) as {
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
   const names = [
      ...new Set(
         sources
            .filter((s) => (s.annotationFields?.name ?? s.name) === "esc_daily")
            .map((s) => s.name ?? ""),
      ),
   ].sort();
   assert.eq(
      "exactly one build target writes esc_daily — the extended reader must not " +
         "inherit #@ persist (malloy PR 3012)",
      names,
      ["daily"],
   );
}
