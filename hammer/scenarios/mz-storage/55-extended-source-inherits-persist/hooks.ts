import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

// KNOWN-RED (malloy #3012): extending a persisted source inherits its `#@ persist`
// annotation, so the extended reader becomes a duplicate build target writing the
// same `name=`. Assert the extended reader is NOT a separate build target — the
// base `daily` should be the only target for `esi_daily`. RED today; GREEN once
// malloy stops propagating `#@ persist` through `extend`.
export async function assertNoDuplicateInheritedTarget(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const on = await api.use("on");
   const pkg = (await on.getPackage("esi")) as {
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
   // Distinct source names that carry the (inherited) persist target `esi_daily`.
   const names = [
      ...new Set(
         sources
            .filter((s) => (s.annotationFields?.name ?? s.name) === "esi_daily")
            .map((s) => s.name ?? ""),
      ),
   ].sort();
   assert.eq(
      "exactly one build target writes esi_daily — the extended reader must not " +
         "inherit #@ persist (malloy #3012)",
      names,
      ["daily"],
   );
}
