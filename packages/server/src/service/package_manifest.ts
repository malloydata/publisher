/**
 * Pure parsing of the package manifest's (publisher.json) `materialization`
 * block. Kept side-effect free (no fs, no worker bootstrap) so it is unit
 * testable in isolation from the package-load worker that consumes it.
 */

/**
 * Freshness policy the control plane resolves onto each build-manifest entry and
 * the publisher applies when it binds the manifest. Each field degrades to null
 * when absent or not a string; the control plane validates the window duration
 * and fallback value.
 */
export interface MaterializationFreshnessConfig {
   /** Duration with a unit suffix (`s`/`m`/`h`/`d`), e.g. `24h`. */
   window: string | null;
   /** `live` (default) | `stale_ok` | `fail`. */
   fallback: string | null;
}

export interface PackageMaterializationConfig {
   /**
    * 5-field UNIX cron the control plane uses to schedule version-level
    * re-materialization. Null when absent or not a string.
    */
   schedule: string | null;
   /**
    * Staleness policy applied at manifest bind time. Null when absent so the API
    * field is null rather than an empty object.
    */
   freshness: MaterializationFreshnessConfig | null;
}

/**
 * Read the manifest's `materialization` object, keeping only recognized fields.
 * Returns null when the block is absent so the API field is null rather than an
 * empty object; non-string leaf values degrade to null. The control plane (not
 * the publisher) validates the cron, the freshness window, and the fallback.
 */
export function parsePackageMaterialization(
   raw: unknown,
): PackageMaterializationConfig | null {
   if (!raw || typeof raw !== "object") {
      return null;
   }
   const schedule = (raw as { schedule?: unknown }).schedule;
   return {
      schedule: typeof schedule === "string" ? schedule : null,
      freshness: parseFreshness((raw as { freshness?: unknown }).freshness),
   };
}

function parseFreshness(raw: unknown): MaterializationFreshnessConfig | null {
   if (!raw || typeof raw !== "object") {
      return null;
   }
   const window = (raw as { window?: unknown }).window;
   const fallback = (raw as { fallback?: unknown }).fallback;
   return {
      window: typeof window === "string" ? window : null,
      fallback: typeof fallback === "string" ? fallback : null,
   };
}
