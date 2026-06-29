/**
 * Pure parsing of the package manifest's (publisher.json) `materialization`
 * block. Kept side-effect free (no fs, no worker bootstrap) so it is unit
 * testable in isolation from the package-load worker that consumes it.
 */

export interface PackageMaterializationConfig {
   /**
    * 5-field UNIX cron the control plane uses to schedule version-level
    * re-materialization. Null when absent or not a string.
    */
   schedule: string | null;
}

/**
 * Read the manifest's `materialization` object, keeping only recognized fields.
 * Returns null when the block is absent so the API field is null rather than an
 * empty object; a non-string schedule degrades to null.
 */
export function parsePackageMaterialization(
   raw: unknown,
): PackageMaterializationConfig | null {
   if (!raw || typeof raw !== "object") {
      return null;
   }
   const schedule = (raw as { schedule?: unknown }).schedule;
   return { schedule: typeof schedule === "string" ? schedule : null };
}
