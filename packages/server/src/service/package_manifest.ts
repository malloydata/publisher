/**
 * Pure parsing of the package manifest's (publisher.json) `materialization`
 * block. Kept side-effect free (no fs, no worker bootstrap) so it is unit
 * testable in isolation from the package-load worker that consumes it.
 */

const FRESHNESS_FALLBACKS = ["live", "stale_ok", "fail"] as const;
export type FreshnessFallback = (typeof FRESHNESS_FALLBACKS)[number];

/**
 * Package-level Malloy Persistence scope mode, declared once at the manifest
 * root. `package` (the default) = artifacts are reused across the package's
 * versions when they satisfy freshness; `version` = each artifact is owned by
 * one published version (the only mode a `materialization.schedule` is legal
 * in). Applied uniformly to every persist source and index in the package.
 */
export const PACKAGE_SCOPES = ["version", "package"] as const;
export type PackageScope = (typeof PACKAGE_SCOPES)[number];

/**
 * Read the manifest root's `scope`, defaulting to `"package"` when absent or
 * null. Any other value is a manifest error: scope is load-bearing (it decides
 * version-owned vs cross-version reuse), so a typo must fail loudly rather than
 * silently pick a default. Throws on an invalid value.
 */
export function parsePackageScope(raw: unknown): PackageScope {
   if (raw === undefined || raw === null) {
      return "package";
   }
   if ((PACKAGE_SCOPES as readonly unknown[]).includes(raw)) {
      return raw as PackageScope;
   }
   throw new Error(
      `Invalid "scope" in the package manifest: ${JSON.stringify(raw)}. ` +
         `Expected "version" or "package" (default "package").`,
   );
}

/**
 * The manifest's `materialization.freshness` block, surfaced verbatim for the
 * control plane (which owns the scheduling and query-time gating logic).
 * Fields are kept only when valid — an invalid value is dropped, never
 * defaulted, so absence on the wire always means "not declared".
 */
export interface PackageFreshnessConfig {
   /** Maximum acceptable staleness, as a duration string (e.g. "24h"). */
   window?: string;
   /** Declared query-time behavior when the window is missed. */
   fallback?: FreshnessFallback;
}

export interface PackageMaterializationConfig {
   /**
    * 5-field UNIX cron the control plane uses to schedule version-level
    * re-materialization. Null when absent or not a string. Publish-gated:
    * only valid when every persist source resolves to explicit
    * `sharing=private` (see Package.scheduleWarnings).
    */
   schedule: string | null;
   /**
    * Freshness policy ({ window, fallback }). Null when the manifest declares
    * none — distinct from an empty object, so the control plane can tell
    * "no policy" from "policy with no valid fields".
    */
   freshness: PackageFreshnessConfig | null;
}

function parseFreshness(raw: unknown): PackageFreshnessConfig | null {
   if (!raw || typeof raw !== "object") {
      return null;
   }
   const { window, fallback } = raw as { window?: unknown; fallback?: unknown };
   const freshness: PackageFreshnessConfig = {};
   if (typeof window === "string") {
      freshness.window = window;
   }
   if (
      typeof fallback === "string" &&
      (FRESHNESS_FALLBACKS as readonly string[]).includes(fallback)
   ) {
      freshness.fallback = fallback as FreshnessFallback;
   }
   return freshness;
}

/**
 * Read the manifest's `materialization` object, keeping only recognized fields.
 * Returns null when the block is absent so the API field is null rather than an
 * empty object; a non-string schedule degrades to null, and an absent or
 * non-object freshness degrades to null.
 */
export function parsePackageMaterialization(
   raw: unknown,
): PackageMaterializationConfig | null {
   if (!raw || typeof raw !== "object") {
      return null;
   }
   const { schedule, freshness } = raw as {
      schedule?: unknown;
      freshness?: unknown;
   };
   return {
      schedule: typeof schedule === "string" ? schedule : null,
      freshness: parseFreshness(freshness),
   };
}
