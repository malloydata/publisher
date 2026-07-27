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
 * Read a `scope` value, defaulting to `"package"` when absent or null. Any other
 * value is a manifest error: scope is load-bearing (it decides version-owned vs
 * cross-version reuse), so a typo must fail loudly rather than silently pick a
 * default. Throws on an invalid value.
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
 * Resolve the package's scope from its two possible homes: `materialization.scope`
 * (canonical — every other build-behavioral knob lives in that block) and the
 * manifest root (the original home, deprecated).
 *
 * The root form keeps working, with a warning, because scope rides the published
 * artifact: a package published before the move must keep parsing until it is
 * republished. Both homes declaring the SAME value is just the deprecation
 * warning; declaring DIFFERENT values throws, because scope decides whether an
 * artifact is version-owned and guessing which one the author meant could reuse
 * a table across versions that was never supposed to be shared.
 */
export function resolvePackageScope(
   rootRaw: unknown,
   materializationRaw: unknown,
): { scope: PackageScope; warnings: string[] } {
   const envelopeRaw =
      materializationRaw && typeof materializationRaw === "object"
         ? (materializationRaw as { scope?: unknown }).scope
         : undefined;
   const rootDeclared = rootRaw !== undefined && rootRaw !== null;
   const envelopeDeclared = envelopeRaw !== undefined && envelopeRaw !== null;

   // Validate both homes before comparing, so a typo is reported as a typo
   // rather than as a conflict.
   const rootScope = parsePackageScope(rootRaw);
   const envelopeScope = parsePackageScope(envelopeRaw);

   if (rootDeclared && envelopeDeclared) {
      if (rootScope !== envelopeScope) {
         throw new Error(
            `Conflicting "scope" in the package manifest: root "${rootScope}" vs ` +
               `"materialization.scope" "${envelopeScope}". Declare it once, in ` +
               `"materialization".`,
         );
      }
      return {
         scope: envelopeScope,
         warnings: [SCOPE_ROOT_DEPRECATION],
      };
   }
   if (rootDeclared) {
      return { scope: rootScope, warnings: [SCOPE_ROOT_DEPRECATION] };
   }
   return { scope: envelopeScope, warnings: [] };
}

const SCOPE_ROOT_DEPRECATION =
   `"scope" at the manifest root is deprecated: declare it as ` +
   `"materialization": { "scope": ... } alongside the other build knobs. The ` +
   `root form still works and will be removed in a future release.`;

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
   /**
    * Package-level per-query metadata: the least specific model-side layer,
    * overridden per property by a model-file, source or per-request declaration.
    * Null when the manifest declares none.
    *
    * Kept VERBATIM (every string-valued property, conforming or not) rather than
    * filtered to what Malloy will accept. A property that violates the contract
    * has to be visible somewhere to be fixable: publish reports it as a warning
    * from this block, and the runtime clamps it with a metric. Filtering here
    * would make an author's typo disappear with nothing to point at.
    */
   queryMetadata: Record<string, string> | null;
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
 * The `materialization.queryMetadata` block: string-valued properties verbatim.
 * A non-object block, or one with no string values, degrades to null so absence
 * on the wire always means "declared nothing usable". A non-string value is
 * dropped here because it cannot round-trip as a property at all; contract
 * violations of the string values are deliberately NOT dropped (see
 * {@link PackageMaterializationConfig.queryMetadata}).
 */
function parseQueryMetadata(raw: unknown): Record<string, string> | null {
   if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      return null;
   }
   const out: Record<string, string> = {};
   for (const [name, value] of Object.entries(raw as Record<string, unknown>)) {
      if (typeof value === "string") out[name] = value;
   }
   return Object.keys(out).length > 0 ? out : null;
}

/**
 * Read the manifest's `materialization` object, keeping only recognized fields.
 * Returns null when the block is absent so the API field is null rather than an
 * empty object; a non-string schedule degrades to null, and an absent or
 * non-object freshness degrades to null.
 *
 * `materialization.scope` is deliberately NOT read here: scope is a package-level
 * mode rather than a layered knob, and it has two possible homes to reconcile —
 * see {@link resolvePackageScope}, which owns that read and surfaces the
 * resolved value on the package itself.
 */
export function parsePackageMaterialization(
   raw: unknown,
): PackageMaterializationConfig | null {
   if (!raw || typeof raw !== "object") {
      return null;
   }
   const { schedule, freshness, queryMetadata } = raw as {
      schedule?: unknown;
      freshness?: unknown;
      queryMetadata?: unknown;
   };
   return {
      schedule: typeof schedule === "string" ? schedule : null,
      freshness: parseFreshness(freshness),
      queryMetadata: parseQueryMetadata(queryMetadata),
   };
}
