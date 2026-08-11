/**
 * Pure parsing of the package manifest's (publisher.json) `materialization`
 * block and its discovery surface (`explores`). Kept side-effect free (no fs,
 * no worker bootstrap) so it is unit testable in isolation from the
 * package-load worker that consumes it.
 */

import { INDEX_MODEL_NAME, normalizeModelPath } from "../constants";

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
 * The root form keeps working because scope rides the published artifact: a
 * package published before the move must keep parsing until it is republished.
 * Declaring DIFFERENT values in the two homes throws, because scope decides
 * whether an artifact is version-owned and guessing which one the author meant
 * could reuse a table across versions that was never supposed to be shared.
 *
 * Only a root-ONLY declaration is deprecated. Both homes agreeing is the
 * transition state the server itself writes (see `writePackageManifest`): the
 * envelope for this build, the root for an older publisher that would otherwise
 * silently default to `package`. Warning about that would be warning an operator
 * about something the server did on their behalf and they cannot fix.
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
         // Names the fix, because this throw fails the package LOAD: the
         // package is skipped and its only trace is /status loadErrors, so the
         // message is the whole diagnosis. Guessing instead would be worse —
         // picking the wrong one reuses a table across versions that was never
         // meant to be shared, silently.
         throw new Error(
            `Conflicting "scope" in publisher.json: root "${rootScope}" vs ` +
               `"materialization.scope" "${envelopeScope}". The package cannot ` +
               `load until they agree. Edit "materialization": { "scope": ... } ` +
               `to the value you want and delete the root-level "scope" (the ` +
               `server rewrites both homes on its next manifest write).`,
         );
      }
      return { scope: envelopeScope, warnings: [] };
   }
   if (rootDeclared) {
      return { scope: rootScope, warnings: [SCOPE_ROOT_DEPRECATION] };
   }
   return { scope: envelopeScope, warnings: [] };
}

const SCOPE_ROOT_DEPRECATION =
   `"scope" at the manifest root is deprecated: declare it as ` +
   `"materialization": { "scope": ... } alongside the other build knobs. The ` +
   `root form still works and will be removed in a future release; until then ` +
   `the server keeps both homes in sync when it writes the manifest, so an ` +
   `older publisher still reads the right value.`;

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
 *
 * Each dropped value is reported, because the drop happens BEFORE the config
 * validation that would otherwise name it: an unquoted `"team": 123` is a
 * plausible hand-edit, and without this it disappears with nothing anywhere to
 * point at.
 */
function parseQueryMetadata(raw: unknown): {
   metadata: Record<string, string> | null;
   warnings: string[];
} {
   if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      return { metadata: null, warnings: [] };
   }
   const out: Record<string, string> = {};
   const warnings: string[] = [];
   for (const [name, value] of Object.entries(raw as Record<string, unknown>)) {
      if (typeof value === "string") {
         out[name] = value;
      } else {
         warnings.push(
            `materialization.queryMetadata: property '${name}' must be a ` +
               `string (got ${value === null ? "null" : typeof value}); it is ` +
               `not attached to any statement`,
         );
      }
   }
   return {
      metadata: Object.keys(out).length > 0 ? out : null,
      warnings,
   };
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
      queryMetadata: parseQueryMetadata(queryMetadata).metadata,
   };
}

/**
 * What the `materialization` parse tolerated but could not keep, for the
 * operator warnings array. Reads the same parse as
 * {@link parsePackageMaterialization} rather than re-deriving it, so the two
 * cannot disagree about what was dropped.
 */
export function packageMaterializationWarnings(raw: unknown): string[] {
   if (!raw || typeof raw !== "object") {
      return [];
   }
   const { queryMetadata } = raw as { queryMetadata?: unknown };
   return parseQueryMetadata(queryMetadata).warnings;
}

/** The resolved discovery surface, plus how it was arrived at. */
export interface ResolvedExplores {
   /**
    * The discovery surface, or undefined when the package is uncurated.
    * Normalized through `normalizeModelPath`, so entries compare equal to the
    * paths `listPackageFiles` produces regardless of input channel.
    */
   explores: string[] | undefined;
   /**
    * True when `explores` came from the `index.malloy` convention rather than
    * from an explicit key in publisher.json.
    *
    * Load-bearing, and the reason this is returned rather than inferred later:
    * a convention-derived surface curates DISCOVERY only. It must not activate
    * the query boundary, because that boundary answers 404 (deliberately
    * indistinguishable from "does not exist"), so inferring it from the mere
    * presence of a filename would silently revoke query access on a package
    * whose author never edited a config. See `Package.applyQueryBoundaryToModels`.
    */
   fromConvention: boolean;
   warnings: string[];
}

/**
 * Resolve a package's discovery surface from its two sources: an explicit
 * `explores` in publisher.json, and the `index.malloy` convention.
 *
 * Explicit always wins. The convention fills in only where the manifest is
 * silent, which keeps every package already in the wild on exactly the
 * behavior it has today.
 *
 * An explicit empty array counts as explicit. `explores: []` reads as a
 * deliberate "do not curate", and today it means every model is listed; having
 * the convention quietly curate it instead would change served behavior off a
 * config the author did write.
 *
 * Only a root-level `index.malloy` triggers the convention. A nested
 * `reports/index.malloy` is an ordinary model: the convention exists so a
 * reader can predict a package's entry point without opening the manifest, and
 * that only works if there is exactly one place to look.
 */
export function resolveExplores(input: {
   /** The raw `explores` value as it appeared in publisher.json. */
   declaredExplores: unknown;
   /** Whether publisher.json carried a `queryableSources` key at all. */
   queryableSourcesDeclared: boolean;
   /** Package-relative model paths, as produced by `listPackageFiles`. */
   modelPaths: readonly string[];
}): ResolvedExplores {
   const { declaredExplores, queryableSourcesDeclared, modelPaths } = input;
   const hasIndexModel = modelPaths.includes(INDEX_MODEL_NAME);
   const declared = Array.isArray(declaredExplores)
      ? declaredExplores.map(normalizeModelPath)
      : undefined;

   if (declared !== undefined) {
      // Explicit wins. Warn only when the package also has an index.malloy,
      // because that is the only case where the author has a convention
      // available to drop the key for. Warning on every `explores` user would
      // fire on packages that curate a multi-file surface the convention
      // cannot express (examples/governed-analytics is one), which is a
      // warning with no available fix.
      const warnings: string[] = [];
      // An empty array curates nothing, so every model including index.malloy
      // is still listed. There is no disagreement to report and nothing hidden.
      if (hasIndexModel && declared.length > 0) {
         warnings.push(
            declared.includes(INDEX_MODEL_NAME)
               ? EXPLORES_REDUNDANT_WITH_CONVENTION
               : exploresDisagreesWithConvention(declared),
         );
      }
      return { explores: declared, fromConvention: false, warnings };
   }

   if (!hasIndexModel) {
      return { explores: undefined, fromConvention: false, warnings: [] };
   }

   // The convention fires. `queryableSources` is inert here (the boundary needs
   // an explicit `explores`), so say so rather than let an operator believe
   // they have a query boundary they do not have.
   return {
      explores: [INDEX_MODEL_NAME],
      fromConvention: true,
      warnings: queryableSourcesDeclared
         ? [QUERYABLE_SOURCES_INERT_UNDER_CONVENTION]
         : [],
   };
}

const EXPLORES_REDUNDANT_WITH_CONVENTION =
   `"explores" in publisher.json lists "${INDEX_MODEL_NAME}", which is now the ` +
   `default for any package that has one. The key still works and still wins ` +
   `over the convention; you can delete it and get the same discovery surface. ` +
   `Keep it if you also rely on "queryableSources": declaring "explores" ` +
   `explicitly is what opts a package into the query boundary.`;

function exploresDisagreesWithConvention(declared: string[]): string {
   return (
      `This package has an "${INDEX_MODEL_NAME}" but its publisher.json "explores" ` +
      `does not list it (${JSON.stringify(declared)}). The explicit key wins, so ` +
      `"${INDEX_MODEL_NAME}" is NOT part of the discovery surface. If that is ` +
      `intended, nothing is broken and you can silence this by renaming the file. ` +
      `If you meant it to be the package's entry point, add it to "explores" or ` +
      `delete the key and let the convention pick it up.`
   );
}

const QUERYABLE_SOURCES_INERT_UNDER_CONVENTION =
   `"queryableSources" in publisher.json has no effect on this package. Its ` +
   `discovery surface comes from the "${INDEX_MODEL_NAME}" convention, and the ` +
   `query boundary is only enforced for a surface declared explicitly, so every ` +
   `source stays queryable by name. Add an explicit "explores" to enforce the ` +
   `boundary. The convention deliberately does not enforce it: the denial is a ` +
   `404, so turning it on because a filename exists would revoke access with no ` +
   `actionable error.`;
