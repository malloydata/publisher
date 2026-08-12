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
 * silent.
 *
 * That is NOT the same as "no package in the wild changes". A deployed package
 * that already has a root `index.malloy` and no `explores` does change: it
 * gains a curated surface it did not have, so its other models drop out of
 * listings and within-file `export {}` curation starts applying. That is the
 * intended behavior of the convention, and it is why the change is a listing
 * change only. Query access is deliberately untouched (see `fromConvention`),
 * so the worst case is a model that has to be un-hidden, never one that has
 * become unreachable.
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
   /**
    * The raw `queryableSources` value as it appeared in publisher.json, or
    * undefined when the key was absent. The raw value rather than a boolean,
    * because "all" and "declared" need different remediation advice.
    */
   declaredQueryableSources: unknown;
   /** Package-relative model paths, as produced by `listPackageFiles`. */
   modelPaths: readonly string[];
}): ResolvedExplores {
   const { declaredExplores, declaredQueryableSources, modelPaths } = input;
   const hasIndexModel = modelPaths.includes(INDEX_MODEL_NAME);
   // An array of strings is the only well-formed shape. A NON-string element is
   // malformed too, and is treated exactly like a malformed key rather than
   // coerced: `String(null)` would build a surface naming "null", which matches
   // no model, so curation would switch on over an empty set and the package
   // would list nothing with no warning to explain it. Rejecting the key is the
   // conservative direction, because it leaves the package uncurated (or on the
   // convention) rather than silently emptying it.
   const declaredIsArray = Array.isArray(declaredExplores);
   const wellFormedArray =
      declaredIsArray &&
      (declaredExplores as unknown[]).every((e) => typeof e === "string");
   const declared = wellFormedArray
      ? (declaredExplores as string[]).map(normalizeModelPath)
      : undefined;

   // A present-but-malformed `explores` (a bare string is the usual typo of the
   // array form; a null or number among the entries is the other) is neither a
   // declaration nor an absence. Treating it as an absence would let the
   // convention quietly curate the package, dropping every model the author was
   // trying to list with no clue why, so it falls through but says so.
   const malformed =
      declaredExplores !== undefined &&
      declaredExplores !== null &&
      !wellFormedArray;

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
         if (!declared.includes(INDEX_MODEL_NAME)) {
            warnings.push(
               exploresDisagreesWithConvention(
                  declared,
                  declaredQueryableSources,
               ),
            );
         } else if (declared.length === 1) {
            // Only a surface of EXACTLY index.malloy is what the convention
            // would produce, so only that one can be deleted for the same
            // result. Saying so for a multi-entry surface would be advice that
            // silently drops every other entry.
            warnings.push(
               exploresRedundantWithConvention(declaredQueryableSources),
            );
         }
      }
      return { explores: declared, fromConvention: false, warnings };
   }

   if (!hasIndexModel) {
      return {
         explores: undefined,
         fromConvention: false,
         warnings: malformed ? [exploresMalformed(declaredExplores)] : [],
      };
   }

   // The convention fires. `queryableSources` is inert here (the boundary needs
   // an explicit `explores`), so say so rather than let an operator believe
   // they have a query boundary they do not have.
   const warnings: string[] = [];
   if (malformed) {
      warnings.push(exploresMalformed(declaredExplores));
   }
   if (declaredQueryableSources !== undefined) {
      warnings.push(
         queryableSourcesInertUnderConvention(declaredQueryableSources),
      );
   }
   return { explores: [INDEX_MODEL_NAME], fromConvention: true, warnings };
}

/**
 * Said when a declared `explores` is exactly what the convention would derive.
 *
 * Takes `queryableSources` because the interesting half is what DELETING the
 * key costs, and that depends on whether the boundary is live. Keying the
 * caveat on whether the author wrote a `queryableSources` key would be wrong:
 * the boundary is on by default, so the author who never wrote one is exactly
 * the author most likely to delete `explores` and silently reopen every hidden
 * source to query.
 */
function exploresRedundantWithConvention(
   declaredQueryableSources: unknown,
): string {
   const head =
      `"explores" in publisher.json lists only "${INDEX_MODEL_NAME}", which is ` +
      `now the default for any package that has one, so the key is no longer ` +
      `needed to get this discovery surface. `;
   return declaredQueryableSources === "all"
      ? head +
           `You have "queryableSources": "all", so no query boundary is in ` +
           `force and deleting the key changes nothing. Both can go.`
      : head +
           `Before deleting it, note that it is ALSO what enforces the query ` +
           `boundary here: "queryableSources" defaults to "declared", so ` +
           `sources outside this surface are refused today, and deleting the ` +
           `key reopens every one of them to query. That holds whether or not ` +
           `you ever wrote a "queryableSources" key. Delete it only if you want ` +
           `curated listings without a query boundary.`;
}

/**
 * Decide whether a package's surface is still convention-derived after a
 * metadata PATCH.
 *
 * A body that names a surface has declared one, and declaring is what opts into
 * the query boundary. The exception is the round trip: `exploresFromConvention`
 * is not on the wire, so a client that GETs a package, edits one text field and
 * PATCHes the whole object back re-sends the convention's own `explores`
 * verbatim without having declared anything. Arming a 404 boundary as a side
 * effect of editing a description would be a nasty surprise, so a body that
 * echoes exactly the current surface leaves the origin alone. Only a body
 * naming a DIFFERENT surface counts as a declaration.
 *
 * Pure and exported so the rule is testable on its own and lives beside the
 * other `explores` rules rather than buried in the PATCH handler.
 */
export function resolvePatchedExploresOrigin(input: {
   /** Whether the surface was convention-derived before this PATCH. */
   previousFromConvention: boolean;
   /** Normalized `explores` from the request body, or undefined if absent. */
   patchedExplores: readonly string[] | undefined;
   /** The surface currently in force. */
   existingExplores: readonly string[] | undefined;
}): boolean {
   const { previousFromConvention, patchedExplores, existingExplores } = input;
   if (patchedExplores === undefined) {
      // Not mentioned: the surface and its origin are both untouched.
      return previousFromConvention;
   }
   if (
      previousFromConvention &&
      sameExploreSet(patchedExplores, existingExplores)
   ) {
      return true;
   }
   return false;
}

/**
 * Whether two `explores` lists name the same surface, order-insensitively.
 * Order does not matter because the surface is consumed as a Set
 * (`Package.exploreSet`), so a reordered list is the same surface and must not
 * read as the author having changed it.
 */
function sameExploreSet(
   a: readonly string[],
   b: readonly string[] | undefined,
): boolean {
   if (b === undefined || a.length !== b.length) return false;
   const other = new Set(b);
   return a.every((entry) => other.has(entry));
}

/**
 * Whether a manifest warning is one of the ones {@link resolveExplores} emits
 * about where the discovery surface came from.
 *
 * Lives here, beside the strings themselves, so the two cannot drift: a warning
 * whose wording changes without this predicate changing would start surviving
 * the origin change it describes. Matched on a stable substring rather than by
 * equality, because two of the three are built per call.
 */
export function isExploresConventionWarning(warning: string): boolean {
   return (
      warning.startsWith(`"explores" in publisher.json`) ||
      warning.startsWith(`This package has an "${INDEX_MODEL_NAME}"`) ||
      warning.startsWith(`"queryableSources" in publisher.json`) ||
      warning.startsWith(EXPLORES_PATCH_IGNORED_PREFIX)
   );
}

/** Stable opening of {@link exploresPatchIgnoredUnderConvention}, so a later
 *  answer to the same question supersedes an earlier one. Exported for the
 *  same reason {@link QUERYABLE_SOURCES_INERT_PREFIX} is: the PATCH path has to
 *  pass it, and the two post-load warnings must behave identically. */
export const EXPLORES_PATCH_IGNORED_PREFIX =
   "An API update re-sent this package's discovery surface";

/**
 * Said when a metadata PATCH carried an `explores` identical to the surface the
 * convention had already produced.
 *
 * That request is ambiguous and the server cannot tell the two readings apart:
 * a client echoing back what it just read (which must stay a no-op, or editing
 * a description would arm a 404 boundary), or an author declaring the surface
 * on purpose to opt INTO that boundary. It is treated as the echo, because
 * guessing the other way silently revokes query access.
 *
 * The cost is that the second author's intent is not applied, and an unsaid
 * "we ignored your declaration" on an access control is the failure mode worth
 * avoiding most: they would believe queries were gated when they are not. So
 * it is said out loud, with the route that does work.
 */
export function exploresPatchIgnoredUnderConvention(
   effectiveQueryableSources: unknown,
): string {
   // The remedy has to account for "all", or it promises a boundary that
   // declaring the surface would still not produce. This is the one builder
   // whose subject is an author actively trying to ARM the boundary, so a
   // wrong remedy here fails in the direction the whole feature guards.
   const remedy =
      effectiveQueryableSources === "all"
         ? `If you meant to enforce the boundary, note that this package also ` +
           `has "queryableSources": "all", which switches it off on its own. ` +
           `You need BOTH: set "queryableSources" to "declared" and add ` +
           `"explores" to the package's publisher.json, then reload.`
         : `If you meant to enforce the boundary, add "explores" to the ` +
           `package's publisher.json and reload; a surface declared there does ` +
           `enforce it.`;
   return (
      `${EXPLORES_PATCH_IGNORED_PREFIX} ("${INDEX_MODEL_NAME}"), which is ` +
      `exactly what the convention already derives, so nothing changed and the ` +
      `query boundary is NOT enforced: every source is still queryable by name. ` +
      `An API update cannot tell that request apart from a client echoing back ` +
      `what it read, and treating it as a declaration would revoke query access ` +
      `from anyone who had only edited a description. ${remedy}`
   );
}

function exploresMalformed(raw: unknown): string {
   return (
      `"explores" in publisher.json is not a list of model file paths (got ` +
      `${JSON.stringify(raw)}), so it was ignored. Write it as an array of ` +
      `strings, for example ["${INDEX_MODEL_NAME}"]. Until you do, this ` +
      `package's discovery surface is whatever the "${INDEX_MODEL_NAME}" ` +
      `convention decides, which is not what the key says. Note that fixing it ` +
      `does more than restore the listing you meant: a declared "explores" also ` +
      `turns on the query boundary (unless "queryableSources" is "all"), so ` +
      `sources you leave out will start being refused rather than merely hidden.`
   );
}

function exploresDisagreesWithConvention(
   declared: string[],
   declaredQueryableSources: unknown,
): string {
   // Deleting the key is the tempting remedy and the dangerous one: it also
   // drops the query boundary, which is on by default. Say so, unless the
   // author already opted out of the boundary with "all".
   const deleteCost =
      declaredQueryableSources === "all"
         ? `delete the key (you have "queryableSources": "all", so no query ` +
           `boundary is in force and deleting changes only the listings)`
         : `delete the key, which ALSO drops the query boundary this package ` +
           `currently enforces and makes every source queryable again`;
   return (
      `This package has an "${INDEX_MODEL_NAME}" but its publisher.json "explores" ` +
      `does not list it (${JSON.stringify(declared)}). The explicit key wins, so ` +
      `"${INDEX_MODEL_NAME}" is NOT part of the discovery surface. If that is ` +
      `intended, nothing is broken and you can silence this by renaming the file. ` +
      `If you meant it to be the package's entry point, add it to "explores", or ` +
      `${deleteCost}.`
   );
}

/** Stable opening of {@link queryableSourcesInertUnderConvention}, so a later
 *  answer to the same question can supersede an earlier one. */
export const QUERYABLE_SOURCES_INERT_PREFIX =
   '"queryableSources" in publisher.json has no effect';

export function queryableSourcesInertUnderConvention(raw: unknown): string {
   // "all" already means "do not enforce", so telling that author to declare
   // `explores` would send them to a state that still does not enforce. Only
   // the author who asked for a boundary needs the two-part remedy.
   const remedy =
      raw === "all"
         ? `Nothing is wrong: "all" also means no boundary, so the setting is ` +
           `simply redundant here and you can delete it. To enforce a boundary ` +
           `you would need BOTH an explicit "explores" and ` +
           `"queryableSources": "declared".`
         : `Add an explicit "explores" naming your surface to enforce the ` +
           `boundary.`;
   return (
      `${QUERYABLE_SOURCES_INERT_PREFIX} on this package. Its ` +
      `discovery surface comes from the "${INDEX_MODEL_NAME}" convention, and ` +
      `the query boundary is only enforced for a surface declared explicitly, ` +
      `so every source stays queryable by name. ${remedy} The convention ` +
      `deliberately does not enforce it: the denial is a 404, so turning it on ` +
      `because a filename exists would revoke access with no actionable error.`
   );
}
