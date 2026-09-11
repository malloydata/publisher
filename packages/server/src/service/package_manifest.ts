// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
 * Resolve the package's per-query metadata from its two homes: the manifest root
 * (canonical) and `materialization.queryMetadata` (the original home, now
 * deprecated).
 *
 * The move is the opposite direction to {@link resolvePackageScope}, for the
 * opposite reason. Scope is a build knob, so it belongs with the build knobs.
 * Query metadata is not one: the properties ride every statement the package's
 * sources issue, a served query as much as a build. Declaring them inside
 * `materialization` describes a scope the feature does not have, and sends an
 * author hunting through build settings for a way to label their traffic.
 *
 * A conflict WARNS and prefers the root, where scope THROWS. Scope decides
 * whether an artifact is version-owned, so guessing could reuse a table across
 * versions that was never meant to be shared — worth failing a load over. A tag
 * is observability only, excluded from `sourceEntityId` and from build identity,
 * so the worst a wrong guess yields is a mislabelled statement. Failing a load
 * over a label would break the rule the whole feature rests on: a tag must never
 * be the reason something refuses to run.
 */
export function resolvePackageQueryMetadata(
   rootRaw: unknown,
   materializationRaw: unknown,
): { queryMetadata: unknown; home: QueryMetadataHome; warnings: string[] } {
   const envelopeRaw =
      materializationRaw && typeof materializationRaw === "object"
         ? (materializationRaw as { queryMetadata?: unknown }).queryMetadata
         : undefined;
   const rootDeclared = rootRaw !== undefined && rootRaw !== null;
   const envelopeDeclared = envelopeRaw !== undefined && envelopeRaw !== null;

   if (rootDeclared && envelopeDeclared) {
      // Both homes agreeing is the transition state the server itself writes,
      // so it is not worth a word to an operator who cannot act on it.
      if (sameQueryMetadataBag(rootRaw, envelopeRaw)) {
         return { queryMetadata: rootRaw, home: "root", warnings: [] };
      }
      return {
         queryMetadata: rootRaw,
         home: "root",
         warnings: [QUERY_METADATA_CONFLICT],
      };
   }
   if (envelopeDeclared) {
      return {
         queryMetadata: envelopeRaw,
         home: "envelope",
         warnings: [QUERY_METADATA_ENVELOPE_DEPRECATION],
      };
   }
   return { queryMetadata: rootRaw, home: "root", warnings: [] };
}

/**
 * Whether two raw `queryMetadata` declarations say the same thing, INSENSITIVE
 * to key order. `{team, tier}` and `{tier, team}` are the same bag; comparing
 * serialized forms directly called them a conflict and sent an author off to
 * reconcile two homes that already agreed.
 *
 * Runs on RAW, pre-validation input, so it cannot assume a flat string map — a
 * value may be a number, a null, an array or an object. Compared through a
 * key-sorted re-serialization that recurses, so nesting is handled rather than
 * assumed away.
 */
function sameQueryMetadataBag(a: unknown, b: unknown): boolean {
   return keySortedJson(a) === keySortedJson(b);
}

function keySortedJson(value: unknown): string {
   if (value === null || typeof value !== "object") {
      // `undefined` has no JSON form; name it so two absent values still match.
      return JSON.stringify(value) ?? "undefined";
   }
   if (Array.isArray(value)) {
      return `[${value.map(keySortedJson).join(",")}]`;
   }
   const entries = Object.entries(value as Record<string, unknown>).sort(
      ([left], [right]) => (left < right ? -1 : left > right ? 1 : 0),
   );
   return `{${entries
      .map(
         ([name, nested]) => `${JSON.stringify(name)}:${keySortedJson(nested)}`,
      )
      .join(",")}}`;
}

/**
 * Which home a resolved bag came from. Carried so a parse warning about one of
 * its properties can name the home the author actually wrote, rather than a
 * hardcoded spelling that is wrong for whichever home did not win.
 */
export type QueryMetadataHome = "root" | "envelope";

const QUERY_METADATA_HOME_LABELS: Record<QueryMetadataHome, string> = {
   root: "queryMetadata",
   envelope: "materialization.queryMetadata",
};

const QUERY_METADATA_ENVELOPE_DEPRECATION =
   `"queryMetadata" inside "materialization" is deprecated: declare it at the ` +
   `manifest root instead. It is not a build setting — the properties ride ` +
   `every statement the package's sources issue, including served queries. The ` +
   `enveloped form still works and will be removed in a future release; until ` +
   `then the server keeps both homes in sync when it writes the manifest, so an ` +
   `older publisher still reads the right value.`;

const QUERY_METADATA_CONFLICT =
   `Conflicting "queryMetadata" in publisher.json: the manifest root and ` +
   `"materialization.queryMetadata" declare different bags, and the root wins. ` +
   `Delete "materialization": { "queryMetadata": ... } — the server rewrites ` +
   `both homes on its next manifest write, so an older publisher still reads ` +
   `the right value.`;

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
function parseQueryMetadata(
   raw: unknown,
   label = QUERY_METADATA_HOME_LABELS.envelope,
): {
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
            `${label}: property '${name}' must be a ` +
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
 * The package's materialization config with `queryMetadata` taken from whichever
 * home won (see {@link resolvePackageQueryMetadata}), so every consumer keeps
 * reading one accessor and no caller has to know there are two homes.
 *
 * Returns a config even when the manifest has NO `materialization` block, which
 * is the shape the canonical form produces: a package that declares tags at the
 * root and nothing else has no build policy to express. Null only when neither
 * home declares anything.
 */
export function materializationWithQueryMetadata(
   parsed: PackageMaterializationConfig | null,
   queryMetadataRaw: unknown,
): PackageMaterializationConfig | null {
   const queryMetadata = parseQueryMetadata(queryMetadataRaw).metadata;
   if (!parsed && !queryMetadata) return null;
   return {
      schedule: parsed?.schedule ?? null,
      freshness: parsed?.freshness ?? null,
      queryMetadata,
   };
}

/**
 * Properties the winning `queryMetadata` home declared but could not keep,
 * named after THAT home. Hardcoding the enveloped spelling pointed the author of
 * a root-only manifest at a `materialization` block their file does not have.
 */
export function queryMetadataParseWarnings(
   raw: unknown,
   home: QueryMetadataHome = "root",
): string[] {
   return parseQueryMetadata(raw, QUERY_METADATA_HOME_LABELS[home]).warnings;
}
