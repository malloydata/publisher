// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Pure parsing of publisher.json's `materialization` block and discovery
 * surface (`explores`). No fs or worker dependencies, so it unit-tests alone.
 */

import { INDEX_MODEL_NAME, normalizeModelPath } from "../constants";
import { PackageManifestError } from "../errors";

const FRESHNESS_FALLBACKS = ["live", "stale_ok", "fail"] as const;
export type FreshnessFallback = (typeof FRESHNESS_FALLBACKS)[number];

/**
 * Package-level Malloy Persistence scope, applied to every persist source and
 * index. `package` (default): artifacts are reused across versions when fresh;
 * `version`: each artifact is owned by one published version (the only mode a
 * `materialization.schedule` is legal in).
 */
export const PACKAGE_SCOPES = ["version", "package"] as const;
export type PackageScope = (typeof PACKAGE_SCOPES)[number];

/**
 * Read a `scope` value, defaulting to `"package"` when absent or null. Any other
 * value throws: scope decides cross-version reuse, so a typo must not silently
 * pick a default.
 */
export function parsePackageScope(raw: unknown): PackageScope {
   if (raw === undefined || raw === null) {
      return "package";
   }
   if ((PACKAGE_SCOPES as readonly unknown[]).includes(raw)) {
      return raw as PackageScope;
   }
   throw new PackageManifestError(
      `Invalid "scope" in the package manifest: ${JSON.stringify(raw)}. ` +
         `Expected "version" or "package" (default "package").`,
   );
}

/**
 * Resolve the package's scope from `materialization.scope` (canonical, with
 * the other build knobs) and the manifest root (deprecated, still read for
 * already-published packages). Different values in the two homes throw:
 * guessing could share a table across versions that was never meant to be.
 *
 * Only a root-ONLY declaration warns. Both homes agreeing is what
 * `writePackageManifest` itself writes, for older publishers that read the root.
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
         // Names the fix: this fails the load, and /status loadErrors is the
         // package's only trace, so the message is the whole diagnosis.
         throw new PackageManifestError(
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
 * Resolve per-query metadata from the manifest root (canonical: the properties
 * ride every statement, not just builds) and `materialization.queryMetadata`
 * (deprecated).
 *
 * A conflict WARNS and prefers the root, where scope throws: a tag is
 * observability only (outside `sourceEntityId` and build identity), and a tag
 * must never be the reason something refuses to run.
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
      // Both homes agreeing is what the server itself writes; no warning.
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
 * Whether two raw `queryMetadata` declarations are equal regardless of key
 * order. The input is pre-validation, so the comparison recurses through any
 * JSON shape rather than assuming a flat string map.
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

/** Which home a resolved bag came from, so warnings name the one the author wrote. */
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
 * `materialization.freshness`, surfaced for the control plane (which owns
 * scheduling and query-time gating). An invalid field is dropped, never
 * defaulted, so absence on the wire means "not declared".
 */
export interface PackageFreshnessConfig {
   /** Maximum acceptable staleness, as a duration string (e.g. "24h"). */
   window?: string;
   /** Declared query-time behavior when the window is missed. */
   fallback?: FreshnessFallback;
}

export interface PackageMaterializationConfig {
   /**
    * 5-field cron for version-level re-materialization; null when absent or not
    * a string. Publish-gated: every persist source must resolve to explicit
    * `sharing=private` (see Package.scheduleWarnings).
    */
   schedule: string | null;
   /** Null when none is declared, distinct from `{}` (a policy with no valid fields). */
   freshness: PackageFreshnessConfig | null;
   /**
    * Package-level per-query metadata, the least specific layer (overridden by
    * model-file, source and per-request declarations); null when none. Every
    * string property is kept, conforming or not, so publish can warn about a
    * bad one and the runtime clamp it with a metric, rather than it vanishing.
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
 * String-valued `queryMetadata` properties verbatim; null when there are none.
 * A non-string value cannot be a property, so it is dropped and reported (the
 * drop happens before the validation that would otherwise name it).
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
 * Read the manifest's `materialization` object, keeping only recognized fields;
 * null when the block is absent. `scope` is read by {@link resolvePackageScope}.
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
 * The materialization config with `queryMetadata` from whichever home won (see
 * {@link resolvePackageQueryMetadata}), so callers read one accessor. Non-null
 * without a `materialization` block when the root declares tags.
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

/** Properties the winning `queryMetadata` home declared but could not keep, named after that home. */
export function queryMetadataParseWarnings(
   raw: unknown,
   home: QueryMetadataHome = "root",
): string[] {
   return parseQueryMetadata(raw, QUERY_METADATA_HOME_LABELS[home]).warnings;
}

// ──────────────────────────────────────────────────────────────────────
// The discovery surface: `explores`, and the `index.malloy` convention
// ──────────────────────────────────────────────────────────────────────

/**
 * Resolve a package's discovery surface from an explicit `explores` in
 * publisher.json or, where the manifest is silent, the `index.malloy`
 * convention. The convention only defaults that one field: nothing downstream
 * knows where a surface came from, so the two cannot behave differently. An
 * explicit `explores: []` still counts as explicit (every model is listed).
 *
 * Only a root `index.malloy` spelled exactly counts; a root file differing
 * only in case gets a warning. Warnings are at most two sentences: what is
 * wrong, then `Fix:` and the one edit.
 *
 * Precedence never throws (conflicting intent is a warning); a MALFORMED
 * `explores` does, and fails the load (see below).
 */
export function resolveExplores(input: {
   /** The raw `explores` value as it appeared in publisher.json. */
   declaredExplores: unknown;
   /**
    * The raw `queryableSources` value, or undefined when absent. Raw because
    * `"all"` and `"declared"` need different deprecation advice.
    */
   declaredQueryableSources: unknown;
   /** Package-relative model paths, as produced by `filterModelPaths`. */
   modelPaths: readonly string[];
   /**
    * Which declared entries are reports (`report.ts`); a report needs no
    * `explores` entry. Absent means none are.
    */
   reportPaths?: ReadonlySet<string>;
}): { explores: string[] | undefined; warnings: string[] } {
   const { declaredExplores, declaredQueryableSources, modelPaths } = input;
   const reportPaths = input.reportPaths ?? new Set<string>();
   const hasIndexModel = modelPaths.includes(INDEX_MODEL_NAME);
   const warnings: string[] = [];

   // Anything but an array of strings fails the load. `explores` decides what
   // is REACHABLE: ignoring a bad value widens the surface, and keeping the
   // parseable entries or coercing invents intent. A refused package shows in
   // `loadErrors`; a silently-uncurated one would not.
   if (
      declaredExplores !== undefined &&
      !(
         Array.isArray(declaredExplores) &&
         declaredExplores.every((entry) => typeof entry === "string")
      )
   ) {
      throw new PackageManifestError(
         `Invalid "explores" in publisher.json: it must be a list of file ` +
            `names, but is ${JSON.stringify(declaredExplores)}. The package ` +
            `was not loaded. Fix: delete "explores" and add an ` +
            `${INDEX_MODEL_NAME}.`,
      );
   }
   const declared = Array.isArray(declaredExplores)
      ? declaredExplores.map((entry) => normalizeModelPath(entry as string))
      : undefined;

   // `"all"` is not warned about: it is the only way to curate listings
   // without refusing queries, so it has no replacement.
   if (
      declaredQueryableSources !== undefined &&
      declaredQueryableSources !== "all"
   ) {
      warnings.push(QUERYABLE_SOURCES_DEPRECATION);
   }

   if (declared !== undefined) {
      // Every use is deprecated; each shape gets the edit that replaces it.
      if (declared.length === 0) {
         warnings.push(
            hasIndexModel
               ? EXPLORES_EMPTY_SUPPRESSES_CONVENTION
               : EXPLORES_EMPTY_IS_UNCURATED,
         );
      } else {
         warnings.push(
            exploresDeprecation(declared, hasIndexModel, reportPaths),
         );
      }
      // In addition to the deprecation: the key is overriding the file.
      if (
         hasIndexModel &&
         declared.length > 0 &&
         !declared.includes(INDEX_MODEL_NAME)
      ) {
         warnings.push(EXPLORES_OMITS_INDEX_MODEL);
      }
      return { explores: declared, warnings };
   }

   if (!hasIndexModel) {
      for (const modelPath of modelPaths) {
         if (
            !modelPath.includes("/") &&
            modelPath.toLowerCase() === INDEX_MODEL_NAME
         ) {
            warnings.push(
               `${modelPath} is ignored: only a root file named exactly ` +
                  `${INDEX_MODEL_NAME} decides what is published. Fix: rename ` +
                  `it to ${INDEX_MODEL_NAME}.`,
            );
         }
      }
      return { explores: undefined, warnings };
   }

   return { explores: [INDEX_MODEL_NAME], warnings };
}

/** `a.malloy`, `a.malloy and b.malloy`, `a.malloy, b.malloy and c.malloy`. */
function listFiles(files: readonly string[]): string {
   return files.length <= 1
      ? (files[0] ?? "")
      : `${files.slice(0, -1).join(", ")} and ${files[files.length - 1]}`;
}

/**
 * The deprecation for a non-empty `explores`, with the edit that replaces it.
 * Entries for index.malloy and reports need no replacement; the rest are what
 * index.malloy has to import.
 */
function exploresDeprecation(
   declared: readonly string[],
   hasIndexModel: boolean,
   reportPaths: ReadonlySet<string>,
): string {
   const lead = `"explores" in publisher.json is deprecated.`;
   const files = declared.filter(
      (entry) => entry !== INDEX_MODEL_NAME && !reportPaths.has(entry),
   );
   if (files.length > 0) {
      return hasIndexModel
         ? `${lead} Fix: import ${listFiles(files)} into ${INDEX_MODEL_NAME}, ` +
              `export the sources you publish from it, then delete "explores".`
         : `${lead} Fix: add an ${INDEX_MODEL_NAME} at the package root that ` +
              `imports ${listFiles(files)} and exports the sources you want to ` +
              `publish, then delete "explores".`;
   }
   if (declared.includes(INDEX_MODEL_NAME)) {
      return (
         `${lead} ${INDEX_MODEL_NAME} already publishes the same thing. ` +
         `Fix: delete "explores".`
      );
   }
   return hasIndexModel
      ? `${lead} Fix: delete "explores", so ${INDEX_MODEL_NAME} decides what is published.`
      : `${lead} Fix: add an ${INDEX_MODEL_NAME} at the package root that ` +
           `exports the sources you want to publish, then delete "explores".`;
}

/** A root index.malloy that the explicit key leaves out, and so ignores. */
const EXPLORES_OMITS_INDEX_MODEL =
   `${INDEX_MODEL_NAME} is ignored because "explores" in publisher.json ` +
   `doesn't list it. Fix: delete "explores" to publish what ` +
   `${INDEX_MODEL_NAME} exports, or rename ${INDEX_MODEL_NAME} (and any import ` +
   `of it) if it isn't meant to decide what is published.`;

/** The old opt-out: an empty array beside an index.malloy. The fix names the
 *  imports because a broken import fails the whole package's compile. */
const EXPLORES_EMPTY_SUPPRESSES_CONVENTION =
   `"explores" in publisher.json is deprecated. Here it stops ` +
   `${INDEX_MODEL_NAME} from limiting what this package publishes. Fix: to ` +
   `publish everything, rename ${INDEX_MODEL_NAME}, point any import of it at ` +
   `the new name, then delete "explores".`;

/** An empty array with no index.malloy: the package is uncurated anyway. */
const EXPLORES_EMPTY_IS_UNCURATED = `"explores": [] in publisher.json does nothing. Fix: delete it.`;

/** `"declared"` is the default, so the key changes nothing. */
const QUERYABLE_SOURCES_DEPRECATION = `"queryableSources" in publisher.json does nothing. Fix: delete it.`;
