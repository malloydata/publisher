// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The layered settings a persist source's plan entry resolves: freshness and
 * query metadata, each read from the source's tag, its model's tags and the
 * package's materialization config, most specific first. Split out of
 * build_plan.ts; behaviour unchanged.
 */

import type { PersistSource } from "@malloydata/malloy";
import { components } from "../../../api";
import { parseRouteTag } from "../../motly";

type WireFreshness = components["schemas"]["Freshness"];
type QueryMetadata = components["schemas"]["QueryMetadata"];
export type WirePackageMaterialization =
   components["schemas"]["PackageMaterializationConfig"];

/** The freshness `fallback` values the publisher recognizes; others are dropped. */
const FRESHNESS_FALLBACKS = ["live", "stale_ok", "fail"] as const;
type FreshnessFallback = (typeof FRESHNESS_FALLBACKS)[number];

/** One layer's contribution to the resolved freshness (all optional). */
interface FreshnessLayer {
   window?: string;
   fallback?: FreshnessFallback;
}

/**
 * Minimal reader over a Malloy `Tag`: scalar reads by path, plus the subtree read
 * a property collection like `queryMetadata { … }` needs.
 */
export interface ReadableTag {
   text(...path: string[]): string | undefined;
   tag(...path: string[]): ReadableTag | undefined;
   entries?(): Iterable<[string, { text(): string | undefined }]>;
}

/**
 * Read `freshness.window` / `freshness.fallback` from one tag layer, keeping only
 * recognized values. Nested keys, so {@link deriveAnnotationFields} misses them.
 * Per-source `sharing`/`schedule` are rejected at publish
 * (Package.persistencePolicyWarnings), not resolved here.
 */
function tagFreshnessLayer(tag: ReadableTag | undefined): FreshnessLayer {
   if (!tag || typeof tag.text !== "function") return {};
   const layer: FreshnessLayer = {};
   const window = tag.text("freshness", "window");
   if (typeof window === "string") layer.window = window;
   const fallback = tag.text("freshness", "fallback");
   if (
      typeof fallback === "string" &&
      (FRESHNESS_FALLBACKS as readonly string[]).includes(fallback)
   ) {
      layer.fallback = fallback as FreshnessFallback;
   }
   return layer;
}

/**
 * The two homes a model-file (`##`) knob can be declared in: the
 * `materialization` envelope (mirroring the manifest's shape), then the bare form
 * (`## freshness.window="24h"`), still read so already-published packages keep
 * resolving. Callers resolve per property, so each home can set different knobs.
 */
function modelTagLayers(
   tag: ReadableTag | undefined,
): (ReadableTag | undefined)[] {
   const envelope =
      tag && typeof tag.tag === "function"
         ? tag.tag("materialization")
         : undefined;
   return [envelope, tag];
}

/** The package-level `materialization.freshness` as a resolution layer. */
function packageFreshnessLayer(
   cfg: WirePackageMaterialization | null | undefined,
): FreshnessLayer {
   if (!cfg) return {};
   const layer: FreshnessLayer = {};
   if (cfg.freshness?.window) layer.window = cfg.freshness.window;
   const fallback = cfg.freshness?.fallback;
   if (
      typeof fallback === "string" &&
      (FRESHNESS_FALLBACKS as readonly string[]).includes(fallback)
   ) {
      layer.fallback = fallback as FreshnessFallback;
   }
   return layer;
}

/** The source's `#@` tag, or undefined if it fails to parse (degrade to unset). */
function safeSourceTag(source: PersistSource): ReadableTag | undefined {
   try {
      return parseRouteTag(source.annotations, "@").tag as ReadableTag;
   } catch {
      return undefined;
   }
}

/**
 * The model-file-level (`##`) tag resolved for the source, or undefined on a
 * parse failure. The model-file layer sits between the source annotation and
 * the package default in most-specific-wins resolution.
 */
function safeModelTag(source: PersistSource): ReadableTag | undefined {
   try {
      return parseRouteTag(source.modelAnnotations).tag as ReadableTag;
   } catch {
      return undefined;
   }
}

/**
 * Resolve a source's EFFECTIVE freshness, most-specific-wins per field: source >
 * model-file > package. Null when unset everywhere, so the control plane can tell
 * "unset" from a declaration. Invalid `fallback` values are dropped, never
 * defaulted.
 */
export function resolveFreshness(
   source: PersistSource,
   packageMaterialization: WirePackageMaterialization | null | undefined,
): WireFreshness | null {
   const layers: FreshnessLayer[] = [
      // `#@ persist` declares knobs bare: it IS the materialization declaration.
      tagFreshnessLayer(safeSourceTag(source)),
      ...modelTagLayers(safeModelTag(source)).map(tagFreshnessLayer),
      packageFreshnessLayer(packageMaterialization),
   ];

   const window = layers.map((l) => l.window).find((v) => v !== undefined);
   const fallback = layers.map((l) => l.fallback).find((v) => v !== undefined);

   if (window === undefined && fallback === undefined) return null;
   const freshness: WireFreshness = {};
   if (window !== undefined) freshness.window = window;
   if (fallback !== undefined) freshness.fallback = fallback;
   return freshness;
}

/**
 * Read the `queryMetadata` property collection (block or dotted form) from one
 * tag layer. Every string property is kept verbatim, even ones violating Malloy's
 * bag contract: publish warns and the runtime clamps, so a typo stays visible.
 */
function tagQueryMetadataLayer(tag: ReadableTag | undefined): QueryMetadata {
   const subtree =
      tag && typeof tag.tag === "function"
         ? tag.tag("queryMetadata")
         : undefined;
   if (!subtree || typeof subtree.entries !== "function") return {};
   const layer: QueryMetadata = {};
   try {
      for (const [name, value] of subtree.entries()) {
         const text = value.text();
         if (text !== undefined) layer[name] = text;
      }
   } catch {
      return {};
   }
   return layer;
}

/**
 * Resolve a source's EFFECTIVE per-query metadata for the build path (see
 * {@link composeDeclaredQueryMetadata} for precedence). Attached to every
 * statement the build issues, and deliberately absent from the content address:
 * changing a tag must never re-address a table.
 */
export function resolveQueryMetadata(
   source: PersistSource,
   packageMaterialization: WirePackageMaterialization | null | undefined,
): QueryMetadata | null {
   return composeDeclaredQueryMetadata({
      packageDeclaration: packageMaterialization?.queryMetadata ?? null,
      modelTag: safeModelTag(source),
      sourceTag: safeSourceTag(source),
   });
}

/**
 * The author-declared layers of one statement's metadata, composed
 * most-specific-wins PER PROPERTY: source `#@ queryMetadata.*` > model-file
 * `## queryMetadata.*` > package `queryMetadata`. Each of the latter two has a
 * deprecated `materialization.`-prefixed spelling that sits UNDERNEATH its
 * canonical form — the opposite of freshness (see the body).
 *
 * Takes tags rather than a {@link PersistSource} so the serve path can compose
 * the same layers from a loaded model. An absent layer contributes nothing, so a
 * package-wide `team` survives a source that only overrides `workload`. Null when
 * no layer declares anything.
 */
export function composeDeclaredQueryMetadata(layers: {
   /** The package's declared bag, not the materialization policy carrying it. */
   packageDeclaration?: QueryMetadata | null;
   modelTag?: ReadableTag;
   sourceTag?: ReadableTag;
}): QueryMetadata | null {
   // Least specific first, so later layers overwrite property by property.
   //
   // `modelTagLayers` yields [envelope, bare]. For query metadata the bare form
   // is canonical and the envelope deprecated, so it is consumed as-is, NOT
   // reversed: reversing would let the deprecated spelling override the
   // canonical one.
   const ordered: QueryMetadata[] = [
      layers.packageDeclaration ?? {},
      ...modelTagLayers(layers.modelTag).map(tagQueryMetadataLayer),
      tagQueryMetadataLayer(layers.sourceTag),
   ];
   const resolved: QueryMetadata = Object.assign({}, ...ordered);
   return Object.keys(resolved).length > 0 ? resolved : null;
}
