// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   AccessModifierEnum,
   type Entity,
   type JoinSourceMetadata,
} from "./internal_types";
import { logger } from "../../logger";
import {
   getAccessModifier,
   getDirectTaglines,
   getEntityFromMalloyField,
   getRawTaglines,
   parseDocAnnotation,
} from "./malloy_indexing_helpers";
import { stableStringify } from "./stable_stringify";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import type {
   Authorize,
   Given as ApiGiven,
   SourceMetadata,
   SourceParameter,
} from "./internal_types";

/** Routes the `Given.annotations` contract excludes.
 *  Must agree with `isReservedRoute` in ../annotations.ts — empty or non-alphanumeric
 *  routes are Malloy's reserved channels (plain `#` tags, `#"` doc strings,
 *  `##!` pragmas) and aren't part of the caller-facing annotation surface. */
const isReservedRoute = (route: string): boolean =>
   route === "" || !/[\p{L}\p{N}]/u.test(route);

import type {
   Explore as MalloyExplore,
   Field as MalloyField,
   Model as MalloyModel,
} from "@malloydata/malloy";

import { errorMessage } from "./errors";
import {
   parseGateAnnotation,
   resolveGates,
   type GateExpr,
   type GateStructDef,
} from "./access_gates";
import {
   accessFilterTermsForGates,
   givenTypeMap,
   makeDimIndexLookupKey,
} from "./dim_index_discovery";
import type { DimIndexFingerprintInputs } from "./dim_index_entity_id";
import { composeSourceName, splitSourcePath } from "./explore_walk";
import { parseFilterAnnotation, type Filter } from "./filter_parsing";
import { parseIndexPolicy } from "./index_annotation_parsing";
import { randomUUID } from "crypto";

/**
 * Extracts the Malloy code from an entity based on location.
 */
function extractCodeFromEntity(
   entity: MalloyExplore | MalloyField,
   docString: string,
): string {
   const codeRange = entity.location?.range;
   if (!codeRange) {
      return "";
   }
   // If the code comes from another file, we need to load that file
   const locationUrl = entity.location?.url;
   if (locationUrl && !locationUrl.endsWith("internal.malloy")) {
      const filePath = fileURLToPath(locationUrl);
      try {
         docString = fs.readFileSync(filePath, "utf8");
      } catch (error) {
         logger.warn(
            `Failed to read file for entity ${entity.name}: ${filePath}`,
            error,
         );
         return "";
      }
   }
   const lines = docString.split("\n");
   const keep: string[] = [];
   for (let i = codeRange.start.line; i <= codeRange.end.line; i++) {
      const line = lines[i];
      if (i < 0 || i >= lines.length || !line) {
         logger.warn("Trying to get code out of bounds");
         break;
      }

      if (i === codeRange.start.line && i === codeRange.end.line) {
         keep.push(
            line.slice(codeRange.start.character, codeRange.end.character),
         );
      } else if (i === codeRange.start.line) {
         keep.push(line.slice(codeRange.start.character));
      } else if (i === codeRange.end.line) {
         keep.push(line.slice(0, codeRange.end.character));
      } else {
         keep.push(line);
      }
   }
   logger.debug(`Code for ${entity.name}:`, keep.join("\n"));
   return keep.join("\n");
}

/**
 * Builds a map of source names to their relative model file paths.
 */
function buildSourceToModelPathMap(
   explores: MalloyExplore[],
   relpath: string,
   packageRoot: string,
): Map<string, string> {
   const sourceToModelPath = new Map<string, string>();
   for (const explore of explores) {
      const url = explore.location?.url;
      if (url) {
         // Convert URL to relative path
         let modelPath: string;
         if (url.endsWith("internal.malloy")) {
            // Internal malloy means it's in the current file
            modelPath = relpath;
         } else if (url.startsWith("file://")) {
            // Extract the file path and make it relative to package root
            const fullPath = fileURLToPath(url);
            if (fullPath.startsWith(packageRoot)) {
               modelPath = fullPath
                  .slice(packageRoot.length)
                  .replace(/^[/\\]/, "");
            } else {
               modelPath = path.basename(fullPath); // Use filename only to avoid leaking absolute paths
            }
         } else {
            modelPath = url; // Fallback for other URL schemes
         }
         sourceToModelPath.set(explore.name, modelPath);
      }
   }
   return sourceToModelPath;
}

/**
 * Parses a join code string and extracts structured information about it.
 *
 * Input shape: Malloy reports a join field's ``location.range`` starting
 * at the *alias* (not the ``join_one:`` / ``join_many:`` keyword), so the
 * joinCode strings parsed here look like:
 *
 *   "aircraft with tail_num"                          → { original_source_name: "aircraft", is_nested_index: false }
 *   "origin is airports on a = b"                     → { original_source_name: "airports", is_nested_index: false }
 *   "a is duckdb.table('...') on ..."                 → { original_source_name: null,       is_nested_index: true  }  // direct-table
 *   "a is b extend { ... } on ..."                    → { original_source_name: "b",        is_nested_index: true  }  // inline extend
 *
 * Caller responsibility: ``joinCode`` MUST be the join field's actual
 * source text. The caller (``getEntitiesFromExplore``) uses
 * ``extractCodeFromEntity`` rather than ``getCode`` precisely so that
 * a field imported from another file gets read from its OWN file —
 * otherwise the line numbers in ``entity.location.range`` would be
 * interpreted against the wrong ``docString`` and this function would
 * be fed garbage (mis-parsing imported joins as direct-table joins and
 * emitting spurious ``<parent>.<join>::<dim>`` entities).
 */
export function parseJoinCode(
   joinCode: string,
   fieldName: string,
   sourceToModelPath: Map<string, string>,
): JoinSourceMetadata {
   // "alias is sourceName" pattern. Group 1 = alias, group 2 = source name.
   const isMatch = joinCode.match(/\b(\w+)\s+is\s+(\w+)/);

   if (!isMatch) {
      // No "is" keyword - this is a simple join like "join_one: aircraft with tail_num"
      // The original source name is the same as the field name
      return {
         original_source_name: fieldName,
         original_source_path: sourceToModelPath.get(fieldName) ?? null,
         is_nested_index: false,
      };
   }

   const extractedSource = isMatch[2];

   // Check for direct table join: source name followed by a dot
   // e.g., "duckdb.table(...)" or "duckdb . table(...)"
   const dotPattern = new RegExp(`\\b${extractedSource}\\s*\\.`);
   const isDirectTableJoin = dotPattern.test(joinCode);

   // Check for extend/include blocks
   const hasExtendOrInclude = /\b(extend|include)\s*\{/.test(joinCode);

   // Needs nested indexing if it's a direct table join or uses extend/include
   const isNestedIndex = isDirectTableJoin || hasExtendOrInclude;

   // Look up the path for the original source
   const originalSourceName = isDirectTableJoin ? null : extractedSource;
   const originalSourcePath = originalSourceName
      ? (sourceToModelPath.get(originalSourceName) ?? null)
      : null;

   return {
      original_source_name: originalSourceName,
      original_source_path: originalSourcePath,
      is_nested_index: isNestedIndex,
   };
}

/**
 * Returns entities for a single explore's fields.
 * @param parentPath - The dot-separated path of parent explores (e.g., "flights.origin")
 * @param sourceToModelPath - Map from source name to relative model file path
 * @param dimIndexFingerprints - Lookup from ``${sourceName}::${fieldName}`` to
 *   the definitional fingerprint (compiled-SQL hash + required-filter set)
 *   reported on ``#(index)``-tagged dimension entities. Fields without an
 *   entry report no fingerprint and keep their per-run ``randomUUID()``.
 */
function getEntitiesFromExplore(
   explore: MalloyExplore,
   docString: string,
   relativePath: string,
   parentPath: string | null,
   sourceToModelPath: Map<string, string>,
   dimIndexFingerprints: ReadonlyMap<string, DimIndexFingerprintInputs>,
   topLevelExploreNames: ReadonlySet<string> = new Set(),
   nonQueryableSourceNames: ReadonlySet<string> = new Set(),
): Entity[] {
   const entities: Entity[] = [];
   let non_public_entities_count = 0;

   const sourceName = composeSourceName(parentPath, explore.name);
   // The runnable source at the head of this (possibly dotted) walk —
   // ``top`` for ``top.mid.deep``. Used below to decide whether re-rooting a
   // non-queryable join target is worthwhile (only when the head is itself a
   // queryable, declared source).
   const rootSourceName = sourceName.split(".")[0];
   const rootIsQueryable = !nonQueryableSourceNames.has(rootSourceName);

   for (const field of explore.allFields) {
      // The access-modifier check must run BEFORE the join recursion
      // below. Otherwise an internal/private inline-defined join (e.g.
      // `join_one: a is duckdb.table('foo') extend { ... }`) would have
      // its entire nested schema indexed even though the join itself is
      // not accessible from outside the declaring source/model file.
      if (getAccessModifier(field) !== AccessModifierEnum.Public) {
         non_public_entities_count++;
         continue;
      }

      let joinMetadata: JoinSourceMetadata | null = null;
      if (field.isExploreField()) {
         // Parse the join to understand what kind it is. Use the
         // file-aware ``extractCodeFromEntity`` (not ``getCode``) so a
         // field imported from another file is read from its OWN file
         // rather than slicing line numbers out of the current
         // ``docString`` (which would mis-parse imported joins as
         // direct-table joins and spuriously recurse the sanitizer).
         const joinCode = extractCodeFromEntity(field, docString);
         joinMetadata = parseJoinCode(joinCode, field.name, sourceToModelPath);

         // Recurse into the joined source when its fields are otherwise
         // unreachable from the top-level walk:
         //
         //   - ``is_nested_index`` covers the explicit cases (extend{},
         //     direct ``conn.table(...)`` joins) — the joined source has
         //     no separate top-level definition, so the only path to its
         //     fields is through the join.
         //
         //   - The new check (``original_source_name`` not in
         //     ``topLevelExploreNames``) catches a deep import chain:
         //     ``bridge_model`` joins ``order_items``, but Malloy's
         //     transitive-import rules mean ``order_items`` never reaches
         //     the entry's ``Model.explores`` for a deep import chain.
         //     If it's not at top level, the join is the only path —
         //     recurse so its fields land in the index under a dotted
         //     source path (``bridge_model.order_items.Created_at``).
         const targetName = joinMetadata.original_source_name;
         const targetIsAtTopLevel =
            targetName != null && topLevelExploreNames.has(targetName);

         //   - The third check (``targetIsNonQueryable`` under a queryable
         //     root) re-roots dimensions of a source outside the package's
         //     declared query surface. Such a source is normally indexed via
         //     its own top-level walk, so we'd skip the join — but an indexer
         //     cannot sample its dimensions (a direct ``run: <source> -> …``
         //     404s), so the ONLY path to a dimension index is through a
         //     declared source that joins it.
         //     Recurse so its ``#(index)`` dims get a copy rooted at the
         //     queryable parent (``order_items.inventory_items.product_category``
         //     → source=``order_items``). Gated on ``rootIsQueryable`` so a
         //     non-queryable head doesn't spawn entities that would only be
         //     dropped again — and so the bridge/deep-import fan-out doesn't
         //     re-walk every non-queryable subtree once per non-queryable head.
         const targetIsNonQueryable =
            targetName != null && nonQueryableSourceNames.has(targetName);

         if (
            joinMetadata.is_nested_index ||
            !targetIsAtTopLevel ||
            (targetIsNonQueryable && rootIsQueryable)
         ) {
            const nestedEntities = getEntitiesFromExplore(
               field as unknown as MalloyExplore,
               docString,
               relativePath,
               sourceName,
               sourceToModelPath,
               dimIndexFingerprints,
               topLevelExploreNames,
               nonQueryableSourceNames,
            );
            entities.push(...nestedEntities);
         }
      }

      // When we're inside a nested-index join chain, `sourceName` is a dotted
      // path (e.g. "principals.people") which is NOT a runnable Malloy source.
      // For LEAF fields (not joins) split it into the top-level runnable source
      // (first segment) and a join-qualified field path (rest + field name), so
      // the entity stores `source="principals"`, `name="people.primaryName"`.
      // That pair is a valid `run: <source> -> { ... `<name>` }` and matches
      // the (sourceName, dimension) form a caller filters or looks values up
      // by. JOIN entities (field.isExploreField()) keep the dotted
      // `sourceName` unchanged — a host's join-edge topology depends on it.
      let entitySource = sourceName;
      let nameOverride: string | null = null;
      if (parentPath && !field.isExploreField()) {
         const { rootSourceName, joinPath } = splitSourcePath(sourceName);
         entitySource = rootSourceName;
         nameOverride = `${joinPath}.${field.name}`;
      }

      const codeEntity: Entity = getEntityFromMalloyField(
         field,
         entitySource,
         relativePath,
         docString,
         explore.sourceStructDef?.connection ?? null,
         explore.sourceStructDef && "tablePath" in explore.sourceStructDef
            ? (explore.sourceStructDef.tablePath ?? null)
            : null,
         joinMetadata,
         nameOverride,
      );
      // ``#(index)``-tagged DIMENSION fields carry a definitional
      // ``dim_index_fingerprint`` (the compiled-SQL hash + required-filter
      // set). A host that indexes the package can derive a cross-version-stable
      // id from it in place of this per-run id, so an unchanged dimension
      // maps onto the same index row across versions and source extensions.
      // Every other field (and dim-index candidates whose fragment failed to
      // compile) keeps its per-run ``randomUUID()`` and reports no fingerprint.
      codeEntity.id = randomUUID();
      const fingerprint = dimIndexFingerprints.get(
         makeDimIndexLookupKey(sourceName, field.name),
      );
      if (fingerprint) {
         codeEntity.dim_index_fingerprint = {
            sql_fingerprint: fingerprint.sqlFingerprint,
            required_filter_dimensions: fingerprint.requiredFilterDimensions,
         };
      }
      // ``#(index)``-tagged dimensions carry their declared per-dimension
      // persistence policy (``refresh`` / ``freshness``) parsed off the
      // annotation — the dimension-grain analog of
      // ``PersistSourcePlan.{refresh,freshness}`` on the entities channel. Scope
      // is package-level (``Package.scope``), so per-dimension ``sharing`` /
      // ``schedule`` are no longer part of the policy (rejected at publish).
      // Absent when the dimension declares no policy (it is opt-in), so a
      // bare ``#(index)`` reports nothing. Reported verbatim for the host that
      // builds the index to schedule and display.
      const indexPolicy = parseIndexPolicy(field.getTaglines?.() ?? []);
      if (indexPolicy) {
         codeEntity.index_policy = indexPolicy;
      }
      entities.push(codeEntity);
   }

   logger.info(
      `Source: ${sourceName} - Non-public entities skipped: ${non_public_entities_count}`,
   );
   return entities;
}

/*
 * Extracts documentation string from an explore's block note annotations.
 * Looks for annotations starting with "#(doc)" and combines them into a single string.
 *
 * @param explore - The explore to extract documentation from
 * @returns The combined documentation string, or null if no doc annotations found
 */
const extractDocsFromExplore = (explore: MalloyExplore): string | null => {
   // Use only notes GENUINELY declared on this source (location-filtered via
   // getDirectTaglines) — NOT the base's notes copied onto an extending source.
   // Malloy copies a base's annotation verbatim onto a child that adds none of
   // its own, so reading `structDef.annotations.blockNotes` blindly would make
   // `source: child is base extend {}` inherit `base`'s `#(doc)`. This is the
   // same guard `#(agent-hidden)` already uses (see getDirectTaglines).
   //
   // Each doc note contributes a line: a single `#|(doc)` block carries its own
   // newlines; the legacy form repeated on consecutive lines arrives as separate
   // notes we join with `\n`. Both forms yield the same multi-line description.
   const parts = getDirectTaglines(explore)
      .map((text) => parseDocAnnotation(text))
      .filter((doc): doc is string => !!doc && doc.length > 0);

   return parts.length > 0 ? parts.join("\n") : null;
};

function normalizeParameterDefaultValue(
   value: unknown,
   sourceName: string,
   parameterName: string,
): string | null {
   if (value === null || value === undefined) {
      return null;
   }
   if (typeof value !== "object") {
      return String(value);
   }

   const nodeValue = value as {
      node?: unknown;
      literal?: unknown;
   };
   const node = nodeValue.node;
   if (node === "stringLiteral" || node === "numberLiteral") {
      return nodeValue.literal === undefined ? null : String(nodeValue.literal);
   }
   if (node === "true" || node === "false") {
      return String(node);
   }

   const serialized = stableStringify(value);
   logger.warn(
      `[INDEXER] Unknown parameter default value shape for ${sourceName}.${parameterName}: ${serialized}`,
   );
   return serialized;
}

const extractParametersFromExplore = (
   explore: MalloyExplore,
): SourceParameter[] => {
   const params = (
      explore.structDef as { parameters?: Record<string, unknown> }
   )?.parameters;
   if (!params) {
      return [];
   }

   return Object.entries(params).map(([name, paramValue]) => {
      const param = (paramValue ?? {}) as {
         type?: unknown;
         value?: unknown;
      };
      return {
         name,
         type: param.type == null ? null : String(param.type),
         defaultValue: normalizeParameterDefaultValue(
            param.value,
            explore.name,
            name,
         ),
      };
   });
};

/**
 * Extracts source metadata from a list of explores.
 *
 * @param explores - The list of explores from the current package version
 * @param relativePath - The relative path of the model file
 * @returns The list of source metadata entries from the explores
 */
/**
 * Parses `#(filter)` annotations on a source. Filters whose `dimension`
 * doesn't exist on the source are dropped with a warning.
 */
const extractFiltersFromExplore = (
   explore: MalloyExplore,
   relativePath: string,
): Filter[] => {
   const fieldsByName = new Map(
      (explore.allFields ?? []).map((f) => [f.name, f] as const),
   );
   const filters: Filter[] = [];
   // Required filters map 1:1 to dim-index partition keys, which feed a
   // `group_by`. Two `#(filter ... required)` annotations on the same
   // dimension would emit `group_by: tier, tier` and break the compile with a
   // cryptic Malloy error; reject the duplicate here with a clear message
   // instead. Enforced per source (one explore).
   const requiredDimensionNames = new Set<string>();
   for (const tagline of explore.getTaglines()) {
      let parsed: Filter | null;
      try {
         parsed = parseFilterAnnotation(tagline.trim());
      } catch (err) {
         logger.warn(
            `[filters] Failed to parse filter annotation on source '${explore.name}' in ${relativePath}: ${errorMessage(err)} — annotation: ${tagline.trim()}`,
         );
         continue;
      }
      if (!parsed) continue;
      const dimensionField = fieldsByName.get(parsed.dimension!);
      if (!dimensionField) {
         if (parsed.required) {
            throw new RequiredFilterMissingDimensionError(
               `${relativePath}: filter '${parsed.name}' on source '${explore.name}' is marked required but references unknown dimension '${parsed.dimension}'.`,
            );
         }
         logger.warn(
            `[filters] Filter '${parsed.name}' on source '${explore.name}' in ${relativePath} references unknown dimension '${parsed.dimension}' — skipping.`,
         );
         continue;
      }
      // A filter's dimension must be publicly accessible. Required filters
      // become dim-index partition keys (a `group_by` in the fingerprint
      // query) and every filter is applied in queries issued against the
      // source's public surface, so an internal/private dimension fails at
      // index and/or query time. Reject a required one up front; drop an
      // optional one with a warning (mirrors the unknown-dimension handling).
      if (getAccessModifier(dimensionField) !== AccessModifierEnum.Public) {
         if (parsed.required) {
            throw new RequiredFilterNonPublicDimensionError(
               `${relativePath}: filter '${parsed.name}' on source '${explore.name}' is marked required but references non-public dimension '${parsed.dimension}'. A required filter must reference a public dimension.`,
            );
         }
         logger.warn(
            `[filters] Filter '${parsed.name}' on source '${explore.name}' in ${relativePath} references non-public dimension '${parsed.dimension}' — skipping.`,
         );
         continue;
      }
      if (parsed.required) {
         if (requiredDimensionNames.has(parsed.dimension!)) {
            throw new DuplicateRequiredFilterError(
               `${relativePath}: source '${explore.name}' declares multiple required filters on dimension '${parsed.dimension}'. A dimension may have at most one required filter.`,
            );
         }
         requiredDimensionNames.add(parsed.dimension!);
      }
      filters.push(parsed);
   }
   return filters;
};

export class RequiredFilterMissingDimensionError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "RequiredFilterMissingDimensionError";
   }
}

/**
 * Thrown when a `#(filter ... required)` annotation references a dimension that
 * exists but is not public (internal/private). A required filter maps to a
 * dim-index partition key and is applied in queries issued against the source's
 * public surface, so a non-public dimension fails at index and/or query time;
 * refuse the package load (an `EntitiesValidationError`, a 400) rather than
 * produce an unusable index.
 */
export class RequiredFilterNonPublicDimensionError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "RequiredFilterNonPublicDimensionError";
   }
}

/** Names of givens referenced in an authorize expression, extracted from
 *  `$NAME` tokens. Matches any-case Malloy identifiers (`[A-Za-z_][A-Za-z0-9_]*`):
 *  customer user attributes (e.g. a LookML `user_attribute: brandName`) are
 *  commonly lower/mixed-case, so the registry must support them, not just
 *  ALL_CAPS. A `$word` inside a string literal can be over-captured, but that is
 *  harmless: a captured name with no matching given declaration has no type, so
 *  a consumer joining these names to the model's givens drops it. */
const extractAuthorizeGivenNames = (expression: string): string[] => {
   const seen = new Set<string>();
   const out: string[] = [];
   for (const match of expression.matchAll(/\$([A-Za-z_][A-Za-z0-9_]*)/g)) {
      const name = match[1];
      if (name && !seen.has(name)) {
         seen.add(name);
         out.push(name);
      }
   }
   return out;
};

/** File-level `##(authorize)` / `##(access_filter)` gates declared on a model.
 *  Read from the model's own taglines (not walked through extends/imports): a
 *  file-level annotation belongs to the model where the source is declared.
 *  Order preserves declaration order. This server refuses a file-level gate at
 *  load (../authorize.ts), so this reports what an author wrote rather than
 *  anything that will gate a query. */
const extractAuthorizeFromModel = (
   model: MalloyModel,
   relativePath: string,
): GateExpr[] => {
   const out: GateExpr[] = [];
   for (const tagline of model.getTaglines()) {
      try {
         const gate = parseGateAnnotation(tagline);
         if (gate !== null) out.push(gate);
      } catch (err) {
         // Skip-and-warn rather than throw: a malformed gate must not drop
         // every source in the file from the index (the model compiled
         // fine). Same best-effort contract as filter parsing.
         logger.warn(
            `[authorize] Failed to parse file-level annotation in ${relativePath}: ${errorMessage(err)} — annotation: ${tagline.trim()}`,
         );
      }
   }
   return out;
};

/** Effective `Authorize` entries for one source: file-level gates (passed in)
 *  first, then this source's own, authorize route before access_filter.
 *
 *  Both call sites run the same `resolveGates` derivation — own notes,
 *  `inherits`, a query-derived struct's base/composite branch, one walk per route
 *  — and both fail closed on a resolution failure. They differ only in the
 *  own-notes step: the entry point reads them raw, while a join call site supplies
 *  its own pre-resolved taglines (`extractJoinAuthorizeMetadata` location-filters
 *  them per tag-kind) and passes them through as the override. A join-only source
 *  is the sole producer of its own row, so anything the join branch skips is
 *  reported ungated with no second pass to catch it. Joins themselves are
 *  still not walked (entry-point-only).
 *
 *  `terms` rides on an access_filter entry only when the whole source's row filter
 *  reduces to queryable `field <op> $GIVEN` terms; see `accessFilterTermsForGates`. */
const buildAuthorizeForExplore = (
   explore: MalloyExplore,
   modelAuthorize: GateExpr[],
   relativePath: string,
   ownTaglines?: string[],
   sourceDefsByName: ReadonlyMap<string, GateStructDef> = new Map(),
   modelGivens: ApiGiven[] = [],
): Authorize[] => {
   const structDef = (explore.structDef as GateStructDef | undefined) ?? {};
   let gates: GateExpr[] = [];
   let composite = false;
   let effectiveModelAuthorize = modelAuthorize;
   const deny = () => {
      gates = [{ route: "authorize", expr: "false" }];
      composite = false;
      effectiveModelAuthorize = [];
   };

   let sawMalformedOwnTagline = false;
   for (const tagline of ownTaglines ?? []) {
      try {
         parseGateAnnotation(tagline);
      } catch (err) {
         sawMalformedOwnTagline = true;
         logger.warn(
            `[authorize] Failed to parse annotation on source '${explore.name}' in ${relativePath}: ${errorMessage(err)} — annotation: ${tagline.trim()}`,
         );
      }
   }
   if (sawMalformedOwnTagline) {
      // Any own tagline that failed to parse denies, even if a sibling tagline
      // parsed fine — a partial parse is not a trustworthy gate.
      deny();
   } else {
      try {
         ({ gates, composite } = resolveGates(
            structDef,
            sourceDefsByName,
            ownTaglines,
         ));
      } catch (err) {
         // A resolution we could not complete is not "no gate": deny, and drop
         // the file-level prefix.
         logger.warn(
            `[authorize] Failed to resolve the access gates for source '${explore.name}' in ${relativePath}: ${errorMessage(err)} — failing closed (deny)`,
         );
         deny();
      }
   }

   const { terms, withheld } = accessFilterTermsForGates({
      gates,
      composite,
      explore,
      givenTypes: givenTypeMap(modelGivens),
   });
   if (withheld) {
      logger.info(
         `[authorize] ${relativePath}: source '${explore.name}' access_filter terms withheld (${withheld})`,
      );
   }
   return [
      ...effectiveModelAuthorize.map((gate) => ({ gate, terms: undefined })),
      ...gates.map((gate, i) => ({ gate, terms: terms[i] })),
   ].map(({ gate, terms: entryTerms }) => ({
      expression: gate.expr,
      given_names: extractAuthorizeGivenNames(gate.expr),
      route: gate.route,
      ...(entryTerms ? { terms: entryTerms } : {}),
   }));
};

/** A non-blocking advisory: a joined (child) source carries an access gate the
 *  parent source's own gate set does not cover — a query run against the parent
 *  can reach the child's data via the join without passing the child's gate.
 *  Route-blind on purpose: the query path gates the ENTRY POINT only and does not
 *  trace joins, so the bypass is the same whether the child's gate is an
 *  `#(authorize)` lock or an `#(access_filter)`. */
export interface AuthorizeJoinBypassAdvisory {
   parentSourceName: string;
   childSourceName: string;
   reason: string;
}

/** Stable identity for an `Authorize` entry when comparing gate sets —
 *  same expression AND same referenced given names (order-independent). */
const authorizeIdentity = (a: Authorize): string =>
   `${a.expression}::${[...a.given_names].sort().join(",")}`;

/** Advisory check (NON-blocking): fires once per join edge where the joined
 *  (child) source carries at least one gate and the parent's
 *  gate set is not a superset of the child's — i.e. the join is a path
 *  around the child's own gate. Mirrors `checkAuthorizeGivensSecure`'s shape
 *  and posture: never fails publish, server-logged only, returned here so
 *  it's unit-testable. */
const checkAuthorizeJoinBypass = (
   parentAuthorize: Authorize[],
   childAuthorize: Authorize[],
   parentSourceName: string,
   childSourceName: string,
): AuthorizeJoinBypassAdvisory[] => {
   // An ungated child has nothing to bypass — intended, not a missed case.
   if (childAuthorize.length === 0) return [];
   // Gates from different declaring sources AND, so the parent is safe only
   // when it carries every gate the child does. The reverse (parent ⊆ child)
   // is what OR alone would want, and it silently passes the bypass this
   // advisory exists to catch: parent gated on ROLE, child on ROLE and
   // TENANT_ID, reached through the join without TENANT_ID.
   //
   // No intra-source OR spoils the comparison. A source may now carry several
   // notes — repeats on one route, and a lock beside a filter — but every one
   // of them ANDs, and file-level `##(...)` fails the model load on both routes
   // (see `parseGateAnnotation` for why this module does not enforce that
   // itself). Were an OR'd pair ever to reach this list, the error would be a
   // spurious advisory rather than a silent one.
   const parentGates = new Set(parentAuthorize.map(authorizeIdentity));
   const isSafe = childAuthorize.every((gate) =>
      parentGates.has(authorizeIdentity(gate)),
   );
   if (isSafe) return [];
   return [
      {
         parentSourceName,
         childSourceName,
         reason:
            `joined source '${childSourceName}' carries access gate(s) not ` +
            `enforced by parent '${parentSourceName}' — a query against the parent ` +
            `can reach '${childSourceName}' without passing its gate`,
      },
   ];
};

/** Givens a fronting proxy sets from the caller's verified identity (GROUPS,
 *  ORG_ROLES) or its own configuration (CELL_ID), rather than accepting them
 *  from the caller. They are trusted independent of any marker, so none is
 *  declared `#(secure)` and the advisory must not flag them. */
const ROUTER_SOURCED_GIVENS: ReadonlySet<string> = new Set([
   "GROUPS",
   "ORG_ROLES",
   "CELL_ID",
]);

/** A non-blocking advisory: an `#(authorize)` gate references a given that
 *  won't be enforced as a server-set value by middleware that resolves/strips
 *  secure givens. */
export interface AuthorizeGivenAdvisory {
   sourceName: string;
   givenName: string;
   reason: string;
}

/** Advisory check (NON-blocking) over a source's `#(authorize)` gates.
 *  `#(authorize)` is usable over ANY given, so this must NOT require any
 *  middleware concept — it never fails the publish. It only flags gates that
 *  would be caller-forgeable *under middleware that resolves/strips secure
 *  givens*: there a gate is a real boundary only if its given is trusted, which
 *  needs BOTH the `#(secure)` marker AND a set-valued (`[]`) type, because such
 *  middleware trusts only set-valued secure givens. Rule per referenced given:
 *    - declared, secure, set-valued (`[]`) → ok (no advisory)
 *    - declared but not secure              → advise (un-marked)
 *    - declared + secure but scalar         → advise (never registered)
 *    - GROUPS / ORG_ROLES / CELL_ID         → ok (proxy-set, set-valued, trusted)
 *    - not a declared given                 → skip (the `$NAME` regex over-captures
 *      `$word` from string literals; an undeclared given fails compile elsewhere).
 *  Callers log these. A `where:` over an un-marked given is NOT flagged — that is a
 *  legitimate caller-supplied query filter. */
const checkAuthorizeGivensSecure = (
   givens: ApiGiven[],
   authorize: Authorize[],
   sourceName: string,
): AuthorizeGivenAdvisory[] => {
   const byName = new Map(givens.map((g) => [g.name, g] as const));
   const advisories: AuthorizeGivenAdvisory[] = [];
   for (const gate of authorize) {
      for (const name of gate.given_names) {
         if (ROUTER_SOURCED_GIVENS.has(name)) continue; // proxy-set, set-valued, trusted
         const given = byName.get(name);
         if (!given) continue; // undeclared → not our concern (regex over-capture)
         const isSecure = given.secure === true;
         const isMultiValue = (given.type ?? "").endsWith("[]");
         if (isSecure && isMultiValue) continue;
         const reason = !isSecure
            ? "not marked #(secure)"
            : `marked #(secure) but scalar ('${given.type}'); secure-given middleware trusts only set-valued secure givens`;
         advisories.push({ sourceName, givenName: name, reason });
      }
   }
   return advisories;
};

/** Convert each `Model.givens` entry into the wire `Given` shape — same
 *  contract this server emits on `Source.givens` so an agent reading metadata
 *  via either path sees the identical schema. The compiled Malloy `Given`
 *  class doesn't expose the rendered default text on its typed surface; the
 *  IR `_internal.defaultText` is the source literal captured at declaration
 *  (e.g. `'WN'`, `2003`, `@2024-01-01`, `f'WN'`), reached via a localised
 *  cast — widening the duck type would collide with the class's own private
 *  `_internal`. */
const extractGivensFromModel = (model: MalloyModel): ApiGiven[] => {
   const out: ApiGiven[] = [];
   for (const given of model.givens.values()) {
      const t = given.type;
      // Render an array given as `<elem>[]` (e.g. `string[]`) — the multi-value
      // convention the trusted-attribute system keys on (Malloy's raw type is
      // just `"array"`, which would be mis-read as scalar). Mirrors the `::`
      // authoring syntax.
      const renderedType =
         t.type === "filter expression"
            ? `filter<${t.filterType}>`
            : t.type === "array"
              ? `${t.elementTypeDef.type}[]`
              : t.type;
      const annotations = given.annotations
         .forRoute(undefined)
         .filter((note) => !isReservedRoute(note.route))
         .map((note) => note.text);
      // Secure-given marker: `#(secure)` on the declaration — its own route,
      // empty content. This is the trust marker — middleware that resolves
      // secure givens treats it as server-resolved and strips it from callers. Presence on the
      // route is the signal; no content to match.
      const secure = given.annotations.forRoute("secure").length > 0;
      // The `Given` class declares `_internal` as private, so we cast through
      // `unknown` to read the IR's pre-rendered default text — the source
      // literal (`'WN'`, `2003`, `@2024-01-01`, `f'WN'`). Mirrors the
      // publisher SDK's accessor exactly.
      const defaultText = (
         given as unknown as {
            _internal?: { defaultText?: string };
         }
      )._internal?.defaultText;
      const entry: ApiGiven = {
         name: given.name,
         type: renderedType,
      };
      if (annotations.length > 0) entry.annotations = annotations;
      if (secure) entry.secure = true;
      if (defaultText !== undefined) entry.default = defaultText;
      out.push(entry);
   }
   return out;
};

/**
 * Thrown when a single source declares more than one `#(filter ... required)`
 * annotation on the same dimension. Such a duplicate would produce a
 * `group_by: dim, dim` in the dim-index fingerprint query and fail the compile;
 * we reject it up front with an actionable message.
 */
export class DuplicateRequiredFilterError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "DuplicateRequiredFilterError";
   }
}

/**
 * Recurses into join fields the top-level metadata walk (below) never sees —
 * mirrors `getEntitiesFromExplore`'s join recursion predicate exactly — and
 * emits an AUTHORIZE row (+ runs the givens-secure and join-bypass advisories)
 * for each join-reachable source, whether or not it's otherwise unreachable.
 *
 * Two things happen per join field:
 *   - The join-bypass advisory always runs (it's meaningful even for a join
 *     target that also has its own top-level metadata row).
 *   - An AUTHORIZE row is only emitted — and the walk only descends further —
 *     when the target is unreachable from the top-level walk, mirroring
 *     `getEntitiesFromExplore`'s `shouldRecurse` predicate (`is_nested_index`,
 *     not top-level, or a non-queryable target under a queryable root).
 *
 * Emission key: an indexer keys rows by (model file, source), and looks a
 * join-only source up by the key below, so the row MUST be emitted under it
 * or it is unreachable:
 *   - `is_nested_index` (inline extend / direct-table): dotted
 *     `${parent}.${alias}`, keyed under `original_source_path ?? relativePath`.
 *   - named join (deep import): bare `original_source_name`, same file
 *     fallback. A direct-table join (`original_source_name: null`) has no
 *     separate named source but is still keyed under the dotted
 *     `is_nested_index` path when it carries a gate (see the inline note below).
 *
 * `modelAuthorize` is only passed to a nested source's own
 * `buildAuthorizeForExplore` call when that source is declared in THIS same
 * model file (`keyModelPath === relativePath`) — mirroring the top-level
 * rule (`definedInThisModelFile ? modelAuthorize : []`). A file-level gate
 * must not leak across an import boundary onto a join-only source from
 * another file, but a same-file child is subject to this file's gate same as
 * any other source defined here.
 */
function extractJoinAuthorizeMetadata(
   explore: MalloyExplore,
   docString: string,
   relativePath: string,
   parentPath: string | null,
   parentAuthorize: Authorize[],
   sourceToModelPath: Map<string, string>,
   topLevelExploreNames: ReadonlySet<string>,
   nonQueryableSourceNames: ReadonlySet<string>,
   modelGivens: ApiGiven[],
   modelAuthorize: GateExpr[],
   seenSourceKeys: Set<string>,
   sourceDefsByName: ReadonlyMap<string, GateStructDef>,
): SourceMetadata[] {
   const metadataEntries: SourceMetadata[] = [];
   const sourceName = composeSourceName(parentPath, explore.name);
   const rootSourceName = sourceName.split(".")[0];
   const rootIsQueryable = !nonQueryableSourceNames.has(rootSourceName);

   for (const field of explore.allFields ?? []) {
      // Some callers construct minimal field stubs (e.g. the FILTERS tests'
      // `{ name: "tier" }`) that don't implement `isExploreField` at all —
      // guard the call rather than assume every field is a real Malloy Field.
      if (
         typeof field.isExploreField !== "function" ||
         !field.isExploreField()
      ) {
         continue;
      }
      if (getAccessModifier(field) !== AccessModifierEnum.Public) continue;

      const joinCode = extractCodeFromEntity(field, docString);
      const joinMetadata = parseJoinCode(
         joinCode,
         field.name,
         sourceToModelPath,
      );

      // Direct-table joins (`a is duckdb.table(...)`) have no separate named
      // source declared elsewhere — `targetName` is null — but the join
      // alias itself CAN still carry its own `#(authorize)` (declared
      // directly above the `join_one:`/`join_many:` line, same file,
      // contiguous — `getDirectTaglines` reads it like any other direct
      // annotation). The emission key for ANY
      // `is_nested_index` target is dotted `${parent}.${alias}` regardless
      // of whether `original_source_name` is null, so a direct-table join's
      // gate must still get a row under that key — it is NOT skipped.
      const targetName = joinMetadata.original_source_name;
      const targetIsAtTopLevel =
         targetName != null && topLevelExploreNames.has(targetName);
      const targetIsNonQueryable =
         targetName != null && nonQueryableSourceNames.has(targetName);
      const shouldRecurse =
         joinMetadata.is_nested_index ||
         !targetIsAtTopLevel ||
         (targetIsNonQueryable && rootIsQueryable);

      const joinedExplore = field as unknown as MalloyExplore;
      // A named join (`is_nested_index === false`, e.g. deep-import
      // `join_many: order_items on ...`) has no `extend` in between — the
      // field's structDef IS the joined source's own struct, copied
      // verbatim, so every attached note is the child's own gate and must
      // be read raw (`getRawTaglines`), not through the extend/inherits
      // contiguity filter `getDirectTaglines` applies (see its rationale).
      // An `extend`-derived or direct-table join still needs that
      // base/own distinction, so it keeps the filtered taglines.
      const childTaglines = joinMetadata.is_nested_index
         ? getDirectTaglines(joinedExplore)
         : getRawTaglines(joinedExplore);
      // Same rule the top-level walk applies (`definedInThisModelFile ?
      // modelAuthorize : []`): a file-level gate is a property of this
      // file's text and must not leak across an import boundary onto a
      // join-only source from ANOTHER file, but a child actually declared in
      // THIS file (`keyModelPath === relativePath`) is subject to this
      // file's `##(authorize)` same as any other source defined here.
      const keyModelPath = joinMetadata.original_source_path ?? relativePath;
      const definedInThisModelFile = keyModelPath === relativePath;
      const childAuthorize = buildAuthorizeForExplore(
         joinedExplore,
         definedInThisModelFile ? modelAuthorize : [],
         relativePath,
         childTaglines,
         sourceDefsByName,
         modelGivens,
      );
      // `targetName` is only ever null when `is_nested_index` is true
      // (the direct-table case — see `parseJoinCode`), so the non-nested
      // branch always has a real name; the fallback is type-safety only.
      const childSourceName = joinMetadata.is_nested_index
         ? `${sourceName}.${field.name}`
         : (targetName ?? field.name);

      for (const advisory of checkAuthorizeJoinBypass(
         parentAuthorize,
         childAuthorize,
         sourceName,
         childSourceName,
      )) {
         logger.warn(
            `[acl-advisory] ${relativePath}: join '${advisory.parentSourceName}' -> '${advisory.childSourceName}': ${advisory.reason}`,
         );
      }

      if (!shouldRecurse) continue;

      const dedupKey = `${keyModelPath}::${childSourceName}`;
      if (seenSourceKeys.has(dedupKey)) continue;
      seenSourceKeys.add(dedupKey);

      for (const a of checkAuthorizeGivensSecure(
         modelGivens,
         childAuthorize,
         childSourceName,
      )) {
         logger.warn(
            `[acl-advisory] ${relativePath}: source '${a.sourceName}' #(authorize) gate references given '${a.givenName}' (${a.reason}); under middleware that resolves/strips secure givens this gate is caller-forgeable — mark it #(secure) and set-valued (\`:: T[]\`) if it is meant as an access boundary.`,
         );
      }

      if (childAuthorize.length > 0) {
         metadataEntries.push({
            modelFilePath: keyModelPath,
            sourceName: childSourceName,
            metadata: "",
            metadataType: "AUTHORIZE",
            authorize: childAuthorize,
         });
      }

      // Mirror the top-level GIVENS emission (`extractMetadataFromExplores`
      // above): a join-only source needs the same row so a lookup of its
      // givens by (model file, source) isn't left empty.
      if (modelGivens.length > 0) {
         metadataEntries.push({
            modelFilePath: keyModelPath,
            sourceName: childSourceName,
            metadata: "",
            metadataType: "GIVENS",
            givens: modelGivens,
         });
      }

      metadataEntries.push(
         ...extractJoinAuthorizeMetadata(
            joinedExplore,
            docString,
            relativePath,
            sourceName,
            childAuthorize,
            sourceToModelPath,
            topLevelExploreNames,
            nonQueryableSourceNames,
            modelGivens,
            modelAuthorize,
            seenSourceKeys,
            sourceDefsByName,
         ),
      );
   }

   return metadataEntries;
}

const extractMetadataFromExplores = (
   explores: MalloyExplore[],
   relativePath: string,
   isModelFileAgentHidden = false,
   modelFileUrl?: string,
   modelGivens: ApiGiven[] = [],
   modelAuthorize: GateExpr[] = [],
   nonQueryableSourceNames: Set<string> = new Set(),
   docString = "",
   packageRoot = "",
   servedFromBySource: ReadonlyMap<
      string,
      NonNullable<SourceMetadata["servedFrom"]>
   > = new Map(),
): SourceMetadata[] => {
   const metadataEntries: SourceMetadata[] = [];
   const sourceToModelPath = buildSourceToModelPathMap(
      explores,
      relativePath,
      packageRoot,
   );
   const topLevelExploreNames = new Set(explores.map((e) => e.name));
   // The model's own top-level sources, keyed by name — the only lookup a
   // string `query_source` `structRef` can resolve against without a
   // `sourceRegistry`/`resolveSourceRef` deep import (see `resolveGates`).
   // Skip an explore with no structDef rather than mapping the name to
   // `undefined`: a present-but-undefined entry makes `get` miss and fabricates
   // a deny for a name that IS resolvable.
   const sourceDefsByName = new Map<string, GateStructDef>(
      explores
         .map(
            (e) => [e.name, e.structDef as GateStructDef | undefined] as const,
         )
         .filter((entry): entry is [string, GateStructDef] => !!entry[1]),
   );
   const seenJoinSourceKeys = new Set<string>();
   // Register every top-level source's own key up front: the top-level
   // loop below always emits its AUTHORIZE (etc.) row under `${relativePath}
   // ::${explore.name}` regardless of iteration order, so a join path
   // reaching the SAME (file, name) — e.g. a same-file child also joined
   // from a queryable root, whose `shouldRecurse` is forced true — must
   // never re-emit a duplicate row for it: a consumer that folds rows by
   // (modelFilePath, sourceName) last-write-wins would let a
   // duplicate join-path row clobber the correct top-level one. Doing
   // this as its own pass (rather than registering per-explore as the loop
   // below reaches each one) makes the dedup independent of `explores`'
   // declaration order.
   for (const explore of explores) {
      seenJoinSourceKeys.add(`${relativePath}::${explore.name}`);
   }

   if (isModelFileAgentHidden) {
      logger.info(
         `[agent-hidden] Model file level agent-hidden tag found on ${relativePath} — will be applied to all sources defined in this file. The tag will not be applied to sources imported from other model files.`,
      );
   }
   for (const explore of explores) {
      const docsString = extractDocsFromExplore(explore);
      const parameters = extractParametersFromExplore(explore);
      // Source-level #(agent-hidden) is detected only on annotations that
      // are directly attached to this explore. We deliberately do NOT use
      // `explore.getTaglines()` here because that walks the
      // `Annotation.inherits` chain, which would cause a source extending
      // a hidden base (e.g. `source: a is base_a extend { ... }`) to be
      // marked hidden too. The hidden status of the base should never
      // carry through `extend`.
      //
      // Imported sources still carry their own #(agent-hidden) because
      // the annotation is attached directly to the imported explore.
      //
      // Model-file-level ##(agent-hidden) only applies to sources
      // actually defined in this model file, not to ones pulled in via
      // import — enforced via the `definedInThisModelFile` check below.
      // Every bracket pair Malloy routes, not just `(` -- `#[agent-hidden]`
      // routes exactly as `#(agent-hidden)` does, and missing it leaves a
      // source the author meant to hide visible to agents. Same grammar fact
      // as AUTHORIZE_TAGLINE_RE above, same failing direction.
      const sourceLevelAgentHidden = getDirectTaglines(explore).some((t) =>
         /^#\|?\s*[([{<]?\s*agent-hidden\b/.test(t),
      );
      const definedInThisModelFile = modelFileUrl
         ? explore.location?.url === modelFileUrl
         : true;
      const agentHidden =
         sourceLevelAgentHidden ||
         (isModelFileAgentHidden && definedInThisModelFile);
      if (sourceLevelAgentHidden) {
         logger.info(
            `[agent-hidden] Source-level agent-hidden found on source '${explore.name}' (defined in ${explore.location?.url ?? "unknown"}) - applied to metadata for model file ${relativePath}.`,
         );
      }
      if (docsString || parameters.length > 0) {
         metadataEntries.push({
            modelFilePath: relativePath,
            sourceName: explore.name,
            metadata: docsString ?? "",
            metadataType: "DOCS",
            parameters,
            agentHidden,
         });
      }

      if (agentHidden) {
         metadataEntries.push({
            modelFilePath: relativePath,
            sourceName: explore.name,
            metadata: "",
            metadataType: "AGENT_HIDDEN",
            parameters,
            agentHidden,
         });
      }

      // Mark sources outside the package's declared query surface.
      // `queryable` defaults true, so — like AGENT_HIDDEN — we only emit a
      // marker for the non-queryable case; an indexer reads it to skip
      // dimension indexing for these non-query-target sources. Keyed by
      // (modelFilePath, sourceName) so the same source can be
      // queryable under a declared model file and not under another.
      // A report's import of a served source, or a source the report derives
      // from one, also says which served source stands for it
      // (query_surface.ts `servedFrom`), so a search scoped to the report can
      // return that source instead.
      if (nonQueryableSourceNames.has(explore.name)) {
         const servedFrom = servedFromBySource.get(explore.name);
         metadataEntries.push({
            modelFilePath: relativePath,
            sourceName: explore.name,
            metadata: "",
            metadataType: "NOT_QUERYABLE",
            parameters,
            ...(servedFrom ? { servedFrom } : {}),
         });
      }

      const filters = extractFiltersFromExplore(explore, relativePath);
      if (filters.length > 0) {
         metadataEntries.push({
            modelFilePath: relativePath,
            sourceName: explore.name,
            metadata: "",
            metadataType: "FILTERS",
            filters,
         });
      }

      // Emit GIVENS for every source reachable in this compile, not just
      // ones defined in this model file. Entities for imported sources
      // get indexed under the importing model's path (this loop's
      // `relativePath`), so the GIVENS row must use the same key or
      // a lookup by (model file, source) misses.
      // `Model.givens` is "every given this model surfaces" per the Malloy
      // SDK docs — when B imports A, B's givens already include the
      // givens A re-surfaces through the import, so attributing B's
      // givens to imported sources matches what a query under B's
      // namespace can actually accept. When A is compiled separately,
      // its own GIVENS row keyed under A's path is written too — same
      // source, different `(uri, source)` keys, no collision.
      //
      // Known gap: a selective import that pulls a source but not its
      // given (`import { src } from "./a.malloy"`) leaves `Model.givens`
      // empty for that name, so the imported source's GIVENS row under
      // B's path also misses it. Closing this needs per-source given
      // attribution via the IR (the query path reads `Source.givens`); the
      // Malloy SDK doesn't surface that on `Explore`.
      if (modelGivens.length > 0) {
         metadataEntries.push({
            modelFilePath: relativePath,
            sourceName: explore.name,
            metadata: "",
            metadataType: "GIVENS",
            givens: modelGivens,
         });
      }

      // Emit AUTHORIZE for every source carrying a gate. A file-level gate
      // (`##(authorize)`, passed in via `modelAuthorize`) is refused at load
      // and so gates nothing; it is still reported here, scoped to sources
      // actually defined in this model file — same scoping as `agent-hidden`
      // above via `definedInThisModelFile`: a file-level gate belongs to "the
      // model where the source is declared." Unlike GIVENS (a namespace property that
      // legitimately covers imported sources), a file-level gate is a property
      // of this file's text and must not leak onto sources imported from
      // another file. Source-level `#(authorize)` is read via
      // `resolveGates` and so travels with an imported source
      // as it should. Ordering: file-level first, then source-level. Keyed
      // under the importing model's path for the same
      // reason GIVENS is — entities for imported sources live under the
      // importer's path, so the AUTHORIZE row must share that key for a
      // `(model file, source)` lookup.
      //
      // Known gap (fail-open, follow-up): an imported source gated at
      // file-level in its *own* home file loses that gate here (this compile
      // only reads its own file taglines). Parallels the selective-import
      // GIVENS gap and needs per-source attribution via the IR (the query
      // path reads `Source.authorize`), which the SDK doesn't expose on `Explore`.
      const authorize = buildAuthorizeForExplore(
         explore,
         definedInThisModelFile ? modelAuthorize : [],
         relativePath,
         undefined,
         sourceDefsByName,
         modelGivens,
      );
      // Advisory (NON-blocking): warn when an #(authorize) gate references a
      // given that middleware resolving/stripping secure givens wouldn't
      // enforce — i.e. not #(secure)+set-valued. #(authorize) is usable over
      // any given, so we never fail the publish; the advisory is logged only.
      for (const a of checkAuthorizeGivensSecure(
         modelGivens,
         authorize,
         explore.name,
      )) {
         logger.warn(
            `[acl-advisory] ${relativePath}: source '${a.sourceName}' #(authorize) gate references given '${a.givenName}' (${a.reason}); under middleware that resolves/strips secure givens this gate is caller-forgeable — mark it #(secure) and set-valued (\`:: T[]\`) if it is meant as an access boundary.`,
         );
      }
      if (authorize.length > 0) {
         metadataEntries.push({
            modelFilePath: relativePath,
            sourceName: explore.name,
            metadata: "",
            metadataType: "AUTHORIZE",
            authorize,
         });
      }

      // Join-aware pass: a source reachable only via a join — a deep
      // transitive import, or an inline `extend`-join alias — never appears
      // in the top-level `explores` list this loop walks, so it would
      // otherwise get NO authorize metadata at all. Recurse the same way
      // `getEntitiesFromExplore` does for entities. The row is reported for a
      // host to look up by the join-only source's key; it is not rolled up onto
      // the queryable parent an agent actually queries.
      metadataEntries.push(
         ...extractJoinAuthorizeMetadata(
            explore,
            docString,
            relativePath,
            null,
            authorize,
            sourceToModelPath,
            topLevelExploreNames,
            nonQueryableSourceNames,
            modelGivens,
            modelAuthorize,
            seenJoinSourceKeys,
            sourceDefsByName,
         ),
      );
   }

   logger.info(
      `Model file: ${relativePath.split("/").pop()}: Extracted ${metadataEntries.length} metadata entries`,
   );
   return metadataEntries;
};

/**
 * Extracts entities from a list of explores.
 *
 * @param explores - The list of explores from the current package version
 * @param relativePath - The relative path of the model file
 * @param docString - The doc string of the model file
 * @returns The list of entities from the explores
 */
const extractEntitiesFromExplores = (
   explores: MalloyExplore[],
   relativePath: string,
   packageRoot: string,
   docString: string,
   dimIndexFingerprints: ReadonlyMap<
      string,
      DimIndexFingerprintInputs
   > = new Map(),
   nonQueryableSourceNames: ReadonlySet<string> = new Set(),
) => {
   const sourceToModelPath = buildSourceToModelPathMap(
      explores,
      relativePath,
      packageRoot,
   );
   // The set of source names that ``Malloy.compile`` exposed at the top
   // level of ``modelDef.contents`` for this entry. Used by
   // ``getEntitiesFromExplore`` to decide whether a join's target source
   // is reachable elsewhere in this compile pass. If it's not, the only
   // path to its fields is through the join, so we must recurse —
   // otherwise transitively-imported joined sources are missing from the
   // entities even though queries against them resolve cleanly.
   const topLevelExploreNames = new Set(explores.map((e) => e.name));
   const entities: Entity[] = [];
   for (const explore of explores) {
      const exploreEntities = getEntitiesFromExplore(
         explore,
         docString,
         relativePath,
         null,
         sourceToModelPath,
         dimIndexFingerprints,
         topLevelExploreNames,
         nonQueryableSourceNames,
      );
      entities.push(...exploreEntities);
   }
   logger.info(
      `Model file: ${relativePath.split("/").pop()}: Extracted ${entities.length} entities`,
   );
   return entities;
};

export const packageSanitizationService = {
   extractCodeFromEntity,
   extractMetadataFromExplores,
   extractEntitiesFromExplores,
   extractGivensFromModel,
   extractAuthorizeFromModel,
   checkAuthorizeGivensSecure,
   checkAuthorizeJoinBypass,
};
