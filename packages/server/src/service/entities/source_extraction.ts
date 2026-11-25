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
} from "./field_helpers";
import path from "path";
import { fileURLToPath } from "url";

import type {
   Authorize,
   Given as ApiGiven,
   SourceMetadata,
} from "./internal_types";

import type {
   Explore as MalloyExplore,
   Model as MalloyModel,
} from "@malloydata/malloy";

import { type Refuse, throwRefusal } from "./dim_index_discovery";
import { errMessage } from "../../utils";
import { marksAgentHidden } from "../annotation_reader";
import { extractCodeFromEntity } from "./model_text";
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
import {
   BoundaryDescents,
   composeSourceName,
   guardDescent,
   guardHolds,
   type JoinDescent,
   type JoinDescentRule,
   type QueryabilityGuard,
   splitSourcePath,
} from "./explore_walk";
import { sourceFilters } from "./filters";
import type { Filter } from "./internal_types";
import { type MalloyGiven, malloyGivenToApi } from "../given";

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
 * Whether the extraction reports a public join's fields under the join's dotted
 * path, rather than only under the joined source's own top-level walk. The
 * `#(index)` candidate walk asks the same question ({@link joinDescent}), so it
 * never fingerprints a dimension the extraction does not emit.
 */
function descendsIntoJoin(
   joinMetadata: JoinSourceMetadata,
   rootSourceName: string,
   topLevelExploreNames: ReadonlySet<string>,
   boundary: BoundaryDescents,
): JoinDescent {
   const targetName = joinMetadata.original_source_name;
   const targetIsAtTopLevel =
      targetName != null && topLevelExploreNames.has(targetName);

   //   - The third check (the target not queryable under a queryable
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
   //     Which sources are queryable is the query route's to say, so this
   //     descent is the boundary's to decide (`BoundaryDescents`): taken
   //     provisionally and guarded, or left untaken when nothing can be
   //     unqueryable.
   if (joinMetadata.is_nested_index || !targetIsAtTopLevel) return true;
   return boundary.descent(rootSourceName, targetName);
}

/**
 * The rule {@link getEntitiesFromExplore} descends into a public join by, for a
 * walk over the same file that must visit exactly the joins it reports.
 */
export function joinDescent(
   explores: MalloyExplore[],
   relativePath: string,
   packageRoot: string,
   docString: string,
   boundary: BoundaryDescents,
): JoinDescentRule {
   const sourceToModelPath = buildSourceToModelPathMap(
      explores,
      relativePath,
      packageRoot,
   );
   const topLevelExploreNames = new Set(explores.map((e) => e.name));
   return (join, parentSourceName) =>
      descendsIntoJoin(
         parseJoinCode(
            extractCodeFromEntity(join, docString),
            join.name,
            sourceToModelPath,
         ),
         parentSourceName.split(".")[0]!,
         topLevelExploreNames,
         boundary,
      );
}

/** An entity, and the guard on the descent that reached it, if any. */
export interface GuardedEntity {
   entity: Entity;
   guard?: QueryabilityGuard;
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
   topLevelExploreNames: ReadonlySet<string>,
   boundary: BoundaryDescents,
   guard: QueryabilityGuard | undefined,
): GuardedEntity[] {
   const entities: GuardedEntity[] = [];

   const sourceName = composeSourceName(parentPath, explore.name);
   // The runnable source at the head of this (possibly dotted) walk —
   // ``top`` for ``top.mid.deep``. Used below to decide whether re-rooting a
   // non-queryable join target is worthwhile (only when the head is itself a
   // queryable, declared source).
   const rootSourceName = sourceName.split(".")[0]!;

   for (const field of explore.allFields) {
      // The access-modifier check must run BEFORE the join recursion
      // below. Otherwise an internal/private inline-defined join (e.g.
      // `join_one: a is duckdb.table('foo') extend { ... }`) would have
      // its entire nested schema indexed even though the join itself is
      // not accessible from outside the declaring source/model file.
      if (getAccessModifier(field) !== AccessModifierEnum.Public) {
         continue;
      }

      let joinMetadata: JoinSourceMetadata | null = null;
      if (field.isExploreField()) {
         // Parse the join to understand what kind it is. Use the
         // file-aware ``extractCodeFromEntity`` (not ``getCode``) so a
         // field imported from another file is read from its OWN file
         // rather than slicing line numbers out of the current
         // ``docString`` (which would mis-parse imported joins as
         // direct-table joins and spuriously recurse the extraction).
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
         const descent = descendsIntoJoin(
            joinMetadata,
            rootSourceName,
            topLevelExploreNames,
            boundary,
         );
         if (descent !== false) {
            const nestedEntities = getEntitiesFromExplore(
               field as unknown as MalloyExplore,
               docString,
               relativePath,
               sourceName,
               sourceToModelPath,
               dimIndexFingerprints,
               topLevelExploreNames,
               boundary,
               descent === true
                  ? guard
                  : guardDescent(
                       guard,
                       rootSourceName,
                       descent.unlessQueryable,
                    ),
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
         docString,
         explore.sourceStructDef?.connection ?? null,
         explore.sourceStructDef && "tablePath" in explore.sourceStructDef
            ? (explore.sourceStructDef.tablePath ?? null)
            : null,
         joinMetadata,
         nameOverride,
         explore.sourceStructDef?.dialect ?? null,
      );
      // An ``#(index)``-tagged dimension carries its definitional fingerprint
      // (the compiled-SQL hash and required-filter set), from which a host can
      // derive an id that stays stable across versions. A candidate whose
      // fingerprint compile failed reports none.
      const fingerprint = dimIndexFingerprints.get(
         makeDimIndexLookupKey(sourceName, field.name),
      );
      if (fingerprint) {
         codeEntity.dim_index_fingerprint = {
            sql_fingerprint: fingerprint.sqlFingerprint,
            required_filter_dimensions: fingerprint.requiredFilterDimensions,
         };
      }
      entities.push(
         guard ? { entity: codeEntity, guard } : { entity: codeEntity },
      );
   }

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

/**
 * Extracts source metadata from a list of explores.
 *
 * @param explores - The list of explores from the current package version
 * @param relativePath - The relative path of the model file
 * @returns The list of source metadata entries from the explores
 */
/**
 * A source's `#(filter)`s whose dimension is a public field of it. An optional
 * one that is not is dropped with a warning; a required one is flagged.
 */
const extractFiltersFromExplore = (
   explore: MalloyExplore,
   relativePath: string,
   refuse: Refuse = throwRefusal,
): Filter[] => {
   const fieldsByName = new Map(
      (explore.allFields ?? []).map((f) => [f.name, f] as const),
   );
   let declared: Filter[];
   try {
      declared = sourceFilters(explore);
   } catch (err) {
      // The query path refuses the same annotation, so the source has no filters.
      logger.warn(
         `[filters] Failed to parse a filter annotation on source '${explore.name}' in ${relativePath}: ${errMessage(err)}`,
      );
      return [];
   }
   const filters: Filter[] = [];
   for (const parsed of declared) {
      const dimensionField = fieldsByName.get(parsed.dimension!);
      if (!dimensionField) {
         if (parsed.required) {
            refuse(
               new RequiredFilterMissingDimensionError(
                  `${relativePath}: filter '${parsed.name}' on source '${explore.name}' is marked required but references unknown dimension '${parsed.dimension}'. Fix: point the filter's dimension at a public dimension of '${explore.name}', or drop 'required'.`,
               ),
            );
            continue;
         }
         logger.warn(
            `[filters] Filter '${parsed.name}' on source '${explore.name}' in ${relativePath} references unknown dimension '${parsed.dimension}' — skipping.`,
         );
         continue;
      }
      // A filter's dimension must be public: every filter is applied in queries
      // against the source's public surface, and a required one is a dim-index
      // partition key. Flag a required one; drop an optional one with a warning.
      if (getAccessModifier(dimensionField) !== AccessModifierEnum.Public) {
         if (parsed.required) {
            refuse(
               new RequiredFilterNonPublicDimensionError(
                  `${relativePath}: filter '${parsed.name}' on source '${explore.name}' is marked required but references non-public dimension '${parsed.dimension}'. Fix: make '${parsed.dimension}' public, or point the filter at a public dimension.`,
               ),
            );
            continue;
         }
         logger.warn(
            `[filters] Filter '${parsed.name}' on source '${explore.name}' in ${relativePath} references non-public dimension '${parsed.dimension}' — skipping.`,
         );
         continue;
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
 * public surface, so a non-public dimension fails at index and/or query time.
 * The load reports it as a package warning and drops the filter.
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
            `[authorize] Failed to parse file-level annotation in ${relativePath}: ${errMessage(err)} — annotation: ${tagline.trim()}`,
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
            `[authorize] Failed to parse annotation on source '${explore.name}' in ${relativePath}: ${errMessage(err)} — annotation: ${tagline.trim()}`,
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
            `[authorize] Failed to resolve the access gates for source '${explore.name}' in ${relativePath}: ${errMessage(err)} — failing closed (deny)`,
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
      logger.debug(
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
 *  around the child's own gate. Never fails the load: it is logged only, and
 *  returned here so it is unit-testable. */
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
   // No intra-source OR spoils the comparison. A source may carry several
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

/** Convert each `Model.givens` entry into the wire `Given` shape — same
 *  contract this server emits on `Source.givens` so an agent reading metadata
 *  via either path sees the identical schema. The compiled Malloy `Given`
 *  class doesn't expose the rendered default text on its typed surface; the
 *  IR `_internal.defaultText` is the source literal captured at declaration
 *  (e.g. `'WN'`, `2003`, `@2024-01-01`, `f'WN'`), reached via a localised
 *  cast — widening the duck type would collide with the class's own private
 *  `_internal`. */
/** The model's givens, serialized as every other server route serializes them. */
const extractGivensFromModel = (model: MalloyModel): ApiGiven[] =>
   [...model.givens.values()].map((given) =>
      malloyGivenToApi(given as unknown as MalloyGiven),
   );

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
function planJoinAuthorizeMetadata(
   explore: MalloyExplore,
   docString: string,
   relativePath: string,
   parentPath: string | null,
   parentAuthorize: Authorize[],
   sourceToModelPath: Map<string, string>,
   topLevelExploreNames: ReadonlySet<string>,
   boundary: BoundaryDescents,
   modelGivens: ApiGiven[],
   modelAuthorize: GateExpr[],
   alwaysSeenSourceKeys: Set<string>,
   guarded: boolean,
   sourceDefsByName: ReadonlyMap<string, GateStructDef>,
): JoinMetadataPlan[] {
   const plan: JoinMetadataPlan[] = [];
   const sourceName = composeSourceName(parentPath, explore.name);
   const rootSourceName = sourceName.split(".")[0]!;

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
      // The entities walk's descent rule, so the two visit the same joins.
      const descent = descendsIntoJoin(
         joinMetadata,
         rootSourceName,
         topLevelExploreNames,
         boundary,
      );

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

      // Logged whenever this level is (resolveJoinMetadata).
      const advisories = checkAuthorizeJoinBypass(
         parentAuthorize,
         childAuthorize,
         sourceName,
         childSourceName,
      ).map(
         (advisory) =>
            `[acl-advisory] ${relativePath}: join '${advisory.parentSourceName}' -> '${advisory.childSourceName}': ${advisory.reason}`,
      );
      // A join with neither advisories nor a descent is left out.
      const entry: JoinMetadataPlan = {};
      if (advisories.length > 0) {
         entry.advisories = advisories;
         plan.push(entry);
      }

      if (descent === false) continue;

      // A key already claimed on every path is skipped on every path, and so
      // is its subtree. A key claimed under a guard is decided when the
      // guards are (resolveJoinMetadata), so the subtree is planned.
      const dedupKey = `${keyModelPath}::${childSourceName}`;
      if (alwaysSeenSourceKeys.has(dedupKey)) continue;
      const childGuarded = guarded || descent !== true;
      if (!childGuarded) alwaysSeenSourceKeys.add(dedupKey);

      const rows: SourceMetadata[] = [];
      if (childAuthorize.length > 0) {
         rows.push({
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
         rows.push({
            modelFilePath: keyModelPath,
            sourceName: childSourceName,
            metadata: "",
            metadataType: "GIVENS",
            givens: modelGivens,
         });
      }

      if (entry.advisories === undefined) plan.push(entry);
      entry.descend = {
         ...(descent !== true && { unlessQueryable: descent.unlessQueryable }),
         dedupKey,
         rows,
         children: planJoinAuthorizeMetadata(
            joinedExplore,
            docString,
            relativePath,
            sourceName,
            childAuthorize,
            sourceToModelPath,
            topLevelExploreNames,
            boundary,
            modelGivens,
            modelAuthorize,
            alwaysSeenSourceKeys,
            childGuarded,
            sourceDefsByName,
         ),
      };
   }

   return plan;
}

/**
 * One public join of a source, as the join-aware metadata walk planned it:
 * the advisories logged whenever its level is visited, and, when the walk
 * descends into it, the rows it adds and the joins below it. A descent taken
 * provisionally holds only while `unlessQueryable` is not queryable and the
 * walk's root is. `dedupKey` is the `(model file, source)` key the rows are
 * filed under, which only the first descent to reach it claims.
 */
export interface JoinMetadataPlan {
   advisories?: string[];
   descend?: {
      unlessQueryable?: string;
      dedupKey: string;
      rows: SourceMetadata[];
      children: JoinMetadataPlan[];
   };
}

/**
 * One top-level source's metadata as planned before its queryability is
 * known: the rows before its NOT_QUERYABLE marker, the rows after it, and its
 * join-aware rows. {@link resolveMetadataPlan} puts them in order.
 */
export interface ExploreMetadataPlan {
   sourceName: string;
   leading: SourceMetadata[];
   trailing: SourceMetadata[];
   joins: JoinMetadataPlan[];
}

/**
 * A file's source metadata, given which of its sources are not queryable
 * (`nonQueryableSourceNames`) and the served source each of those names, from
 * the plan {@link planMetadataFromExplores} made without knowing them.
 */
function resolveMetadataPlan(
   plan: readonly ExploreMetadataPlan[],
   relativePath: string,
   nonQueryableSourceNames: ReadonlySet<string>,
   servedFromBySource: ReadonlyMap<
      string,
      NonNullable<SourceMetadata["servedFrom"]>
   > = new Map(),
): SourceMetadata[] {
   const metadataEntries: SourceMetadata[] = [];
   // See planMetadataFromExplores: every top-level key is claimed up front.
   const seenSourceKeys = new Set(
      plan.map((explore) => `${relativePath}::${explore.sourceName}`),
   );
   for (const explore of plan) {
      metadataEntries.push(...explore.leading);
      // Mark sources outside the package's declared query surface.
      // `queryable` defaults true, so — like AGENT_HIDDEN — we only emit a
      // marker for the non-queryable case; an indexer reads it to skip
      // dimension indexing for these non-query-target sources. Keyed by
      // (modelFilePath, sourceName) so the same source can be
      // queryable under a declared model file and not under another.
      // A report's import of a served source, or a source the report derives
      // from one, also says which served source stands for it
      // (extract.ts `advertisedQueryability`), so a search scoped to the
      // report can return that source instead.
      if (nonQueryableSourceNames.has(explore.sourceName)) {
         const servedFrom = servedFromBySource.get(explore.sourceName);
         metadataEntries.push({
            modelFilePath: relativePath,
            sourceName: explore.sourceName,
            metadata: "",
            metadataType: "NOT_QUERYABLE",
            ...(servedFrom ? { servedFrom } : {}),
         });
      }
      metadataEntries.push(...explore.trailing);
      resolveJoinMetadata(
         explore.joins,
         nonQueryableSourceNames,
         !nonQueryableSourceNames.has(explore.sourceName),
         seenSourceKeys,
         metadataEntries,
      );
   }
   return metadataEntries;
}

/** Walk a join plan as the walk would have, with the queryability known. */
function resolveJoinMetadata(
   plan: readonly JoinMetadataPlan[],
   nonQueryableSourceNames: ReadonlySet<string>,
   rootIsQueryable: boolean,
   seenSourceKeys: Set<string>,
   out: SourceMetadata[],
): void {
   for (const entry of plan) {
      for (const advisory of entry.advisories ?? []) logger.warn(advisory);
      const descend = entry.descend;
      if (!descend) continue;
      if (
         descend.unlessQueryable !== undefined &&
         !(
            rootIsQueryable &&
            nonQueryableSourceNames.has(descend.unlessQueryable)
         )
      ) {
         continue;
      }
      if (seenSourceKeys.has(descend.dedupKey)) continue;
      seenSourceKeys.add(descend.dedupKey);
      out.push(...descend.rows);
      resolveJoinMetadata(
         descend.children,
         nonQueryableSourceNames,
         rootIsQueryable,
         seenSourceKeys,
         out,
      );
   }
}

/**
 * The source metadata of a file whose unqueryable sources are
 * `nonQueryableSourceNames`, each naming the served source in
 * `servedFromBySource` where it has one.
 */
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
   refuse: Refuse = throwRefusal,
): SourceMetadata[] =>
   resolveMetadataPlan(
      planMetadataFromExplores(
         explores,
         relativePath,
         isModelFileAgentHidden,
         modelFileUrl,
         modelGivens,
         modelAuthorize,
         docString,
         packageRoot,
         refuse,
         new BoundaryDescents(nonQueryableSourceNames.size > 0),
      ),
      relativePath,
      nonQueryableSourceNames,
      servedFromBySource,
   );

/**
 * Plan a file's source metadata before which of its sources are queryable is
 * known: every row but the NOT_QUERYABLE markers, with the descents that
 * depend on queryability decided by `boundary`. {@link resolveMetadataPlan}
 * finishes it once they are known.
 */
const planMetadataFromExplores = (
   explores: MalloyExplore[],
   relativePath: string,
   isModelFileAgentHidden: boolean,
   modelFileUrl: string | undefined,
   modelGivens: ApiGiven[],
   modelAuthorize: GateExpr[],
   docString: string,
   packageRoot: string,
   refuse: Refuse,
   boundary: BoundaryDescents,
): ExploreMetadataPlan[] => {
   const plan: ExploreMetadataPlan[] = [];
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
   // Registered here for the plan, and again where it is resolved.
   for (const explore of explores) {
      seenJoinSourceKeys.add(`${relativePath}::${explore.name}`);
   }

   if (isModelFileAgentHidden) {
      logger.debug(
         `[agent-hidden] Model file level agent-hidden tag found on ${relativePath} — will be applied to all sources defined in this file. The tag will not be applied to sources imported from other model files.`,
      );
   }
   for (const explore of explores) {
      const leading: SourceMetadata[] = [];
      const trailing: SourceMetadata[] = [];
      const docsString = extractDocsFromExplore(explore);
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
      // source the author meant to hide visible to agents.
      const sourceLevelAgentHidden = getDirectTaglines(explore).some((t) =>
         marksAgentHidden(t, "source"),
      );
      const definedInThisModelFile = modelFileUrl
         ? explore.location?.url === modelFileUrl
         : true;
      const agentHidden =
         sourceLevelAgentHidden ||
         (isModelFileAgentHidden && definedInThisModelFile);
      if (sourceLevelAgentHidden) {
         logger.debug(
            `[agent-hidden] Source-level agent-hidden found on source '${explore.name}' (defined in ${explore.location?.url ?? "unknown"}) - applied to metadata for model file ${relativePath}.`,
         );
      }
      if (docsString) {
         leading.push({
            modelFilePath: relativePath,
            sourceName: explore.name,
            metadata: docsString,
            metadataType: "DOCS",
            agentHidden,
         });
      }

      if (agentHidden) {
         leading.push({
            modelFilePath: relativePath,
            sourceName: explore.name,
            metadata: "",
            metadataType: "AGENT_HIDDEN",
            agentHidden,
         });
      }

      // The NOT_QUERYABLE marker goes here (resolveMetadataPlan).

      const filters = extractFiltersFromExplore(explore, relativePath, refuse);
      if (filters.length > 0) {
         trailing.push({
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
         trailing.push({
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
      if (authorize.length > 0) {
         trailing.push({
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
      const joins = planJoinAuthorizeMetadata(
         explore,
         docString,
         relativePath,
         null,
         authorize,
         sourceToModelPath,
         topLevelExploreNames,
         boundary,
         modelGivens,
         modelAuthorize,
         seenJoinSourceKeys,
         false,
         sourceDefsByName,
      );
      plan.push({ sourceName: explore.name, leading, trailing, joins });
   }

   return plan;
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
): Entity[] =>
   planEntitiesFromExplores(
      explores,
      relativePath,
      packageRoot,
      docString,
      dimIndexFingerprints,
      new BoundaryDescents(nonQueryableSourceNames.size > 0),
   )
      .filter(({ guard }) => guardHolds(guard, nonQueryableSourceNames))
      .map(({ entity }) => entity);

/**
 * The entities of a file's explores before which of its sources are queryable
 * is known: each with the guard on reaching it, the descents that depend on
 * queryability decided by `boundary`.
 */
const planEntitiesFromExplores = (
   explores: MalloyExplore[],
   relativePath: string,
   packageRoot: string,
   docString: string,
   dimIndexFingerprints: ReadonlyMap<string, DimIndexFingerprintInputs>,
   boundary: BoundaryDescents,
): GuardedEntity[] => {
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
   const entities: GuardedEntity[] = [];
   for (const explore of explores) {
      const exploreEntities = getEntitiesFromExplore(
         explore,
         docString,
         relativePath,
         null,
         sourceToModelPath,
         dimIndexFingerprints,
         topLevelExploreNames,
         boundary,
         undefined,
      );
      entities.push(...exploreEntities);
   }
   return entities;
};

export const sourceExtraction = {
   extractMetadataFromExplores,
   planMetadataFromExplores,
   resolveMetadataPlan,
   extractEntitiesFromExplores,
   planEntitiesFromExplores,
   extractGivensFromModel,
   joinDescent,
   extractAuthorizeFromModel,
   checkAuthorizeJoinBypass,
};
