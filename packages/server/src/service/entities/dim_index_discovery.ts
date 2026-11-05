// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Dim-index entity-id discovery helpers.
 *
 * Companion to {@link "./dim_index_entity_id"}: where that module
 * owns the compile-derived *inputs* to a stable id (the fingerprint
 * query + the hash of its compiled SQL), this module owns *discovery* —
 * finding the fields that need a fingerprint in the first place, and the
 * shape the lookup hits during extraction.
 *
 * Lives next to derivation rather than inside ``source_extraction``
 * because every export here is purely dim-index-shaped: filter
 * parsing, index-tag detection, lookup-key formatting, candidate
 * walking. The extraction only consumes these — it doesn't define
 * them.
 */

import {
   parseAccessFilterBody,
   resolveGates,
   type AccessFilterTerm,
   type GateExpr,
   type GateStructDef,
} from "./access_gates";
import { sourceFilters } from "./filters";
import { findForbiddenIndexPolicyKeys } from "./index_annotation_parsing";
import { walkExploreTree, type JoinDescentRule } from "./explore_walk";
import { getAccessModifier } from "./field_helpers";
import { AccessModifierEnum } from "./internal_types";
import type {
   Explore as MalloyExplore,
   Field as MalloyField,
} from "@malloydata/malloy";

/**
 * Lookup-key shape shared between the extraction (where we read it)
 * and ``extract.ts`` (where the map is populated). Extraction hands
 * the extraction a ``ReadonlyMap`` keyed by this function; the extraction's
 * ``getEntitiesFromExplore`` looks each field up via the same call,
 * so a ``get(key)`` miss is the signal that the field should keep
 * its per-run ``randomUUID()`` id.
 *
 * Source names already carry the parent-path prefix used elsewhere
 * (e.g. ``flights.origin`` for nested-join sources), so combining
 * them with the field name gives a globally-unique key inside one
 * model file.
 */
export function makeDimIndexLookupKey(
   sourceName: string,
   fieldName: string,
): string {
   return `${sourceName}::${fieldName}`;
}

/**
 * A single ``#(index)``-tagged DIMENSION declaration that needs a
 * derived dim-index entity-id. Caller compiles the fingerprint
 * fragment for each one and reports the resulting SQL hash plus this
 * candidate's ``requiredFilterDimensions`` as the entity's
 * ``dim_index_fingerprint``, from which a host derives a stable id.
 */
export interface DimIndexCandidate {
   sourceName: string;
   fieldName: string;
   /**
    * The dimension's partition-key columns: the root's `#(filter ... required)`
    * dimensions, then its `#(access_filter)` gate columns ({@link gateDimensions}).
    * Reported on the entity as the fingerprint's `required_filter_dimensions`.
    */
   requiredFilterDimensions: string[];
   /**
    * The subset of {@link requiredFilterDimensions} contributed by the root's
    * `#(access_filter)` terms (dotted field paths). Empty when the root has no
    * gate, or its gate can't be reduced to queryable terms.
    */
   gateDimensions: string[];
   /**
    * Names of the source's *required* parameters — declared parameters with no
    * default value (e.g. ``report_id::number``, ``p_period::string``). An
    * indexer issues its value query with no parameter bindings, so an indexed
    * dimension on a source carrying any required parameter can never be
    * queried. Drives {@link assertNoRequiredParametersOnIndexedDimension}.
    * Empty for the common case of a non-parameterized source.
    */
   requiredParameterNames: string[];
}

/** Why an ``#(index)`` tag was rejected at publish-time compile. */
export type DimIndexRejectionReason =
   | "multiple_required_filters"
   | "parameterized_source"
   | "not_a_dimension"
   | "schedule_not_allowed_on_dimension"
   | "sharing_not_allowed_on_dimension";

/**
 * Base for the load-time ``#(index)`` placement rejections. {@link reason} is
 * the short code the load reports each refusal under.
 */
export abstract class DimIndexRejectionError extends Error {
   abstract readonly reason: DimIndexRejectionReason;
}

/**
 * Where an extraction reports an annotation the index refuses. The default
 * throws; the load passes one that collects, so it can name every refused
 * declaration in one 400 instead of the first per file.
 */
export type Refuse = (error: Error) => void;

export const throwRefusal: Refuse = (error) => {
   throw error;
};

/**
 * Thrown when a single ``#(index)``-tagged dimension would be partitioned by
 * more than one author-declared required ``#(filter)``. The index maps the
 * required filter to a partition key and supports exactly one, so >1 on an
 * indexed dimension is unrepresentable and must refuse the package load (an
 * ``EntitiesValidationError``, a 400) rather than produce a wrong index.
 * Gate columns derived from ``#(access_filter)`` are not counted.
 */
export class MultipleRequiredFiltersOnIndexedDimensionError extends DimIndexRejectionError {
   readonly reason = "multiple_required_filters" as const;
   constructor(message: string) {
      super(message);
      this.name = "MultipleRequiredFiltersOnIndexedDimensionError";
   }
}

/**
 * Enforce that no ``#(index)`` dimension declares more than one required
 * ``#(filter)``. ``requiredFilterDimensions`` is sourced per-explore, so a
 * candidate carrying >1 means its source declares multiple required filters. The
 * root's ``#(access_filter)`` gate columns are partition keys too but are derived
 * from the gate, not declared by the author, so they are not counted.
 *
 * @throws {MultipleRequiredFiltersOnIndexedDimensionError}
 */
export function assertAtMostOneRequiredFilterPerIndexedDimension(
   candidates: DimIndexCandidate[],
   modelFilePath: string,
): void {
   for (const candidate of candidates) {
      const filters = candidate.requiredFilterDimensions.filter(
         (dimension) => !candidate.gateDimensions.includes(dimension),
      );
      if (filters.length > 1) {
         throw new MultipleRequiredFiltersOnIndexedDimensionError(
            `${modelFilePath}: indexed dimension '${candidate.sourceName}.${candidate.fieldName}' has ` +
               `${filters.length} required filters ` +
               `(${filters.join(", ")}); a #(index) dimension may declare at most one required filter. Fix: drop 'required' from all but one of them, or move the #(index) to a source that declares one.`,
         );
      }
   }
}

/**
 * Thrown when an ``#(index)``-tagged dimension belongs to a source that
 * declares one or more *required* parameters (parameters with no default
 * value, e.g. ``source: report(report_id::number) ...`` or
 * ``source: period_report(p_period::string, ...)``).
 *
 * An indexer discovers a source's indexed dimensions and issues a ``count()``
 * query to enumerate their distinct values — but it has no parameter values to
 * bind, so the query fails with "Argument not provided for required parameter
 * '<p>'" and the dimension can never be indexed. Such a package compiles fine
 * but is permanently un-indexable, so the package load refuses it (an
 * ``EntitiesValidationError``, a 400) rather than letting it wedge an
 * indexer with queries that fail forever.
 *
 * The fix for a model author is to give the parameter a default value (so the
 * count query can run) or remove the ``#(index)`` tag and use a ``#(filter)``
 * annotation for query-time scoping instead — the supported, indexable pattern.
 */
export class IndexedDimensionOnParameterizedSourceError extends DimIndexRejectionError {
   readonly reason = "parameterized_source" as const;
   constructor(message: string) {
      super(message);
      this.name = "IndexedDimensionOnParameterizedSourceError";
   }
}

/**
 * Enforce that no ``#(index)`` dimension belongs to a source with required
 * parameters. ``requiredParameterNames`` is sourced per-explore, so a candidate
 * carrying any name means its source is parameterized and this indexed
 * dimension's count query would fail for want of a parameter binding.
 *
 * @throws {IndexedDimensionOnParameterizedSourceError}
 */
export function assertNoRequiredParametersOnIndexedDimension(
   candidates: DimIndexCandidate[],
   modelFilePath: string,
): void {
   for (const candidate of candidates) {
      if (candidate.requiredParameterNames.length > 0) {
         throw new IndexedDimensionOnParameterizedSourceError(
            `${modelFilePath}: indexed dimension '${candidate.sourceName}.${candidate.fieldName}' ` +
               `belongs to a source with required parameter(s) ` +
               `(${candidate.requiredParameterNames.join(", ")}); an index's value query ` +
               `cannot supply source parameters, so a #(index) dimension may not be declared on a ` +
               `parameterized source. Give the parameter a default value, or remove the #(index) tag ` +
               `and use a #(filter) annotation for query-time scoping instead.`,
         );
      }
   }
}

/**
 * Thrown when an ``#(index)``-tagged field is not a dimension — a measure,
 * view/query, or join. A dimension index holds the *distinct values* of a
 * dimension; an aggregate/query/join has no value set to enumerate, so the tag
 * is meaningless. Without this check ``collectDimIndexCandidates`` silently
 * skips the field and the author gets no index and no error. Refuses the
 * package load (an ``EntitiesValidationError``, a 400) so the mistake is
 * caught at publish.
 */
export class IndexedFieldNotADimensionError extends DimIndexRejectionError {
   readonly reason = "not_a_dimension" as const;
   constructor(message: string) {
      super(message);
      this.name = "IndexedFieldNotADimensionError";
   }
}

/**
 * Validate that a *public* ``#(index)``-tagged field is an indexable
 * placement: a dimension (or column). Takes the field's already-resolved
 * classification rather than a ``Field`` so the rule is a pure function over
 * plain data — the AST extraction stays in {@link collectDimIndexCandidates}.
 *
 * Non-public fields never reach this assert: the tag is inert on any field
 * the outside world can't query (dimension or not), and — because Malloy has
 * no way to strip an inherited annotation — a child source that internalizes
 * a tagged base field (``internal: *``) carries the tag unavoidably. Those
 * fields are skipped instead of rejected.
 *
 * @throws {IndexedFieldNotADimensionError} the field is a measure/view/join.
 */
export function assertIndexedFieldIsDimension(args: {
   sourceName: string;
   fieldName: string;
   isDimension: boolean;
   modelFilePath: string;
}): void {
   if (!args.isDimension) {
      throw new IndexedFieldNotADimensionError(
         `${args.modelFilePath}: field '${args.sourceName}.${args.fieldName}' carries an ` +
            `#(index) tag but is not a dimension (it is a measure, view, or ` +
            `join); only dimension/column fields can be indexed. Remove the #(index) tag or move ` +
            `it to a dimension.`,
      );
   }
}

/**
 * Thrown when an ``#(index)``-tagged dimension declares a ``schedule=`` cron.
 * Scope and cadence are package-level (``Package.scope`` +
 * ``materialization.schedule``): a ``schedule`` is legal only at the package
 * level and only in ``scope: version`` mode, never per dimension. Refuses the
 * package load (an ``EntitiesValidationError``, a 400).
 */
export class IndexScheduleNotAllowedError extends DimIndexRejectionError {
   readonly reason = "schedule_not_allowed_on_dimension" as const;
   constructor(message: string) {
      super(message);
      this.name = "IndexScheduleNotAllowedError";
   }
}

/**
 * Thrown when an ``#(index)``-tagged dimension declares a ``sharing=`` value.
 * Scope is a package-level mode (``Package.scope``: ``version`` |
 * ``package``), so per-dimension ``sharing`` is unrepresentable and rejected at
 * publish (an ``EntitiesValidationError``, a 400).
 */
export class IndexSharingNotAllowedError extends DimIndexRejectionError {
   readonly reason = "sharing_not_allowed_on_dimension" as const;
   constructor(message: string) {
      super(message);
      this.name = "IndexSharingNotAllowedError";
   }
}

/**
 * Enforce that no ``#(index)`` dimension declares a per-dimension ``sharing`` or
 * ``schedule`` key: both are package-level (``Package.scope`` and
 * ``materialization.schedule``). Reads the field's raw taglines.
 *
 * @throws {IndexSharingNotAllowedError | IndexScheduleNotAllowedError}
 */
export function assertNoForbiddenIndexPolicyKeys(
   candidate: DimIndexCandidate,
   taglines: string[],
   modelFilePath: string,
): void {
   const forbidden = findForbiddenIndexPolicyKeys(taglines);
   if (forbidden.includes("sharing")) {
      throw new IndexSharingNotAllowedError(
         `${modelFilePath}: indexed dimension '${candidate.sourceName}.${candidate.fieldName}' ` +
            `declares sharing= on its #(index) annotation, but scope is package-level. Fix: remove ` +
            `sharing= from #(index) and set materialization.scope in publisher.json.`,
      );
   }
   if (forbidden.includes("schedule")) {
      throw new IndexScheduleNotAllowedError(
         `${modelFilePath}: indexed dimension '${candidate.sourceName}.${candidate.fieldName}' ` +
            `declares schedule= on its #(index) annotation; a schedule is package-level and ` +
            `scope: version only (materialization.schedule in publisher.json). ` +
            `Remove schedule= from #(index), or use freshness.window for a refresh objective.`,
      );
   }
}

// This file is the authority on which taglines opt a field into the index; keep
// the accepted #(index)/#(index_values) tag set in sync with
// index_annotation_parsing.ts's INDEX_TAG_BODY when adding a variant. A host that
// recognizes index tags itself must accept the same set.
//
// Parenthesis-only on purpose, unlike the gate and agent-hidden matchers, which
// cover all four bracket pairs Malloy routes. `#[index]` routes there too, so
// this misses it — but the two failures point opposite ways. Missing a gate or
// an agent-hidden tag exposes something the author meant to withhold; missing
// an index tag leaves a dimension UNindexed, so its values stay out of value
// search and the author sees their own index not working. Widening it is also
// three coordinated edits plus the parity test, and it changes which dimension
// values get indexed — a data-surface change that belongs in its own change.
const INDEX_TAG_PATTERN = /^#\s*\(\s*index(?:_values)?\b/;

/**
 * Returns true iff one of the field's taglines opts the field into
 * the dimension index (``#(index)`` or ``#(index_values) n=...``).
 *
 * Robust to whitespace inside the parens — Malloy authors write both
 * ``#(index)`` and ``#( index_values n=200 )`` in the wild.
 */
function hasIndexTag(tags: string[]): boolean {
   return tags.some((t) => INDEX_TAG_PATTERN.test(t));
}

/**
 * Pull required-filter dimension names off an explore's ``#(filter)``
 * annotations. We do this independently of ``extractFiltersFromExplore``
 * (which is keyed off the metadata path) because we need the names
 * here even for explores that don't contribute metadata entries.
 *
 * Returns an empty array for explores with no required filters.
 */
function requiredFilterDimensionsForExplore(explore: MalloyExplore): string[] {
   try {
      // A dimension appears once even when two filters require it (a range):
      // each is one partition key, and one group_by term.
      return [
         ...new Set(
            sourceFilters(explore)
               .filter((f) => f.required && f.dimension)
               .map((f) => f.dimension!),
         ),
      ];
   } catch {
      // A malformed annotation is logged by the extraction; the query path
      // reads no filters for the source either.
      return [];
   }
}

/**
 * Pull the names of an explore's *required* parameters — declared parameters
 * with no default value. Mirrors ``extractParametersFromExplore`` in the
 * extraction (same ``structDef.parameters`` shape) but keeps only the names we
 * need here and only for the required ones: a parameter is required iff its
 * ``value`` (the default) is null/undefined, exactly the normalization the
 * extraction applies. A source with any required parameter cannot have its
 * indexed dimensions enumerated by the dim-index count query.
 *
 * Returns an empty array for non-parameterized sources (the common case).
 */
function requiredParameterNamesForExplore(explore: MalloyExplore): string[] {
   const params = (
      explore.structDef as { parameters?: Record<string, unknown> }
   )?.parameters;
   if (!params) {
      return [];
   }
   const out: string[] = [];
   for (const [name, paramValue] of Object.entries(params)) {
      const param = (paramValue ?? {}) as { value?: unknown };
      if (param.value === null || param.value === undefined) {
         out.push(name);
      }
   }
   return out;
}

/** Name -> rendered type of the model's givens, for the access_filter arity check. */
export function givenTypeMap(
   givens: ReadonlyArray<{ name: string; type: string }>,
): ReadonlyMap<string, string> {
   return new Map(givens.map((g) => [g.name, g.type]));
}

/**
 * Whether `segments` names a public dimension/column reachable from `explore`
 * through public `join_one` joins, i.e. something `group_by` on the root can
 * select and this server's gate accepts.
 */
function isQueryableDimensionPath(
   explore: MalloyExplore,
   segments: string[],
): boolean {
   let current = explore;
   for (let i = 0; i < segments.length; i++) {
      const field = (current.allFields ?? []).find(
         (f) => f.name === segments[i],
      );
      if (!field || getAccessModifier(field) !== AccessModifierEnum.Public) {
         return false;
      }
      const isLast = i === segments.length - 1;
      if (field.isExploreField()) {
         if (isLast) return false;
         current = field as unknown as MalloyExplore;
         // Publisher refuses a gate path through a fan-out join at load.
         if (
            (current.structDef as { join?: string } | undefined)?.join !== "one"
         ) {
            return false;
         }
         continue;
      }
      return isLast && isDimensionField(field);
   }
   return false;
}

/** Why a source's `#(access_filter)` gate was not reduced to terms. */
export type GateTermsWithheldReason =
   | "composite"
   | "unparsed_gate"
   | "gate_column_unqueryable";

/**
 * The structured terms for each gate, parallel to `gates` (`undefined` = opaque).
 *
 * A source's row filter is trusted as terms all-or-nothing, so every
 * access_filter entry loses its terms when the source is composite-rooted (which
 * branch gates a query is unknown) or when any gate column is not a public
 * dimension on `explore`: a column an indexer can't `group_by`
 * can't be a partition key, so the source stays withheld rather than failing the
 * publish. A body that does not parse loses its own terms and so also stays
 * withheld.
 */
export function accessFilterTermsForGates(args: {
   gates: GateExpr[];
   composite: boolean;
   explore: MalloyExplore;
   givenTypes: ReadonlyMap<string, string>;
}): {
   terms: Array<AccessFilterTerm[] | undefined>;
   withheld?: GateTermsWithheldReason;
} {
   const { gates, composite, explore, givenTypes } = args;
   const opaque = gates.map(() => undefined);
   if (!gates.some((g) => g.route === "access_filter"))
      return { terms: opaque };
   if (composite) return { terms: opaque, withheld: "composite" };

   const terms = gates.map((g) => {
      if (g.route !== "access_filter") return undefined;
      const parsed = parseAccessFilterBody(g.expr, givenTypes);
      return parsed.ok ? parsed.terms : undefined;
   });
   const unqueryable = terms.some((entryTerms) =>
      entryTerms?.some(
         (t) => !isQueryableDimensionPath(explore, t.field_path.split(".")),
      ),
   );
   if (unqueryable) {
      return { terms: opaque, withheld: "gate_column_unqueryable" };
   }
   const unparsed = gates.some(
      (g, i) => g.route === "access_filter" && terms[i] === undefined,
   );
   return {
      terms,
      ...(unparsed ? { withheld: "unparsed_gate" as const } : {}),
   };
}

// The given types a gate term can key an index by. A host that matches a caller's
// givens against gate terms must treat the same set as scalar.
export const SCALAR_GIVEN_TYPES: ReadonlySet<string> = new Set([
   "number",
   "string",
   "boolean",
   "date",
   "timestamp",
]);

/** A gate keys a dimension only through `=` over a scalar-typed given. */
function isScalarEqualityTerm(
   term: AccessFilterTerm,
   givenTypes: ReadonlyMap<string, string>,
): boolean {
   const declared = givenTypes.get(term.given)?.trim().toLowerCase();
   return (
      term.op === "=" &&
      declared !== undefined &&
      SCALAR_GIVEN_TYPES.has(declared)
   );
}

/**
 * The root's gate columns (dotted field paths, declared order, deduplicated), or
 * `[]` when the root is not gated by access_filter, its gate can't be fully
 * reduced to queryable terms (see {@link accessFilterTermsForGates}), or any term
 * is not scalar equality: a list key part would leave rows unkeyable.
 */
function gateColumnsForRoot(
   rootExplore: MalloyExplore,
   sourceDefsByName: ReadonlyMap<string, GateStructDef>,
   givenTypes: ReadonlyMap<string, string>,
): { columns: string[]; withheld?: GateTermsWithheldReason } {
   let resolved;
   try {
      resolved = resolveGates(
         (rootExplore.structDef as GateStructDef | undefined) ?? {},
         sourceDefsByName,
      );
   } catch {
      // Unresolvable: the source's AUTHORIZE row denies, so it stays withheld.
      return { columns: [] };
   }
   const { terms, withheld } = accessFilterTermsForGates({
      gates: resolved.gates,
      composite: resolved.composite,
      explore: rootExplore,
      givenTypes,
   });
   if (withheld) return { columns: [], withheld };
   const allScalarEquality = terms.every((entryTerms) =>
      (entryTerms ?? []).every((t) => isScalarEqualityTerm(t, givenTypes)),
   );
   if (!allScalarEquality) return { columns: [] };
   const columns: string[] = [];
   for (const entryTerms of terms) {
      for (const term of entryTerms ?? []) {
         if (!columns.includes(term.field_path)) columns.push(term.field_path);
      }
   }
   return { columns };
}

/**
 * Mirrors {@link inferType} but specialized to the DIMENSION branch
 * so we don't pull a full ``EntityTypeEnum`` switch into the
 * candidate-walk. A DIMENSION is an atomic, non-aggregate, non-
 * query, Malloy-defined field; we can't easily replicate the full
 * ``isMalloyDefined`` check here without reading the source string,
 * so we accept a slight over-inclusion (raw COLUMNs that happen to
 * carry an ``#(index)`` tag will also be derived) — that's a
 * defensible behavior for anyone who deliberately tags a column for
 * indexing.
 */
function isDimensionField(field: MalloyField): boolean {
   return field.isAtomicField() && !field.isCalculation();
}

/**
 * Walk explores (including nested-join sub-sources) to collect every
 * ``#(index)``-tagged DIMENSION field that needs a derived id. Source
 * names follow the same composition rule the extraction uses (see
 * {@link composeSourceName}) so resolver lookups hit by
 * ``${sourceName}::${fieldName}``.
 */
export function collectDimIndexCandidates(
   explores: MalloyExplore[],
   modelFilePath: string,
   givenTypes: ReadonlyMap<string, string> = new Map(),
   refuse: Refuse = throwRefusal,
   descend?: JoinDescentRule,
): DimIndexCandidate[] {
   const candidates: DimIndexCandidate[] = [];
   // The lookup a string query_source base resolves against, as in extractMetadataFromExplores.
   const sourceDefsByName = new Map<string, GateStructDef>(
      explores
         .map(
            (e) => [e.name, e.structDef as GateStructDef | undefined] as const,
         )
         .filter((entry): entry is [string, GateStructDef] => !!entry[1]),
   );
   for (const rootExplore of explores) {
      // Required filters and parameters are attributed to the ROOT explore,
      // not the (possibly nested) explore declaring the field. Every query an
      // indexer issues runs against the runnable root (``run: <root> -> …``),
      // and a dimension's required filters are resolved by its entity's root
      // ``source`` — a nested source's own annotations never reach that query.
      // Deriving the fingerprint (or rejecting the publish) from the nested
      // explore's filters/parameters would therefore key off a query shape
      // that never runs: a nested source's required parameters are
      // already bound by the join expression, and its required filters are
      // not part of the partition grain.
      const declaredFilterDimensions =
         requiredFilterDimensionsForExplore(rootExplore);
      // A gate column already partitioned by a required filter is not repeated.
      const gate = gateColumnsForRoot(
         rootExplore,
         sourceDefsByName,
         givenTypes,
      );
      const gateDimensions = gate.columns.filter(
         (column) => !declaredFilterDimensions.includes(column),
      );
      const requiredFilterDimensions = [
         ...declaredFilterDimensions,
         ...gateDimensions,
      ];
      const requiredParameterNames =
         requiredParameterNamesForExplore(rootExplore);
      for (const { explore, sourceName } of walkExploreTree(
         [rootExplore],
         descend,
      )) {
         for (const field of explore.allFields) {
            const tags = field.getTaglines?.() ?? [];
            // A field is only our concern once it opts into the dimension
            // index. Untagged fields (the vast majority) are ignored,
            // never rejected.
            if (!hasIndexTag(tags)) continue;
            // The access check runs BEFORE the dimension assert: the tag is
            // inert on ANY non-public field (measure or dimension alike), and
            // an inherited tag internalized via `internal: *` can't be
            // stripped in Malloy, so a non-public placement must never fail
            // the load.
            if (getAccessModifier(field) !== AccessModifierEnum.Public) {
               continue;
            }
            const candidate: DimIndexCandidate = {
               sourceName,
               fieldName: field.name,
               requiredFilterDimensions,
               gateDimensions,
               requiredParameterNames,
            };
            try {
               // The field IS public and #(index)-tagged: it must be a
               // dimension/column. A measure/view/join is refused at load
               // rather than left with an index that never materializes.
               assertIndexedFieldIsDimension({
                  sourceName,
                  fieldName: field.name,
                  isDimension:
                     !field.isExploreField() && isDimensionField(field),
                  modelFilePath,
               });
               // Scope and schedule are package-level: refuse a per-dimension
               // sharing= or schedule= on the #(index) annotation.
               assertNoForbiddenIndexPolicyKeys(candidate, tags, modelFilePath);
            } catch (error) {
               if (!(error instanceof DimIndexRejectionError)) throw error;
               refuse(error);
               continue;
            }
            candidates.push(candidate);
         }
      }
   }
   return candidates;
}
