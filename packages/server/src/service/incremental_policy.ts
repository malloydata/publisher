import { components } from "../api";
import {
   describeDialects,
   INCREMENTAL_DIALECT_ALLOWLIST,
   MERGE_CAPABLE_DIALECTS,
} from "./incremental_apply";
import type {
   IncrementalDeclaration,
   ResolvedName,
} from "./incremental_declaration";

type ApiPackage = components["schemas"]["Package"];
type ApiPackageWarning = NonNullable<ApiPackage["warnings"]>[number];

/**
 * The publish gate for a source's INCREMENTAL declaration, plus its advisory
 * (non-blocking) findings.
 *
 * Split the same way the persistence-policy gate is split: STRICT at
 * publish/PATCH (a 400 whose body is these messages joined) and LOG-ONLY at
 * load, so a package published before these rules existed still loads and
 * serves. The advisories never block anything and ride the package's operator
 * warnings array.
 *
 * Why a gate at all, rather than letting the build path cope: `refresh=` is
 * ALREADY populated on published packages that nobody ever validated, because it
 * used to be inert metadata pass-through. The moment the build path reads the
 * mode, a package that says `refresh="incremental"` and nothing else becomes
 * silently incremental-eligible. These rules must therefore be live no later
 * than that read — and gates-first is the safe order, since a rejection changes
 * no behavior.
 *
 * Every rule reads the EFFECTIVE MERGED tag (see resolveIncrementalDeclaration):
 * a source that extends a persisted parent inherits the parent's declaration, so
 * asking "did this source declare it locally" would let an inherited
 * `refresh="incremental"` through ungated. That is also why several messages
 * offer `-watermark` as a fix: for an inheriting child, removing the key means
 * negating it.
 */

/** One source's inputs to the gate. */
export interface IncrementalPolicySource {
   sourceName: string;
   /** Package-relative model path, for the warning's `model` field. */
   modelPath?: string;
   /** The source's Malloy dialect name (`postgres`, `standardsql`, …). */
   dialect?: string;
   /** The source's `#@ persist storage=` destination, when it declares one. */
   storageDestination?: string;
   declaration: IncrementalDeclaration;
}

const MODE = `refresh="incremental"`;

/** `"a", "b"` for a message. */
function quoteList(names: string[]): string {
   return names.map((n) => `"${n}"`).join(", ");
}

/** The declared names that did not resolve to an output column. */
function unresolved(names: (ResolvedName | undefined)[]): ResolvedName[] {
   return names.filter(
      (n): n is ResolvedName => n !== undefined && n.kind === "unresolved",
   );
}

/** The declared names that resolved to an `aggregate:`-produced column. */
function aggregateBacked(names: (ResolvedName | undefined)[]): ResolvedName[] {
   return names.filter(
      (n): n is ResolvedName => n !== undefined && n.kind === "aggregate",
   );
}

function malformedMessage(
   sourceName: string,
   key: string,
   problem: string,
   detail: string | undefined,
): string {
   const where = `#@ persist source "${sourceName}"`;
   switch (problem) {
      case "array":
         return (
            `${where} declares ${key}=[…] as a tag ARRAY. Write ONE quoted ` +
            `string instead: ${key}="order_date"` +
            (key === "merge_key"
               ? `, and separate a compound key with commas inside that one ` +
                 `string — merge_key="order_id, region".`
               : `.`) +
            ` An array value is dropped from the annotation's scalar fields, so ` +
            `the key would otherwise read as undeclared.`
         );
      case "empty":
         return (
            `${where} declares ${key}="" (empty). Name an output dimension of ` +
            `the source, or remove the key (use -${key} to strip one inherited ` +
            `from a parent source).`
         );
      case "empty-entry":
         return (
            `${where} declares a merge_key= list with an empty entry (e.g. ` +
            `merge_key="order_id,,region"). Remove the stray comma — an empty ` +
            `entry would silently narrow row identity to fewer columns than ` +
            `intended.`
         );
      case "duplicate":
         return (
            `${where} declares merge_key= with "${detail}" repeated. List each ` +
            `dimension once.`
         );
      default:
         return `${where} declares an unusable ${key}= value.`;
   }
}

/**
 * Every incremental-declaration REJECTION for one package's sources, in source
 * order then rule order. A non-empty result fails a publish.
 */
export function incrementalPolicyRejections(
   sources: IncrementalPolicySource[],
): string[] {
   const rejections: string[] = [];
   for (const source of sources) {
      rejections.push(...rejectionsForSource(source));
   }
   return rejections;
}

function rejectionsForSource(source: IncrementalPolicySource): string[] {
   const { sourceName, declaration: d } = source;
   const where = `#@ persist source "${sourceName}"`;
   const out: string[] = [];

   // Rule 4: the mode itself must be one of the two known values. Checked first
   // because a misspelled mode is why the coherence rules below see no mode.
   if (d.invalidRefresh !== undefined) {
      out.push(
         `${where} declares refresh=${JSON.stringify(d.invalidRefresh)}, which ` +
            `is not a refresh mode. Use refresh="full" (the default: rebuild the ` +
            `whole table) or refresh="incremental" (advance it by a bounded ` +
            `delta). The value is case-sensitive.`,
      );
   }

   // Rule 5: malformed key VALUES. Detected on the raw tag entries, because the
   // scalar read drops a non-text value — without this the array form would
   // misdiagnose as rule 2's "no watermark declared".
   for (const m of d.malformed) {
      out.push(malformedMessage(sourceName, m.key, m.problem, m.detail));
   }

   // ── The coherence chain: merge_key ⇒ watermark ⇒ incremental ──────────
   // Each broken link gets its own message, because each has a different fix.

   // Rule 1: a key that only means something in incremental mode, on a source
   // that is not in it.
   if (!d.incremental && (d.declaredWatermark || d.declaredMergeKey)) {
      const keys = [
         d.declaredWatermark ? "watermark=" : undefined,
         d.declaredMergeKey ? "merge_key=" : undefined,
      ].filter((k): k is string => k !== undefined);
      out.push(
         `${where} declares ${keys.join(" and ")} but is not ${MODE}` +
            (d.refresh
               ? ` (it declares refresh=${JSON.stringify(d.refresh)})`
               : "") +
            `. These keys describe how an INCREMENTAL refresh advances the ` +
            `table and are ignored by a full rebuild. Either add ${MODE}, or ` +
            `remove the key — and if the key is inherited from a parent source, ` +
            `strip it with a negation (e.g. -watermark), since removing the ` +
            `parent's key is not an option from the child.`,
      );
   }

   // Rule 3: merge_key= without watermark=, in ANY mode. Distinct from rule 2 so
   // the message can state the whole chain rather than the generic "no
   // watermark" — a merge key alone is not half a declaration, it is the far end
   // of one.
   if (d.declaredMergeKey && !d.declaredWatermark) {
      out.push(
         `${where} declares merge_key= without watermark=. The declaration is a ` +
            `chain: merge_key= needs watermark=, and watermark= needs ${MODE}. ` +
            `merge_key= only says how to match a row the delta rewrites; ` +
            `watermark= is what BOUNDS the delta in the first place, so a merge ` +
            `key on its own has no range to apply. Declare all three, or none.`,
      );
   }

   // Rule 2: the mode without the one key it cannot work without.
   if (d.incremental && !d.declaredWatermark) {
      out.push(
         `${where} declares ${MODE} but no watermark=. An incremental refresh ` +
            `derives its range from a monotone non-decreasing output dimension, ` +
            `so name one: watermark="order_date". Without it there is no ` +
            `boundary to advance and nothing to bound the delta.`,
      );
   }

   // The remaining rules are about MACHINERY, so they only fire for a source
   // that actually asked to be incremental. On a non-incremental source rule 1
   // has already named the real problem, and piling on would bury it.
   if (!d.incremental) return out;

   // Rule 6: a dialect whose transactional delta apply the publisher has
   // proven. An allowlist rather than a capability probe for this phase: a
   // delta writes into a table that is already serving, so "we have not proven
   // this engine" must read as no.
   const dialect = source.dialect ?? "";
   if (!INCREMENTAL_DIALECT_ALLOWLIST.has(dialect)) {
      out.push(
         `${where} declares ${MODE}, which is not supported on dialect ` +
            `"${dialect || "unknown"}". Incremental refresh applies its delta to ` +
            `the live serving table, so it is enabled only where the publisher ` +
            `has proven transactional DML: ` +
            `${describeDialects(INCREMENTAL_DIALECT_ALLOWLIST)}. Use ` +
            `refresh="full" here; the supported set widens in a later release.`,
      );
   } else if (d.declaredMergeKey && !MERGE_CAPABLE_DIALECTS.has(dialect)) {
      // Rule 7: kept distinct from rule 6 even though the allowlist currently
      // subsumes it, so the two can widen independently — a dialect can gain
      // transactional DML long before it gains MERGE.
      out.push(
         `${where} declares merge_key=, which needs a MERGE statement that ` +
            `dialect "${dialect}" does not have. Remove merge_key= to replace ` +
            `the watermark range instead of merging by row identity.`,
      );
   }

   // Rule 13: `storage=` sends the table to a DuckDB/DuckLake destination, built
   // through a separate build session on a different engine than the one the
   // dialect rule above just cleared. The delta path applies its DML on the
   // SOURCE connection, so pairing the two would aim a Postgres or BigQuery
   // statement at a table that does not live there.
   if (source.storageDestination) {
      out.push(
         `${where} declares ${MODE} together with ` +
            `storage="${source.storageDestination}". A stored source is ` +
            `materialized into the storage destination's own engine, while an ` +
            `incremental delta is applied on the source warehouse — so the two ` +
            `cannot be combined yet. Drop one: refresh="full" keeps the storage ` +
            `destination, removing storage= keeps the incremental refresh in the ` +
            `source warehouse.`,
      );
   }

   // Rule 8: a name that is not a column of the compiled output schema — a typo,
   // a rename that moved on, or a derived dimension that compiles and queries but
   // is never materialized.
   const dangling = unresolved([d.watermark, ...d.mergeKeys]);
   if (dangling.length > 0) {
      out.push(
         `${where} declares ${quoteList(dangling.map((n) => n.name))} which ` +
            `${dangling.length === 1 ? "is not" : "are not"} ` +
            `${dangling.length === 1 ? "a column" : "columns"} of the source's ` +
            `materialized output. Available: ${quoteList(d.outputColumns)}. Note ` +
            `that a derived dimension: on a table-backed source is queryable but ` +
            `NOT materialized, so it cannot be a watermark or merge key.`,
      );
   }

   // Rule 11: a name that resolved to an aggregate-produced column. Read from
   // the query definition's field kinds, because on the output schema an
   // aggregate's column is indistinguishable from a dimension.
   const aggregates = aggregateBacked([d.watermark, ...d.mergeKeys]);
   if (aggregates.length > 0) {
      out.push(
         `${where} declares ${quoteList(aggregates.map((n) => n.name))} which ` +
            `${aggregates.length === 1 ? "is" : "are"} produced by aggregate: in ` +
            `the source's query. An aggregate is a measured VALUE of a row, not ` +
            `an identity or an arrival order: it changes when new input lands, so ` +
            `it can neither bound a delta nor match a row. Name a group_by: ` +
            `dimension instead.`,
      );
   }

   // Rule 10: nothing to build a `<` range from. Note that struct/array columns
   // never reach the output schema at all, so they land in rule 8 instead.
   if (
      d.watermark !== undefined &&
      d.watermark.kind !== "unresolved" &&
      !d.watermarkOrderable
   ) {
      out.push(
         `${where} declares watermark="${d.watermark.name}", of type ` +
            `${d.watermark.malloyType}, which has no ordering to build a range ` +
            `from. A watermark must be a date, timestamp, number or string ` +
            `dimension that only moves forward.`,
      );
   }

   // Rule 9: a merge_key= that repeats the watermark. Redundant at best, and
   // actively misleading: it opts the author into the WEAKER guarantee while they
   // believe they asked for the stronger one — a row restated with an advanced
   // watermark no longer matches its predecessor, so the merge inserts a
   // duplicate exactly where the author wanted it replaced.
   if (d.watermarkInMergeKeys && d.watermark !== undefined) {
      out.push(
         `${where} declares merge_key= including the watermark dimension ` +
            `"${d.watermark.name}". Row identity must be independent of the ` +
            `watermark: a row restated with a LATER watermark value would not ` +
            `match its predecessor, so it would be inserted alongside the old ` +
            `row rather than replacing it — the failure merge_key= exists to ` +
            `prevent. List only the identity dimensions.`,
      );
   }

   // Rule 12: any window field. Conservative on purpose, with the eventual
   // criterion stated so the message survives the narrowing.
   if (d.calculateFields.length > 0) {
      out.push(
         `${where} declares ${MODE} but computes ` +
            `${quoteList([...new Set(d.calculateFields)])} with calculate:. A ` +
            `window function is not supported on an incremental source yet: a ` +
            `delta recomputes only rows inside its range, so a frame that reaches ` +
            `outside that range assigns already-materialized rows values that ` +
            `later data changes, below the range start where no delta ever ` +
            `rewrites them. Frames strictly BACKWARD along the watermark ordering ` +
            `are stable and will become legal; every other frame will not. Use ` +
            `refresh="full", or move the window computation into the query that ` +
            `reads this source. (Plain non-additive aggregates like ` +
            `count_distinct are fine — a delta recomputes whole output rows from ` +
            `full input and never merges partial aggregates.)`,
      );
   }

   return out;
}

/**
 * Advisory findings: surfaced on the package's warnings array, never blocking.
 * Both are cases where the declaration is legal and does something the author
 * probably did not intend.
 */
export function incrementalPolicyAdvisories(
   sources: IncrementalPolicySource[],
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   for (const source of sources) {
      const { declaration: d } = source;
      const at = { model: source.modelPath ?? "", subject: source.sourceName };

      // An unknown key is passed through by design (a new persist directive must
      // not need a publisher change), which means a typo is passed through too.
      // This warning is the ONLY guard for a misspelled `merge_key=`, which
      // otherwise degrades silently to a keyless delta — the weaker strategy,
      // chosen by nobody.
      for (const key of d.unknownKeys) {
         warnings.push({
            ...at,
            message:
               `declares an unrecognized #@ persist key "${key}", which the ` +
               `publisher passes through untouched. If it is a typo the intended ` +
               `behavior is silently absent — note that a misspelled merge_key= ` +
               `leaves an incremental source applying its delta by range replace ` +
               `instead of by row identity.`,
         });
      }

      if (d.incremental && d.watermark !== undefined && !d.declaredMergeKey) {
         warnings.push({
            ...at,
            message:
               `is ${MODE} with no merge_key=, so each refresh REPLACES the ` +
               `watermark range it just computed. Two consequences worth ` +
               `checking: (a) a row that is later restated with an ADVANCED ` +
               `watermark value appears twice rather than replacing its ` +
               `predecessor, since the older row sits below the new range — ` +
               `declare merge_key= with the row's identity dimensions if that ` +
               `happens; (b) if the destination table is not partitioned or ` +
               `clustered on "${d.watermark.name}", the range DELETE may re-read ` +
               `the whole table on every run.`,
         });
      }
   }
   return warnings;
}
