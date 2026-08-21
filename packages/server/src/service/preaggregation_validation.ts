// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Publish-time validation of `#@ preaggregate` declarations (handoff Work item 4).
 *
 * ## Why this is strict, and why it scans places the feature never reads
 *
 * Pre-aggregation is invisible when it works: a query never names a rollup and
 * gets the same answer whether one is used or not. That is the point, and it is
 * also why a declaration that quietly does nothing is the worst outcome — the
 * author sees correct numbers, assumes the rollup is serving them, and finds out
 * from a bill. So every `#@ preaggregate` must either take effect or be refused
 * with a message naming the thing and the fix.
 *
 * That is why this module scans for annotations in places the synthesizer would
 * never look. `#@ preaggregate` on a dimension, a join, a view, or a source all
 * compile silently today and are all visible in the IR — verified against the
 * pinned compiler. A synthesizer that only reads measures would skip them
 * without a word, which is exactly the failure above. Finding a misplaced
 * annotation is therefore a rejection, not a shrug.
 *
 * ## Where this runs
 *
 * Same gate pattern as the rest of persistence: strict at publish/PATCH, and —
 * per the settled deviation from the design doc's "warn" — strict at load too,
 * so a package that would be refused at publish cannot serve by having been
 * published earlier. Callers turn a non-empty result into a 400 (publish) or a
 * load failure.
 *
 * Pure: takes compiled IR, returns findings, throws nothing, touches no I/O.
 */

import {
   expressionIsAggregate,
   expressionIsAnalytic,
   isJoined,
} from "@malloydata/malloy";
import {
   readPreaggregateAnnotation,
   type AnnotatableMeasure,
} from "./preaggregation_annotation";
import { classifyMeasureAdditivity } from "./preaggregation_classifier";

/**
 * Why a declaration is refused. Stable identifiers so callers can count and
 * group them; the paired `message` is what a human acts on.
 */
export type PreaggregateViolationCode =
   // The annotation is somewhere it can never take effect.
   | "misplaced_on_source"
   | "misplaced_on_dimension"
   | "misplaced_on_join"
   | "misplaced_on_view"
   // The declaration itself is unusable.
   | "missing_grain"
   | "empty_grain"
   | "invalid_namespace"
   | "conflicting_namespace"
   | "non_additive_measure"
   // The grain does not resolve against the base source.
   | "unknown_grain_dimension"
   | "grain_dimension_is_measure"
   | "grain_dimension_is_view"
   | "grain_dimension_is_join"
   | "grain_path_through_join"
   | "grain_truncation_expression"
   | "truncation_on_non_temporal"
   // v1 scope restrictions.
   | "base_source_has_fanout_join";

export interface PreaggregateViolation {
   code: PreaggregateViolationCode;
   /** The source the declaration was found on. */
   sourceName: string;
   /** The field the annotation sits on, absent for a source-level annotation. */
   fieldName?: string;
   /** Names the offending object and the fix; becomes user-facing error text. */
   message: string;
}

/** The shape of a compiled field this module reads. */
interface FieldLike {
   name: string;
   as?: string;
   type?: string;
   join?: string;
   expressionType?: string;
   annotations?: unknown;
}

/** The shape of a compiled source this module reads. */
export interface ValidatableSource {
   type?: string;
   annotations?: unknown;
   fields?: FieldLike[];
}

const fieldName = (f: FieldLike): string => f.as ?? f.name;

/**
 * The temporal field types, which are the only ones a `.unit` truncation can
 * apply to. Written out rather than imported: Malloy declares `isTemporalType`
 * in its types but does not re-export it at runtime, and importing it fails at
 * load rather than at compile.
 */
function isTemporalType(type: string): boolean {
   return type === "date" || type === "timestamp" || type === "timestamptz";
}

/** A view/turtle. Views are dropped from composites, so they can never carry one. */
function isView(f: FieldLike): boolean {
   return f.type === "turtle";
}

/**
 * A join that can multiply rows: `join_many`, `join_cross`, and a repeated record,
 * which the IR also spells `join: "many"`. A `join_one` is excluded because it
 * cannot duplicate a row, which is the whole basis of the restriction.
 *
 * Malloy does not *enforce* the declared cardinality — a `join_one` on a
 * non-unique key really does fan out at the warehouse — so this trusts the model
 * exactly as far as Malloy's own symmetric-aggregate machinery already does.
 */
function isFanoutJoin(f: FieldLike): boolean {
   return isJoined(f as never) && f.join !== "one";
}

/**
 * A measure — the only valid target. Analytic expressions are excluded here as
 * well as by the classifier: they are not measures in the sense that matters,
 * and calling them "not a measure" is a clearer message than "non-additive".
 */
function isMeasure(f: FieldLike): boolean {
   return (
      expressionIsAggregate(f.expressionType as never) &&
      !expressionIsAnalytic(f.expressionType as never)
   );
}

/**
 * A groupable dimension: a declared scalar or a raw column off the source. Raw
 * columns carry no `expressionType` at all, which is why absence is accepted
 * rather than treated as unknown.
 */
function isGroupableDimension(f: FieldLike): boolean {
   if (isJoined(f as never) || isView(f)) return false;
   if (f.expressionType === undefined) return true;
   return f.expressionType === "scalar";
}

/**
 * Resolve one grain dimension against the base source.
 *
 * A grain may name only a dimension the source itself declares. Every dotted
 * form is therefore refused — but `a.b` is ambiguous in the annotation text,
 * being either a path through a join (`customer.region`) or a time truncation
 * (`order_time.day`), so they are told apart by what `a` resolves to and each
 * gets its own fix rather than a shared "no such field".
 */
function validateGrainDimension(
   sourceName: string,
   measure: string,
   source: ValidatableSource,
   grainDimension: string,
): PreaggregateViolation | undefined {
   const violation = (
      code: PreaggregateViolationCode,
      message: string,
   ): PreaggregateViolation => ({
      code,
      sourceName,
      fieldName: measure,
      message,
   });

   const parts = grainDimension.split(".");
   const head = parts[0];
   const fields = source.fields ?? [];
   const field = fields.find((f) => fieldName(f) === head);

   if (!field) {
      return violation(
         "unknown_grain_dimension",
         `Measure \`${measure}\` declares \`#@ preaggregate\` at a grain naming \`${grainDimension}\`, but \`${head}\` is not a field of \`${sourceName}\`. Use a dimension that exists on the source; a rollup can only group by the source's own dimensions.`,
      );
   }

   if (isJoined(field as never)) {
      return violation(
         "grain_path_through_join",
         `Measure \`${measure}\` declares a grain of \`${grainDimension}\`, which reaches through the join \`${head}\`. Pre-aggregating across a join is not supported in this version; copy the value into a dimension on \`${sourceName}\` and group by that instead.`,
      );
   }

   // `a.b` where `a` is not a join: a time truncation, and it must be refused.
   // preaggregation_grain_semantics.spec.ts is the evidence. A rollup storing a
   // truncated value can name that column one of two ways, and both are broken:
   // under the base's own name it satisfies an UNTRUNCATED query and answers it
   // from truncated rows — a silent wrong number — and under any other name no
   // query an author writes will reference it, so it never serves anything and
   // stops compiling when the rollup expires. Naming a dimension the source
   // already declares has neither problem, and a coarser truncation of it still
   // routes, so nothing is lost but the annotation's shorthand.
   if (parts.length > 1) {
      const type = field.type ?? "";
      if (!isTemporalType(type)) {
         return violation(
            "truncation_on_non_temporal",
            `Measure \`${measure}\` declares a grain of \`${grainDimension}\`, but \`${head}\` is a \`${type || "non-temporal"}\` field of \`${sourceName}\`, so \`.${parts.slice(1).join(".")}\` reads as neither a join path nor a time truncation. Name a dimension of \`${sourceName}\` instead.`,
         );
      }
      const unit = parts[parts.length - 1];
      const suggested = `${head}_${unit}`;
      return violation(
         "grain_truncation_expression",
         `Measure \`${measure}\` declares a grain of \`${grainDimension}\`. A grain may name only dimensions \`${sourceName}\` already declares, not a truncation written inline: a rollup built this way either answers untruncated queries from truncated rows, or is never used at all. Add \`dimension: ${suggested} is ${grainDimension}\` to \`${sourceName}\` and write \`grain="${suggested}"\`. Queries that group by \`${suggested}\`, or by a coarser truncation of it, will then be served by the rollup.`,
      );
   }

   if (isView(field)) {
      return violation(
         "grain_dimension_is_view",
         `Measure \`${measure}\` declares a grain naming \`${grainDimension}\`, which is a view, not a dimension. A grain must list dimensions to group by.`,
      );
   }
   if (isMeasure(field)) {
      return violation(
         "grain_dimension_is_measure",
         `Measure \`${measure}\` declares a grain naming \`${grainDimension}\`, which is itself a measure. A grain lists the dimensions the rollup groups by, not the values it aggregates.`,
      );
   }
   if (!isGroupableDimension(field)) {
      return violation(
         "unknown_grain_dimension",
         `Measure \`${measure}\` declares a grain naming \`${grainDimension}\`, which is not a dimension that can be grouped by. Use a scalar dimension or a column of \`${sourceName}\`.`,
      );
   }
   return undefined;
}

/**
 * Validate every `#@ preaggregate` declaration on one compiled source.
 *
 * Returns every violation found rather than the first, so an author fixing a
 * model sees the whole list instead of peeling them off one publish at a time.
 */
export function validateSourcePreaggregation(
   sourceName: string,
   source: ValidatableSource,
): PreaggregateViolation[] {
   const violations: PreaggregateViolation[] = [];
   const fields = source.fields ?? [];

   // A source-level annotation: the grain would have no measure to apply to.
   if (
      readPreaggregateAnnotation({
         name: sourceName,
         annotations: source.annotations,
      }).declared
   ) {
      violations.push({
         code: "misplaced_on_source",
         sourceName,
         message: `Source \`${sourceName}\` carries \`#@ preaggregate\`, which applies to measures, not to sources. Move it onto each measure that should be pre-aggregated.`,
      });
   }

   const declaredFields: FieldLike[] = [];
   // Canonical grain -> each namespace named at it, and the first measure that
   // named it. Joined on the same NUL separator `planSourcePreaggregation` keys
   // `byGrain` with, so "one grain" cannot mean two things across the two modules.
   const namespacesByGrain = new Map<string, Map<string, string>>();
   const grainTextByKey = new Map<string, string>();
   for (const field of fields) {
      const name = fieldName(field);
      const declaration = readPreaggregateAnnotation(
         field as AnnotatableMeasure,
      );
      if (!declaration.declared) continue;
      declaredFields.push(field);

      // Misplacement first: telling someone their dimension is "non-additive"
      // would be true and useless.
      if (isJoined(field as never)) {
         violations.push({
            code: "misplaced_on_join",
            sourceName,
            fieldName: name,
            message: `The join \`${name}\` on \`${sourceName}\` carries \`#@ preaggregate\`, which applies to measures, not to joins. Move it onto the measures that should be pre-aggregated.`,
         });
         continue;
      }
      if (isView(field)) {
         violations.push({
            code: "misplaced_on_view",
            sourceName,
            fieldName: name,
            message: `The view \`${name}\` on \`${sourceName}\` carries \`#@ preaggregate\`, which applies to measures, not to views. Annotate the measures the view aggregates instead; a rollup serves a view through its measures.`,
         });
         continue;
      }
      if (!isMeasure(field)) {
         violations.push({
            code: "misplaced_on_dimension",
            sourceName,
            fieldName: name,
            message: `\`${name}\` on \`${sourceName}\` carries \`#@ preaggregate\`, but it is a dimension, not a measure. Dimensions are named in a measure's \`grain=\`; annotate the measure you want pre-aggregated and list \`${name}\` in its grain.`,
         });
         continue;
      }

      // The declaration is on a measure. Is it usable? One entry per unusable
      // `#@ preaggregate` line, since a measure may carry several.
      if (declaration.errors.length > 0) {
         for (const error of declaration.errors) {
            violations.push({
               code: error.kind,
               sourceName,
               fieldName: name,
               message: error.message,
            });
         }
         continue;
      }

      const additivity = classifyMeasureAdditivity(field as never);
      if (!additivity.additive) {
         violations.push({
            code: "non_additive_measure",
            sourceName,
            fieldName: name,
            message: additivity.detail,
         });
         // Still validate the grain: an author who fixes the measure should not
         // then discover a second problem in the same annotation.
      }

      // Every grain the measure declares, each independently: a measure rolled up
      // at two grains can have one good and one broken, and an author fixing the
      // model should see both.
      for (const grain of declaration.grains) {
         for (const grainDimension of grain.dimensions) {
            const violation = validateGrainDimension(
               sourceName,
               name,
               source,
               grainDimension,
            );
            if (violation) violations.push(violation);
         }
         // Only an additive measure reaches a rollup, so only one can put a
         // namespace on it — mirroring the same skip in `planSourcePreaggregation`
         // so the two never disagree about which grains exist.
         if (grain.namespace !== undefined && additivity.additive) {
            const key = grain.dimensions.join("\u0000");
            const named = namespacesByGrain.get(key) ?? new Map();
            if (!named.has(grain.namespace)) named.set(grain.namespace, name);
            namespacesByGrain.set(key, named);
            grainTextByKey.set(key, grain.text);
         }
      }
   }

   // One grain is one table, so it can be created in exactly one place. Measures
   // sharing a grain but naming different namespaces is therefore unsatisfiable —
   // refused rather than resolved by field order, which would put a rollup
   // somewhere an author never chose and give no sign it happened.
   for (const [key, named] of namespacesByGrain) {
      if (named.size < 2) continue;
      const choices = [...named.entries()]
         .map(([ns, measure]) => `\`${ns}\` (on \`${measure}\`)`)
         .join(", ");
      violations.push({
         code: "conflicting_namespace",
         sourceName,
         message: `Measures on \`${sourceName}\` declare \`#@ preaggregate\` at the grain \`${grainTextByKey.get(key)}\` with different namespaces: ${choices}. One grain is one rollup table, so it can only be created in one of them. Give every measure at this grain the same \`namespace=\`, or move one to a different grain.`,
      });
   }

   // A v1 scope restriction rather than a mistake, so it is reported once for the
   // source instead of once per measure, and only when something asked for it.
   //
   // Scoped to FAN-OUT joins, not to joins at all. The reason the design doc gives
   // for the restriction is row multiplicity: a rollup summing a measure computed
   // against duplicated rows returns a wrong total. A `join_one` cannot duplicate
   // rows, so it does not have that problem, and refusing it excluded every
   // candidate in `examples/storefront` — where the measures worth rolling up sit
   // on the one source with joins, all of them `join_one`.
   //
   // Nothing extra is needed in synthesis to support them: the rollup computes the
   // base's own measure by name, so whatever joins that measure reaches through are
   // reproduced by the base itself.
   const fanout = declaredFields.length > 0 ? fields.filter(isFanoutJoin) : [];
   if (fanout.length > 0) {
      const joins = fanout.map((f) => `\`${fieldName(f)}\``).join(", ");
      violations.push({
         code: "base_source_has_fanout_join",
         sourceName,
         message: `Source \`${sourceName}\` declares \`#@ preaggregate\` but joins ${joins} with a fan-out join. A rollup over a fan-out join can return a wrong total, because every measure was computed against duplicated rows, so pre-aggregating such a source is not supported in this version. Pre-aggregate a source whose joins are all \`join_one\`, or remove the annotations.`,
      });
   }

   return violations;
}

/**
 * Validate every source in a compiled model's contents.
 *
 * `contents` is the same map the discovery surface reads, so a `#@ preaggregate`
 * anywhere in the model is reached — including on sources nothing else
 * references.
 */
export function validateModelPreaggregation(
   contents: Record<string, unknown>,
): PreaggregateViolation[] {
   const violations: PreaggregateViolation[] = [];
   for (const [name, object] of Object.entries(contents)) {
      const source = object as ValidatableSource;
      if (!source || typeof source !== "object") continue;
      // Anything with a field list is worth scanning; a non-source object cannot
      // carry a measure, and scanning it costs one annotation parse.
      violations.push(...validateSourcePreaggregation(name, source));
   }
   return violations;
}
