// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package warnings and publish rejections materialization raises, each a
 * pure function of what the facade holds (the plan, the bound tables, the
 * models): one wording per code, read by `Package` through the facade.
 */

import {
   findingSeverity,
   MATERIALIZATION_STORAGE_MODE_CODE,
   MATERIALIZE_NOT_BUILT_CODE,
   PERSIST_NOT_BUILT_CODE,
   PERSIST_SOURCE_REFUSED_ALIAS_CODE,
   PREAGGREGATE_NOT_BUILT_CODE,
   type PackageAnnotationFinding,
} from "../annotation_reader";
import { components } from "../../api";
import { PACKAGE_MANIFEST_NAME } from "../../constants";
import { CLASSIFICATION_COVERAGE } from "../classification";
import type { Model } from "../model";
import type { RefusalNotices, RefusedRollup } from "./plan/build_plan";
import { preaggregateGateLines, refusalCodeOf } from "./plan/load_gate_text";
import { CronEvaluator } from "./run/cron_evaluator";
import type { Binding } from "./domain/binding";
import type { MaterializePlan } from "./domain/table_plans";
import type { BuildPlan, FreshnessManifest } from "./domain/types";
import {
   SERVED_COPY_WARNING_CODE,
   type ServedCopyProblem,
} from "./serve/served_copy_serve";
import { namesServedCopyAlias } from "./serve/served_copy_config";
import {
   NATIVE_SCHEMA_MISSING_CODE,
   type NativeSchemaNotice,
   nativeSchemaNotices,
} from "./serve/native_schema_notices";
import {
   VERDICT_WARNING_CODES,
   type ServeVerdict,
} from "./serve/serve_verdict";
import { STORED_COLUMN_TYPE_MISMATCH_CODE } from "./stored_column_types";

type ApiPackage = components["schemas"]["Package"];
type ApiPackageWarning = NonNullable<ApiPackage["warnings"]>[number];
type Models = ReadonlyMap<string, Model>;

/** The code on the warning for a rollup whose build found its join_one key repeated. */
const PREAGGREGATE_JOIN_ONE_KEY_REPEATED_CODE =
   "preaggregate-join-one-key-repeated";

/**
 * The findings on authored annotations: an unservable one (a valid
 * annotation not built here) is `warn`. A malformed one fails the load, so a
 * serving package carries none; it would be `error`.
 */
export function annotationFindingWarnings(
   findings: readonly PackageAnnotationFinding[],
): ApiPackageWarning[] {
   return findings.map((finding) => ({
      ...(finding.model ? { model: finding.model } : {}),
      ...(finding.subject ? { subject: finding.subject } : {}),
      message: finding.rewrite
         ? `${finding.message}; ${finding.rewrite}`
         : finding.message,
      code: finding.code,
      severity: findingSeverity(finding),
   }));
}

/**
 * One warning per bound `#(preaggregate)` rollup whose last build found its
 * join_one key repeated (known limitation 29's check, `keyUniqueness`
 * false): the data breaks a `join_one` declared on that key, so queries that
 * read it beside other aggregates are answered live. Recomputed from what is
 * bound, so it clears once a build finds the key unique, or forgets the check.
 */
export function repeatedJoinOneKeyWarnings(
   plan: BuildPlan | null,
   colocatedEntries: FreshnessManifest | undefined,
   storageBindings: readonly Binding[],
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   const bySourceEntity = new Map(
      Object.values(plan?.sources ?? {}).flatMap((source) =>
         source.sourceEntityId ? [[source.sourceEntityId, source]] : [],
      ),
   );
   // The rollup's own key check only: an entry may carry just the query
   // sources' primary key checks (`parentKeys`), which say nothing of a
   // join_one keyed on the rollup's grain. A stored rollup's rides on its
   // binding, once per table.
   const checks = new Map<string, { keyColumn?: string; unique?: boolean }>(
      Object.entries(colocatedEntries ?? {}).flatMap(([id, entry]) =>
         entry.keyUniqueness ? [[id, entry.keyUniqueness]] : [],
      ),
   );
   for (const b of storageBindings) {
      if (b.rollupKey) checks.set(b.virtualHandle, b.rollupKey);
   }
   for (const [sourceEntityId, check] of checks) {
      if (check?.unique !== false || check.keyColumn === undefined) continue;
      const source = bySourceEntity.get(sourceEntityId);
      const rollup = source?.preaggregate;
      if (!rollup) continue;
      warnings.push({
         ...(source?.modelPath ? { model: source.modelPath } : {}),
         subject: rollup.baseSourceName,
         code: PREAGGREGATE_JOIN_ONE_KEY_REPEATED_CODE,
         severity: "warn",
         message:
            `holds \`${check.keyColumn}\` more than once, found by its ` +
            `#(preaggregate) rollup's last build (grain ` +
            `${rollup.grainDimensions.join(", ")}): a join_one keyed on it ` +
            `is not unique in the data, so live repeats each joining row ` +
            `once per match. Queries that read that join_one beside other ` +
            `aggregates are answered live. Fix the data or declare the ` +
            `join as join_many.`,
      });
   }
   return warnings;
}

/**
 * One `*-not-built` warning per refusal record in the plan's
 * `refusedSources`, whatever the mechanism. A record a planner's finding
 * made carries that finding's code and subject ({@link RefusalNotices}). A
 * compile-time gate's refusal is coded by its mechanism with the gate's
 * message verbatim: a rollup's {@link PREAGGREGATE_NOT_BUILT_CODE}, naming
 * its base and grain, with its BASE source as `subject` (its own name is
 * generated). A `#@ persist` source's gate refusal is repeated under
 * {@link PERSIST_SOURCE_REFUSED_ALIAS_CODE} for one release.
 */
export function persistRefusalWarnings(
   plan: BuildPlan | null,
   refusedRollups: Record<string, RefusedRollup>,
   materializePlans: Record<string, MaterializePlan>,
   notices: RefusalNotices = {},
): ApiPackageWarning[] {
   const refused = plan?.refusedSources;
   if (!refused) return [];
   return Object.entries(refused).flatMap(([sourceID, source]) => {
      const notice = notices[sourceID];
      if (notice) {
         return [
            {
               ...(source.modelPath ? { model: source.modelPath } : {}),
               ...(notice.subject !== undefined
                  ? { subject: notice.subject }
                  : {}),
               message: source.message,
               code: notice.code,
               severity: "warn" as const,
            },
         ];
      }
      const preaggregate = source.tier === "preaggregate";
      const rollup = preaggregate
         ? (refusedRollups[sourceID] ?? plan?.sources?.[sourceID]?.preaggregate)
         : undefined;
      const subject = preaggregate ? rollup?.baseSourceName : source.name;
      const code = preaggregate
         ? PREAGGREGATE_NOT_BUILT_CODE
         : materializePlans[sourceID]
           ? MATERIALIZE_NOT_BUILT_CODE
           : refusalCodeOf(sourceID, source.name);
      const warning: ApiPackageWarning = {
         message: preaggregate
            ? (rollup
                 ? `#(preaggregate) rollup of source "${rollup.baseSourceName}" at ` +
                   `(${rollup.grainDimensions.join(", ")})`
                 : "A #(preaggregate) rollup") +
              ` is not built, so its measures are served live: ` +
              source.message
            : source.message,
         code,
         severity: "warn",
      };
      if (source.modelPath) warning.model = source.modelPath;
      if (subject) warning.subject = subject;
      // A refusal no earlier release raised (`storage_source_unsupported`)
      // never carried the former code, so it is not repeated.
      return code === PERSIST_NOT_BUILT_CODE &&
         source.reason !== "storage_source_unsupported"
         ? [warning, { ...warning, code: PERSIST_SOURCE_REFUSED_ALIAS_CODE }]
         : [warning];
   });
}

/**
 * Warnings for `storage=` sources the serve path is not honoring, per
 * PERSIST_STORAGE_MODE: `off` ignores the annotation, `write-only` builds but
 * does not route. `on` warns nothing.
 */
export function storageWarnings(
   plan: BuildPlan | null,
   mode: string,
): ApiPackageWarning[] {
   if (mode === "on" || !plan?.sources) return [];
   const warnings: ApiPackageWarning[] = [];
   for (const source of Object.values(plan.sources)) {
      const storage = source.annotationFields?.storage?.trim();
      if (!storage) continue;
      // A rollup is named by its base source and grain (what the author
      // typed), never its generated name; the grain is a SET, sorted on the
      // wire. In the default `off` mode this is the only signal.
      const rollup = source.preaggregate;
      const at = rollup
         ? `Measures of \`${rollup.baseSourceName}\` pre-aggregated at grain ` +
           `(${rollup.grainDimensions.join(", ")}) declare `
         : "";
      const message = rollup
         ? mode === "off"
            ? `${at}storage="${storage}", but PERSIST_STORAGE_MODE is off, so ` +
              `the rollup is not built. Queries that would have used it are ` +
              `answered from \`${rollup.baseSourceName}\` instead — correct, ` +
              `but unaccelerated.`
            : `${at}storage="${storage}", and the rollup is built, but ` +
              `PERSIST_STORAGE_MODE is write-only, so the serve path does not ` +
              `route to it. Queries are answered from ` +
              `\`${rollup.baseSourceName}\` — correct, but unaccelerated.`
         : mode === "off"
           ? `declares storage="${storage}" but PERSIST_STORAGE_MODE is off; ` +
             `the annotation is ignored and the source is served live from ` +
             `its own warehouse.`
           : `is materialized into storage "${storage}" but ` +
             `PERSIST_STORAGE_MODE is write-only; the serve path is not ` +
             `routed to the materialized table (served live).`;
      warnings.push({
         model: source.modelPath ?? "",
         subject: rollup ? rollup.baseSourceName : source.name,
         message,
         code: MATERIALIZATION_STORAGE_MODE_CODE,
         severity: "warn",
      });
   }
   return warnings;
}

/**
 * Warnings for a model whose served copy leaves sources unserved: it did not
 * compile, a join it moved across connections reads a source that is not
 * stored, or a source could not be re-rooted. Those sources answer live.
 */
export function servedCopyWarnings(models: Models): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   for (const [modelPath, model] of models) {
      const problems = model.servedCopyProblems?.() ?? [];
      // A join already inlined, but kept as written: its own warning says
      // why, so the cross-connection one does not advise inlining it.
      const keptInline = new Set(
         problems
            .filter((p) => p.reason === "inline_join_kept")
            .map((p) => `${p.sourceName}\u0000${p.join}`),
      );
      for (const p of problems) {
         const subject = p.sourceName ? { subject: p.sourceName } : {};
         warnings.push({
            model: modelPath,
            ...subject,
            code: SERVED_COPY_WARNING_CODE,
            severity: "warn",
            message: servedCopyProblemMessage(
               p,
               keptInline.has(`${p.sourceName}\u0000${p.join}`),
            ),
         });
         // The coded warning the compile of record's own check raises.
         for (const message of p.coverage ?? []) {
            warnings.push({
               model: modelPath,
               ...subject,
               code: CLASSIFICATION_COVERAGE,
               severity: "warn",
               message,
            });
         }
      }
   }
   return warnings;
}

/**
 * Warnings for stored columns whose type does not serve as the type Malloy
 * declares, one per source and column, read from the bindings bound now. Keyed
 * by the source's sourceID, so two models' same-named sources both report and
 * a source several models import reports once.
 */
export function storedColumnTypeWarnings(models: Models): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   const seen = new Set<string>();
   for (const [modelPath, model] of models) {
      for (const {
         sourceName,
         sourceID,
         mismatches,
      } of model.storedColumnTypeMismatches?.() ?? []) {
         for (const m of mismatches) {
            const key = `${sourceID ?? `${modelPath}\u0000${sourceName}`}\u0000${m.column}`;
            if (seen.has(key)) continue;
            seen.add(key);
            warnings.push({
               model: modelPath,
               subject: sourceName,
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               severity: "warn",
               message:
                  `stores column \`${m.column}\` as ${m.storedType}, not its ` +
                  `declared Malloy type \`${m.declaredType}\`. ` +
                  (m.handling === "cast"
                     ? `It is read through a cast to \`${m.declaredType}\`, ` +
                       `so it still serves from storage; the next build ` +
                       `stores the declared type.`
                     : `No cast reproduces its values faithfully, so queries ` +
                       `that read it are answered live. Give the field a ` +
                       `native cast in the model (for example \`::"double"\`) ` +
                       `so the warehouse returns the declared type.`),
            });
         }
      }
   }
   return warnings;
}

/**
 * Warnings for colocated tables bound with no captured warehouse type for a
 * big-integer column, one per source (by sourceID).
 */
export function nativeSchemaWarnings(
   bindings: readonly Binding[],
   models: Models,
   materializePlans: Record<string, MaterializePlan>,
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   const seen = new Set<string>();
   for (const [modelPath, model] of models) {
      const modelDef = model.getModelDef?.();
      if (!modelDef) continue;
      for (const n of nativeSchemaNotices(
         bindings,
         modelDef,
         materializePlans,
      )) {
         if (seen.has(n.sourceID)) continue;
         seen.add(n.sourceID);
         warnings.push({
            model: modelPath,
            subject: n.sourceName,
            code: NATIVE_SCHEMA_MISSING_CODE,
            severity: "warn",
            message: nativeSchemaMissingMessage(n),
         });
      }
   }
   return warnings;
}

/**
 * The publish warnings a serve verdict carries: `unreachable`, and the
 * deployment facts (a `storage=` destination the environment does not
 * configure, a colocated source on a connection that cannot create tables).
 * Unservable, not malformed: the source serves live, and a deployment warning
 * clears once the deployment changes, with no reload.
 */
export function verdictWarnings(
   verdicts: readonly ServeVerdict[],
): ApiPackageWarning[] {
   return verdicts.flatMap((v) => {
      const code = v.reason ? VERDICT_WARNING_CODES[v.reason] : undefined;
      if (!code) return [];
      return [
         {
            model: v.modelPath ?? "",
            subject: v.sourceName,
            code,
            severity: "warn" as const,
            message: v.message + " " + (v.fixes ?? []).join(" "),
         },
      ];
   });
}

/**
 * The package manifest's Malloy Persistence policy REJECTION rules: a
 * non-empty result fails a publish and disarms the scheduler (warn-only at
 * load). Cross-package scope rules are not checked: the build plan is
 * intra-package only.
 */
export function persistencePolicyWarnings(
   plan: BuildPlan | null,
   metadata: ApiPackage,
): string[] {
   const warnings: string[] = [];
   const sources = plan?.sources ? Object.values(plan.sources) : [];
   const packageSchedule = metadata.materialization?.schedule ?? null;
   const packageFreshness = metadata.materialization?.freshness ?? null;
   const scope = metadata.scope ?? "package";

   // A package-level cron is legal only with scope: version.
   if (packageSchedule && scope !== "version") {
      warnings.push(
         `materialization.schedule (cron) in ${PACKAGE_MANIFEST_NAME} requires ` +
            `"scope": "version": a package-scoped lineage is reused across ` +
            `versions, so a single per-version cadence is meaningless. Set ` +
            `"scope": "version", or remove the schedule.`,
      );
   }

   // Schedule and freshness are mutually exclusive.
   if (packageSchedule) {
      const freshnessDeclared =
         !!(
            packageFreshness &&
            (packageFreshness.window || packageFreshness.fallback)
         ) || sources.some((s) => s.freshness != null);
      if (freshnessDeclared) {
         warnings.push(
            `materialization.schedule and freshness are mutually exclusive in ` +
               `${PACKAGE_MANIFEST_NAME}: declare either a schedule (power tier) ` +
               `or freshness (objective tier), never both.`,
         );
      }
   }

   // A valid 5-field cron, here so publish, PATCH, load and the scheduler all
   // apply one rule and a bad cron cannot silently never arm.
   if (packageSchedule && !new CronEvaluator().isValid(packageSchedule)) {
      warnings.push(
         `materialization.schedule in ${PACKAGE_MANIFEST_NAME} is not a valid ` +
            `5-field UNIX cron: ${JSON.stringify(packageSchedule)}. Use ` +
            `"minute hour day-of-month month day-of-week" (no L/W/#/? ` +
            `extensions).`,
      );
   }

   return warnings;
}

/**
 * REJECTION lines for invalid `#@ preaggregate` declarations, each with its
 * code, strict at publish AND load: an ignored declaration would show correct
 * numbers and no acceleration, with no one told. A valid declaration Publisher
 * cannot build is refused alone (`preaggregate-not-built`).
 */
export function formatInvalidPreaggregatePolicy(models: Models): string {
   return [...models]
      .flatMap(([modelPath, model]) =>
         preaggregateGateLines(modelPath, model.preaggregateViolations()),
      )
      .join("\n");
}

/**
 * The compiler's own reasons in a compile error, as `: <reason>; <reason>.`:
 * each diagnostic's message without its file and source excerpt, so a package
 * warning names why without the server's paths. A reason that names a
 * served-copy alias is left out: the alias stays in the server log. `.` when
 * there are none.
 */
function compilerReasons(detail: string | undefined): string {
   const reasons = [...(detail ?? "").matchAll(/^line \d+: (.+)$/gm)]
      .map((m) => m[1].trim())
      .filter((reason) => !namesServedCopyAlias(reason));
   return reasons.length > 0 ? `: ${reasons.join("; ")}.` : ".";
}

/** The `stored-native-schema-missing` sentence for one source. */
function nativeSchemaMissingMessage(n: NativeSchemaNotice): string {
   const names = (columns: string[]) =>
      columns.map((c) => "`" + c + "`").join(", ");
   const parts: string[] = [];
   const types = [...new Set(n.inferred.map((i) => i.as))];
   if (n.inferred.length > 0) {
      parts.push(
         `${names(n.inferred.map((i) => i.column))} ` +
            `${n.inferred.length === 1 ? "is" : "are"} read as ` +
            `${names(types)}, the warehouse's only integer type, so ` +
            `${n.inferred.length === 1 ? "it still serves" : "they still serve"} ` +
            "from the table",
      );
   }
   if (n.leftOut.length > 0) {
      parts.push(
         `${names(n.leftOut)} cannot be read exactly without one, so ` +
            `${n.leftOut.length === 1 ? "it is" : "they are"} left out of the table's read: ` +
            "the source answers live wherever it needs " +
            `${n.leftOut.length === 1 ? "it" : "them"}, and wholly live when its other fields ` +
            `cannot be served without ${n.leftOut.length === 1 ? "it" : "them"} (the ` +
            "`served-copy-not-served` warning on the source says so)",
      );
   }
   return (
      "This source's colocated table is bound with no captured warehouse " +
      "type for its big-integer columns (its manifest entry records none): " +
      `${parts.join("; ")}. The next full rebuild captures the types.`
   );
}

/**
 * A caller-safe sentence for why a served copy leaves a source unserved. The
 * compiler's own message stays in the logs: it can name the destination alias.
 */
function servedCopyProblemMessage(
   problem: ServedCopyProblem,
   inlinedButKept = false,
): string {
   const { reason, join } = problem;
   switch (reason) {
      case "compile_failed":
         // With a source: one extension table that does not compile over
         // its kept blocks, so the copy is compiled again without them.
         return problem.sourceName
            ? "This source's table does not compile with its kept blocks, so the served copy re-roots it with its own blocks only, or leaves it as written."
            : "This model's served copy does not compile, so its stored sources answer live.";
      case "cross_connection_unbuilt":
         return "This model's served copy re-roots a source onto its storage destination, which moves a join of it across connections, and the join cannot be left out; the copy compiles once both sides are built into one destination, and serves live until then.";
      case "column_types":
         return "This source's stored column types differ from the types the model declares, or one cannot be declared on its table, so the served copy leaves it as written and it answers live.";
      case "no_declaration":
         return "This source's declaration was not found in the compiled model, so the served copy leaves it as written and it answers live.";
      case "range_not_found":
      case "definition_not_parsed":
         if (problem.hiddenUnstored?.length) {
            return (
               "This source's `include` hides " +
               problem.hiddenUnstored.map((c) => "`" + c + "`").join(", ") +
               " (private or internal), which its v1 `storage=` table does not hold (another source " +
               "with the same SQL may have built the table, without that column). The served copy " +
               "leaves the source as written, so it answers live."
            );
         }
         return "This source's declaration could not be re-rooted on its table, so the served copy leaves it as written and it answers live.";
      case "pair_join_not_found":
         return "This source inlines a join_many that is not declared on its own in its own trailing `extend` (a source it is built from declares it, or its `join_many:` statement declares other joins too), so its pair table cannot be read with the join as a record and the served copy leaves it as written.";
      case "pair_not_compiled":
         return "This source's pair table (an inlined join_many) could not be read as the source with its join as a record (its text reads a measure or view of the joined source through the join, which the record does not carry), so the served copy leaves it as written.";
      case "reroot_not_compiled":
         return (
            "This source's re-root on its stored table does not compile in the served copy (the server log has the compiler's message)." +
            " The served copy leaves it as written, so it answers live, and the model's other stored sources still serve. This is a Publisher bug; please report it."
         );
      case "read_terms_not_kept":
         return "This source filters by a given in a place its stored table cannot keep, or is filtered and reached here only through a join, so the served copy leaves it as written rather than answer every caller alike.";
      case "fields_not_carried":
         return "This source inherits fields (a measure, view or join declared on the source it is built from) that its stored table does not carry, so the served copy leaves it as written.";
      case "cross_connection_join":
         return (
            `This source is stored in a destination its join${join ? ` \`${join}\`` : ""} cannot reach: ` +
            "the joined source reads another connection. Queries that use the join are answered live; " +
            "the others read the stored table. To serve them too, " +
            (inlinedButKept
               ? "fix what keeps the inlined join from being answered from its stored columns (the warning on " +
                 "this join says what), materialize the joined source into the same destination, or keep this " +
                 "source colocated."
               : "inline the join (inline=), materialize the joined source into the same destination, or keep " +
                 "this source colocated.")
         );
      case "inline_join_kept":
         return (
            `This source inlines its join${join ? ` \`${join}\`` : ""} (inline=), but its served copy does not compile ` +
            "with the join answered from the stored columns, so the join is kept as written and every query " +
            "through it reads the joined source through the join" +
            compilerReasons(problem.detail) +
            " A definition that reads the joined source past one hop (a join of the joined source), or reads " +
            "one of its measures or views, is the usual cause."
         );
      case "classification_not_kept":
         return (
            "This source's re-root changed what it exposes or how a field is classified, which the load's " +
            "classification checks flag on the served copy, so its queries are not answered from the " +
            "stored tables (a Publisher bug; please report it): " +
            [...(problem.leaks ?? []), ...(problem.coverage ?? [])].join(" ")
         );
   }
}
