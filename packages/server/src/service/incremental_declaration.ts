// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";
import type { Tag } from "@malloydata/malloy-tag";

import type { IncrementalStrategy } from "../storage/DatabaseInterface";
import { deriveColumns } from "./build_plan";
import { readTimeDynamicTerms } from "./persist_dynamic_terms";

// Shared with the store: the strategy a declaration implies is what the ledger records.
export type { IncrementalStrategy };

/**
 * Resolves a persist source's INCREMENTAL declaration — the `refresh=`,
 * `watermark=` and `merge_key=` keys of its `#@ persist` tag — into a structure
 * the publish gates and the build path both read: which names are real output
 * columns, the watermark's type, the implied strategy, and every malformation.
 *
 * Three compiler facts shape the module (pinned in
 * incremental_compiler_contract.spec.ts):
 *
 *  1. Rules read the EFFECTIVE MERGED tag. A source that `extend`s a persisted
 *     parent inherits its persist tag; checking only local declarations would let
 *     an inherited `refresh="incremental"` through ungated.
 *  2. An array-valued key (`watermark=["a","b"]`) is ABSENT from the scalar map,
 *     so the raw tag is read too — otherwise it is misreported as undeclared.
 *  3. On the compiled OUTPUT schema an `aggregate:` column is indistinguishable
 *     from a dimension, so the aggregate check reads the QUERY DEFINITION.
 */

/** The `refresh=` values the publisher recognizes; unset means "full". */
const REFRESH_MODES = ["full", "incremental"] as const;

/**
 * Persist-tag keys the publisher knows about; anything else is a load-time
 * WARNING, never a rejection, so a new directive needs no publisher change.
 * `sharing` and `schedule` are RETIRED and rejected by name in the
 * persistence-policy gate; listing them avoids a second, vaguer "unknown" message.
 */
export const RECOGNIZED_PERSIST_KEYS: ReadonlySet<string> = new Set([
   "persist",
   "name",
   "storage",
   "realization",
   "refresh",
   "watermark",
   "merge_key",
   "freshness",
   "queryMetadata",
   "partition",
   "sharing",
   "schedule",
]);

/**
 * Malloy types a `<`/`>=` range can be built over. Struct/array columns never
 * reach the output schema, so they fail name resolution instead.
 */
const ORDERABLE_TYPES: ReadonlySet<string> = new Set([
   "number",
   "string",
   "date",
   "timestamp",
]);

/** What a declared name turned out to be on the compiled source. */
export type ResolvedName =
   | {
        name: string;
        kind: "dimension" | "aggregate";
        /** The column's Malloy type (`date`, `timestamp`, `number`, …). */
        malloyType: string;
     }
   | { name: string; kind: "unresolved" };

/** A key whose VALUE is unusable, as opposed to absent. */
interface MalformedValue {
   key: "refresh" | "watermark" | "merge_key";
   problem:
      | "array" // tag-array form: watermark=["a","b"]
      | "empty" // empty string: watermark=""
      | "empty-entry" // an empty name in the split list: merge_key="a,,b"
      | "duplicate"; // a repeated name: merge_key="a, a"
   /** The offending name, when one name of several is at fault. */
   detail?: string;
}

export interface IncrementalDeclaration {
   /** The effective `refresh=` value verbatim; undefined when unset. */
   refresh?: string;
   /** True only when `refresh=` is exactly `"incremental"`. */
   incremental: boolean;
   /** Set when `refresh=` is declared but outside {full, incremental}. */
   invalidRefresh?: string;
   /** Whether the key is present on the raw tag, whatever its value's shape. */
   declaredWatermark: boolean;
   declaredMergeKey: boolean;
   /** The watermark, resolved against the output schema. */
   watermark?: ResolvedName;
   /** Whether the watermark's type supports a `<` range. */
   watermarkOrderable: boolean;
   /** `merge_key=` split on commas, in declared order, each resolved. */
   mergeKeys: ResolvedName[];
   /**
    * The source's own columns its stripped dynamic terms constrain. A
    * `merge_key=` is chosen against the source filtered to one caller, but the
    * stored table holds every caller's rows; these columns restore the intended
    * relation inside the merge's match.
    */
   scopeColumns: string[];
   /**
    * True when a stripped term contributes no column of this source (it reaches
    * through a join, or its field usage could not be read). A caller must refuse,
    * never partially scope: scoping by the other terms alone still matches across
    * callers.
    */
   scopeIncomplete: boolean;
   /** True when `merge_key=` repeats the watermark dimension. */
   watermarkInMergeKeys: boolean;
   /**
    * The strategy the keys imply, present only when the declaration is coherent
    * enough to derive one (incremental + a resolved watermark).
    */
   strategy?: IncrementalStrategy;
   malformed: MalformedValue[];
   /** Unrecognized persist-tag keys, in tag order. */
   unknownKeys: string[];
   /**
    * Names of `calculate:` (window) fields anywhere in the query definition.
    * Presence alone gates an incremental source for now (see the publish rules).
    */
   calculateFields: string[];
   /** Output column names, for "did you mean" context in messages. */
   outputColumns: string[];
}

/** The source's `#@` tag, or undefined when it does not parse. */
function safeTag(source: PersistSource): Tag | undefined {
   try {
      return source.annotations.parseAsTag("@").tag;
   } catch {
      return undefined;
   }
}

/**
 * Field kinds read from the QUERY DEFINITION (contract fact 3). `aggregates`
 * covers the FINAL segment — the output columns. `analytics` covers EVERY
 * segment: an earlier `calculate:` still feeds the rows a delta rewrites. A
 * table-backed source has no query definition, so both are empty.
 */
function queryDefinitionFieldKinds(source: PersistSource): {
   aggregates: Set<string>;
   analytics: string[];
} {
   const aggregates = new Set<string>();
   const analytics: string[] = [];
   try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const def = source._sourceDef as any;
      const pipeline: unknown[] = def?.query?.pipeline ?? [];
      pipeline.forEach((rawSegment, index) => {
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const segment = rawSegment as any;
         const isFinalSegment = index === pipeline.length - 1;
         for (const field of segment?.queryFields ?? []) {
            const expressionType = String(field?.expressionType ?? "");
            const name =
               field?.type === "fieldref"
                  ? // A path's output column takes the LAST segment's name
                    // (`group_by: joined.region` outputs `region`).
                    field.path?.[field.path.length - 1]
                  : field?.name;
            if (typeof name !== "string" || name.length === 0) continue;
            if (expressionType.includes("analytic")) {
               analytics.push(name);
            } else if (expressionType === "aggregate" && isFinalSegment) {
               aggregates.add(name);
            }
         }
      });
   } catch {
      // Fail open on an unrecognized IR shape: only the aggregate and window
      // gates are lost. The gates that protect the TABLE (name resolution,
      // dialect allowlist, shape check) do not depend on this read.
   }
   return { aggregates, analytics };
}

/**
 * Output columns as name -> Malloy type: the list the seed CTAS materializes, so
 * a name absent here is absent from the physical table.
 */
function outputColumnTypes(source: PersistSource): Map<string, string> {
   const columns = new Map<string, string>();
   for (const column of deriveColumns(source)) {
      if (column.name && column.type) columns.set(column.name, column.type);
   }
   return columns;
}

/** Resolve one declared name against the compiled output schema. */
function resolveName(
   name: string,
   columns: Map<string, string>,
   aggregates: Set<string>,
): ResolvedName {
   const malloyType = columns.get(name);
   if (malloyType === undefined) return { name, kind: "unresolved" };
   return {
      name,
      kind: aggregates.has(name) ? "aggregate" : "dimension",
      malloyType,
   };
}

/** Read one persist-tag key: its scalar text, or why there is none. */
function readKey(
   tag: Tag | undefined,
   key: "refresh" | "watermark" | "merge_key",
): { declared: boolean; text?: string; malformed?: MalformedValue } {
   if (!tag || !tag.has(key)) return { declared: false };
   // The array form vanishes from the scalar read; name it or it looks absent.
   if (tag.array(key) !== undefined) {
      return { declared: true, malformed: { key, problem: "array" } };
   }
   const text = tag.text(key);
   if (text === undefined) {
      return { declared: true, malformed: { key, problem: "empty" } };
   }
   if (text.trim().length === 0) {
      return { declared: true, malformed: { key, problem: "empty" } };
   }
   return { declared: true, text: text.trim() };
}

/**
 * Turn a source's persist-tag strings into a gate-ready declaration. Pure: runs
 * no query and decides nothing — the gate composes every rejection from the
 * result (see `incrementalPolicyWarnings`).
 */
export function resolveIncrementalDeclaration(
   source: PersistSource,
   annotationFields: Record<string, string>,
): IncrementalDeclaration {
   const tag = safeTag(source);
   const columns = outputColumnTypes(source);
   // Read off the source's own filters, not the positional classification: a
   // classifier refusal yields no terms, which would be indistinguishable from
   // "not caller-scoped" and emit an unscoped merge.
   const dynamicTerms = readTimeDynamicTerms(source);
   // Resolved against the STORED schema: a term may name a read-time
   // `dimension:` the build never materializes, and an unresolved name in the
   // merge's `ON` would fail every refresh.
   const scopeColumns = [
      ...new Set(dynamicTerms.flatMap((term) => term.columns)),
   ].filter((name) => columns.has(name));
   const scopeIncomplete =
      dynamicTerms.some((term) => term.columns.length === 0) ||
      dynamicTerms.some((term) =>
         term.columns.some((name) => !columns.has(name)),
      );
   const { aggregates, analytics } = queryDefinitionFieldKinds(source);
   const malformed: MalformedValue[] = [];

   // The scalar map is authoritative; the raw read reports an array value as
   // malformed rather than as an unrecognized mode.
   const rawRefresh = readKey(tag, "refresh");
   if (rawRefresh.malformed) malformed.push(rawRefresh.malformed);
   const refresh = annotationFields.refresh ?? rawRefresh.text;
   const incremental = refresh === "incremental";
   const invalidRefresh =
      refresh !== undefined &&
      !(REFRESH_MODES as readonly string[]).includes(refresh)
         ? refresh
         : undefined;

   const rawWatermark = readKey(tag, "watermark");
   if (rawWatermark.malformed) malformed.push(rawWatermark.malformed);
   const watermark =
      rawWatermark.text === undefined
         ? undefined
         : resolveName(rawWatermark.text, columns, aggregates);
   const watermarkOrderable =
      watermark !== undefined &&
      watermark.kind !== "unresolved" &&
      ORDERABLE_TYPES.has(watermark.malloyType);

   const rawMergeKey = readKey(tag, "merge_key");
   if (rawMergeKey.malformed) malformed.push(rawMergeKey.malformed);
   const mergeKeys: ResolvedName[] = [];
   if (rawMergeKey.text !== undefined) {
      const seen = new Set<string>();
      for (const piece of rawMergeKey.text.split(",")) {
         const name = piece.trim();
         if (name.length === 0) {
            // `merge_key="id,,region"` would otherwise silently narrow row identity.
            if (
               !malformed.some(
                  (m) => m.key === "merge_key" && m.problem === "empty-entry",
               )
            ) {
               malformed.push({ key: "merge_key", problem: "empty-entry" });
            }
            continue;
         }
         if (seen.has(name)) {
            malformed.push({
               key: "merge_key",
               problem: "duplicate",
               detail: name,
            });
            continue;
         }
         seen.add(name);
         mergeKeys.push(resolveName(name, columns, aggregates));
      }
   }

   const watermarkInMergeKeys =
      watermark !== undefined &&
      mergeKeys.some((k) => k.name === watermark.name);

   // Undefined for anything incoherent, which the gates reject.
   const strategy: IncrementalStrategy | undefined =
      incremental && watermark !== undefined && watermark.kind === "dimension"
         ? mergeKeys.length > 0
            ? "merge"
            : "range_replace"
         : undefined;

   const unknownKeys: string[] = [];
   if (tag) {
      try {
         for (const [key] of tag.entries()) {
            if (!RECOGNIZED_PERSIST_KEYS.has(key)) unknownKeys.push(key);
         }
      } catch {
         // Degrade to no unknown keys — mirrors deriveAnnotationFields.
      }
   }

   return {
      refresh,
      incremental,
      invalidRefresh,
      declaredWatermark: rawWatermark.declared,
      declaredMergeKey: rawMergeKey.declared,
      watermark,
      watermarkOrderable,
      mergeKeys,
      scopeColumns,
      scopeIncomplete,
      watermarkInMergeKeys,
      strategy,
      malformed,
      unknownKeys,
      calculateFields: analytics,
      outputColumns: [...columns.keys()],
   };
}

/**
 * Whether a declaration says ANYTHING about incremental refresh; lets most
 * sources skip every gate and ledger read.
 */
export function declaresIncremental(
   declaration: IncrementalDeclaration,
): boolean {
   return (
      declaration.incremental ||
      declaration.declaredWatermark ||
      declaration.declaredMergeKey ||
      declaration.invalidRefresh !== undefined
   );
}
