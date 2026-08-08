import type { PersistSource } from "@malloydata/malloy";
import type { Tag } from "@malloydata/malloy-tag";

import type { IncrementalStrategy } from "../storage/DatabaseInterface";
import { deriveColumns } from "./build_plan";

// The strategy a declaration implies is also what the ledger records, so the
// type is shared with the store rather than restated here.
export type { IncrementalStrategy };

/**
 * Resolution of a persist source's INCREMENTAL declaration — the `refresh=`,
 * `watermark=` and `merge_key=` keys of its `#@ persist` tag — from strings into
 * a structure the publish gates and the build path can both read.
 *
 * Discovery needs no new compiler reading: `deriveAnnotationFields` already
 * captures every scalar `key="value"` on the persist tag, the two new keys
 * included. What this module adds is MEANING: which declared names are real
 * output columns, what type the watermark is, which strategy the keys imply, and
 * every way the declaration can be malformed.
 *
 * Three compiler facts shape the whole module (each pinned in
 * incremental_compiler_contract.spec.ts):
 *
 *  1. Every rule reads the EFFECTIVE MERGED tag. A source that `extend`s a
 *     persisted parent inherits the parent's whole persist tag, so
 *     "did this source declare it locally" is not a question worth asking — and
 *     asking it would let an inherited `refresh="incremental"` through ungated.
 *  2. An array-valued key (`watermark=["a","b"]`) is ABSENT from the scalar map.
 *     So the raw tag is read alongside it: without that, a declared-but-array
 *     watermark would be diagnosed as "no watermark declared", pointing the
 *     author at a key they did declare.
 *  3. On the compiled OUTPUT schema an `aggregate:`-produced column is
 *     indistinguishable from a dimension (`isCalculation()` is false for both).
 *     The aggregate check therefore reads the QUERY DEFINITION's field kinds.
 */

/** The `refresh=` values the publisher recognizes; unset means "full". */
export const REFRESH_MODES = ["full", "incremental"] as const;
export type RefreshMode = (typeof REFRESH_MODES)[number];

/**
 * Persist-tag keys the publisher knows about. Anything else is reported as an
 * unknown key (a load-time WARNING, never a rejection — `deriveAnnotationFields`
 * deliberately passes unknown keys through so a new directive needs no publisher
 * change). `sharing` and `schedule` are listed because they are RETIRED and
 * rejected by name in the persistence-policy gate: reporting them as "unknown"
 * on top of that would be a second, vaguer message about the same key.
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
   "sharing",
   "schedule",
]);

/**
 * Malloy types a `<`/`>=` range can be built over. A watermark outside this set
 * has no ordering to derive a range from. Note that struct/array columns never
 * even reach the output schema (they are not atomic), so they fail NAME
 * resolution rather than landing here.
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
export interface MalformedValue {
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
 * Field kinds read from the QUERY DEFINITION rather than the output schema,
 * because the output schema cannot tell an aggregate from a dimension (contract
 * fact 3).
 *
 * `aggregates` covers the FINAL pipeline segment — that is what the output
 * columns are. `analytics` covers EVERY segment: a `calculate:` in an earlier
 * stage still contributes values to the rows the delta would rewrite.
 *
 * A table-backed source (`sql_select`, `table`) has no query definition, so both
 * come back empty — correct, since it has no aggregate or window fields.
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
      // Fail open on an IR shape we don't recognize: the aggregate and window
      // gates then don't fire. Everything that protects the TABLE (name
      // resolution, the dialect allowlist, the shape check) is independent of
      // this read, so a compiler IR change degrades these two advisory-grade
      // gates rather than admitting a delta against an unproven table.
   }
   return { aggregates, analytics };
}

/**
 * The compiled source's output columns as name -> Malloy type: the same column
 * list the wire plan reports, which is the list the seed CTAS materializes. A
 * name absent from here is a name the physical table does not have.
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

/**
 * Read one persist-tag key in the two shapes it can legitimately arrive in, plus
 * the two it cannot. Returns the scalar text when there is one, and otherwise
 * says why there isn't.
 */
function readKey(
   tag: Tag | undefined,
   key: "refresh" | "watermark" | "merge_key",
): { declared: boolean; text?: string; malformed?: MalformedValue } {
   if (!tag || !tag.has(key)) return { declared: false };
   // The array form parses fine and vanishes from the scalar read, so it must be
   // named explicitly or it would masquerade as an absent key.
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
 * Turn a source's persist-tag strings into a gate-ready declaration. Pure and
 * synchronous: it reads the compiled source only, runs no query, and never
 * decides anything — every rejection message is composed by the gate from what
 * this returns (see `incrementalPolicyWarnings`).
 */
export function resolveIncrementalDeclaration(
   source: PersistSource,
   annotationFields: Record<string, string>,
): IncrementalDeclaration {
   const tag = safeTag(source);
   const columns = outputColumnTypes(source);
   const { aggregates, analytics } = queryDefinitionFieldKinds(source);
   const malformed: MalformedValue[] = [];

   // `refresh=` — the scalar map is authoritative for the value; the raw read
   // still runs so an array-valued refresh is reported as malformed rather than
   // as an unrecognized mode.
   const rawRefresh = readKey(tag, "refresh");
   if (rawRefresh.malformed) malformed.push(rawRefresh.malformed);
   const refresh = annotationFields.refresh ?? rawRefresh.text;
   const incremental = refresh === "incremental";
   const invalidRefresh =
      refresh !== undefined &&
      !(REFRESH_MODES as readonly string[]).includes(refresh)
         ? refresh
         : undefined;

   // `watermark=` — one output dimension name.
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

   // `merge_key=` — comma-separated output dimension names.
   const rawMergeKey = readKey(tag, "merge_key");
   if (rawMergeKey.malformed) malformed.push(rawMergeKey.malformed);
   const mergeKeys: ResolvedName[] = [];
   if (rawMergeKey.text !== undefined) {
      const seen = new Set<string>();
      for (const piece of rawMergeKey.text.split(",")) {
         const name = piece.trim();
         if (name.length === 0) {
            // `merge_key="id,,region"` — a typo that would otherwise silently
            // narrow row identity to one fewer column.
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

   // A strategy is derivable only from a coherent declaration; the gates reject
   // every incoherent one, so this stays undefined for anything the build path
   // must not act on.
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
      watermarkInMergeKeys,
      strategy,
      malformed,
      unknownKeys,
      calculateFields: analytics,
      outputColumns: [...columns.keys()],
   };
}

/**
 * Whether a declaration says ANYTHING about incremental refresh. Used to skip
 * every gate and every ledger read for the overwhelming majority of sources,
 * which declare none of these keys.
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
