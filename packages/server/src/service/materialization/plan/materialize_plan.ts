// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Planning a model's `#(materialize)` sources: reading each declaration,
 * refusing what cannot be stored and served equal to live, and deciding the
 * rest's stored columns, inlined joins and pair layout, read-time terms,
 * key checks and the parent each extension table builds over. The
 * generated text is `build_file_generator.ts`'s.
 *
 * `planModelMaterialize` is read by the companion compiler
 * (`materialize_compile.ts`); the refusal findings by the build plan.
 *
 * Pure: it reads a compiled model's contents and the probe compiler's
 * answers, and returns plans and refusals.
 */

import { refusalKey, tierOf } from "./refusal_key";
import { freshnessValueProblem } from "../../annotation_reader";
import { createHash } from "crypto";
import {
   CARRIED_KEYS,
   MATERIALIZE_ROUTE,
   PAIR_PARENT_ROW,
   PAIR_WATERMARK,
   pairLevelRowColumn,
   PAIR_LANE_COLUMN,
   PAIR_BLOCK_WATERMARK,
   buildSourceName,
   pairKeyChecks,
   givenReadTerms,
   type SourceDefLike,
   type FieldLike,
} from "./build_file_generator";
import {
   readNotes,
   declares,
   notesDeclarePersist,
   placementOf,
   EMPTY_STORAGE_REASON,
   type RouteTag,
} from "./declaration";
import {
   ENV_REFERENCE_REASON,
   tableNameProblem,
} from "./persist_annotation_validation";
import { declaredNotes } from "../../governance/ir/declared_notes";
import {
   routeOfNote,
   PUBLISHER_ROUTES,
   MATERIALIZE_INVALID_CODE,
   MATERIALIZE_KEYS,
   MATERIALIZE_NOT_BUILT_CODE,
   nearestKey,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import { Annotations } from "@malloydata/malloy";
import { sameNotePosition } from "../../annotations";
import { hiddenAccess } from "../../governance/ir/compiled_model";
import {
   SourceMap,
   type FieldJoinReads,
   type SourceJoin,
} from "../text/source_map";
import {
   splitSourceID,
   type MaterializeColumn,
   type LeftOutDimension,
   type CallerInput,
   type MaterializePair,
   type MaterializePairBlocks,
   type MaterializePlan,
   type InlineUniquenessCheck,
} from "../domain/table_plans";
import {
   currentTimeFields,
   givenReadingFields,
   withinDeclaration,
   classifyWhereTerms,
} from "./persist_dynamic_terms";
import { callerIndependent, type FieldDependence } from "../../probe_compiler";
import {
   declarationOf,
   readTimeTermsOf,
   withDeclaration,
} from "./materialize_read_time";
import type { MalloyRange, TermUse } from "../domain/read_time_terms";
import { capabilitiesOf, describeDialects } from "../domain/dialect";
import type { RefusedReason } from "../domain/types";
import {
   rollingWindowTerms,
   leftOutTermReadElsewhere,
   leftOutWindowTerms,
} from "./rolling_window";

/**
 * The keys carried as a group of string properties (`freshness.window="24h"`,
 * `queryMetadata.team="ops"`), as `#@ persist` reads them.
 */
const CARRIED_GROUPS = ["freshness", "queryMetadata"] as const;

/** The keys of a `freshness` group. */
const FRESHNESS_PROPERTIES: ReadonlySet<string> = new Set([
   "window",
   "fallback",
]);

/** A property name inside a carried group that the persist line can hold bare. */
const PLAIN_PROPERTY = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** A table name `#@ persist name=` accepts (see persist_annotation_validation.ts). */
const PLAIN_TABLE_NAME = /^[A-Za-z0-9_-]+$/;

/**
 * The table name a `#(materialize)` source's build takes: the source's own
 * name when it is plain, else its plain characters (runs of the rest folded
 * to `_`) and a hash of the whole name, so a source named outside ASCII, or
 * with a space, is still built, and two names that fold alike never share a
 * table.
 */
export function materializeTableName(sourceName: string): string {
   if (PLAIN_TABLE_NAME.test(sourceName)) return sourceName;
   const folded = sourceName
      .replace(/[^A-Za-z0-9_-]+/g, "_")
      .replace(/^_+|_+$/g, "");
   const hash = createHash("sha256").update(sourceName).digest("hex");
   return `${folded || "source"}_${hash.slice(0, 8)}`;
}

/** Keys `#(materialize)` reads for the plan itself, not the persist line. */
const PLANNED_KEYS = ["inline"] as const;

/**
 * The code of the warning on a pair table whose `refresh="incremental"` is
 * not honoured: the table is rebuilt in full on every refresh, and served.
 */
const MATERIALIZE_PAIR_FULL_REFRESH_CODE = "materialize-pair-full-refresh";

/**
 * Every key the generator reads, which must be the route's closed set in the
 * annotation reader (`MATERIALIZE_KEYS`); a spec holds the two equal.
 */
export const GENERATOR_KEYS: readonly string[] = [
   ...CARRIED_KEYS,
   ...CARRIED_GROUPS,
   ...PLANNED_KEYS,
];

/** The code of a `#(materialize)` key outside the route's closed set. */
const MATERIALIZE_UNKNOWN_KEY_CODE = "materialize-unknown-key";

/** What a `watermark=[…]` list anywhere but an incremental pair table is told. */
const WATERMARK_LIST_REASON =
   "#(materialize) `watermark=` takes a list only on an incremental pair " +
   "table (an inlined join_many); name one dimension";

/** The code of a `#(materialize)` key the route no longer reads. */
const MATERIALIZE_RETIRED_KEY_CODE = "materialize-retired-key";

/** The code of a `#(materialize)` line that does not parse. */
const MATERIALIZE_UNPARSEABLE_CODE = "materialize-unparseable";

/** A declaration the generator could not plan, and why. */
export interface MaterializeRefusal {
   sourceName: string;
   sourceID?: string;
   reason: string;
   /**
    * `malformed`: the declaration is invalid (it does not parse, has an
    * unknown or retired key, a bad value or a contradictory combination), so
    * the finding is `malformed` under this code and fails the publish and the
    * load; `whenRelaxed` is its refusal where the deployment refuses it alone
    * instead (a collision under `PERSIST_COLLISION_ENFORCE=false`).
    * `refused`: a valid declaration Publisher cannot build, its reason and
    * tier in `refusedSources`.
    */
   outcome:
      | { malformed: string; whenRelaxed?: TierRefusal }
      | { refused: TierRefusal };
}

/** A refusal's reason, and the tier the table would have been built in. */
interface TierRefusal {
   reason: RefusedReason;
   tier: "database" | "storage";
}

/** The Malloy atomic types a stored column can hold. */
const ATOMIC_TYPES = new Set([
   "string",
   "number",
   "boolean",
   "date",
   "timestamp",
   "timestamptz",
   "json",
   "sql native",
]);

/**
 * The source's own `#(materialize)` declaration, read from the notes its own
 * statement declares (not inherited: an extension of a materialized source is
 * planned only when it declares its own). `contents` is the compiled model's
 * `contents`, `def` among them (see {@link declaredNotes}). Undefined when the
 * source declares none.
 */
function readMaterializeDeclaration(
   def: SourceDefLike,
   contents: Readonly<Record<string, unknown>> = {},
):
   | {
        tag: RouteTag;
        keys: string[];
        /** Parse errors: a declaration with any is not read at all. */
        errors: string[];
        /** A line read `@env.`, which the reader dropped. */
        readsEnv: boolean;
     }
   | undefined {
   const notes = declaredNotes(def, contents).filter(
      (note) => routeOfNote(note.text) === MATERIALIZE_ROUTE,
   );
   if (notes.length === 0) return undefined;
   const { tag, log } = readNotes(notes, MATERIALIZE_ROUTE);
   return {
      tag,
      keys: [...tag.entries()].map(([key]) => key),
      errors: log
         .filter((entry) => entry.severity === "error")
         .map((entry) => entry.message),
      readsEnv: log.some((entry) => entry.code === "env-reference-dropped"),
   };
}

/** Whether the source carries its own `#@ persist` (not an inherited one). */
function declaresOwnPersist(
   def: SourceDefLike,
   contents: Readonly<Record<string, unknown>>,
): boolean {
   return notesDeclarePersist(declaredNotes(def, contents));
}

/**
 * Whether the source is persisted through a `#@ persist` it INHERITS (it
 * extends or renames a persisted source, here or in an imported file) rather
 * than declares, and the source that declares it when this model holds it.
 * Read as Malloy reads it: the whole annotation chain decides whether the
 * source persists, and {@link declaredNotes} decides which of those notes its
 * own statement wrote. Undefined when it does not persist, or persists by its
 * own annotation (see {@link declaresOwnPersist}).
 */
function inheritedPersist(
   def: SourceDefLike,
   contents: Readonly<Record<string, unknown>>,
): { from?: string } | undefined {
   const chain = new Annotations(
      (def.annotations ?? {}) as ConstructorParameters<typeof Annotations>[0],
   );
   if (!declares(chain, "@", "persist")) return undefined;
   const own = declaredNotes(def, contents);
   const inherited = chain
      .forRoute("@")
      .filter(
         (note) =>
            !own.some(
               (o) => o.at && note.at && sameNotePosition(o.at, note.at),
            ),
      );
   if (inherited.length === 0) return undefined;
   for (const [name, object] of Object.entries(contents)) {
      if (object === def || !object || typeof object !== "object") continue;
      const other = object as SourceDefLike;
      if (!declaresOwnPersist(other, contents)) continue;
      const declares = declaredNotes(other, contents).some((o) =>
         inherited.some(
            (note) => o.at && note.at && sameNotePosition(o.at, note.at),
         ),
      );
      if (declares) return { from: name };
   }
   return {};
}

/**
 * The declaration's retired key (`PUBLISHER_ROUTES.materialize.retiredKeys`),
 * with what to do instead, as a malformed refusal, or undefined.
 */
function retiredKeyRefusal(
   declaration: NonNullable<ReturnType<typeof readMaterializeDeclaration>>,
): string | undefined {
   const retired = PUBLISHER_ROUTES.materialize?.retiredKeys ?? {};
   const key = declaration.keys.find((k) => Object.hasOwn(retired, k));
   if (key === undefined) return undefined;
   return `#(materialize) no longer reads \`${key}=\`; ${retired[key]}`;
}

/**
 * The declaration's key outside the route's closed set, as a malformed
 * refusal, or undefined. A misspelt key must never fall back to a default
 * (a misspelt `storage=` would build into the source's own warehouse), so
 * the source is not built, and the publish is refused.
 */
function unknownKeyRefusal(
   declaration: NonNullable<ReturnType<typeof readMaterializeDeclaration>>,
): string | undefined {
   const unknown = declaration.keys.filter((key) => !MATERIALIZE_KEYS.has(key));
   if (unknown.length === 0) return undefined;
   const near = unknown
      .map((k) => nearestKey(k, MATERIALIZE_KEYS))
      .find((k) => k !== undefined);
   return (
      `#(materialize) does not accept ${unknown.map((k) => `\`${k}=\``).join(", ")}; ` +
      `it reads ${[...CARRIED_KEYS, ...CARRIED_GROUPS, ...PLANNED_KEYS].join(", ")}` +
      (near ? ` (did you mean \`${near}=\`?)` : "")
   );
}

/**
 * The declaration's `#@ persist` keys by key path, or why one cannot be
 * carried: a value of the wrong shape (an array where a string goes would
 * otherwise be dropped silently). Unknown keys are refused first
 * ({@link unknownKeyRefusal}).
 */
function carriedPersistKeys(
   declaration: NonNullable<ReturnType<typeof readMaterializeDeclaration>>,
):
   | { persist: Record<string, string>; watermarks?: string[] }
   | { reason: string; code?: string } {
   const persist: Record<string, string> = {};
   let watermarks: string[] | undefined;
   for (const key of CARRIED_KEYS) {
      if (!declaration.keys.includes(key)) continue;
      // A list of watermark fields is a pair table's (see pairIncremental);
      // the caller refuses it on any other source.
      const list = key === "watermark" ? declaration.tag.array(key) : undefined;
      if (list !== undefined) {
         const names = list.map((i) => i.text());
         if (names.some((n) => typeof n !== "string" || n.trim() === "")) {
            return {
               reason:
                  "#(materialize) `watermark=` must be a string, or a list of " +
                  'field names such as watermark=["updated_at", "items.loaded_at"]',
            };
         }
         watermarks = [...new Set((names as string[]).map((n) => n.trim()))];
         continue;
      }
      const value = declaration.tag.text(key);
      if (value === undefined) {
         return { reason: `#(materialize) \`${key}=\` must be a string` };
      }
      persist[key] = value;
   }
   for (const group of CARRIED_GROUPS) {
      if (!declaration.keys.includes(group)) continue;
      const properties = [...(declaration.tag.tag(group)?.entries() ?? [])];
      if (properties.length === 0) {
         return {
            reason: `#(materialize) \`${group}\` takes properties, as \`${group}.<name>="…"\``,
         };
      }
      if (group === "freshness") {
         const problem = freshnessValueProblem(declaration.tag.tag(group)!);
         if (problem !== undefined)
            return { reason: `#(materialize) ${problem}` };
      }
      for (const [property, value] of properties) {
         const text = value.text();
         // `freshness` is a closed group: `window` and `fallback`.
         if (group === "freshness" && !FRESHNESS_PROPERTIES.has(property)) {
            return {
               reason: `#(materialize) \`freshness.${property}\` is not a key it reads; use freshness.window= or freshness.fallback=`,
               code: MATERIALIZE_UNKNOWN_KEY_CODE,
            };
         }
         if (!PLAIN_PROPERTY.test(property) || text === undefined) {
            return {
               reason: `#(materialize) \`${group}.${property}\` must be a plain name with a string value`,
            };
         }
         persist[`${group}.${property}`] = text;
      }
   }
   return watermarks ? { persist, watermarks } : { persist };
}

/**
 * A compiled field's name when it is an atomic dimension a table can hold,
 * public or not (see {@link hiddenAccess}).
 */
function storableDimension(field: FieldLike): string | undefined {
   const dimension =
      typeof field.as === "string"
         ? field.as
         : typeof field.name === "string"
           ? field.name
           : undefined;
   if (dimension === undefined) return undefined;
   if (field.join !== undefined) return undefined;
   if (typeof field.type !== "string" || !ATOMIC_TYPES.has(field.type)) {
      return undefined;
   }
   if (
      field.expressionType === "aggregate" ||
      field.expressionType === "analytic" ||
      field.expressionType === "ungrouped_aggregate"
   ) {
      return undefined;
   }
   return dimension;
}

/**
 * Whether a compiled field is an array or record its table can hold as it
 * is: a persisted query's `nest:` (a repeated record), or an array or record
 * (struct) column the source reads, none computed. A declared `join_one:` or
 * `join_many:` is the joined source's type, never `array` or `record`, so it
 * is not one. A hidden one is a candidate, as a hidden dimension is, marked
 * with its `access` by {@link materializeColumns}.
 */
function storableNested(field: FieldLike): string | undefined {
   if (field.type !== "array" && field.type !== "record") return undefined;
   if (field.e !== undefined) return undefined;
   const dimension =
      typeof field.as === "string"
         ? field.as
         : typeof field.name === "string"
           ? field.name
           : undefined;
   return dimension;
}

/**
 * The stored columns of a source: its atomic dimensions, arrays and records
 * ({@link storableNested}), less any whose value reads through a join that is
 * not inlined (a reference join is read at query time, so what reads through
 * it is computed at read). A field whose join usage is unknown is stored only
 * when the source has no join. `inline` names the inlined joins; `joins` comes
 * from the source map.
 *
 * A private or internal dimension is a candidate too, marked with its
 * `access`: whether the build can select it is settled on the build
 * copy (`materialize_compile.ts`), which drops the ones it cannot, such as a
 * field private on a source this one extends. One with a timeframe is stored
 * too: the served copy restores its timeframe with the same `__stored`
 * redefinition a public one gets, both statements carrying the field's own
 * modifier (`served_copy.ts`).
 */
export function materializeColumns(
   def: SourceDefLike,
   options: {
      joinReads?: ReadonlyMap<string, FieldJoinReads | undefined>;
      inline?: ReadonlySet<string>;
      hasJoins?: boolean;
      /** Dimensions the build leaves out because each depends on the caller. */
      leftOut?: ReadonlySet<string>;
      /** Stored computed dimensions the probe cleared of the time zone. */
      zoneFree?: ReadonlySet<string>;
   } = {},
): MaterializeColumn[] {
   const out: MaterializeColumn[] = [];
   const perRead = currentTimeFields(def.fields);
   // A dimension that reads a given is never stored, whoever asks.
   const perCaller = givenReadingFields(def.fields);
   for (const raw of def.fields ?? []) {
      const field = raw as FieldLike;
      const nested = storableNested(field);
      if (nested !== undefined) {
         const access = hiddenAccess(field);
         out.push({
            dimension: nested,
            column: nested,
            computed: false,
            ...(access ? { access } : {}),
         });
         continue;
      }
      const dimension = storableDimension(field);
      if (dimension === undefined) continue;
      if (perRead.has(dimension)) continue;
      if (field.e !== undefined && perCaller.has(dimension)) continue;
      if (options.leftOut?.has(dimension)) continue;
      const access = hiddenAccess(field);
      const computed = field.e !== undefined;
      if (computed && options.joinReads) {
         const reads = options.joinReads.get(dimension);
         if (reads === undefined) {
            if (options.hasJoins) continue;
         } else if (
            reads.nested ||
            [...reads.joins].some((j) => !options.inline?.has(j))
         ) {
            continue;
         }
      }
      out.push({
         dimension,
         column: dimension,
         computed,
         ...(computed && options.zoneFree?.has(dimension)
            ? { zoneFree: true as const }
            : {}),
         ...(access ? { access } : {}),
      });
   }
   return out;
}

/**
 * Which of a materialized source's computed dimensions depend on the caller
 *, from the probe compiler's answers (`probed`, by dimension; absent
 * when no probe ran) and the IR walks, and which stored ones the probe
 * cleared of the time zone. A caller-dependent dimension is left out when
 * the served copy keeps its definition: one the source's own declaration
 * writes. A given-reading one a base declares is a refusal: the served copy
 * could not compute it at read, so the source answers live. So is an inlined
 * join through which a given is read: its stored columns would hold one
 * caller's values. A zone-dependent one a base declares is stored as built.
 */
function callerDependentDimensions(
   def: SourceDefLike,
   probed: ReadonlyMap<string, FieldDependence> | undefined,
   inline: readonly string[] = [],
):
   | { leftOut: LeftOutDimension[]; zoneFree: Set<string> }
   | { reason: string; refusal: RefusedReason } {
   const givenIR = givenReadingFields(def.fields);
   for (const j of inline) {
      // Every level of a path (`items.options`) is stored, its rows and its
      // fields, so a given read at any level holds one caller's values.
      const levels = j
         .split(".")
         .map((_, i, all) => all.slice(0, i + 1).join("."));
      if (
         levels.some(
            (level) =>
               givenIR.has(level) ||
               [...givenIR].some((path) => path.startsWith(`${level}.`)),
         )
      ) {
         return {
            reason:
               `inline= names ${quotedName(j)}, through which a given is read, ` +
               "so its stored columns would hold one caller's values; " +
               "reference the join instead",
            refusal: "reads_given",
         };
      }
   }
   const timeIR = currentTimeFields(def.fields);
   const declaration = declarationOf(def);
   // The served copy keeps the field's definition only when the source's own
   // declaration writes it.
   const kept = (field: FieldLike): boolean => {
      const at = (
         field as { location?: { url?: unknown; range?: MalloyRange } }
      ).location;
      return (
         typeof at?.url === "string" &&
         at.range !== undefined &&
         withinDeclaration([{ url: at.url, range: at.range }], declaration)
      );
   };
   const leftOut: LeftOutDimension[] = [];
   const zoneFree = new Set<string>();
   for (const raw of def.fields ?? []) {
      const field = raw as FieldLike;
      const dimension = storableDimension(field);
      if (dimension === undefined || field.e === undefined) continue;
      const probe = probed?.get(dimension);
      const reads: CallerInput[] = [];
      if (givenIR.has(dimension) || (probe?.givens?.length ?? 0) > 0) {
         reads.push("given");
      }
      if (timeIR.has(dimension) || probe?.currentTime === true) {
         reads.push("current_time");
      }
      if (probe?.timeZone === true) reads.push("time_zone");
      const own = reads.length > 0 && kept(field);
      if (reads.includes("given") && !own) {
         return {
            reason: dimensionNotOwnReason(dimension, "a given"),
            refusal: "inherited_where_with_given",
         };
      }
      if (reads.includes("current_time") && !own) {
         return {
            reason: dimensionNotOwnReason(dimension, "the current time"),
            refusal: "current_time",
         };
      }
      const perRead =
         reads.includes("given") ||
         reads.includes("current_time") ||
         (reads.includes("time_zone") && own);
      if (perRead) {
         leftOut.push({ dimension, reads });
      } else if (reads.length === 0 && callerIndependent(probe)) {
         zoneFree.add(dimension);
      }
   }
   return { leftOut, zoneFree };
}

/** {@link givenWhereNotOwnReason}, for a dimension that reads a given or the current time. */
function dimensionNotOwnReason(
   dimension: string,
   reads: "a given" | "the current time",
): string {
   return (
      `the dimension ${quotedName(dimension)} reads ${reads} and is not ` +
      "declared on this source, so it could not be computed over the " +
      "stored table at read; the source is answered live. To serve it " +
      `from a table, declare the ${reads === "a given" ? "given-reading" : "current-time"} ` +
      "dimension on the materialized source itself"
   );
}

/** A dimension's Malloy type on a compiled source, or "" when unknown. */
function columnType(def: SourceDefLike, dimension: string): string {
   for (const raw of def.fields ?? []) {
      const field = raw as FieldLike;
      const name = typeof field.as === "string" ? field.as : field.name;
      if (name === dimension && typeof field.type === "string") {
         return field.type;
      }
   }
   return "";
}

/**
 * The columns of one inlined join's joined source: its public atomic
 * dimensions (never a hidden one) that read no join of their own (one hop),
 * each stored as `<join>__<field>`, or, for a join further down an inlined
 * path of join_manys, as `<join>__<next join>__…__<field>` (`path` names the
 * joins from the source to this one). When the join itself is private or
 * internal on the source, `access` is its modifier and each column carries
 * it: the build copy lifts the join so its build can read it, and the
 * served copy's record keeps the join's modifier, so a caller reads
 * `<join>.<field>` exactly where live lets it.
 */
function inlinedJoinColumns(
   map: SourceMap,
   join: SourceJoin,
   path: readonly string[] = [join.name],
   access?: "private" | "internal",
): MaterializeColumn[] {
   const joined = map.joinedReadsOf(join);
   const reads = joined.fieldJoins;
   const hasJoins = joined.joins.length > 0;
   // Computed at read, like the source's own (Rolling windows).
   const perRead = currentTimeFields(join.fields);
   const out: MaterializeColumn[] = [];
   for (const raw of join.fields) {
      const field = raw as FieldLike;
      const dimension = storableDimension(field);
      if (dimension === undefined) continue;
      // The joined source's exposed dimensions only: its hidden fields are
      // its own, not this source's to store.
      if (hiddenAccess(field)) continue;
      if (perRead.has(dimension)) continue;
      if (field.e !== undefined) {
         const r = reads.get(dimension);
         if (r === undefined ? hasJoins : r.joins.size > 0 || r.nested) {
            continue;
         }
      }
      out.push({
         dimension,
         column: `${path.join("__")}__${dimension}`,
         computed: true,
         join: path[0],
         ...(path.length > 1 ? { path: [...path] } : {}),
         ...(access ? { access } : {}),
      });
   }
   return out;
}

/** The Malloy types a pair table can store as a typed null (`null::<type>`). */
const NULLABLE_TYPES: ReadonlySet<string> = new Set([
   "string",
   "number",
   "boolean",
   "date",
   "timestamp",
   "timestamptz",
]);

/**
 * The joins an `inline=` entry names, from the source down (`"items"`, or a
 * path of join_manys, `"items.options"`), or why it cannot be inlined. The
 * first is checked as {@link inlineRefusal} checks any inlined join; each one
 * past it must be a join_many of the joined source before it, declared as a
 * source, on the source's connection, keyed on that joined source's own
 * columns. A path through a join_one is refused: live aggregates a join_many
 * past a join_one once per row of that join_many however many parents share
 * the joined row (a symmetric aggregate on its key), which no flat row
 * reproduces.
 */
function inlinedPathOf(
   entry: string,
   joins: readonly SourceJoin[],
   map: SourceMap,
   connection: unknown,
): { path: SourceJoin[] } | { reason: string; invalid?: true } {
   const names = entry.split(".");
   const quoted = quotedName(entry);
   // Names no join: invalid, whatever the model is.
   if (names.some((n) => n === "") || !joins.some((j) => j.name === names[0])) {
      return {
         reason: `inline= names ${quoted}, which is not a join or a path of joins of this source`,
         invalid: true,
      };
   }
   const first = inlineRefusal(names[0], joins, connection);
   if (first) return { reason: first };
   const path = [joins.find((j) => j.name === names[0]) as SourceJoin];
   if (names.length === 1) return { path };
   const throughOne =
      `inline= names ${quoted}, a path through a join_one; only a path of ` +
      "join_manys is stored as a pair table (live counts a row past a " +
      "join_one once however many parents share it, which no stored row " +
      "reproduces), so inline the join_one alone";
   if (path[0].relationship !== "many") return { reason: throughOne };
   for (let k = 1; k < names.length; k++) {
      const prev = path[k - 1];
      const next = map
         .joinedReadsOf(prev)
         .joins.find((j) => j.name === names[k]);
      if (!next) {
         return {
            reason: `inline= names ${quoted}, and ${quotedName(names[k])} is not a join of the source ${quotedName(prev.name)} joins`,
            invalid: true,
         };
      }
      if (next.relationship !== "many") return { reason: throughOne };
      // A join the joined source hides is read by no source joining it, the
      // build's included, so the build's query would not compile, and with it
      // the companion every `#(materialize)` source of the model builds from.
      const hidden = prev.fields.some((raw) => {
         const f = raw as FieldLike;
         return (
            f.join !== undefined &&
            (f.as ?? f.name) === names[k] &&
            hiddenAccess(f) !== undefined
         );
      });
      if (hidden) {
         return {
            reason:
               `inline= names ${quoted}, whose ${quotedName(names[k])} is not ` +
               `public on the source ${quotedName(prev.name)} joins, so no ` +
               "source joining it reads through it",
         };
      }
      if (
         typeof connection === "string" &&
         next.connection !== undefined &&
         next.connection !== connection
      ) {
         return {
            reason:
               `inline= names ${quoted}, whose ${quotedName(names[k])} reads ` +
               "another connection than the source, so one build query cannot read both",
         };
      }
      const through = next.condition
         ? [...next.condition.joins].filter((j) => j !== next.name)
         : undefined;
      if (!through || through.length > 0 || next.condition?.nested) {
         return {
            reason:
               `inline= names ${quoted}, whose ${quotedName(names[k])} is keyed ` +
               "on another join than the one before it; inline only a path " +
               "whose every join is keyed on the joined source before it",
         };
      }
      if (!next.sourceID || !splitSourceID(next.sourceID)) {
         return {
            reason:
               `inline= names ${quoted}, whose ${quotedName(names[k])} joins a ` +
               "source defined in place; declare it as a source so the served " +
               "copy can read it from the pair table",
         };
      }
      path.push(next);
   }
   return { path };
}

/** A name as a refusal message shows it, in backticks. */
function quotedName(name: string): string {
   return "`" + name + "`";
}

/**
 * Why `inline=` cannot inline the join named `name` on a source, or undefined
 * when it can: a join_one or join_many on the source's own connection, its
 * condition reading no other join, joining a declared source; a join_one keyed
 * by one equality on a column of it (what the uniqueness check groups by).
 * What a join_many needs beyond this is {@link pairRefusal}'s.
 */
function inlineRefusal(
   name: string,
   joins: readonly SourceJoin[],
   connection: unknown,
): string | undefined {
   const join = joins.find((j) => j.name === name);
   if (!join) {
      return `inline= names \`${name}\`, which is not a join of this source`;
   }
   if (join.relationship === "cross") {
      return (
         `inline= names \`${name}\`, a join_cross: inlining a join_cross ` +
         "arrives in a later release"
      );
   }
   if (
      typeof connection === "string" &&
      join.connection !== undefined &&
      join.connection !== connection
   ) {
      return (
         `inline= names \`${name}\`, which reads another connection than the ` +
         "source, so one build query cannot read both"
      );
   }
   const through = join.condition
      ? [...join.condition.joins].filter((j) => j !== name)
      : undefined;
   if (!through || through.length > 0 || join.condition?.nested) {
      return (
         `inline= names \`${name}\`, whose condition reads ` +
         (through && through.length > 0
            ? `another join (${through.map(quotedName).join(", ")})`
            : "another join") +
         ", so a source row could be stored once per row of that join; " +
         "inline only a join keyed on the source's own columns"
      );
   }
   if (join.relationship === "one" && !join.keyColumn) {
      return (
         `inline= names \`${name}\`, whose condition is not one equality on a ` +
         "column of the joined source, so its uniqueness cannot be checked"
      );
   }
   if (!join.sourceID || !splitSourceID(join.sourceID)) {
      return (
         `inline= names \`${name}\`, which joins a source defined in place; ` +
         (join.relationship === "one"
            ? "declare it as a source so its uniqueness can be checked at build"
            : "declare it as a source so the served copy can read it from the pair table")
      );
   }
   return undefined;
}

/**
 * Why an inlined path of join_manys cannot be stored as a pair table, or its
 * layout (see the module comment for each rule). `path` is the path's joins,
 * the source's own first; `columns` are the source's own stored columns;
 * `joined` every inlined join's columns.
 */
function pairRefusal(
   path: readonly SourceJoin[],
   def: SourceDefLike,
   map: SourceMap,
   columns: readonly MaterializeColumn[],
   joined: readonly MaterializeColumn[],
): { pair: MaterializePair } | { reason: string } {
   const join = path[0];
   const names = path.map((j) => j.name);
   const name = quotedName(names.join("."));
   const parentKey =
      typeof def.primaryKey === "string" ? def.primaryKey : undefined;
   if (!parentKey || !columns.some((c) => c.column === parentKey)) {
      return {
         reason:
            `inline= names ${name}, a join_many, and a pair table needs the ` +
            "source's own primary key, declared and stored (a public " +
            "dimension), to keep one row per parent",
      };
   }
   for (let k = 0; k < path.length; k++) {
      const key = path[k].primaryKey;
      const prefix = names.slice(0, k + 1).join("__");
      if (!key || !joined.some((c) => c.column === `${prefix}__${key}`)) {
         return {
            reason:
               `inline= names ${name}, a join_many, and a pair table needs the ` +
               (path.length > 1
                  ? `primary key of ${quotedName(names[k])}'s joined source, `
                  : "joined source's primary key, ") +
               "declared and stored (a public dimension), to keep one row per " +
               (k === path.length - 1 ? "child" : "row of that level"),
         };
      }
      const own = map.joinedReadsOf(path[k]).joins;
      const others = own.filter((j) => j.name !== names[k + 1]);
      if (others.length > 0) {
         return {
            reason:
               path.length === 1
                  ? `inline= names ${name}, a join_many whose joined source has joins ` +
                    "of its own; a pair table stores one hop, so inline only a " +
                    "join_many to a source with no joins"
                  : `inline= names ${name}, and ${quotedName(names[k])}'s joined ` +
                    `source has joins off the path (${others.map((j) => quotedName(j.name)).join(", ")}); ` +
                    "a pair table stores only the joins it names",
         };
      }
   }
   const taken = new Set([...columns, ...joined].map((c) => c.column));
   const flags = [
      PAIR_PARENT_ROW,
      PAIR_WATERMARK,
      ...path.slice(1).map((_, k) => pairLevelRowColumn(k + 1)),
   ];
   const clash = flags.find((c) => taken.has(c));
   if (clash) {
      return {
         reason:
            `the pair table's \`${clash}\` column has the name of one of the ` +
            "source's dimensions",
      };
   }
   // A middle level whose join, and every join above it, is keyed on its
   // parent level's primary key: each of its rows has one parent row.
   const oneParentLevels: number[] = [];
   for (let k = 1; k < path.length; k++) {
      const parentLevelKey = k === 1 ? parentKey : path[k - 2].primaryKey;
      const keyed =
         path[k - 1].baseKey !== undefined &&
         path[k - 1].baseKey === parentLevelKey;
      if (!keyed) break;
      oneParentLevels.push(k);
   }
   return {
      pair: {
         join: join.name,
         parentKey,
         childKey: join.primaryKey as string,
         childSourceID: join.sourceID as string,
         parentRowColumn: PAIR_PARENT_ROW,
         ...(oneParentLevels.length > 0 ? { oneParentLevels } : {}),
         ...(path.length > 1
            ? {
                 hops: path.slice(1).map((j, k) => ({
                    join: j.name,
                    key: j.primaryKey as string,
                    sourceID: j.sourceID as string,
                    rowColumn: pairLevelRowColumn(k + 1),
                 })),
              }
            : {}),
      },
   };
}

/**
 * The blocks of a pair table that stores more than one inlined path, or why
 * it cannot: every block's columns stored on every row (the other blocks' as
 * typed nulls, so each joined column needs a type a null can take), no block
 * column or level flag named like a stored column, and no incremental
 * refresh.
 */
function pairBlocksOf(
   pairs: readonly MaterializePair[],
   paths: readonly (readonly SourceJoin[])[],
   columns: readonly MaterializeColumn[],
   connection: unknown,
): { blocks: MaterializePairBlocks } | { reason: string } {
   const listed = paths.map((p) => quotedName(p.map((j) => j.name).join(".")));
   if (typeof connection !== "string") {
      return {
         reason:
            `inline= names more than one join_many (${listed.join(", ")}), and ` +
            "the source's connection is unknown, which the pair table's build reads",
      };
   }
   const columnTypes: Record<string, string> = {};
   for (const path of paths) {
      for (let k = 0; k < path.length; k++) {
         const prefix = path.slice(0, k + 1).map((j) => j.name);
         for (const c of columns) {
            if (c.join !== prefix[0]) continue;
            const at = c.path ?? [c.join];
            if (at.join(".") !== prefix.join(".")) continue;
            const type = joinedColumnType(path[k], c.dimension);
            if (!NULLABLE_TYPES.has(type)) {
               return {
                  reason:
                     `inline= names more than one join_many (${listed.join(", ")}), ` +
                     "so each one's columns are stored as nulls on the other's " +
                     `rows, and \`${c.column}\` has a type no null can take` +
                     (type ? ` (${type})` : ""),
               };
            }
            columnTypes[c.column] = type;
         }
      }
   }
   const levels = Math.max(...pairs.map((p) => 1 + (p.hops?.length ?? 0)));
   const taken = new Set(columns.map((c) => c.column));
   const clash = [
      PAIR_LANE_COLUMN,
      PAIR_BLOCK_WATERMARK,
      ...Array.from({ length: levels - 1 }, (_, k) =>
         pairLevelRowColumn(k + 1),
      ),
   ].find((c) => taken.has(c));
   if (clash) {
      return {
         reason:
            `the pair table's \`${clash}\` column has the name of one of the ` +
            "source's dimensions",
      };
   }
   return {
      blocks: {
         blocks: [...pairs],
         laneColumn: PAIR_LANE_COLUMN,
         connection,
         columnTypes,
         levels,
      },
   };
}

/** A joined source's field's Malloy type, or "" when it has none. */
function joinedColumnType(join: SourceJoin, dimension: string): string {
   for (const raw of join.fields) {
      const field = raw as FieldLike;
      const name = typeof field.as === "string" ? field.as : field.name;
      if (name === dimension && typeof field.type === "string") {
         return field.type;
      }
   }
   return "";
}

/** The Malloy types a pair table's watermark can be built over. */
const PAIR_WATERMARK_TYPES: ReadonlySet<string> = new Set([
   "date",
   "timestamp",
   "number",
   "string",
]);

/**
 * How an incremental pair table refreshes, or why it is rebuilt in full.
 *
 * The unit a refresh replaces is a whole parent: every pair row of each
 * parent whose watermark moved is deleted by the parent key and written
 * again, so `__parent_row` stays exactly one row per parent. The watermark
 * is {@link PAIR_WATERMARK}, per parent the greatest of the named fields
 * over the parent's row and all its child rows: the same value on every row
 * of one parent, so a watermark range selects whole parents, and the flag
 * the build's window writes over them is the full build's. A child-only
 * change (a child added to a stored parent, a late-arriving child, a child
 * restated in place) is seen only through a child field that moves with it,
 * so the declaration must name one: `watermark=["updated_at",
 * "segments.loaded_at"]`. Without one, or without a field of the source's own
 * (a new childless parent would not move it), the table is rebuilt in full,
 * with a warning. So too for a `merge_key=` other than the source's primary
 * key (the unit is the parent), and a rolling window over a child field
 * (its delete would remove part of a parent).
 *
 * A path or a table of several blocks needs a field at every level of every
 * path: a row added at a level with nothing below it moves its parent only
 * through its own level's field. Each level's row flag partitions on the keys
 * from the parent down, so replacing whole parents keeps it exact too.
 *
 * Returns nothing to do when no watermark is named: the incremental gate
 * refuses that as it refuses it on any source.
 */
function pairIncremental(args: {
   /** The table's paths: one, or one per block of a table of several. */
   pairs: readonly MaterializePair[];
   persist: Readonly<Record<string, string>>;
   /**
    * The watermark fields as declared: a source field, or a path to a field
    * of a level of the inlined path (`items.loaded_at`,
    * `items.options.loaded_at`).
    */
   names: readonly string[];
   columns: readonly MaterializeColumn[];
   columnTypeOf: (c: MaterializeColumn) => string;
   /** The stored columns the source's rolling windows read. */
   windowFields: readonly string[];
}):
   | { watermark: { column: string; parts: string[] } }
   | { fullRefresh: string }
   | { none: true } {
   const { pairs, names, columns } = args;
   const [pair] = pairs;
   if (names.length === 0 || !pair) return { none: true };
   const paths = pairs.map((p) => [
      p.join,
      ...(p.hops ?? []).map((h) => h.join),
   ]);
   const join = quotedName(pairs.map((p) => p.join).join("`, `"));
   const pathName = paths.map((p) => quotedName(p.join("."))).join(", ");
   // The level a stored column sits at: 0 for the source's own, k for a
   // path's k-th join's joined source.
   const levelOf = (c: MaterializeColumn): number => {
      if (c.join === undefined) return 0;
      const at = c.path ?? [c.join];
      return paths.some((path) => at.every((n, i) => path[i] === n))
         ? at.length
         : -1;
   };
   const parts: MaterializeColumn[] = [];
   for (const name of names) {
      const segments = name.split(".");
      const field = segments[segments.length - 1];
      const at = segments.slice(0, -1);
      const column = columns.find(
         (c) =>
            c.dimension === field &&
            (at.length === 0
               ? c.join === undefined
               : levelOf(c) === at.length &&
                 (c.path ?? [c.join]).join(".") === at.join(".")),
      );
      if (!column) {
         return {
            fullRefresh:
               `watermark= names \`${name}\`, which is not a stored dimension ` +
               `of the source or of a level of the inlined path ${pathName}`,
         };
      }
      parts.push(column);
   }
   if (!parts.some((c) => c.join !== undefined)) {
      return {
         fullRefresh:
            `watermark= names only the source's own fields, so a child added ` +
            `to a stored parent through ${join} would not move it and would ` +
            `never be stored; name a field of ${join} that moves with each ` +
            `child as well, such as watermark=["${names[0]}", ` +
            `"${pair.join}.loaded_at"]`,
      };
   }
   if (!parts.some((c) => c.join === undefined)) {
      return {
         fullRefresh:
            `watermark= names only fields of ${join}, so a new parent with ` +
            `no children would not move it; name one of the source's own ` +
            `fields as well`,
      };
   }
   // A row added at a level moves its parent only through a field of that
   // level: a new middle row with no rows below it carries nulls in every
   // deeper column. Each block's path, each of its levels.
   for (const path of paths) {
      for (let level = 1; level <= path.length; level++) {
         const at = path.slice(0, level).join(".");
         if (parts.some((c) => (c.path ?? [c.join]).join(".") === at)) continue;
         return {
            fullRefresh:
               `watermark= names no field of \`${at}\`, so a row added there ` +
               `under a stored parent would not move the parent's watermark and ` +
               `would never be stored; name one of its fields as well, such as ` +
               `"${at}.loaded_at"`,
         };
      }
   }
   const types = new Set(parts.map((c) => args.columnTypeOf(c)));
   const [type] = [...types];
   if (types.size !== 1 || !PAIR_WATERMARK_TYPES.has(type)) {
      return {
         fullRefresh:
            `the watermark= fields must all be of one type, a date, ` +
            `timestamp, number or string (they are ${[...types]
               .map((t) => t || "unknown")
               .join(", ")})`,
      };
   }
   const mergeKey = args.persist.merge_key;
   if (mergeKey !== undefined && mergeKey.trim() !== pair.parentKey) {
      return {
         fullRefresh:
            `merge_key= names \`${mergeKey}\`; a pair table replaces whole ` +
            `parents, by the source's primary key \`${pair.parentKey}\`, so ` +
            `name that or leave merge_key= out`,
      };
   }
   const childWindow = args.windowFields.find((f) =>
      columns.some((c) => c.column === f && c.join !== undefined),
   );
   if (childWindow !== undefined) {
      return {
         fullRefresh:
            `its rolling window reads \`${childWindow}\` of ${join}, and ` +
            `deleting below it would remove part of a parent`,
      };
   }
   return {
      watermark: {
         column: PAIR_WATERMARK,
         parts: [...new Set(parts.map((c) => c.column))],
      },
   };
}

/**
 * Plan the `#(materialize)` sources a compiled model's `contents` declares,
 * sorted by source name so the generated text does not depend on declaration
 * order. Only a source DECLARED in this model is planned (`declaredHere`
 * decides from its `sourceID`'s file): a source another model imports or
 * re-exports through `index.malloy` is that file's to build, so a source has
 * one build source, one address and one table however many models import it.
 * Without `declaredHere` every source with a declaration is planned. Only a
 * source whose own statement declares `#(materialize)` is planned, never an
 * extension or rename that merely inherits its base's (see
 * {@link declaredNotes}).
 */
export function planModelMaterialize(
   contents: Record<string, unknown>,
   declaredHere?: (declaringURL: string) => boolean,
   givens?: Record<string, unknown>,
   /**
    * The probe compiler's answers for each source's computed dimensions, by
    * `sourceID` then dimension. Absent: the IR walks alone decide, and
    * no stored dimension is cleared of the time zone.
    */
   probed?: ReadonlyMap<string, ReadonlyMap<string, FieldDependence>>,
   /**
    * The sources, by `sourceID`, whose rows the probe compiler found the same
    * in every query time zone. Absent, or a source not in it: a
    * `timezone:` query over its table is answered live.
    */
   zoneFreeRows?: ReadonlySet<string>,
): {
   plans: MaterializePlan[];
   refused: MaterializeRefusal[];
} {
   const plans: MaterializePlan[] = [];
   const refused: MaterializeRefusal[] = [];
   // The joins each source has, read through the source map.
   const map = SourceMap.of({ contents });
   const ctx: PlanContext = { contents, map, givens, probed, zoneFreeRows };
   // A plain code-unit order, not localeCompare: the generated text must not
   // depend on the host's locale.
   for (const [name, object] of Object.entries(contents).sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
   )) {
      if (!object || typeof object !== "object") continue;
      const def = object as SourceDefLike;
      if (typeof def.sourceID !== "string") continue;
      const declaredIn = splitSourceID(def.sourceID)?.url;
      if (declaredHere && declaredIn && !declaredHere(declaredIn)) continue;
      const planned = planSource(name, def as PlannedDef, ctx);
      if (planned instanceof Declined) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: planned.reason,
            outcome:
               "malformed" in planned.outcome
                  ? planned.outcome
                  : {
                       refused: {
                          reason: planned.outcome.refused,
                          tier: declaredTier(def, ctx.contents),
                       },
                    },
         });
      } else if (planned) {
         plans.push(planned);
      }
   }
   chainExtensionTables(plans, map);
   return { plans, refused };
}

/** What planning one source reads beyond the source itself. */
interface PlanContext {
   contents: Record<string, unknown>;
   map: SourceMap;
   givens?: Record<string, unknown>;
   probed?: ReadonlyMap<string, ReadonlyMap<string, FieldDependence>>;
   zoneFreeRows?: ReadonlySet<string>;
}

/** A source def the planner has checked carries its `sourceID`. */
type PlannedDef = SourceDefLike & { sourceID: string };

/**
 * Why one source is not planned: an invalid declaration's malformed code,
 * which a publish refuses it with, or a valid one's refusal reason.
 */
class Declined {
   constructor(
      readonly reason: string,
      readonly outcome: { malformed: string } | { refused: RefusedReason },
   ) {}
}

/** Where a declaration that is not planned would have built its table. */
function declaredTier(
   def: SourceDefLike,
   contents: Record<string, unknown>,
): "database" | "storage" {
   try {
      const declaration = readMaterializeDeclaration(def, contents);
      const placed = declaration ? placementOf(declaration.tag) : undefined;
      return tierOf(
         placed && !("refused" in placed) && placed.placement.tier === "storage"
            ? placed.placement.destination
            : undefined,
      );
   } catch {
      return "database";
   }
}

/** One stage of planning a source: its value, or why the source is not planned. */
type Staged<T> = T | Declined;

/**
 * Plan one source, stage by stage, each of which may decline it: its
 * declaration, its inlined joins, its stored columns, its read-time terms,
 * its placement and table name, and a pair table's layout. Undefined for a
 * source that declares no `#(materialize)`.
 */
function planSource(
   name: string,
   def: PlannedDef,
   ctx: PlanContext,
): Staged<MaterializePlan> | undefined {
   const declaration = materializeDeclarationOf(def, ctx.contents);
   if (declaration === undefined || declaration instanceof Declined) {
      return declaration;
   }
   const invalid = invalidValuesOf(declaration);
   if (invalid) return invalid;
   const inlined = inlinedJoinsOf(def, declaration, ctx.map);
   if (inlined instanceof Declined) return inlined;
   const stored = storedColumnsOf(def, inlined, ctx);
   if (stored instanceof Declined) return stored;
   const { columns } = stored;
   const terms = readTermsOf(def, columns, inlined.inline, ctx.givens);
   if (terms instanceof Declined) return terms;
   const placed = placedPersistOf(name, def, declaration);
   if (placed instanceof Declined) return placed;
   const { persist } = placed;
   const layout = pairLayoutOf(def, ctx.map, {
      paths: inlined.paths,
      columns,
      joinedColumns: stored.joinedColumns,
      persist,
      watermarks: placed.watermarks,
      windowFields: terms.window.terms.map((t) => t.window.field),
   });
   if (layout instanceof Declined) return layout;
   const { pair, pairBlocks, pairFullRefresh } = layout;
   const build = buildSourceName(name);
   const uniquenessChecks = [
      ...inlined.ones.map((j): InlineUniquenessCheck => {
         const join = inlined.joins.find((x) => x.name === j) as SourceJoin;
         return {
            join: j,
            keyColumn: join.keyColumn as string,
            queryName: `${build}__unique__${j}`,
            joinedSourceID: join.sourceID as string,
            ...(join.connection ? { connectionName: join.connection } : {}),
         };
      }),
      ...(pair
         ? pairKeyChecks(
              pair,
              pairBlocks,
              def.sourceID,
              build,
              typeof def.connection === "string" ? def.connection : undefined,
           )
         : []),
   ];
   const { window, readTime } = terms;
   const { inline } = inlined;
   const { dependence } = stored;
   return {
      sourceName: name,
      sourceID: def.sourceID,
      buildSourceName: build,
      columns,
      persist,
      ...(window.terms.length + readTime.terms.length > 0
         ? { readTimeTerms: [...window.terms, ...readTime.terms] }
         : {}),
      ...(readTime.terms.length > 0
         ? {
              declaration: readTime.declaration,
              givenDeclarations: readTime.givenDeclarations,
           }
         : columns.some((c) => c.access)
           ? withDeclaration(declarationOf(def))
           : {}),
      ...(inline.length > 0 ? { inline } : {}),
      ...(dependence.leftOut.length > 0 ? { leftOut: dependence.leftOut } : {}),
      ...(ctx.zoneFreeRows?.has(def.sourceID)
         ? { zoneFreeRows: true as const }
         : {}),
      ...(uniquenessChecks.length > 0 ? { uniquenessChecks } : {}),
      ...(pair ? { pair } : {}),
      ...(pairBlocks ? { pairBlocks } : {}),
      ...(pairFullRefresh ? { pairFullRefresh } : {}),
      ...(declaration.keys.includes("name") ? {} : { nameDefaulted: true }),
   };
}

type ReadDeclaration = NonNullable<
   ReturnType<typeof readMaterializeDeclaration>
>;

/**
 * The source's own `#(materialize)` declaration, read and checked for what
 * refuses it before anything else: a declaration that does not parse, a
 * retired or unknown key (malformed, refused at publish), a source that also
 * carries or inherits `#@ persist`, and a deferred key.
 */
function materializeDeclarationOf(
   def: PlannedDef,
   contents: Record<string, unknown>,
): Staged<ReadDeclaration> | undefined {
   let declaration;
   try {
      declaration = readMaterializeDeclaration(def, contents);
   } catch (error) {
      return new Declined(
         `#(materialize) does not parse: ${error instanceof Error ? error.message : String(error)}`,
         { malformed: MATERIALIZE_UNPARSEABLE_CODE },
      );
   }
   if (!declaration) return undefined;
   if (declaration.errors.length > 0) {
      return new Declined(
         `#(materialize) does not parse: ${declaration.errors.join("; ")}`,
         { malformed: MATERIALIZE_UNPARSEABLE_CODE },
      );
   }
   // Before any other refusal: a retired key, or one outside the closed
   // set, is malformed (it fails the load), whatever else the source would
   // be refused for.
   const retiredKey = retiredKeyRefusal(declaration);
   if (retiredKey)
      return new Declined(retiredKey, {
         malformed: MATERIALIZE_RETIRED_KEY_CODE,
      });
   const unknownKey = unknownKeyRefusal(declaration);
   if (unknownKey)
      return new Declined(unknownKey, {
         malformed: MATERIALIZE_UNKNOWN_KEY_CODE,
      });
   // Before anything else is read: the line's directives were dropped, so
   // building it would put a table where its author did not say.
   if (declaration.readsEnv) {
      return new Declined(ENV_REFERENCE_REASON, {
         malformed: MATERIALIZE_INVALID_CODE,
      });
   }
   if (declaresOwnPersist(def, contents)) {
      return new Declined(
         "the source also carries its own #@ persist; declare one or the other",
         { malformed: MATERIALIZE_INVALID_CODE },
      );
   }
   // A source that inherits `#@ persist` is already its base's table under
   // v1 (an extension is its base's table): the plan holds it under its
   // own sourceID, so a generated build source presented as it would
   // overwrite that entry and read itself in the build graph.
   const inherited = inheritedPersist(def, contents);
   if (inherited) {
      return new Declined(
         // malloy-text: a refusal for a person, not Malloy
         `the source inherits #@ persist from ${inherited.from ? `\`${inherited.from}\`` : "the source it is built on"}, ` +
            "so it is already stored as that source's table; put #(materialize) on a " +
            "source that does not inherit #@ persist, or drop the #@ persist",
         { refused: "inherited_persist" },
      );
   }
   return declaration;
}

/**
 * Why the values a declaration carries make it invalid, read before any
 * refusal of a valid declaration so an invalid one always fails the load: an
 * `inline=` that is not a list of names, a carried key of the wrong shape, a
 * `storage=` that names nothing, a `name=` no table can take, and a list of
 * watermarks off an incremental pair table.
 */
function invalidValuesOf(declaration: ReadDeclaration): Declined | undefined {
   if (declaration.keys.includes("inline")) {
      const items = declaration.tag.array("inline")?.map((i) => i.text());
      if (!items || items.some((i) => typeof i !== "string" || i === "")) {
         return new Declined(
            'inline= must be a list of join names, such as inline=["carriers"]',
            { malformed: MATERIALIZE_INVALID_CODE },
         );
      }
   }
   const carried = carriedPersistKeys(declaration);
   if ("reason" in carried) {
      return new Declined(carried.reason, {
         malformed: carried.code ?? MATERIALIZE_INVALID_CODE,
      });
   }
   if ("refused" in placementOf(declaration.tag)) {
      return new Declined(EMPTY_STORAGE_REASON, {
         malformed: MATERIALIZE_INVALID_CODE,
      });
   }
   const namedTable = carried.persist.name?.trim();
   const nameProblem = namedTable ? tableNameProblem(namedTable) : undefined;
   if (nameProblem)
      return new Declined(nameProblem, { malformed: MATERIALIZE_INVALID_CODE });
   // A list of watermarks is a pair table's: it needs an incremental refresh
   // and an inlined join (the pair, checked once laid out).
   const inlines =
      (declaration.tag.array("inline")?.length ?? 0) > 0 &&
      declaration.keys.includes("inline");
   if (
      carried.watermarks &&
      !(carried.persist.refresh === "incremental" && inlines)
   ) {
      return new Declined(WATERMARK_LIST_REASON, {
         malformed: MATERIALIZE_INVALID_CODE,
      });
   }
   return undefined;
}

/** The joins a declaration's `inline=` names, resolved against the source. */
interface InlinedJoins {
   /** The entries of `inline=`, deduplicated. */
   inline: string[];
   /** Every join the source declares. */
   joins: SourceJoin[];
   /** The inlined join_ones, by name. */
   ones: string[];
   /** One path of join_manys below each join_many inlined (a pair table). */
   paths: SourceJoin[][];
   /** A private or internal join's access, by name. */
   joinAccess: Map<string, "private" | "internal">;
}

/** The joins `inline=` names, or why they cannot be inlined. */
function inlinedJoinsOf(
   def: PlannedDef,
   declaration: ReadDeclaration,
   map: SourceMap,
): Staged<InlinedJoins> {
   let inline: string[] = [];
   if (declaration.keys.includes("inline")) {
      const items = declaration.tag.array("inline")?.map((i) => i.text());
      if (!items || items.some((i) => typeof i !== "string" || i === "")) {
         return new Declined(
            'inline= must be a list of join names, such as inline=["carriers"]',
            { malformed: MATERIALIZE_INVALID_CODE },
         );
      }
      inline = [...new Set(items as string[])];
   }
   const joins = map.joinsOf(def.sourceID);
   // Each entry's joins from the source down: a join, or a path of
   // join_manys (`items.options`).
   const resolved = inline.map((entry) =>
      inlinedPathOf(entry, joins, map, def.connection),
   );
   const inlineProblem = resolved.find(
      (r): r is { reason: string; invalid?: true } => "reason" in r,
   );
   if (inlineProblem) {
      return new Declined(
         inlineProblem.reason,
         inlineProblem.invalid
            ? { malformed: MATERIALIZE_INVALID_CODE }
            : { refused: "inline_unsupported" },
      );
   }
   // A private or internal join's access, by name. Its build reads the
   // join through the build copy's lift, and its joined columns carry
   // the modifier: the served copy declares an inlined join_one's record,
   // and a pair table's (an inlined join_many's), under it, and renames
   // the stored columns behind it `private` in the record's own block.
   const joinAccess = new Map<string, "private" | "internal">();
   for (const raw of def.fields ?? []) {
      const f = raw as FieldLike;
      const access = hiddenAccess(f);
      const fieldName = f.as ?? f.name;
      if (access && typeof fieldName === "string" && f.join !== undefined) {
         joinAccess.set(fieldName, access);
      }
   }
   const entries = (resolved as { path: SourceJoin[] }[]).map((r) => r.path);
   const ones = entries
      .filter((p) => p[0].relationship !== "many")
      .map((p) => p[0].name);
   // An inlined join_many is a pair table: one path of join_manys
   // below each one inlined. A path that is the start of a longer one is
   // stored by the longer one's (its level flag answers it).
   const byFirst = new Map<string, SourceJoin[][]>();
   for (const p of entries.filter((e) => e[0].relationship === "many")) {
      byFirst.set(p[0].name, [...(byFirst.get(p[0].name) ?? []), p]);
   }
   const pathKey = (p: readonly SourceJoin[]) => p.map((j) => j.name).join(".");
   const paths: SourceJoin[][] = [];
   for (const group of byFirst.values()) {
      const longest = group.reduce((a, b) => (b.length > a.length ? b : a));
      const apart = group.find(
         (p) =>
            !pathKey(longest).startsWith(pathKey(p) + ".") &&
            pathKey(p) !== pathKey(longest),
      );
      if (apart) {
         return new Declined(
            `inline= names two paths through ${quotedName(longest[0].name)} that ` +
               `part (${quotedName(pathKey(apart))}, ${quotedName(pathKey(longest))}); ` +
               "a pair table stores one path below each join it inlines",
            { refused: "inline_unsupported" },
         );
      }
      paths.push(longest);
   }
   return { inline, joins, ones, paths, joinAccess };
}

type Dependence = Exclude<
   ReturnType<typeof callerDependentDimensions>,
   { reason: string }
>;

/**
 * The source's stored columns, its own and its inlined joins', and the
 * computed dimensions left out because each depends on the caller.
 */
function storedColumnsOf(
   def: PlannedDef,
   inlined: InlinedJoins,
   ctx: Pick<PlanContext, "map" | "probed">,
): Staged<{
   columns: MaterializeColumn[];
   joinedColumns: MaterializeColumn[];
   dependence: Dependence;
}> {
   const { map } = ctx;
   const { inline, joins, ones, paths, joinAccess } = inlined;
   // A dimension that depends on the caller is left out and computed at
   // read.
   const dependence = callerDependentDimensions(
      def,
      ctx.probed?.get(def.sourceID),
      inline,
   );
   if ("reason" in dependence) {
      return new Declined(dependence.reason, { refused: dependence.refusal });
   }
   // What reads through an inlined join_many has one value per pair, not
   // per parent, so it is computed at read through the served join.
   const columns = materializeColumns(def, {
      joinReads: map.fieldJoinsOf(def.sourceID),
      inline: new Set(ones),
      hasJoins: joins.length > 0,
      leftOut: new Set(dependence.leftOut.map((d) => d.dimension)),
      zoneFree: dependence.zoneFree,
   });
   const ownColumns = new Set(columns.map((c) => c.column));
   const joinedColumns = [
      ...ones.flatMap((j) =>
         inlinedJoinColumns(
            map,
            joins.find((x) => x.name === j) as SourceJoin,
            undefined,
            joinAccess.get(j),
         ),
      ),
      ...paths.flatMap((p) =>
         p.flatMap((j, k) =>
            inlinedJoinColumns(
               map,
               j,
               p.slice(0, k + 1).map((x) => x.name),
               joinAccess.get(p[0].name),
            ),
         ),
      ),
   ];
   const clash = joinedColumns.find((c) => ownColumns.has(c.column));
   if (clash) {
      return new Declined(
         `the inlined column \`${clash.column}\` has the name of one of the ` +
            "source's own dimensions",
         { refused: "inline_unsupported" },
      );
   }
   columns.push(...joinedColumns);
   if (columns.length === 0) {
      return new Declined("the source has no dimension to store", {
         refused: "no_stored_columns",
      });
   }
   return { columns, joinedColumns, dependence };
}

type WindowTerms = Extract<ReturnType<typeof rollingWindowTerms>, { ok: true }>;
type ReadTimeTerms = Extract<ReturnType<typeof readTimeTermsOf>, { ok: true }>;

/**
 * The source's read-time terms: its rolling windows and its given-reading
 * `where:` terms, each left out of the build and applied again at read.
 */
function readTermsOf(
   def: PlannedDef,
   columns: MaterializeColumn[],
   inline: readonly string[],
   givens: Record<string, unknown> | undefined,
): Staged<{ window: WindowTerms; readTime: ReadTimeTerms }> {
   const classified = classifyWhereTerms(def);
   const window = rollingWindowTerms(
      classified,
      new Map(columns.map((c) => [c.column, columnType(def, c.dimension)])),
   );
   if (!window.ok) {
      return new Declined(window.detail, { refused: "current_time" });
   }
   // Only an inlined join's columns are stored; any other join is read
   // through the served join, over its own source, at read.
   const joinedWindow = leftOutTermReadElsewhere(
      (def.fields ?? []).filter((raw) =>
         inline.includes(
            String((raw as FieldLike).as ?? (raw as FieldLike).name),
         ),
      ),
      window.terms,
   );
   if (joinedWindow !== undefined) {
      return new Declined(
         `the \`where:\` term \`${joinedWindow}\` keeps rows older than a ` +
            "moving cut-off and is also applied by a join `inline=` names, so " +
            "leaving it out of the build would store joined rows the read " +
            "cannot take back out",
         { refused: "current_time" },
      );
   }
   // A join whose rows read the current time (its `where:`, its `on`, or the
   // query of a query-shaped source it joins), at any level of an inlined
   // path: its stored columns would hold the rows as of the build.
   const timed = currentTimeFields(def.fields);
   const timedInline = inline.find((j) =>
      j
         .split(".")
         .some((_, i, all) => timed.has(all.slice(0, i + 1).join("."))),
   );
   if (timedInline !== undefined) {
      return new Declined(
         `inline= names ${quotedName(timedInline)}, whose joined rows ` +
            "read the current time, so its stored columns would hold the " +
            "rows as of the build; reference the join instead",
         { refused: "current_time" },
      );
   }
   const readTime = readTimeTermsOf(def, classified, columns, givens);
   if (!readTime.ok) {
      return new Declined(readTime.reason, { refused: readTime.refusal });
   }
   return { window, readTime };
}

/**
 * The `#@ persist` keys the declaration carries onto its build source, with
 * its place (by the one placement rule, declaration.ts) and its table's name.
 */
function placedPersistOf(
   name: string,
   def: PlannedDef,
   declaration: ReadDeclaration,
): Staged<{ persist: Record<string, string>; watermarks?: string[] }> {
   const carried = carriedPersistKeys(declaration);
   if ("reason" in carried) {
      return new Declined(carried.reason, {
         malformed: carried.code ?? MATERIALIZE_INVALID_CODE,
      });
   }
   const { persist } = carried;
   const placed = placementOf(declaration.tag);
   if ("refused" in placed) {
      return new Declined(EMPTY_STORAGE_REASON, {
         malformed: MATERIALIZE_INVALID_CODE,
      });
   }
   if (placed.placement.tier === "storage") {
      persist.storage = placed.placement.destination;
   }
   // Without `name=`, the source's own name (made plain when it is not, see
   // materializeTableName) is only a placeholder: the self-instructed run
   // replaces it with the readable `<package>__<file>__<source>__p<8 hex>`
   // name (selfTableName, run_instructions.ts). A
   // source is planned in the model that declares it (see
   // `declaredHere`), so its name here is the name its author gave it.
   // With `name=`, the table is the author's: written at that name (its
   // schema or dataset included) by an unversioned or `scope: package`
   // run, under that name's container with the version's suffix on its
   // leaf by a `scope: version` run, and under the host's name, in that
   // container, by an orchestrated one. No view is made at it.
   const nameDefaulted = persist.name === undefined;
   // A BigQuery table needs a dataset, which only a `name=` can give a
   // table built into the source's own warehouse: without one the build
   // would fail on BigQuery's own error, so the source is not built.
   if (
      persist.storage === undefined &&
      capabilitiesOf(def.dialect).needsQualifiedTable &&
      !persist.name?.includes(".")
   ) {
      return new Declined(
         "a #(materialize) source built into BigQuery needs a " +
            "dataset-qualified name= (`dataset.table`), which places its table",
         { refused: "needs_qualified_name" },
      );
   }
   if (nameDefaulted) persist.name = materializeTableName(name);
   return {
      persist,
      ...(carried.watermarks ? { watermarks: carried.watermarks } : {}),
   };
}

/**
 * A pair table's layout: one block per inlined path, its key checks'
 * inputs, and, for an incremental one, its per-parent watermark or the
 * reason it is rebuilt in full. Nothing for a source that inlines no
 * join_many. Sets the watermark and merge key on `persist`, or removes its
 * incremental keys, as the layout decides.
 */
function pairLayoutOf(
   def: PlannedDef,
   map: SourceMap,
   args: {
      paths: SourceJoin[][];
      columns: MaterializeColumn[];
      joinedColumns: MaterializeColumn[];
      persist: Record<string, string>;
      watermarks?: string[];
      windowFields: string[];
   },
): Staged<{
   pair?: MaterializePair;
   pairBlocks?: MaterializePairBlocks;
   pairFullRefresh?: string;
}> {
   const { paths, columns, persist } = args;
   let pair: MaterializePair | undefined;
   let pairBlocks: MaterializePairBlocks | undefined;
   if (paths.length > 0) {
      const laid = paths.map((p) =>
         pairRefusal(
            p,
            def,
            map,
            columns.filter((c) => c.join === undefined),
            args.joinedColumns,
         ),
      );
      const problem = laid.find((l): l is { reason: string } => "reason" in l);
      if (problem) {
         return new Declined(problem.reason, { refused: "inline_unsupported" });
      }
      const ungrouped = ungroupablePairColumn(def, map, columns);
      if (ungrouped) {
         return new Declined(ungrouped, { refused: "inline_unsupported" });
      }
      const pairs = (laid as { pair: MaterializePair }[]).map((l) => l.pair);
      pair = pairs[0];
      if (pairs.length > 1) {
         const blocks = pairBlocksOf(pairs, paths, columns, def.connection);
         if ("reason" in blocks) {
            return new Declined(blocks.reason, {
               refused: "inline_unsupported",
            });
         }
         pairBlocks = blocks.blocks;
      }
   }
   if (args.watermarks && !(pair && persist.refresh === "incremental")) {
      return new Declined(WATERMARK_LIST_REASON, {
         malformed: MATERIALIZE_INVALID_CODE,
      });
   }
   let pairFullRefresh: string | undefined;
   if (pair && persist.refresh === "incremental") {
      let incremental = pairIncremental({
         pairs: pairBlocks?.blocks ?? [pair],
         persist,
         names:
            args.watermarks ??
            (persist.watermark !== undefined ? [persist.watermark] : []),
         columns,
         // A column of a path's k-th join is typed on that join's joined
         // source.
         columnTypeOf: (c) => {
            if (c.join === undefined) return columnType(def, c.dimension);
            const at = c.path ?? [c.join];
            const path = paths.find((p) =>
               at.every((n, i) => p[i]?.name === n),
            );
            const hop = path?.[at.length - 1];
            return hop ? joinedColumnType(hop, c.dimension) : "";
         },
         windowFields: args.windowFields,
      });
      // A pair table's delta replaces whole parents in DuckDB only
      // (pairDeltaStatements): the target of every `storage=` table, and of a
      // database one in a DuckDB warehouse. A database one anywhere else is
      // rebuilt in full every refresh, which the package says.
      if (
         "watermark" in incremental &&
         persist.storage === undefined &&
         !capabilitiesOf(def.dialect).pairDelta
      ) {
         incremental = {
            fullRefresh:
               `its table is in its source's warehouse (dialect ` +
               `${String(def.dialect)}), and a pair table's delta is applied only in DuckDB ` +
               `(a \`storage=\` destination, or a DuckDB warehouse); declare ` +
               `storage= to refresh it incrementally`,
         };
      }
      if ("watermark" in incremental) {
         if (pairBlocks) {
            pairBlocks = { ...pairBlocks, watermark: incremental.watermark };
         } else {
            pair = { ...pair, watermark: incremental.watermark };
         }
         persist.watermark = incremental.watermark.column;
         persist.merge_key = pair.parentKey;
      } else if ("fullRefresh" in incremental) {
         pairFullRefresh = incremental.fullRefresh;
         for (const key of INCREMENTAL_KEYS) delete persist[key];
      }
   }
   return {
      ...(pair ? { pair } : {}),
      ...(pairBlocks ? { pairBlocks } : {}),
      ...(pairFullRefresh ? { pairFullRefresh } : {}),
   };
}

/**
 * Why a pair table cannot be built in its source's dialect: its build groups
 * by every stored column (build_file_generator.ts, `pairBody`), and one of
 * them has a type that dialect's `GROUP BY` refuses (BigQuery: GEOGRAPHY,
 * JSON, ARRAY, STRUCT; Postgres: json, xml, the geometric types, and arrays
 * of them). Undefined when every column can be grouped.
 */
function ungroupablePairColumn(
   def: PlannedDef,
   map: SourceMap,
   columns: readonly MaterializeColumn[],
): string | undefined {
   const refused = capabilitiesOf(def.dialect).ungroupableTypes;
   if (!refused) return undefined;
   for (const c of columns) {
      const type = ungroupedTypeOf(storedFieldOf(def, map, c), refused);
      if (type === undefined) continue;
      return (
         `inline= names a join_many, whose pair table's build groups by every ` +
         `stored column, and \`${c.column}\` is a ${type.toUpperCase()} column, ` +
         `which ${describeDialects(new Set([String(def.dialect)]))} cannot ` +
         "group by; leave the column out of the source (and of the joined " +
         "source's public dimensions), or inline no join_many"
      );
   }
   return undefined;
}

/**
 * The type of `field` a dialect's `GROUP BY` refuses (`refused`), as named
 * in the refusal, or undefined when it groups: a `sql native` field by its
 * raw type in lower case, an array by the Malloy type `array` or by its
 * element (`json[]`), any other by its Malloy type.
 */
function ungroupedTypeOf(
   field: FieldLike | undefined,
   refused: { malloy: readonly string[]; native: readonly string[] },
): string | undefined {
   if (field?.type === "sql native") {
      const raw =
         typeof field.rawType === "string" ? field.rawType.toLowerCase() : "";
      return refused.native.includes(raw) ? raw : undefined;
   }
   if (typeof field?.type !== "string") return undefined;
   if (refused.malloy.includes(field.type)) return field.type;
   if (field.type === "array") {
      const element = ungroupedTypeOf(
         field.elementTypeDef as FieldLike | undefined,
         refused,
      );
      return element === undefined ? undefined : `${element}[]`;
   }
   return undefined;
}

/**
 * The compiled field a stored column reads: the source's own, or the field
 * of the joined source at the end of the column's join path.
 */
function storedFieldOf(
   def: PlannedDef,
   map: SourceMap,
   c: MaterializeColumn,
): FieldLike | undefined {
   let fields = def.fields ?? [];
   let joins = map.joinsOf(def.sourceID);
   for (const name of c.join === undefined ? [] : (c.path ?? [c.join])) {
      const join = joins.find((j) => j.name === name);
      if (!join) return undefined;
      fields = join.fields;
      joins = map.joinedReadsOf(join).joins;
   }
   return fields.find((raw) => {
      const f = raw as FieldLike;
      return (typeof f.as === "string" ? f.as : f.name) === c.dimension;
   }) as FieldLike | undefined;
}

/**
 * The `#(materialize)` keys that make a table incremental. A chained build
 * reads its parent's table, whose rows an incremental refresh moves on its
 * own schedule, so neither side of a chain may carry one.
 */
const INCREMENTAL_KEYS = ["refresh", "watermark", "merge_key", "lookback"];

/**
 * Give each plan that can build over its nearest materialized ancestor's build
 * source its `buildsOver`. The ancestor is the first planned source up
 * the chain, reached through `extend` and renames only (a query-shaped hop
 * starts a new segment, which the build cannot re-root on a table), declared
 * in the same file (the build copy re-roots it in that file's text; one in
 * another file is the companion compiler's, `crossFileEntries`), as is
 * every declaration between them (`path`). Both must be on one tier (both
 * database, or both `storage=` the same destination), carry no incremental
 * key, and the parent no given-reading `where:` (its build source's own
 * `extend` re-applies that term, which would scope the extension's build to
 * one caller) and no current-time `where:` its build leaves out (the
 * re-rooted parent keeps it as written). A plan that does not qualify
 * builds over the raw chain: the same rows.
 */
function chainExtensionTables(plans: MaterializePlan[], map: SourceMap): void {
   const byID = new Map(plans.map((p) => [p.sourceID, p]));
   const declarationOfID = (sourceID: string) =>
      declarationOf(map.node(sourceID)?.def as SourceDefLike);
   for (const plan of plans) {
      let node = map.node(plan.sourceID);
      let parent: MaterializePlan | undefined;
      // The sources from the extension up to just below the parent.
      const below: string[] = [];
      for (let hops = 0; node && hops < 64 && !parent; hops++) {
         const base = node.base;
         if (!base || base.hop === "query" || base.sourceID === undefined) {
            break;
         }
         below.push(node.sourceID);
         parent = byID.get(base.sourceID);
         node = map.node(base.sourceID);
      }
      if (!parent || parent === plan) continue;
      const declaration = declarationOfID(parent.sourceID);
      const path = below.reverse().map(declarationOfID);
      if (
         !declaration ||
         path.some((d) => d === undefined || d.url !== declaration.url)
      ) {
         continue;
      }
      if (!chainsOver(plan, parent)) continue;
      plan.buildsOver = {
         sourceID: parent.sourceID,
         sourceName: parent.sourceName,
         declaration,
         path: path as TermUse[],
      };
   }
}

/**
 * Whether an extension's plan may build over its parent's build source
 *, whichever file declares the parent: both on one tier (both
 * database, whose build reads the parent's build source in its own
 * warehouse, or one storage destination, whose build reads the parent's
 * table there, see `overStored`), neither incremental, neither a table of
 * several inlined paths, the parent no pair table, and the parent with no
 * given-reading `where:` and no current-time `where:` its build leaves out.
 */
export function chainsOver(
   plan: MaterializePlan,
   parent: MaterializePlan,
): boolean {
   if (plan.persist.storage !== parent.persist.storage) return false;
   const incremental = (p: MaterializePlan) =>
      Object.keys(p.persist).some((k) => INCREMENTAL_KEYS.includes(k));
   if (incremental(plan) || incremental(parent)) return false;
   // A table that stacks several inlined paths is a SQL source over the
   // author's model: neither builds over the other's build source.
   if (plan.pairBlocks || parent.pairBlocks) return false;
   // A pair table holds its source once per child of the inlined
   // join_many, not once per row: an extension built over it would store
   // (and its served copy count) each parent once per child.
   if (parent.pair) return false;
   if (givenReadTerms(parent).length > 0) return false;
   // The re-rooted parent keeps its `where:` as written, so a term its
   // table leaves out would be applied to the extension's build.
   if (leftOutWindowTerms(parent.readTimeTerms).length > 0) return false;
   return true;
}

/**
 * For each plan with no parent in its own file, where its chain enters
 * another file (across files): the first source it is built on, through
 * `extend` and renames only as {@link chainExtensionTables} walks a chain,
 * that is declared in another file, with the declarations from just below
 * it down to the plan's own, all in the plan's file. The walk stops at a
 * declaration of the plan's own file that declares `#(materialize)`: that
 * one is this file's to chain on. Which source of the other file the plan
 * builds over is that file's to say ({@link chainWithinFile}). Plans whose
 * chain never enters another file are not listed.
 */
export function crossFileEntries(
   plans: readonly MaterializePlan[],
   contents: Record<string, unknown>,
): {
   plan: MaterializePlan;
   entry: { sourceID: string; url: string };
   path: TermUse[];
}[] {
   const map = SourceMap.of({ contents });
   const declarationOfID = (sourceID: string) => {
      const def = map.node(sourceID)?.def as SourceDefLike | undefined;
      return def ? declarationOf(def) : undefined;
   };
   const declares = (sourceID: string) =>
      declaresMaterialize(map, contents, sourceID);
   const out: ReturnType<typeof crossFileEntries> = [];
   for (const plan of plans) {
      if (plan.buildsOver) continue;
      const own = declarationOfID(plan.sourceID);
      if (!own) continue;
      let node = map.node(plan.sourceID);
      const below: string[] = [];
      for (let hops = 0; node && hops < 64; hops++) {
         const base = node.base;
         if (!base || base.hop === "query" || base.sourceID === undefined) {
            break;
         }
         below.push(node.sourceID);
         const url = splitSourceID(base.sourceID)?.url;
         if (url === undefined) break;
         if (url !== own.url) {
            const path = below.reverse().map(declarationOfID);
            if (path.every((d) => d !== undefined && d.url === own.url)) {
               out.push({
                  plan,
                  entry: { sourceID: base.sourceID, url },
                  path: path as TermUse[],
               });
            }
            break;
         }
         if (declares(base.sourceID)) break;
         node = map.node(base.sourceID);
      }
   }
   return out;
}

/**
 * Whether the source `sourceID` declares `#(materialize)` itself, planned or
 * refused; an unreadable declaration counts as one. A chain walk stops at
 * such a source: whether it is a parent is its own file's plan to say.
 */
function declaresMaterialize(
   map: SourceMap,
   contents: Record<string, unknown>,
   sourceID: string,
): boolean {
   const def = map.node(sourceID)?.def as PlannedDef | undefined;
   if (!def) return false;
   try {
      return readMaterializeDeclaration(def, contents) !== undefined;
   } catch {
      return true;
   }
}

/**
 * The sources of one file a chain entering it at `entrySourceID` is built
 * on (across files), nearest first, read in that file's own compile:
 * the entry, then each source it is built on through `extend` and renames
 * only, declared in the same file. Each with its declaration and the
 * declarations from just below it down to the entry (root first), which
 * the chain's path in the importing file continues.
 */
export function chainWithinFile(
   modelDef: { contents?: unknown; sourceRegistry?: unknown },
   entrySourceID: string,
): { sourceID: string; declaration: TermUse; path: TermUse[] }[] {
   const map = SourceMap.of(modelDef);
   const declarationOfID = (sourceID: string) => {
      const def = map.node(sourceID)?.def as SourceDefLike | undefined;
      return def ? declarationOf(def) : undefined;
   };
   const out: ReturnType<typeof chainWithinFile> = [];
   const url = splitSourceID(entrySourceID)?.url;
   const below: TermUse[] = [];
   let sourceID: string | undefined = entrySourceID;
   for (let hops = 0; sourceID !== undefined && hops < 64; hops++) {
      const declaration = declarationOfID(sourceID);
      if (!declaration || declaration.url !== url) break;
      out.push({ sourceID, declaration, path: [...below].reverse() });
      below.push(declaration);
      const base: { hop: string; sourceID: string | undefined } | undefined =
         map.node(sourceID)?.base;
      if (!base || base.hop === "query") break;
      sourceID = base.sourceID;
   }
   return out;
}

/**
 * A file a chain passes through on its way to a parent in a file beyond it
 * (across files), read in that file's own compile from the source the
 * chain enters it at: the declarations of the sources it walks there
 * ({@link chainWithinFile}), root first; whether any of them declares
 * `#(materialize)` itself (`declares`), planned or refused, so the file is no
 * pass-through; and the source the walk leaves the file at, by `extend` or a
 * rename, when it does (`next`). No `next` means the chain ends in the file,
 * at a table or a query-shaped hop.
 */
export function chainThroughFile(
   modelDef: { contents?: unknown; sourceRegistry?: unknown },
   entrySourceID: string,
): {
   declarations: TermUse[];
   declares: boolean;
   next?: { sourceID: string; url: string };
} {
   const walked = chainWithinFile(modelDef, entrySourceID);
   const map = SourceMap.of(modelDef);
   const contents = (modelDef.contents ?? {}) as Record<string, unknown>;
   const declarations = walked.map((w) => w.declaration).reverse();
   const declares = walked.some((w) =>
      declaresMaterialize(map, contents, w.sourceID),
   );
   const last = walked[walked.length - 1];
   const base = last ? map.node(last.sourceID)?.base : undefined;
   const url =
      base && base.hop !== "query" && base.sourceID !== undefined
         ? splitSourceID(base.sourceID)?.url
         : undefined;
   return {
      declarations,
      declares,
      ...(url !== undefined && url !== last?.declaration.url
         ? { next: { sourceID: base!.sourceID!, url } }
         : {}),
   };
}

/**
 * A refused declaration as a package warning: the source is not materialized
 * and serves live, which is the `unservable` tier, or `malformed` (it fails
 * the publish and the load) when the declaration is invalid.
 */
export function materializeRefusalFinding(
   refusal: MaterializeRefusal,
   model: string,
): PackageAnnotationFinding {
   const { outcome, sourceName, sourceID } = refusal;
   const key = "malformed" in outcome ? outcome.whenRelaxed : outcome.refused;
   return {
      route: MATERIALIZE_ROUTE,
      ...("malformed" in outcome
         ? {
              tier: "malformed" as const,
              code: outcome.malformed,
              message: `#(materialize) on source "${sourceName}" is invalid: ${refusal.reason}.`,
           }
         : {
              tier: "unservable" as const,
              code: MATERIALIZE_NOT_BUILT_CODE,
              message: `#(materialize) on source "${sourceName}" is not built, so the source is served live: ${refusal.reason}.`,
           }),
      subject: sourceName,
      model,
      ...(key && sourceID
         ? { refused: refusalKey(sourceID, sourceName, key.reason, key.tier) }
         : {}),
   };
}

/**
 * A pair table whose `refresh="incremental"` is not honoured, as a package
 * warning: it is built, served, and rebuilt in full on every refresh (the
 * `unservable` tier: well-formed, not honoured here).
 */
export function pairFullRefreshFinding(
   plan: Pick<MaterializePlan, "sourceName">,
   reason: string,
   model: string,
): PackageAnnotationFinding {
   return {
      route: MATERIALIZE_ROUTE,
      tier: "unservable",
      code: MATERIALIZE_PAIR_FULL_REFRESH_CODE,
      message:
         `#(materialize) on source "${plan.sourceName}" declares ` +
         `refresh="incremental", but its pair table is rebuilt in full on ` +
         `every refresh: ${reason}.`,
      subject: plan.sourceName,
      model,
   };
}
