import {
   InMemoryURLReader,
   type LookupConnection,
   type Connection as MalloyConnection,
   Runtime,
} from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../errors";
import { logger } from "../logger";
import {
   recordEligibilityRefused,
   recordServeShapeTypeFallback,
} from "../materialization_metrics";
import type { ManifestEntry } from "../storage/DatabaseInterface";
import { quoteManifestTablePath } from "./quoting";

/**
 * The per-source serve pointer the virtual-source serve transform consumes. Two
 * origins, one shape (the "injectable binding seam"): the publisher self-derives
 * it from a build's manifest entry (standalone), or a host (control plane)
 * supplies it. `tablePath` is the LOGICAL, unquoted physical table path; `schema`
 * is the AUTHORITATIVE DuckDB column schema captured post-build (raw DuckDB type
 * strings), which the transform declares verbatim — see {@link buildServeShapeModel}.
 */
/**
 * A dimension or measure defined on the materialized source in the author's
 * model, re-declared on the serve shape's virtual base so it is computed at
 * serve time from the stored columns rather than forcing a live fallback.
 * `code` is the original Malloy expression text.
 */
export interface FieldRefinement {
   kind: "dimension" | "measure";
   name: string;
   code: string;
}

/**
 * A join defined on the materialized source in the author's model, re-declared
 * on the serve shape's virtual base so a query traversing it serves from the
 * materialized tables (DuckDB runs the join over the stored tables) instead of
 * falling back to live.
 *
 * A join is only re-emitted when its joined source is ITSELF materialized (has a
 * binding) — the serve shape is one model, so a join to a source that is not a
 * sibling virtual source would fail to compile the WHOLE shape and disable
 * storage serving for the entire package (see {@link extractJoins}'s gate). The
 * `text` is the author's verbatim join declaration (`<alias> is <source> [on|
 * with ...]`), lifted from the source by location — the on-condition is an
 * arbitrary expression, so lifting text carries any condition for free, whereas
 * reconstructing it from the compiled expression tree would not.
 */
export interface JoinRefinement {
   kind: "join";
   /** The join alias (`join_one: <alias> is ...`); used for logging/ordering. */
   name: string;
   /** The join keyword to emit, from the compiled join relationship. */
   keyword: "join_one" | "join_many" | "join_cross";
   /** Verbatim author declaration `<alias> is <source> [on|with ...]`. */
   text: string;
   /** The joined source's author name; emitted only when it is materialized. */
   dependsOn: string;
}

/**
 * A view (turtle) defined on the materialized source in the author's model,
 * re-declared on the serve shape so a query invoking it by name serves from the
 * materialized tables. A view is a nested query pipeline, not a single
 * expression, so — like a join — its declaration `text` is lifted verbatim from
 * the author's source by location rather than reconstructed.
 *
 * A view has no cheap dependency gate (it can reference any of the source's
 * columns, dimensions, measures, and joins): it is emitted optimistically and,
 * if the resulting shape does not compile (it reaches a refinement not carried —
 * a join to a non-materialized source, a nested view), the serve path drops the
 * view category and falls back for those queries (see the shape-compile
 * escalation in the model's serve path).
 */
export interface ViewRefinement {
   kind: "view";
   /** The view name (`view: <name> is { ... }`); used for logging/ordering. */
   name: string;
   /** Verbatim author declaration `<name> is { ... }`. */
   text: string;
}

/**
 * A refinement to re-declare on the serve shape's virtual base: a dimension or
 * measure ({@link FieldRefinement}), a join ({@link JoinRefinement}), or a view
 * ({@link ViewRefinement}).
 */
export type SourceRefinement =
   | FieldRefinement
   | JoinRefinement
   | ViewRefinement;

export interface ServeBinding {
   /** The Malloy source name to rebind (`source: <sourceName> is ...`). */
   sourceName: string;
   /** The DuckDB/DuckLake connection the physical table lives in. */
   connectionName: string;
   /** The virtualMap handle for this source (its build-posture identity). */
   virtualHandle: string;
   /** Logical (unquoted) physical table path; quoted for DuckDB at bind time. */
   tablePath: string;
   /** Authoritative DuckDB columns captured post-build (raw DuckDB types). */
   schema: { name: string; type: string }[];
   /**
    * Refinements defined on the source in the author's model, re-emitted as an
    * `extend {}` on the virtual base so queries using them serve from the
    * materialized tables instead of falling back to live: dimensions/measures
    * (computed over the stored columns), joins whose target is also materialized
    * (the join runs over the stored tables), and views (turtles) reproducible
    * from those. Analytic source-fields are still not carried — a query using
    * one falls back. Attached at serve time from the compiled model, not the
    * build.
    */
   refinements?: SourceRefinement[];
   /** Optional freshness anchor (data-as-of instant); carried through verbatim. */
   freshAsOf?: string;
}

/**
 * Derive the publisher's self-maintained serve bindings from a build's manifest
 * entries — the standalone half of the injectable binding seam (a host/control
 * plane can supply {@link ServeBinding}s directly instead). Only entries that
 * were materialized into a storage destination (carrying `storageConnectionName`
 * and a captured `schema`) produce a binding; in-warehouse (path-C) entries do
 * not (they serve through the same-connection manifest, not the transform).
 *
 * The virtual handle is the source's build-posture **content identity**
 * (`sourceEntityId`): identity-scoped so two imports of the same source dedup to
 * one shared virtual table, and stable across generations — the generation lives
 * in the mapped table path (`physicalTableName`), not the handle. This is the one
 * hard cross-producer contract: whoever supplies the binding (this function, or
 * a host) must key the handle the same way the build did.
 */
export function deriveServeBindings(
   entries: Record<string, ManifestEntry>,
): ServeBinding[] {
   const bindings: ServeBinding[] = [];
   for (const entry of Object.values(entries)) {
      // Need the source name to rebind it, plus a storage destination + table.
      if (
         !entry.sourceName ||
         !entry.storageConnectionName ||
         !entry.physicalTableName
      ) {
         continue;
      }
      // The wire Column has optional name/type; keep only complete columns.
      const schema = (entry.schema ?? [])
         .filter((c) => c.name && c.type)
         .map((c) => ({ name: c.name as string, type: c.type as string }));
      if (schema.length === 0) continue;
      bindings.push({
         sourceName: entry.sourceName,
         connectionName: entry.storageConnectionName,
         virtualHandle: entry.sourceEntityId,
         // Qualify the table with the destination catalog (the attach alias) so
         // the serve reads `<store>.<table>` — the build wrote it there, and an
         // unqualified name would resolve against the serve session's default
         // catalog, not the attached store.
         tablePath: `${entry.storageConnectionName}.${entry.physicalTableName}`,
         schema,
         freshAsOf: entry.dataAsOf,
      });
   }
   return bindings;
}

/**
 * Map a DuckDB column type (as reported by `DESCRIBE`) to a Malloy basic type
 * for the serve-shape `type:` declaration.
 *
 * Only Malloy's basic scalar types are legal in a `type:` field
 * (`number` | `string` | `boolean` | `date` | `timestamp` | `json`), so every
 * DuckDB type must collapse onto one. Numeric widths all become `number`;
 * temporal-with-time becomes `timestamp`; anything unrecognized (nested/array/
 * struct/enum/blob) falls back to `json` — a safe, lossy carrier so an exotic
 * column never breaks the whole serve shape — and is logged so the gap is
 * visible. Matching is case-insensitive and ignores precision args (`DECIMAL(18,2)`)
 * and array suffixes (`INTEGER[]`).
 */
export function duckdbTypeToMalloy(duckdbType: string): string {
   const raw = duckdbType.trim();
   // Arrays / nested collections carry as json (lossy but safe) for now.
   if (/\[\s*\]/.test(raw)) {
      recordServeShapeTypeFallback("array");
      logger.warn(
         "Mapping DuckDB array/collection type to json for serve shape",
         {
            duckdbType: raw,
         },
      );
      return "json";
   }
   // Strip precision/scale args: DECIMAL(18,2) -> DECIMAL, VARCHAR(255) -> VARCHAR.
   const base = raw
      .replace(/\(.*\)/, "")
      .trim()
      .toUpperCase();

   // Temporal-with-time first (so "TIMESTAMP WITH TIME ZONE" doesn't fall through).
   if (base === "DATE") return "date";
   if (
      base.startsWith("TIMESTAMP") ||
      base === "DATETIME" ||
      base === "TIMESTAMPTZ"
   ) {
      return "timestamp";
   }
   if (base === "TIME") {
      // Malloy has no time-of-day scalar; carry as string to preserve the value.
      return "string";
   }

   switch (base) {
      case "TINYINT":
      case "SMALLINT":
      case "INTEGER":
      case "INT":
      case "INT4":
      case "BIGINT":
      case "INT8":
      case "HUGEINT":
      case "UTINYINT":
      case "USMALLINT":
      case "UINTEGER":
      case "UBIGINT":
      case "UHUGEINT":
      case "DOUBLE":
      case "FLOAT":
      case "FLOAT4":
      case "FLOAT8":
      case "REAL":
      case "DECIMAL":
      case "NUMERIC":
         return "number";
      case "VARCHAR":
      case "TEXT":
      case "CHAR":
      case "BPCHAR":
      case "STRING":
      case "UUID":
         return "string";
      case "BOOLEAN":
      case "BOOL":
         return "boolean";
      default:
         recordServeShapeTypeFallback("unrecognized");
         logger.warn(
            "Unrecognized DuckDB type; mapping to json for serve shape",
            {
               duckdbType: raw,
            },
         );
         return "json";
   }
}

/** A Malloy identifier that needs no quoting. */
const BARE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Emit a field name bare when it is a plain identifier, else backtick-quoted. */
function emitFieldName(name: string): string {
   return BARE_IDENTIFIER.test(name) ? name : `\`${name.replace(/`/g, "``")}\``;
}

/**
 * Generate the transient serve-shape model text that rebinds a materialized
 * source to a virtual source on its storage connection, declaring the captured
 * authoritative schema.
 *
 * The declared `type:` fields are the schema-authority contract: the compiler
 * does NOT type-check a virtual source's declared columns against the real
 * table, so whatever this declares is trusted — a wrong or approximated schema
 * surfaces only as a serve-time execution error. That is why the binding must
 * carry the post-build DESCRIBE schema and this function declares exactly it.
 *
 * Field syntax is DuckDB user-type double-colon (`name::type`); the source line
 * binds to `<conn>.virtual('<handle>')` with the `::<Shape>` constraint. The
 * `##! experimental.virtual_source` flag is injected on this transient model
 * ONLY — it never touches the author's model.
 */
export function buildServeShapeModel(
   sourceName: string,
   binding: ServeBinding,
): { modelText: string; shapeTypeName: string } {
   const shapeTypeName = `${sourceName}__shape`;
   const fields = binding.schema
      .map((c) => `   ${emitFieldName(c.name)}::${duckdbTypeToMalloy(c.type)}`)
      .join(",\n");
   const modelText = `##! experimental.virtual_source
type: ${shapeTypeName} is {
${fields}
}
source: ${sourceName} is ${binding.connectionName}.virtual('${binding.virtualHandle}')::${shapeTypeName}
`;
   return { modelText, shapeTypeName };
}

/**
 * The `type:` + `source:` fragment that rebinds ONE materialized source to its
 * virtual form (no flag line — callers emit `##! experimental.virtual_source`
 * once for the whole model).
 */
function serveShapeFragment(binding: ServeBinding): string {
   const shapeTypeName = `${binding.sourceName}__shape`;
   const fields = binding.schema
      .map((c) => `   ${emitFieldName(c.name)}::${duckdbTypeToMalloy(c.type)}`)
      .join(",\n");
   let source =
      `source: ${binding.sourceName} is ` +
      `${binding.connectionName}.virtual('${binding.virtualHandle}')::${shapeTypeName}`;
   // Re-declare the source's refinements on the virtual base so queries that use
   // them are computed from the stored tables at serve time (the wrapper) rather
   // than falling back to live. Emission order matters for resolution: joins
   // first (a dimension/measure/view may reference a joined field), then
   // dimensions/measures, then views (a view may reference any of them).
   // Everything here references the shape's columns, a sibling virtual source, or
   // an earlier refinement; anything it references that the shape lacks makes the
   // serve shape fail to compile, which safely falls back.
   const refinements = binding.refinements ?? [];
   const lines: string[] = [];
   for (const r of refinements) {
      if (r.kind === "join") lines.push(`   ${r.keyword}: ${r.text}`);
   }
   for (const r of refinements) {
      if (r.kind === "dimension" || r.kind === "measure") {
         lines.push(`   ${r.kind}: ${r.name} is ${r.code}`);
      }
   }
   for (const r of refinements) {
      if (r.kind === "view") lines.push(`   view: ${r.text}`);
   }
   if (lines.length > 0) {
      source += ` extend {\n${lines.join("\n")}\n}`;
   }
   return `type: ${shapeTypeName} is {\n${fields}\n}\n${source}`;
}

/**
 * Generate ONE transient serve-shape model rebinding every supplied
 * materialized source to its virtual form — the model the serve path compiles
 * queries against when `PERSIST_STORAGE_MODE=on`. The `##! experimental.virtual_source`
 * flag is emitted once. Each source declares the authoritative captured schema
 * (see {@link buildServeShapeModel} for why the declared schema is trusted on
 * faith and must match the built table).
 *
 * Coverage note: this rebinds each source's BASE to the virtual table with the
 * captured columns, and re-declares the source's dimensions, measures,
 * materialized-target joins, and views (see {@link ServeBinding.refinements}) on
 * top. A query relying on a refinement not carried here (an analytic field, or a
 * join/view that reaches a non-materialized source) does not compile against
 * this model, and the serve path falls back to serving it live.
 */
export function buildServeShapeModelForBindings(bindings: ServeBinding[]): {
   modelText: string;
} {
   const fragments = orderBindingsByJoinDeps(bindings)
      .map(serveShapeFragment)
      .join("\n");
   return {
      modelText: `##! experimental.virtual_source\n${fragments}\n`,
   };
}

/**
 * Assemble the transient BUILD model for a chained `storage=` source — the
 * "stack on the parent" (Tier 3) build. Every materialized upstream is rebound
 * to a virtual source on its storage connection (via the SAME serve-shape rebind
 * as the serve path), so the downstream computes over the parents' STORED lake
 * tables in DuckDB; the downstream source is then re-declared over them and
 * annotated `#@ persist` so it surfaces as a persist source when the caller
 * compiles this model and reads `PersistSource.getSQL({ virtualMap })` (the exact
 * mirror of the warehouse build's `getSQL`, only over rebound parents).
 *
 * `downstreamDefText` is the RHS of the author's `source: <name> is …` statement,
 * lifted verbatim by location (a top-level source's `location.range` starts just
 * after the `source: ` keyword), so this prepends `source: `. The `#@ persist
 * storage=<dest>` annotation only makes the source appear in the transient build
 * plan — the value is not otherwise consumed (the build session already knows the
 * destination), but naming the real destination keeps the model self-consistent.
 *
 * Upstream rebinds here are base-only (the captured schema, no re-emitted
 * dimensions/joins/views): the dominant chained case is a rollup over the
 * parent's stored OUTPUT columns. A downstream that references a parent
 * refinement not carried here fails to compile against this model, and the
 * caller falls back to recompute-from-raw (Tier 2) — re-emitting parent
 * refinements to widen coverage is a follow-on.
 */
export function buildChainedStorageBuildModel(params: {
   upstreams: ServeBinding[];
   downstreamName: string;
   downstreamDefText: string;
   destinationName: string;
}): string {
   const upstreamFragments = orderBindingsByJoinDeps(params.upstreams)
      .map(serveShapeFragment)
      .join("\n");
   return (
      `##! experimental { persistence virtual_source }\n` +
      `${upstreamFragments}\n` +
      `#@ persist storage=${params.destinationName}\n` +
      `source: ${params.downstreamDefText}\n`
   );
}

/**
 * Order bindings so a joined source is declared before the source that joins it:
 * Malloy resolves `source:` statements top-to-bottom, so a join to a source not
 * yet declared is an "undefined object" error. Only intra-set join dependencies
 * are ordered (a join is emitted only when its target is in the set anyway).
 * Stable: independent sources keep their original order. On a dependency cycle
 * (mutual joins, which cannot be single-pass forward-referenced), the remaining
 * sources are appended in original order — the resulting shape fails to compile
 * and the caller drops joins / falls back, rather than looping.
 */
function orderBindingsByJoinDeps(bindings: ServeBinding[]): ServeBinding[] {
   const present = new Set(bindings.map((b) => b.sourceName));
   const dependsOn = new Map<string, Set<string>>();
   for (const b of bindings) {
      const set = new Set<string>();
      for (const r of b.refinements ?? []) {
         if (
            r.kind === "join" &&
            r.dependsOn !== b.sourceName &&
            present.has(r.dependsOn)
         ) {
            set.add(r.dependsOn);
         }
      }
      dependsOn.set(b.sourceName, set);
   }
   const ordered: ServeBinding[] = [];
   const emitted = new Set<string>();
   const remaining = new Set(present);
   while (remaining.size > 0) {
      let progressed = false;
      for (const b of bindings) {
         if (!remaining.has(b.sourceName)) continue;
         const ready = [...dependsOn.get(b.sourceName)!].every((d) =>
            emitted.has(d),
         );
         if (ready) {
            ordered.push(b);
            emitted.add(b.sourceName);
            remaining.delete(b.sourceName);
            progressed = true;
         }
      }
      if (!progressed) {
         for (const b of bindings) {
            if (remaining.has(b.sourceName)) {
               ordered.push(b);
               remaining.delete(b.sourceName);
            }
         }
      }
   }
   return ordered;
}

/**
 * Extract a materialized source's re-emittable refinements — the dimensions and
 * measures defined on it in the author's model — from its compiled field list,
 * so they can be re-declared on the serve shape's virtual base.
 *
 * A derived field carries its original expression as `code` and an
 * `expressionType`; the source's raw output columns have neither (they are the
 * stored columns, already in the shape's `::` type). Scalar → dimension,
 * aggregate → measure. Joins are handled separately ({@link extractJoins});
 * views (turtles) and analytic/calculation fields are deliberately skipped —
 * re-emitting them is out of scope, and a query that uses one simply falls back
 * to live (safe).
 */
/**
 * Whether a compiled field carries a non-public access modifier
 * (`private`/`internal`). Such a field is hidden by the live model's access
 * control, so it must NEVER be re-emitted onto the serve shape: the served
 * virtual source carries no access modifiers, so re-declaring a private/internal
 * dimension, measure, join, or view would expose — over the stored table — a
 * field the live path refuses. Skipping it makes any query that references it
 * fall back to live, where the modifier is enforced (fail-safe). Defensive on
 * the value: anything other than an absent/`public` modifier is treated as
 * restricted, so a future modifier kind also fails closed.
 */
function isAccessRestricted(field: unknown): boolean {
   const am = (field as { accessModifier?: unknown }).accessModifier;
   return am != null && am !== "public";
}

export function extractRefinements(
   fields: readonly unknown[] | undefined,
): FieldRefinement[] {
   const out: FieldRefinement[] = [];
   for (const field of fields ?? []) {
      if (isAccessRestricted(field)) continue; // never re-emit a hidden field
      const f = field as {
         name?: string;
         code?: unknown;
         expressionType?: unknown;
      };
      if (typeof f.name !== "string") continue;
      if (typeof f.code !== "string" || f.code.length === 0) continue; // raw column
      if (f.expressionType === "scalar") {
         out.push({ kind: "dimension", name: f.name, code: f.code });
      } else if (f.expressionType === "aggregate") {
         out.push({ kind: "measure", name: f.name, code: f.code });
      }
      // analytic / calculation / ungrouped-aggregate and non-atomic fields
      // (joins, turtles have no `code`) are skipped → those queries fall back.
   }
   return out;
}

/** A zero-based Malloy source range (as carried on a compiled field's `location`). */
export interface SourceRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** A compiled field's source location: the file URL plus the range it spans. */
export interface SourceLocation {
   url: string;
   range: SourceRange;
}

/**
 * Slice the substring covered by a Malloy `location.range` out of the full
 * source text (both line and character are zero-based). Returns undefined when
 * the range is out of bounds — a stale or mismatched source — so the caller
 * skips the refinement and falls back to live rather than emitting garbage.
 */
export function sliceSourceRange(
   text: string,
   range: SourceRange,
): string | undefined {
   const lines = text.split("\n");
   const { start, end } = range;
   if (start.line < 0 || start.line > end.line || end.line >= lines.length) {
      return undefined;
   }
   if (start.line === end.line) {
      return lines[start.line].slice(start.character, end.character);
   }
   let out = lines[start.line].slice(start.character);
   for (let i = start.line + 1; i < end.line; i++) {
      out += "\n" + lines[i];
   }
   out += "\n" + lines[end.line].slice(0, end.character);
   return out;
}

/** What {@link extractJoins} needs beyond the compiled field list. */
export interface JoinExtractionContext {
   /**
    * The joined source's author name, keyed by the compiled join field's
    * `sourceID` (which equals the joined source's own `sourceID`). Only a join
    * whose target maps here — a named, in-model source — is a candidate; an
    * anonymous/inline join target is absent from the map and is skipped.
    */
   sourceNameById: Map<string, string>;
   /** Author source names that are materialized (have a serve binding). */
   materializedSourceNames: ReadonlySet<string>;
   /** Lift a field's verbatim source text from its `location`, or undefined. */
   liftText: (location: SourceLocation) => string | undefined;
}

/**
 * Extract a materialized source's re-emittable joins from its compiled field
 * list. A join is carried only when BOTH hold: (1) its joined source is a
 * named, in-model source (its `sourceID` maps to a name), and (2) that source
 * is itself materialized (has a binding). The second is load-bearing, not an
 * optimization: the serve shape is one model, so a join to a source it does not
 * declare would fail the whole shape's compile and disable storage serving for
 * every source in the package.
 *
 * The declaration text is lifted verbatim from the author's source by location
 * (the on-condition is an arbitrary expression; lifting text carries any
 * condition, whereas reconstructing it from the compiled expression tree would
 * not); the keyword comes from the compiled relationship. A join we cannot map
 * or lift is skipped, and a query using it falls back to live (safe).
 */
export function extractJoins(
   fields: readonly unknown[] | undefined,
   ctx: JoinExtractionContext,
): JoinRefinement[] {
   const out: JoinRefinement[] = [];
   for (const field of fields ?? []) {
      if (isAccessRestricted(field)) continue; // never re-emit a hidden join
      const f = field as {
         as?: unknown;
         name?: unknown;
         join?: unknown;
         sourceID?: unknown;
         location?: SourceLocation;
      };
      if (typeof f.join !== "string") continue;
      const keyword =
         f.join === "one"
            ? "join_one"
            : f.join === "many"
              ? "join_many"
              : f.join === "cross"
                ? "join_cross"
                : undefined;
      if (!keyword) continue;
      if (typeof f.sourceID !== "string") continue;
      const dependsOn = ctx.sourceNameById.get(f.sourceID);
      if (!dependsOn) continue; // anonymous/inline join target → skip
      if (!ctx.materializedSourceNames.has(dependsOn)) continue; // the gate
      if (!f.location) continue;
      const text = ctx.liftText(f.location);
      if (!text) continue; // couldn't recover the declaration → skip
      const alias =
         typeof f.as === "string"
            ? f.as
            : typeof f.name === "string"
              ? f.name
              : dependsOn;
      out.push({ kind: "join", name: alias, keyword, text, dependsOn });
   }
   return out;
}

/**
 * Extract a materialized source's re-emittable views (turtles) from its compiled
 * field list. A view is a nested query pipeline, not a single expression, so its
 * declaration text is lifted verbatim from the author's source by location
 * rather than reconstructed. Unlike a join, a view has no cheap dependency gate
 * (it can reference any column/dimension/measure/join of the source), so it is
 * emitted optimistically: if the resulting shape does not compile — the view
 * reaches something not carried (a join to a non-materialized source, a nested
 * view) — the serve path drops the view category and those queries fall back to
 * live (safe). A view we cannot lift is skipped.
 */
export function extractViews(
   fields: readonly unknown[] | undefined,
   liftText: (location: SourceLocation) => string | undefined,
): ViewRefinement[] {
   const out: ViewRefinement[] = [];
   for (const field of fields ?? []) {
      if (isAccessRestricted(field)) continue; // never re-emit a hidden view
      const f = field as {
         type?: unknown;
         name?: unknown;
         location?: SourceLocation;
      };
      if (f.type !== "turtle") continue;
      if (typeof f.name !== "string") continue;
      if (!f.location) continue;
      const text = liftText(f.location);
      if (!text) continue;
      out.push({ kind: "view", name: f.name, text });
   }
   return out;
}

/**
 * Assemble the per-call `virtualMap` (`connectionName -> handle -> canonical
 * table path`) core resolves a virtual source through. The table path is quoted
 * canonical for DuckDB — core validates every entry is canonical SQL for some
 * dialect and then pastes it verbatim (it does not quote it), so an unquoted or
 * mis-cased path is a run-time error. Multiple bindings on one connection fold
 * into that connection's inner map.
 *
 * This is a FROM-bind site, so it uses {@link quoteManifestTablePath} — the same
 * quoting authority as the build-side manifest seed and the serve-side manifest
 * bind (publisher #904) — which quotes for the dialect UNLESS the path is already
 * canonical SQL (an author-supplied `name=` the author already quoted), so an
 * already-quoted name is never double-quoted.
 */
export function buildVirtualMap(
   bindings: ServeBinding[],
): Map<string, Map<string, string>> {
   const map = new Map<string, Map<string, string>>();
   for (const b of bindings) {
      let inner = map.get(b.connectionName);
      if (!inner) {
         inner = new Map<string, string>();
         map.set(b.connectionName, inner);
      }
      inner.set(b.virtualHandle, quoteManifestTablePath(b.tablePath, "duckdb"));
   }
   return map;
}

/**
 * The DuckDB-compile portability gate (the third materialization-eligibility
 * check, deferred from the pre-build pass because it needs the post-build
 * schema): compile the serve-shape model against DuckDB and refuse the source if
 * it does not compile. Because the served table lives in DuckDB, a source
 * authored against a warehouse must have a DuckDB-compilable served shape — this
 * turns a serve-time 500 into a build-time refusal.
 *
 * Scope note: the current serve shape declares the captured columns (a
 * simple-semantic-layer serve); preserving a rich extend block (measures/dims/
 * joins) across the base swap and compiling THAT in DuckDB is the
 * base-swap-preserve-refinements follow-on. This gate therefore proves the
 * captured schema forms a valid DuckDB virtual source; it is a floor, not the
 * full semantic-layer portability check.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) if the serve shape does
 *   not compile in DuckDB.
 */
export async function assertServesInDuckDB(
   sourceName: string,
   binding: ServeBinding,
   connections: LookupConnection<MalloyConnection>,
): Promise<void> {
   const { modelText } = buildServeShapeModel(sourceName, binding);
   const root = "file:///serve-shape/";
   const urlReader = new InMemoryURLReader(
      new Map([[`${root}shape.malloy`, modelText]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   try {
      await runtime
         .loadModel(new URL(`${root}shape.malloy`), {
            importBaseURL: new URL(root),
         })
         .getModel();
   } catch (err) {
      recordEligibilityRefused("not_duckdb_portable");
      throw new MaterializationEligibilityError({
         message:
            `Source '${sourceName}' cannot be served from its storage ` +
            `destination: its materialized shape does not compile in DuckDB ` +
            `(${err instanceof Error ? err.message : String(err)}). A source ` +
            `materialized into a DuckDB/DuckLake store must have a ` +
            `DuckDB-portable served shape.`,
      });
   }
}
