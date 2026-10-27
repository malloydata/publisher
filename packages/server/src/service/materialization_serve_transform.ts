// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   InMemoryURLReader,
   type LookupConnection,
   type Connection as MalloyConnection,
   Runtime,
} from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../errors";
import { logger } from "../logger";
import { givenScopedJoinTargets } from "./persist_dynamic_terms";
import {
   recordEligibilityRefused,
   recordServeShapeTypeFallback,
} from "../materialization_metrics";
import type { ManifestEntry } from "../storage/DatabaseInterface";
import { quoteManifestTablePath } from "./quoting";
import { storageTablePath } from "./storage_table_path";

/**
 * A dimension or measure from the author's source, re-declared on the serve
 * shape so it is computed from the stored columns instead of serving live.
 * `code` is the original Malloy expression text.
 */
interface FieldRefinement {
   kind: "dimension" | "measure";
   name: string;
   code: string;
}

/**
 * A join from the author's source, re-declared on the serve shape so DuckDB runs
 * it over the stored tables.
 *
 * Emitted only when the joined source is ITSELF materialized: the serve shape is
 * one model, so a join to a non-sibling would fail the WHOLE shape and disable
 * storage serving for the package (see {@link extractJoins}). `text` is lifted
 * verbatim by location, which carries an arbitrary on-condition for free.
 */
interface JoinRefinement {
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
 * A view (turtle) from the author's source, re-declared on the serve shape;
 * `text` is lifted verbatim by location, like a join's.
 *
 * A view can reference anything on the source, so there is no cheap dependency
 * gate: it is emitted optimistically, and if the shape then fails to compile the
 * serve path's escalation drops the view category.
 */
interface ViewRefinement {
   kind: "view";
   /** The view name (`view: <name> is { ... }`); used for logging/ordering. */
   name: string;
   /** Verbatim author declaration `<name> is { ... }`. */
   text: string;
}

/**
 * A `where:` from the source's extend block, re-declared on the serve shape.
 *
 * The build SQL is the persisted relation alone; the `where:` applies on read.
 * The colocated tier keeps it for free (only the `FROM` is swapped), but the
 * storage tier re-declares the source, so it must carry the filter or serve rows
 * the source excludes.
 *
 * One refinement per `filterList` entry, each its own `where:` line: joining two
 * entries with `and` would reassociate a top-level `or`.
 */
interface FilterRefinement {
   kind: "filter";
   /** Verbatim author expression, e.g. `not is_deleted`. */
   code: string;
}

/**
 * A `given:` to declare on the serve-shape model, copied from the author's.
 *
 * A re-emitted {@link FilterRefinement} such as `where: org_id = $ORG_ID`
 * compiles only if the shape declares `ORG_ID`. Malloy then inlines the
 * request's value, so the artifact is read with the caller's own predicate (and
 * a partition-column equality prunes files).
 *
 * `defaultText` keeps an unbound request on the tier: without it the shape
 * compile fails ("no value and no default") and the request silently serves live.
 */
export interface ServeShapeGiven {
   name: string;
   /** The declared type as Malloy renders it, e.g. `number`, `filter<string>`. */
   type: string;
   /** The default as a source literal (`'acme'`, `2003`, `f'WN'`), if any. */
   defaultText?: string;
}

/**
 * A refinement to re-declare on the serve shape's virtual base: a dimension or
 * measure ({@link FieldRefinement}), a join ({@link JoinRefinement}), a view
 * ({@link ViewRefinement}), or a source-level filter ({@link FilterRefinement}).
 */
export type SourceRefinement =
   | FieldRefinement
   | JoinRefinement
   | ViewRefinement
   | FilterRefinement;

/**
 * The per-source serve pointer the serve transform consumes, either derived from
 * a build's manifest entry ({@link deriveServeBindings}) or supplied by a host.
 */
export interface ServeBinding {
   /** The Malloy source name to rebind (`source: <sourceName> is ...`). */
   sourceName: string;
   /**
    * `persist` for a modeler-annotated source, `preaggregate` for a synthesized
    * rollup; absent means `persist`. A rollup must not take the ordinary
    * author-model lookups (see {@link rollupServeBindings}).
    */
   origin?: "persist" | "preaggregate";
   /** The storage destination the physical table lives in. */
   destinationName: string;
   /** The virtualMap handle for this source (its build-posture identity). */
   virtualHandle: string;
   /** Logical (unquoted) physical table path; quoted for DuckDB at bind time. */
   tablePath: string;
   /** Authoritative DuckDB columns captured post-build (raw DuckDB types). */
   schema: { name: string; type: string }[];
   /**
    * The author source's refinements, re-emitted as an `extend {}` on the
    * virtual base. Attached at serve time from the compiled model. Analytic
    * fields are not carried; a query using one serves live.
    */
   refinements?: SourceRefinement[];
   /** Optional freshness anchor (data-as-of instant); carried through verbatim. */
   freshAsOf?: string;
   /**
    * Gates this binding per query the same way the colocated serve does: a
    * stale binding with fallback `live`/`fail` is dropped from the shape.
    */
   freshnessWindowSeconds?: number;
   freshnessFallback?: "live" | "stale_ok" | "fail";
}

/**
 * For each persist source in a build plan, the sources that materialize into its
 * table — the `aliasesBySourceName` argument {@link deriveServeBindings} takes.
 *
 * Grouped by content address (which decides who shares a table), then keyed by
 * name, the only identifier a manifest entry carries that means the same thing
 * whoever built it (an instructed build stamps the caller's `sourceEntityId`).
 *
 * A name declared at more than one address is dropped from aliasing: names are
 * not unique in a package, and binding one name to two tables puts two `source:`
 * declarations in the package-wide serve shape, which fails to compile and
 * silently takes the storage tier out for every model. Dropping costs only that
 * source its routing. The source that OWNS the name still binds it (see
 * {@link deriveServeBindings}).
 */
export function groupAliasesByName(
   planSources: { name?: string; sourceEntityId?: string }[],
): Record<string, string[]> {
   const namesByAddress = new Map<string, string[]>();
   for (const source of planSources) {
      if (!source.sourceEntityId || !source.name) continue;
      const group = namesByAddress.get(source.sourceEntityId);
      if (!group) namesByAddress.set(source.sourceEntityId, [source.name]);
      else if (!group.includes(source.name)) group.push(source.name);
   }

   const addressesPerName = new Map<string, number>();
   for (const group of namesByAddress.values()) {
      for (const name of group) {
         addressesPerName.set(name, (addressesPerName.get(name) ?? 0) + 1);
      }
   }

   const byName: Record<string, string[]> = {};
   for (const group of namesByAddress.values()) {
      const unambiguous = group.filter(
         (name) => addressesPerName.get(name) === 1,
      );
      for (const name of unambiguous) byName[name] = unambiguous;
   }
   return byName;
}

/**
 * Derive serve bindings from a build's manifest entries (a host may supply
 * {@link ServeBinding}s directly instead). Only storage-destination entries with
 * a captured schema produce one; colocated entries serve through the
 * same-connection manifest.
 *
 * The virtual handle is the source's content identity (`sourceEntityId`), so two
 * imports of one source share a virtual table, and stable across generations
 * (the generation lives in `physicalTableName`). Whoever supplies a binding must
 * key the handle the same way the build did.
 *
 * One entry can serve several sources: a base and its `extend`s share a content
 * address and one table, but the entry names only the builder, so
 * `aliasesBySourceName` supplies the rest, all bound to the same handle.
 *
 * Aliases are keyed by NAME, not `entry.sourceEntityId`: on an instructed build
 * that id is the host's opaque value, which need not match the publisher's own
 * content addresses the grouping is computed from.
 */
export function deriveServeBindings(
   entries: Record<string, ManifestEntry>,
   aliasesBySourceName: Record<string, string[]>,
   /** A destination's schema by name (see storageTablePath); absent, or answering undefined, is `main`. */
   destinationSchema?: (destinationName: string) => string | undefined,
): ServeBinding[] {
   const bindings: ServeBinding[] = [];
   // Names that OWN a table. An alias never claims one, or the name would be
   // bound twice and declared twice in one serve shape.
   const builders = new Set(
      Object.values(entries)
         .map((entry) => entry.sourceName)
         .filter((name): name is string => !!name),
   );
   for (const entry of Object.values(entries)) {
      if (
         !entry.sourceName ||
         !entry.storageDestinationName ||
         !entry.physicalTableName
      ) {
         continue;
      }
      // The wire Column has optional name/type; keep only complete columns.
      const schema = (entry.schema ?? [])
         .filter((c) => c.name && c.type)
         .map((c) => ({ name: c.name as string, type: c.type as string }));
      if (schema.length === 0) continue;
      // Builder first so it wins any ordering downstream; deduplicated because
      // the builder is normally in its own alias group.
      const names = [
         entry.sourceName,
         ...(aliasesBySourceName[entry.sourceName] ?? []).filter(
            (name) => !builders.has(name),
         ),
      ].filter((name, i, all) => all.indexOf(name) === i);
      for (const sourceName of names) {
         bindings.push({
            sourceName,
            // Carried, not inferred: a manifest travels without its build plan.
            origin:
               entry.origin === "preaggregate" ? "preaggregate" : "persist",
            destinationName: entry.storageDestinationName,
            virtualHandle: entry.sourceEntityId,
            // Qualified with the attach alias: unqualified, it would resolve
            // against the serve session's default catalog.
            tablePath: storageTablePath(
               entry.storageDestinationName,
               entry.physicalTableName,
               destinationSchema?.(entry.storageDestinationName),
            ),
            schema,
            freshAsOf: entry.dataAsOf,
            freshnessWindowSeconds: entry.freshnessWindowSeconds,
            freshnessFallback: entry.freshnessFallback,
         });
      }
   }
   return bindings;
}

/**
 * Map a DuckDB column type (as reported by `DESCRIBE`) to a Malloy basic type
 * for the serve-shape `type:` declaration.
 *
 * Only Malloy's basic scalar types are legal in `type:`, so every DuckDB type
 * collapses onto one. Anything unrecognized falls back to `json`, a lossy
 * carrier so an exotic column never breaks the whole serve shape, and is logged.
 */
export function duckdbTypeToMalloy(duckdbType: string): string {
   const raw = duckdbType.trim();
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
   // DECIMAL(18,2) -> DECIMAL
   const base = raw
      .replace(/\(.*\)/, "")
      .trim()
      .toUpperCase();

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
 * Generate the transient serve-shape model that rebinds a materialized source to
 * a virtual source on its storage connection.
 *
 * The compiler does NOT check a virtual source's declared columns against the
 * real table, so a wrong schema surfaces only as a serve-time execution error.
 * Hence the binding carries the post-build DESCRIBE schema, declared exactly.
 * The `##! experimental.virtual_source` flag is set on this transient model
 * only, never the author's.
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
source: ${sourceName} is ${binding.destinationName}.virtual('${binding.virtualHandle}')::${shapeTypeName}
`;
   return { modelText, shapeTypeName };
}

/**
 * One `given:` declaration line, with the author's default when it has one.
 *
 * The default is the already-rendered source literal, so a string keeps its
 * quoting and a filter its `f''` form without type-specific printing.
 */
function serveShapeGivenLine(given: ServeShapeGiven): string {
   const suffix =
      given.defaultText !== undefined ? ` is ${given.defaultText}` : "";
   return `  ${given.name} :: ${given.type}${suffix}`;
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
      `${binding.destinationName}.virtual('${binding.virtualHandle}')::${shapeTypeName}`;
   // Anything a refinement references that the shape lacks fails the shape
   // compile, which safely falls back.
   const lines = refinementLines(binding.refinements ?? []);
   if (lines.length > 0) {
      source += ` extend {\n${lines.join("\n")}\n}`;
   }
   return `type: ${shapeTypeName} is {\n${fields}\n}\n${source}`;
}

/**
 * The body of an `extend { … }` block, in the one order that resolves.
 *
 * Joins first (a dimension, measure, filter or view may reference a joined
 * field), then dimensions and measures, then the source's own `where:` clauses
 * (which may reference either), then views (which may reference any of them).
 * One line per filter (see {@link FilterRefinement}).
 *
 * Shared by the rebind of a materialized source and the lift of a derived one so
 * the two cannot drift into different orders.
 */
function refinementLines(refinements: readonly SourceRefinement[]): string[] {
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
      if (r.kind === "filter") lines.push(`   where: ${r.code}`);
   }
   for (const r of refinements) {
      if (r.kind === "view") lines.push(`   view: ${r.text}`);
   }
   return lines;
}

/**
 * The two source sets that explain a serve-shape fallback. "Reference to
 * undefined object" reads the same whether a source is unpersisted or withheld
 * by the freshness gate, so the sets are reported beside it. A name in NEITHER
 * is not materialized at all.
 */
export function serveShapeDiagnostics(
   allBindings: ServeBinding[],
   freshBindings: ServeBinding[],
): { shapeSources: string[]; staleSources: string[] } {
   const shapeSources = freshBindings.map((b) => b.sourceName);
   const fresh = new Set(shapeSources);
   return {
      shapeSources,
      staleSources: allBindings
         .map((b) => b.sourceName)
         .filter((name) => !fresh.has(name)),
   };
}

/**
 * A source the author did NOT persist, carried onto the serve shape because
 * everything it is built from is.
 *
 * Without it a query naming such a source cannot compile against the shape and
 * serves live. This is the entry point for a caller term kept out of artifacts
 * by living one level above the persisted sources.
 */
export interface DerivedSourceLift {
   /** The source name a query names. */
   sourceName: string;
   /**
    * The source it extends, or the source its query reads. Emitted earlier in
    * the shape, by construction.
    */
   base: string;
   /** Only what this source ADDS to its base — see {@link liftDerivedSources}. */
   refinements: SourceRefinement[];
   /**
    * A query-derived source (`X is base -> { … }`), carried verbatim: its output
    * is a new relation, so nothing is inherited to subtract. When set,
    * `refinements` is empty.
    */
   text?: string;
}

/** One lifted source, as `source: X is <base> extend { … }`. */
function derivedSourceFragment(lift: DerivedSourceLift): string {
   if (lift.text !== undefined) return `source: ${lift.text}`;
   const lines = refinementLines(lift.refinements);
   const body = lines.length > 0 ? ` extend {\n${lines.join("\n")}\n}` : "";
   return `source: ${lift.sourceName} is ${lift.base}${body}`;
}

/** The compiled-model facts {@link liftDerivedSources} reads. */
interface DerivedLiftContext {
   /** The model's `contents`, by source name. */
   contents: Record<string, DerivedSourceDef>;
   /** sourceID -> author source name. */
   sourceNameById: Map<string, string>;
   /** The FRESH materialized sources on the shape: the bases a lift may extend. */
   shapeSourceNames: ReadonlySet<string>;
   /**
    * Every source with its own serve binding, fresh or not. Candidates are
    * excluded against this, not {@link shapeSourceNames}: lifting a withheld
    * source would serve its base's artifact under its name as
    * `servedFrom: storage`, defeating a `live`/`fail` `freshnessFallback`.
    */
   boundSourceNames?: ReadonlySet<string>;
   liftText: (location: SourceLocation) => string | undefined;
   /**
    * Carry a persist source that EXTENDS another as a lift over its base (its
    * base's table plus what it adds). Off for the serve path, which binds such
    * an extension as an ALIAS of the base's entry, so a lift would declare the
    * name twice. The chained build binds no alias, so it needs the lift.
    */
   carryPersistExtensions?: boolean;
   /**
    * When given, only these names may be lifted (others stay in `contents` as
    * bases). The serve path passes what a query can name, so a hidden
    * dependency cannot fail the shape.
    */
   candidateNames?: ReadonlySet<string>;
}

/** The subset of a compiled source definition this reads. */
export interface DerivedSourceDef {
   type?: unknown;
   /** The sourceID of the source this one extends, when it extends one. */
   extends?: unknown;
   /**
    * A query-derived source's query. Malloy sets no `extends` on one, so
    * `structRef` is the only link to the source it reads.
    */
   query?: { structRef?: unknown; pipeline?: unknown };
   /** Where the author declared the source, for lifting its text verbatim. */
   location?: SourceLocation;
   fields?: unknown[];
   filterList?: unknown[];
   /** The compiled model's own annotation record, read for `#@ -persist`. */
   annotations?: unknown;
   /** Malloy's own flag: this source carries `#@ persist`, inherited or not. */
   persistent?: unknown;
}

/**
 * Whether a source opts out of the pre-built table with `#@ -persist`.
 *
 * Read from the source's OWN block notes, not the `persistent` flag, which is
 * also false for a source that never inherited `#@ persist`.
 */
function optsOutOfPersist(def: DerivedSourceDef): boolean {
   const notes = (
      def.annotations as { blockNotes?: { text?: unknown }[] } | undefined
   )?.blockNotes;
   return (notes ?? []).some(
      (note) =>
         typeof note?.text === "string" &&
         /^\s*#@\s*-persist\b/.test(note.text),
   );
}

/**
 * Choose the non-persisted sources the shape can carry, and reduce each to what
 * it ADDS to its base.
 *
 * An extending source's field list repeats every inherited field, and its
 * `filterList` starts with the base's. Re-emitting those over the base is
 * `Cannot redefine`, which fails the whole shape, so fields are subtracted by
 * name and filters as a prefix by code.
 *
 * Selection is a fixpoint, so a chain of derived sources is carried whole and a
 * base is always emitted before its extenders.
 *
 * Fails closed:
 *  - A source is carried only when its base is ALREADY on the shape (not
 *    materialized or withheld as stale leaves it off, serving live).
 *  - A source with any join this cannot carry is left off ENTIRELY, not emitted
 *    without the join: its own `where:` may read the alias, and an uncompilable
 *    shape costs every source in the model its tier.
 */
export function liftDerivedSources(
   ctx: DerivedLiftContext,
): DerivedSourceLift[] {
   const lifts: DerivedSourceLift[] = [];
   const available = new Set<string>(ctx.shapeSourceNames);
   // Candidates keep their declaration order, so a fixpoint pass emits a base
   // before its extenders without a topological sort.
   const pending = Object.entries(ctx.contents).filter(
      ([name, def]) =>
         derivedFrom(def) !== undefined &&
         (ctx.candidateNames === undefined || ctx.candidateNames.has(name)) &&
         !(ctx.boundSourceNames ?? ctx.shapeSourceNames).has(name) &&
         // A build target (including a plain extension, which inherits
         // `#@ persist`) is never a candidate. `shapeSourceNames` cannot stand
         // in: a withheld target is absent from it and would be lifted over its
         // base, serving the base's artifact as `servedFrom: storage`.
         (def?.persistent !== true ||
            (ctx.carryPersistExtensions === true &&
               typeof def.extends === "string")) &&
         // `#@ -persist` means recompute instead of reading the pre-built table
         // (pinned by `opt-out-persist-recomputes`); a lift would read it.
         !optsOutOfPersist(def),
   );
   let progressed = true;
   while (progressed) {
      progressed = false;
      for (let i = 0; i < pending.length; i++) {
         const entry = pending[i];
         if (!entry) continue;
         const [name, def] = entry;
         const base = ctx.sourceNameById.get(derivedFrom(def) as string);
         if (!base || !available.has(base)) continue;
         const lift =
            typeof def.extends === "string"
               ? liftOneDerivedSource(name, def, base, available, ctx)
               : liftQueryDerivedSource(name, def, base, available, ctx);
         // A refusal is final: nothing later can make a join target materialized.
         pending[i] = undefined as unknown as (typeof pending)[number];
         progressed = true;
         if (!lift) continue;
         lifts.push(lift);
         available.add(name);
      }
   }
   return lifts;
}

/**
 * The sourceID a derived source is built from: the source it extends, or the
 * source a query-derived source's query reads. Undefined for anything else.
 */
function derivedFrom(def: DerivedSourceDef | undefined): string | undefined {
   if (typeof def?.extends === "string") return def.extends;
   if (def?.type !== "query_source") return undefined;
   return (
      structRefSourceId(def.query?.structRef) ??
      inlineExtendBase(def.query?.structRef)
   );
}

/**
 * The base of an inline extension in `h is (base extend { … }) -> { … }`. The
 * compiler embeds it as the `structRef`: a definition with `extends` and its
 * own join fields but no `sourceID`. Its text travels with the declaration.
 */
function inlineExtendBase(structRef: unknown): string | undefined {
   if (structRef === null || typeof structRef !== "object") return undefined;
   if (structRefSourceId(structRef) !== undefined) return undefined;
   const ext = (structRef as { extends?: unknown }).extends;
   return typeof ext === "string" ? ext : undefined;
}

/** The join fields an inline parenthesized extension declares (see {@link inlineExtendBase}). */
function inlineExtendJoins(structRef: unknown): unknown[] {
   if (inlineExtendBase(structRef) === undefined) return [];
   return ((structRef as { fields?: unknown[] }).fields ?? []).filter(
      (f) => typeof (f as { join?: unknown }).join === "string",
   );
}

/**
 * The sourceID a `structRef` names: the reference itself, or the `sourceID` of
 * an embedded definition. Undefined for an inline source.
 */
function structRefSourceId(structRef: unknown): string | undefined {
   if (typeof structRef === "string") return structRef;
   const sourceID = (structRef as { sourceID?: unknown } | null)?.sourceID;
   return typeof sourceID === "string" && sourceID.length > 0
      ? sourceID
      : undefined;
}

/**
 * Carry a query-derived source (`X is base -> { … } [extend { … }]`) as the
 * author's declaration, verbatim.
 *
 * This serves the private-fact / public-wrapper idiom: `#@ persist` on the fact,
 * queries on a wrapper that reads it. The wrapper has no `extends` and is not
 * `persistent`, so it has no binding of its own.
 *
 * Carried only when everything it names (the base, every source a pipeline stage
 * reads or joins, every declared join) is already on the shape, so an
 * uncompilable lift serves live instead of failing the rung for every source.
 */
function liftQueryDerivedSource(
   sourceName: string,
   def: DerivedSourceDef,
   base: string,
   available: ReadonlySet<string>,
   ctx: DerivedLiftContext,
): DerivedSourceLift | undefined {
   if (!def.location) return undefined;
   const text = ctx.liftText(def.location);
   if (!text) return undefined;
   const reached = referencedSourceIds(def.query?.pipeline);
   for (const sourceID of reached) {
      const name = ctx.sourceNameById.get(sourceID);
      if (!name || !available.has(name)) return undefined;
   }
   const declaredJoins = [
      ...(def.fields ?? []).filter(
         (f) => typeof (f as { join?: unknown }).join === "string",
      ),
      // A join the inline parenthesized base declares is read by the same text.
      ...inlineExtendJoins(def.query?.structRef),
   ];
   for (const join of declaredJoins) {
      const sourceID = (join as { sourceID?: unknown }).sourceID;
      const name =
         typeof sourceID === "string"
            ? ctx.sourceNameById.get(sourceID)
            : undefined;
      if (!name || !available.has(name)) return undefined;
   }
   return { sourceName, base, refinements: [], text };
}

/**
 * Every source a query pipeline names: each stage's `structRef` and each join's
 * `sourceID`. An inline source yields "", which maps to no name and refuses the
 * lift. A named source is not descended into: what it reads is its own
 * binding's concern.
 */
function referencedSourceIds(pipeline: unknown): string[] {
   const out: string[] = [];
   // Too deep refuses this lift rather than throwing, which would escape lift
   // selection and cost every other lift.
   let tooDeep = false;
   const walk = (node: unknown, depth: number): void => {
      if (tooDeep) return;
      if (depth > 200) {
         tooDeep = true;
         return;
      }
      if (node === null || typeof node !== "object") return;
      if (Array.isArray(node)) {
         for (const item of node) walk(item, depth + 1);
         return;
      }
      const record = node as Record<string, unknown>;
      if (typeof record.join === "string") {
         out.push(typeof record.sourceID === "string" ? record.sourceID : "");
         return;
      }
      if (record.structRef !== undefined) {
         out.push(structRefSourceId(record.structRef) ?? "");
      }
      for (const [key, value] of Object.entries(record)) {
         if (key === "structRef") continue;
         walk(value, depth + 1);
      }
   };
   walk(pipeline, 0);
   return tooDeep ? [""] : out;
}

function liftOneDerivedSource(
   sourceName: string,
   def: DerivedSourceDef,
   base: string,
   available: ReadonlySet<string>,
   ctx: DerivedLiftContext,
): DerivedSourceLift | undefined {
   const baseDef = ctx.contents[base];
   const baseFieldNames = new Set(
      (baseDef?.fields ?? []).map((f) => fieldKey(f)).filter(Boolean),
   );
   const ownFields = (def.fields ?? []).filter(
      (f) => !baseFieldNames.has(fieldKey(f)),
   );
   // Every join this source declares must be carried, or none of it is.
   const declaredJoins = ownFields.filter(
      (f) => typeof (f as { join?: unknown }).join === "string",
   ).length;
   const joins = extractJoins(ownFields, {
      sourceNameById: ctx.sourceNameById,
      materializedSourceNames: available,
      liftText: ctx.liftText,
   });
   if (joins.length !== declaredJoins) return undefined;
   return {
      sourceName,
      base,
      refinements: [
         ...joins,
         ...extractRefinements(ownFields),
         ...extractSourceFilters(
            ownFilterList(def.filterList, baseDef?.filterList),
         ),
         ...extractViews(ownFields, ctx.liftText),
      ],
   };
}

/** `as` when the field was renamed, else `name` — how a shape refers to it. */
function fieldKey(field: unknown): string {
   const f = field as { as?: unknown; name?: unknown };
   if (typeof f?.as === "string") return f.as;
   return typeof f?.name === "string" ? f.name : "";
}

/**
 * The filters this source adds, i.e. its `filterList` with the base's stripped
 * from the front.
 *
 * Matched by `code` and only as a PREFIX: inherited filters arrive first, so
 * anything after the first mismatch is the source's own even if it repeats the
 * base's text.
 */
function ownFilterList(
   filterList: unknown[] | undefined,
   baseFilterList: unknown[] | undefined,
): unknown[] {
   const own = filterList ?? [];
   const inherited = baseFilterList ?? [];
   let shared = 0;
   while (
      shared < inherited.length &&
      shared < own.length &&
      (own[shared] as { code?: unknown })?.code ===
         (inherited[shared] as { code?: unknown })?.code
   ) {
      shared++;
   }
   return own.slice(shared);
}

/**
 * Generate ONE transient serve-shape model rebinding every supplied materialized
 * source to its virtual form, with its refinements re-declared on top. This is
 * the model the serve path compiles queries against; a query relying on
 * anything not carried fails to compile here and serves live.
 */
export function buildServeShapeModelForBindings(
   bindings: ServeBinding[],
   /**
    * Pre-aggregation groups. Their members are NOT in `bindings`: a rollup's
    * synthesized name is never queried, so it reaches the shape only here.
    */
   rollupGroups: RollupShapeGroup[] = [],
   /**
    * The author model's WHOLE given surface, not just what re-emitted filters
    * read: an unreferenced given is inert, and the routed query itself may read
    * a given, so the shape must accept what the author's model accepts.
    */
   givens: ServeShapeGiven[] = [],
   /**
    * Non-persisted sources built from materialized ones, in dependency order
    * (see {@link liftDerivedSources}), emitted after the sources they extend.
    */
   derived: DerivedSourceLift[] = [],
   /**
    * The `##!` flags of the author files `derived` came from: lifted text
    * written under e.g. `access_modifiers` compiles only under the same flag.
    */
   documentFlags: string[] = [],
): {
   modelText: string;
} {
   const fragments = orderBindingsByJoinDeps(bindings)
      .map(serveShapeFragment)
      .join("\n");
   // After the materialized sources, before the rollup groups: a lift extends a
   // source emitted above it, and nothing below it can reference one.
   const lifted = derived.map(derivedSourceFragment).join("\n");
   // Rollup groups last: nothing above can reference a group's base, since joins
   // are emitted only to bound sources. Joining TO rollup-backed rows would not
   // mean what the author's join means, so such a query rightly serves live.
   const groups = rollupGroups.map(rollupServeShapeFragment).join("\n");
   // Each flag only when what it enables is emitted.
   const enabled = ["virtual_source"];
   if (rollupGroups.length) enabled.push("composite_sources");
   if (givens.length) enabled.push("givens");
   const ownFlags =
      enabled.length === 1
         ? "##! experimental.virtual_source"
         : `##! experimental { ${enabled.join(" ")} }`;
   const flags = [...documentFlags, ownFlags].join("\n");
   // Before the sources, because a source's re-emitted `where:` reads them.
   const givenBlock = givens.length
      ? `given:\n${givens.map(serveShapeGivenLine).join("\n")}\n`
      : "";
   const body = [fragments, lifted, groups].filter(Boolean).join("\n");
   return { modelText: `${flags}\n${givenBlock}${body}\n` };
}

/**
 * Refinement kinds that are SEMANTICS, carried by every tier of the ladder:
 * dropping a join or view only costs the tier, but dropping a `where:` answers
 * with rows the source excludes.
 */
export const NEVER_THINNED: readonly string[] = ["filter"];

/**
 * The refinement kinds each tier keeps, richest first, down to
 * {@link NEVER_THINNED}. Shared by the serve shape and the chained build so a
 * parent's table is read and thinned the same way in both.
 */
export const REFINEMENT_TIER_KINDS: ReadonlyArray<ReadonlySet<string>> = [
   new Set([...NEVER_THINNED, "join", "dimension", "measure", "view"]),
   new Set([...NEVER_THINNED, "join", "dimension", "measure"]),
   new Set([...NEVER_THINNED, "dimension", "measure"]),
   new Set(NEVER_THINNED),
];

/**
 * Run `attempt` over `bindings` thinned to each tier, richest first, until one
 * succeeds. A tier identical to the previous one is skipped; an error `retryOn`
 * rejects passes through at once; if every tier fails, the last error is thrown.
 */
export async function buildOverTiers<T>(
   bindings: ServeBinding[],
   tiers: ReadonlyArray<ReadonlySet<string>>,
   attempt: (parents: ServeBinding[]) => Promise<T>,
   retryOn: (err: unknown) => boolean,
): Promise<T> {
   let previous: string | undefined;
   let lastError: unknown;
   let attempted = false;
   for (const keep of tiers) {
      const parents = bindings.map((b) => ({
         ...b,
         refinements: (b.refinements ?? []).filter((r) => keep.has(r.kind)),
      }));
      const shape = parents
         .map((p) => (p.refinements ?? []).map((r) => r.kind).join("/"))
         .join("|");
      if (attempted && shape === previous) continue;
      previous = shape;
      attempted = true;
      try {
         return await attempt(parents);
      } catch (err) {
         if (!retryOn(err)) throw err;
         lastError = err;
      }
   }
   throw lastError;
}

/**
 * One rung of the serve-shape escalation ladder: which refinement kinds to keep,
 * and whether pre-aggregation groups are carried.
 */
interface ServeShapeTier {
   keep: ReadonlySet<string>;
   groups: RollupShapeGroup[];
}

/**
 * The ladder `Model.compileServeShape` walks, richest first. Built here so the
 * invariant can be asserted: EVERY tier, including the floor, keeps
 * {@link NEVER_THINNED}.
 */
export function buildServeShapeTiers(
   rollupGroups: RollupShapeGroup[],
): ServeShapeTier[] {
   const always = [...NEVER_THINNED];
   const keepKinds: Array<ReadonlySet<string>> = [...REFINEMENT_TIER_KINDS];
   const hasGroups = rollupGroups.length > 0;
   return [
      { keep: keepKinds[0], groups: rollupGroups },
      // Then groups DROPPED while every authored refinement is kept, so a
      // group-caused failure costs the rollups and nothing else.
      ...(hasGroups
         ? [{ keep: keepKinds[0], groups: [] as RollupShapeGroup[] }]
         : []),
      // Then the thinning ladder, still WITH groups: dropping groups alone did
      // not help, so an authored refinement is implicated.
      ...keepKinds.slice(1).map((keep) => ({ keep, groups: rollupGroups })),
      // The floor: no optional refinements and no groups (without groups the
      // last thinning tier is already this). Must still keep NEVER_THINNED, or
      // a package with any rollup serves a filtered source unfiltered.
      ...(hasGroups
         ? [{ keep: new Set(always), groups: [] as RollupShapeGroup[] }]
         : []),
   ];
}

/** One base source re-exposed over its rollup members. */
export interface RollupShapeGroup {
   baseSourceName: string;
   members: ServeBinding[];
}

/**
 * The fragment that re-exposes ONE base source name over its rollups: each member
 * as its own virtual source, then a `compose()` binding the author's name to them.
 *
 * `compose()` even for one member: it routes identically to a direct rebind
 * (pinned in preaggregation_virtual_compose_contract.spec.ts), and one code path
 * for one grain and several leaves no branch for a grain-count bug to hide in.
 *
 * The composite has no base member (the base is on another connection), so a
 * query no rollup covers fails to compile here and serves live.
 */
function rollupServeShapeFragment(group: RollupShapeGroup): string {
   const members = group.members.map(serveShapeFragment).join("\n");
   const names = group.members.map((m) => m.sourceName).join(", ");
   return `${members}\nsource: ${group.baseSourceName} is compose(${names})`;
}

/**
 * Assemble the transient BUILD model for a chained `storage=` source. Every
 * materialized upstream is rebound to its virtual source (the same rebind as
 * the serve path), so the downstream computes over the parents' STORED tables
 * in DuckDB. The downstream is re-declared over them under `#@ persist` so the
 * caller can read `PersistSource.getSQL({ virtualMap })` from this model.
 *
 * `downstreamDefText` is the RHS of the author's `source: <name> is …`, lifted
 * by location (a top-level source's range starts after `source: `). The
 * `storage=<dest>` value only puts the source in the transient build plan.
 *
 * `derived` carries non-persisted intermediates between the downstream and its
 * stored parents (see {@link liftDerivedSources}), emitted between the two so
 * each resolves what it names. `documentFlags` are the author files' `##!`
 * lines, which lifted declarations need to compile.
 */
export function buildChainedStorageBuildModel(params: {
   upstreams: ServeBinding[];
   downstreamName: string;
   downstreamDefText: string;
   destinationName: string;
   derived?: DerivedSourceLift[];
   documentFlags?: string[];
   /**
    * The author model's given surface. The build binds no given, but carried
    * text reading `$REGION` compiles only if `REGION` is declared.
    */
   givens?: ServeShapeGiven[];
}): string {
   // A rollup is never referenceable, so one here means a caller widened its
   // set by mistake. Asserted rather than filtered so the mistake is loud, not
   // silently tolerated if rollups ever carry refinements into build models.
   const rollup = params.upstreams.find((b) => b.origin === "preaggregate");
   if (rollup) {
      throw new Error(
         `buildChainedStorageBuildModel received a pre-aggregation rollup as an ` +
            `upstream (${rollup.sourceName}). Rollups are not referenceable, so a ` +
            `caller has widened its binding set without filtering by origin.`,
      );
   }
   const upstreamFragments = orderBindingsByJoinDeps(params.upstreams)
      .map(serveShapeFragment)
      .join("\n");
   // Malloy accumulates `##!` lines, so repeating an author's flag is harmless.
   const givens = params.givens ?? [];
   const flags = [
      ...(params.documentFlags ?? []),
      givens.length
         ? "##! experimental { persistence virtual_source givens }"
         : "##! experimental { persistence virtual_source }",
   ].join("\n");
   const givenBlock = givens.length
      ? `given:\n${givens.map(serveShapeGivenLine).join("\n")}\n`
      : "";
   const derived = (params.derived ?? []).map(derivedSourceFragment).join("\n");
   return (
      `${flags}\n` +
      givenBlock +
      `${upstreamFragments}\n` +
      (derived ? `${derived}\n` : "") +
      `#@ persist storage=${params.destinationName}\n` +
      `source: ${params.downstreamDefText}\n`
   );
}

/** What {@link reachedPersistedSources} found on the paths out of a source. */
interface ReachedSources {
   /**
    * The persist sources the walk stopped at. Names are neither tables (several
    * share one) nor package-unique, so the caller resolves each to its content
    * address by `sourceID`.
    */
   persisted: { name: string; sourceID: string }[];
   /**
    * Some path ends at a source with no stored table behind it (a table or SQL
    * source, or a reference with no in-model identity).
    */
   raw: boolean;
   /** The sources those paths ended at (`regions`), for the reason text. */
   rawLeaves: string[];
   /** The non-persisted sources that reach them (`daily_regional`). */
   rawVia: string[];
   /**
    * Every named source the walk passed through or stopped at, root excluded:
    * exactly the sources a build over the stored parents needs declared.
    */
   visited: string[];
}

/**
 * What a source reaches, walking the author model from `name` through the
 * sources it is built from and stopping at each persist source, as
 * `isPersisted` (answered from the build plan) says.
 *
 * The plan, not the annotation, decides: `#@ persist` is inherited, so an
 * `extend` or rename of a persisted source shares its table, and nothing on a
 * definition says which name "declared" it. The walk records names; the caller
 * resolves each to its address group.
 *
 * An extension's own joins are NOT in its table: `daily_regional is daily
 * extend { join_one: r is regions … }` computes the join at read time, so the
 * walk still follows the joins a persisted extension adds, and nothing else.
 *
 * An embedded definition (inline extension, join written in place) is walked
 * as itself; only a reference with no identity, or to a name the model lacks,
 * is raw.
 *
 * A chained build's refusal turns on this: a stored table not bound is an
 * upstream `strictUpstreams` refuses; a raw path can only be recomputed from
 * the warehouse; a source reaching neither that still cannot be carried is a
 * carrying limit, which strict must not treat as licence to recompute.
 */
export function reachedPersistedSources(
   ctx: Pick<DerivedLiftContext, "contents" | "sourceNameById">,
   name: string,
   isPersisted: (sourceName: string, sourceID: string) => boolean,
): ReachedSources {
   const persisted = new Map<string, { name: string; sourceID: string }>();
   const rawLeaves = new Set<string>();
   const rawVia = new Set<string>();
   const visited = new Set<string>();
   const seen = new Set<unknown>();
   const defName = (def: DerivedSourceDef): string | undefined => {
      const id = (def as { sourceID?: unknown }).sourceID;
      return typeof id === "string" ? ctx.sourceNameById.get(id) : undefined;
   };
   const reachRaw = (leaf: string, via: string): void => {
      rawLeaves.add(leaf);
      rawVia.add(via);
   };
   // A `sourceID` is `name@<model URL>`; only the name may appear in a reason,
   // since the URL is the server's file path.
   const nameOf = (sourceID: string): string => sourceID.split("@")[0];
   // A reference is a sourceID, an embedded definition, or nothing usable.
   const follow = (ref: unknown, via: string): void => {
      if (typeof ref === "string") {
         const next = ctx.sourceNameById.get(ref);
         if (next !== undefined) {
            visit(ctx.contents[next], next, false, via);
            return;
         }
         // Not in this model's view (e.g. via an import) but known to the plan
         // as a persist source: a stop, never raw.
         const name = nameOf(ref);
         if (isPersisted(name, ref)) {
            visited.add(name);
            persisted.set(ref, { name, sourceID: ref });
            return;
         }
         reachRaw(name, via);
         return;
      }
      if (ref !== null && typeof ref === "object") {
         const def = ref as DerivedSourceDef;
         visit(def, defName(def), false, via);
         return;
      }
      reachRaw("(unnamed)", via);
   };
   const followJoins = (
      fields: unknown[] | undefined,
      sourceName: string,
   ): void => {
      for (const field of fields ?? []) {
         const f = field as { join?: unknown; sourceID?: unknown };
         if (typeof f.join !== "string") continue;
         follow(
            typeof f.sourceID === "string" ? f.sourceID : field,
            sourceName,
         );
      }
   };
   // The joins an extension adds: its join fields minus the ones its base
   // already carries, matched by the name a shape refers to them by.
   const ownJoins = (def: DerivedSourceDef): unknown[] => {
      const baseName =
         typeof def.extends === "string"
            ? ctx.sourceNameById.get(def.extends)
            : undefined;
      const baseFields = new Set(
         (baseName ? (ctx.contents[baseName]?.fields ?? []) : []).map(fieldKey),
      );
      return (def.fields ?? []).filter((f) => !baseFields.has(fieldKey(f)));
   };
   // `sourceName` is undefined for an embedded definition. `via` is the source
   // that referenced this one, which the reason names alongside the leaf.
   const visit = (
      def: DerivedSourceDef | undefined,
      sourceName: string | undefined,
      root: boolean,
      via: string,
   ): void => {
      const label = sourceName ?? via;
      if (!def) {
         reachRaw(label, via);
         return;
      }
      if (seen.has(def)) return;
      seen.add(def);
      if (!root && sourceName !== undefined) visited.add(sourceName);
      // The root is the source being built; only its descendants stop the walk.
      const sourceID = (def as { sourceID?: unknown }).sourceID;
      if (
         !root &&
         sourceName !== undefined &&
         typeof sourceID === "string" &&
         isPersisted(sourceName, sourceID)
      ) {
         persisted.set(sourceID, { name: sourceName, sourceID });
         // Its table is its base's; only what it adds is computed over it.
         if (typeof def.extends === "string") {
            followJoins(ownJoins(def), sourceName);
         }
         return;
      }
      const base =
         typeof def.extends === "string"
            ? def.extends
            : def.type === "query_source"
              ? def.query?.structRef
              : undefined;
      if (base === undefined) {
         // A table or a SQL source: nothing stored stands behind it.
         reachRaw(label, via);
         return;
      }
      follow(base, label);
      // An extension's `query` is its base's, copied; the base is walked above.
      if (typeof def.extends !== "string") {
         for (const ref of pipelineReferences(def.query?.pipeline)) {
            follow(ref, label);
         }
      }
      followJoins(def.fields, label);
   };
   visit(ctx.contents[name], name, true, name);
   return {
      persisted: [...persisted.values()],
      raw: rawLeaves.size > 0,
      rawLeaves: [...rawLeaves],
      rawVia: [...rawVia],
      visited: [...visited],
   };
}

/**
 * Which stored tables a walk's `persisted` names stand for, and whether each is
 * available to a build: the names sharing a table (`aliasesBySourceName`, from
 * {@link groupAliasesByName} over the plan) are one table, present when ANY of
 * them is among `bound`. Returns the names whose table is not.
 */
export function missingPersistedTables(
   persisted: readonly string[],
   aliasesBySourceName: Record<string, readonly string[]>,
   bound: ReadonlySet<string>,
): string[] {
   return persisted.filter(
      (name) =>
         !(aliasesBySourceName[name] ?? [name]).some((alias) =>
            bound.has(alias),
         ),
   );
}

/**
 * Every reference a query pipeline makes (stage `structRef`s and joins), as the
 * sourceID when named, else the embedded definition so the caller can walk an
 * inline source. Named sources are not descended into; the caller looks them up.
 */
function pipelineReferences(pipeline: unknown): unknown[] {
   const out: unknown[] = [];
   const walk = (node: unknown, depth: number): void => {
      if (depth > 200 || node === null || typeof node !== "object") return;
      if (Array.isArray(node)) {
         for (const item of node) walk(item, depth + 1);
         return;
      }
      const record = node as Record<string, unknown>;
      if (typeof record.join === "string") {
         out.push(typeof record.sourceID === "string" ? record.sourceID : node);
         return;
      }
      if (record.structRef !== undefined) out.push(record.structRef);
      for (const [key, value] of Object.entries(record)) {
         if (key === "structRef") continue;
         walk(value, depth + 1);
      }
   };
   walk(pipeline, 0);
   return out;
}

/**
 * What a bound source adds to its table's relation (joins, dimensions,
 * measures, filters, views), which the build SQL leaves out and the virtual
 * binding must re-declare. Shared by the serve shape and the chained build so a
 * table is read the same way in both; a binding missing its `where:` silently
 * reads unfiltered rows. Joins to sources outside `materializedSourceNames` are
 * left off.
 */
export function authorRefinementsFor(
   sourceName: string,
   ctx: Pick<DerivedLiftContext, "contents" | "sourceNameById" | "liftText">,
   materializedSourceNames: ReadonlySet<string>,
): SourceRefinement[] {
   const fields = ctx.contents[sourceName]?.fields;
   return [
      ...extractJoins(fields, {
         sourceNameById: ctx.sourceNameById,
         materializedSourceNames,
         liftText: ctx.liftText,
      }),
      ...extractRefinements(fields),
      ...extractSourceFilters(ctx.contents[sourceName]?.filterList),
      ...extractViews(fields, ctx.liftText),
   ];
}

/**
 * The `##!` flags of every author file the lifts come from, deduplicated in
 * first-seen order: what a model carrying their text must enable.
 */
export function documentFlagsForLifts(
   lifts: readonly { sourceName: string }[],
   ctx: {
      contents: Record<string, { location?: { url?: string } } | undefined>;
      fileText: (url: string) => string | undefined;
   },
): string[] {
   const files = new Set<string>();
   for (const lift of lifts) {
      const url = ctx.contents[lift.sourceName]?.location?.url;
      if (url) files.add(url);
   }
   return [...files]
      .flatMap((url) => documentFlagLines(ctx.fileText(url) ?? ""))
      .filter((line, i, all) => all.indexOf(line) === i);
}

/** The `##!` lines of an author file, in order, trimmed. */
export function documentFlagLines(text: string): string[] {
   // Block comments first, so a `##!` quoted inside one is not a flag.
   return text
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.startsWith("##!"));
}

/**
 * The compiled-model facts a lift reads: `contents` by source name, a sourceID
 * index, and readers for a declaration's verbatim text or a whole file. Shared
 * by the serve path and the chained build so both read ONE view of the model.
 */
type AuthorModelLiftContext = {
   contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>;
   sourceNameById: Map<string, string>;
   liftText: (location: SourceLocation) => string | undefined;
   fileText: (url: string) => string | undefined;
};

/**
 * One context per compiled model, which is immutable (a republish compiles a
 * new one), so author files are read once rather than per persist source.
 */
const liftContextByModel = new WeakMap<object, AuthorModelLiftContext>();

export function authorModelLiftContext(
   modelDef: unknown,
   readFile: (url: string) => string | undefined,
): AuthorModelLiftContext {
   const cacheable = modelDef !== null && typeof modelDef === "object";
   if (cacheable) {
      const cached = liftContextByModel.get(modelDef);
      if (cached) return cached;
   }
   const built = buildAuthorModelLiftContext(modelDef, readFile);
   if (cacheable) liftContextByModel.set(modelDef, built);
   return built;
}

function buildAuthorModelLiftContext(
   modelDef: unknown,
   readFile: (url: string) => string | undefined,
): AuthorModelLiftContext {
   type Def = DerivedSourceDef & { sourceID?: unknown };
   const md = modelDef as
      | {
           contents?: Record<string, Def>;
           sourceRegistry?: Record<
              string,
              { entry?: (Def & { type?: unknown; as?: unknown }) | undefined }
           >;
        }
      | undefined;
   const namespace = md?.contents ?? {};
   // The namespace plus hidden dependencies from `sourceRegistry`:
   // `import { weekly } from "orders.malloy"` leaves `daily`, which `weekly` is
   // built from, out of `contents`. Including them lets a stored parent reached
   // only through an import be found. The namespace wins a name clash.
   const contents: Record<string, Def> = {};
   for (const value of Object.values(md?.sourceRegistry ?? {})) {
      const entry = value?.entry;
      if (!entry || entry.type === "source_registry_reference") continue;
      const name = typeof entry.as === "string" ? entry.as : undefined;
      if (!name || name in namespace || name in contents) continue;
      contents[name] = entry;
   }
   Object.assign(contents, namespace);
   const sourceNameById = new Map<string, string>();
   for (const [name, def] of Object.entries(contents)) {
      if (typeof def?.sourceID === "string") {
         sourceNameById.set(def.sourceID, name);
      }
   }
   const fileCache = new Map<string, string | null>();
   const fileText = (url: string): string | undefined => {
      if (!url.startsWith("file:")) return undefined;
      if (!fileCache.has(url)) fileCache.set(url, readFile(url) ?? null);
      return fileCache.get(url) ?? undefined;
   };
   const liftText = (location: SourceLocation): string | undefined => {
      if (!location?.url) return undefined;
      const text = fileText(location.url);
      return text ? sliceSourceRange(text, location.range) : undefined;
   };
   return { contents, sourceNameById, liftText, fileText };
}

/**
 * Order bindings so a joined source is declared before the source joining it
 * (Malloy resolves `source:` top-to-bottom). Stable. On a cycle the rest are
 * appended in original order; the shape then fails to compile and the caller
 * thins joins, rather than looping.
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
 * Whether a compiled field carries a non-public access modifier. Such a field
 * must NEVER be re-emitted: the virtual source carries no access modifiers, so
 * it would expose what the live path refuses. Anything but absent/`public`
 * counts as restricted, so an unknown future modifier fails closed.
 */
function isAccessRestricted(field: unknown): boolean {
   const am = (field as { accessModifier?: unknown }).accessModifier;
   return am != null && am !== "public";
}

/**
 * Narrow a binding's captured DESCRIBE schema to the columns the source
 * PUBLICLY exposes, dropping any physical column the source hides.
 *
 * A column hidden by `except:` or an access modifier is still materialized and
 * captured by `DESCRIBE`; declaring it would expose it through the virtual
 * source. The serve surface must reproduce the source's PUBLIC surface exactly,
 * never widen it. A query touching a dropped column serves live, where
 * visibility is enforced.
 *
 * A captured name matches EXACTLY first, then case-insensitively, and is
 * emitted under the AUTHOR's spelling; an ambiguous fold is dropped. See the
 * body for why each rule matters.
 */
export function narrowSchemaToPublic(
   schema: { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
): { name: string; type: string }[] {
   // `fields` has the author's spelling; `schema` has DESCRIBE's, which an
   // upper-folding warehouse (Snowflake, Oracle, Redshift, Teradata) reports in
   // its own case. Exact-only matching would narrow to nothing there, and the
   // caller drops an empty binding, silently losing the tier.
   const exactNames = new Set<string>();
   const byFoldedName = new Map<string, string[]>();
   for (const f of fields ?? []) {
      const name = (f as { name?: unknown }).name;
      if (typeof name === "string" && !isAccessRestricted(f)) {
         exactNames.add(name);
         const folded = name.toLowerCase();
         const sameFold = byFoldedName.get(folded);
         if (sameFold) {
            sameFold.push(name);
         } else {
            byFoldedName.set(folded, [name]);
         }
      }
   }
   const emitted = new Set<string>();
   const out: { name: string; type: string }[] = [];
   for (const c of schema) {
      let authorName: string | undefined;
      if (exactNames.has(c.name)) {
         // Exact first: Malloy allows fields differing only in case, and folding
         // first could declare a stored column under a dimension's name, which
         // serves the raw column in place of the computed one (wrong rows).
         authorName = c.name;
      } else {
         const candidates = byFoldedName.get(c.name.toLowerCase());
         // Exactly one, or drop (serves live) rather than guess.
         if (candidates?.length === 1) {
            authorName = candidates[0];
         }
      }
      // Not on the public surface: dropped. Neither lookup can invent a field.
      if (authorName === undefined) {
         continue;
      }
      // A folded and an exact hit can reach one field; declaring it twice fails
      // the shape, so the first in captured order wins.
      if (emitted.has(authorName)) {
         continue;
      }
      emitted.add(authorName);
      out.push({ name: authorName, type: c.type });
   }
   return out;
}

/**
 * The source-level filters to re-declare on the serve shape, one per
 * `filterList` entry of the materialized source's compiled definition.
 *
 * EVERY entry is carried, even one the shape cannot reproduce (through an
 * unmaterialized join, or over a hidden column): the shape must then fail to
 * compile, because a dropped filter serves rows the source excludes.
 * `Model.compileServeShape` probes bindings alone and withholds only the ones
 * whose filters do not reproduce, so siblings keep their tier.
 *
 * The main case is a `where:` over a given: the build leaves it out, so the
 * artifact holds every caller's rows, and this re-emission puts the term back
 * per caller. `filterList` accumulates through `extend`, so inherited filters
 * are included.
 *
 * A rollup member carries no filter refinement: its build reads the base, so
 * the filter is already baked in.
 *
 * A malformed entry (non-string `code`) yields {@link UNREPRODUCIBLE_FILTER},
 * withholding the binding rather than under-filtering.
 */
export function extractSourceFilters(
   filterList: readonly unknown[] | undefined,
): FilterRefinement[] {
   const out: FilterRefinement[] = [];
   for (const entry of filterList ?? []) {
      const code = (entry as { code?: unknown })?.code;
      out.push({
         kind: "filter",
         code: typeof code === "string" ? code : UNREPRODUCIBLE_FILTER,
      });
   }
   return out;
}

/**
 * Emitted for a filter whose text could not be read. Deliberately not valid
 * Malloy: failing the shape compile serves live, where dropping the entry would
 * serve the unfiltered relation.
 */
export const UNREPRODUCIBLE_FILTER = "__unreproducible_filter__";

/**
 * The dimensions and measures defined on a materialized source, from its
 * compiled fields. Raw columns have no `code`; joins and views are extracted
 * separately, and analytic/calculation fields are skipped (those queries serve
 * live).
 */
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
      } else if (
         f.expressionType === "aggregate" ||
         // `all(…)` / `exclude(…)`: carried views often read one.
         f.expressionType === "ungrouped_aggregate"
      ) {
         out.push({ kind: "measure", name: f.name, code: f.code });
      }
   }
   return out;
}

/** A zero-based Malloy source range (as carried on a compiled field's `location`). */
interface SourceRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** A compiled field's source location: the file URL plus the range it spans. */
export interface SourceLocation {
   url: string;
   range: SourceRange;
}

/**
 * Slice the text covered by a zero-based Malloy `location.range`. Undefined
 * when out of bounds (stale or mismatched source), so the caller skips the
 * refinement rather than emitting garbage.
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
interface JoinExtractionContext {
   /**
    * sourceID -> author source name. An inline join target is absent, so its
    * join is skipped.
    */
   sourceNameById: Map<string, string>;
   /** Author source names that are materialized (have a serve binding). */
   materializedSourceNames: ReadonlySet<string>;
   /** Lift a field's verbatim source text from its `location`, or undefined. */
   liftText: (location: SourceLocation) => string | undefined;
}

/**
 * Extract a materialized source's re-emittable joins. A join is carried only
 * when its target is a named in-model source that is itself materialized: a
 * join to a source the shape does not declare fails the whole shape (see
 * {@link JoinRefinement}). A join that cannot be mapped or lifted is skipped,
 * and queries using it serve live.
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
 * Extract a materialized source's re-emittable views, emitted optimistically
 * (see {@link ViewRefinement}). A view whose text cannot be lifted is skipped.
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
 * Assemble the per-call `virtualMap` (`destinationName -> handle -> table
 * path`). Core pastes each path verbatim, so it must be quoted canonical
 * DuckDB SQL. {@link quoteManifestTablePath} is the same quoting authority the
 * manifest binds use, and never double-quotes an author-quoted `name=`.
 */
export function buildVirtualMap(
   bindings: ServeBinding[],
): Map<string, Map<string, string>> {
   const map = new Map<string, Map<string, string>>();
   for (const b of bindings) {
      let inner = map.get(b.destinationName);
      if (!inner) {
         inner = new Map<string, string>();
         map.set(b.destinationName, inner);
      }
      inner.set(b.virtualHandle, quoteManifestTablePath(b.tablePath, "duckdb"));
   }
   return map;
}

/**
 * The DuckDB portability gate, run post-build because it needs the captured
 * schema: refuse a source whose serve shape does not compile in DuckDB, turning
 * a serve-time 500 into a build-time refusal.
 *
 * It compiles only the base shape (captured columns, no refinements), so it is
 * a floor, not a full check that the source's refinements are DuckDB-portable.
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

/**
 * The bindings left once every binding with a caller-scoped join the serve shape
 * cannot reproduce is withheld, to a fixpoint; and the names withheld.
 *
 * A join to a given-scoped source reproduces the caller's scope only through
 * that source's own binding (see `joinedSourceRefusal`). It cannot be re-emitted
 * when the target is unbound, bound on another destination (no cross-connection
 * joins), or the join is access-restricted; fields reading through it then fail
 * the shape. Left in, the ladder would thin EVERY binding in the model;
 * withholding confines the cost to the joining source, which serves live.
 *
 * A fixpoint, because a withheld binding stops being a valid target.
 * Ordinary unmaterialized join targets are not handled here: extraction skips
 * them and the ladder answers whatever reads through them.
 */
export function withholdUnreproducibleCallerScopedJoins(
   bindings: ServeBinding[],
   contents: Record<string, { fields?: unknown }> | undefined,
   sourceNameById: ReadonlyMap<string, string>,
): { kept: ServeBinding[]; withheld: string[] } {
   let kept = bindings;
   for (;;) {
      const destinationOf = new Map(
         kept.map((b) => [b.sourceName, b.destinationName]),
      );
      const next = kept.filter((b) =>
         givenScopedJoinTargets(contents?.[b.sourceName]?.fields).every(
            (sourceID) => {
               const target =
                  sourceID === undefined
                     ? undefined
                     : sourceNameById.get(sourceID);
               return (
                  target !== undefined &&
                  destinationOf.has(target) &&
                  destinationOf.get(target) === b.destinationName
               );
            },
         ),
      );
      if (next.length === kept.length) {
         const keptNames = new Set(kept.map((b) => b.sourceName));
         return {
            kept,
            withheld: bindings
               .map((b) => b.sourceName)
               .filter((name) => !keptNames.has(name)),
         };
      }
      kept = next;
   }
}
