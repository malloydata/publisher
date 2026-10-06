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

/**
 * The per-source serve pointer the virtual-source serve transform consumes. Two
 * origins, one shape (the "injectable binding seam"): the publisher self-derives
 * it from a build's manifest entry (standalone), or a host
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
 * A `where:` written in the materialized source's extend block, re-declared on
 * the serve shape's virtual base.
 *
 * The filter is NOT part of the materialized relation: Malloy's build SQL for a
 * persisted source is the persisted relation alone, and an extend-block `where:`
 * refines that relation when it is read. The colocated tier gets this for free
 * (substitution swaps only the `FROM`, so the reading query keeps its own
 * `WHERE`); the storage tier re-declares the source instead, so it has to carry
 * the filter itself or serve rows the source excludes.
 *
 * `code` is the author's verbatim expression text. One refinement per
 * `filterList` entry, emitted as its own `where:` line: Malloy parenthesises
 * each entry and ANDs them, so joining two entries' `code` with `and` would
 * reassociate a top-level `or`.
 */
export interface FilterRefinement {
   kind: "filter";
   /** Verbatim author expression, e.g. `not is_deleted`. */
   code: string;
}

/**
 * A `given:` to declare on the serve-shape model, copied from the author's.
 *
 * The other half of {@link FilterRefinement}. A re-emitted `where:` carries the
 * author's verbatim text, so a term over a given arrives as `where: org_id =
 * $ORG_ID` — which compiles only if the shape declares `ORG_ID`. Declaring it is
 * what turns a stripped term back into the filter it was: Malloy substitutes the
 * value the request bound as an inline literal, so the artifact is read with the
 * caller's own predicate, and an equality on a partition column prunes the
 * files it does not name.
 *
 * `defaultText` is the author's default as a source literal, carried so a caller
 * who supplies nothing gets the answer the live path would give them. Without
 * it, such a request fails the shape compile ("no value and no default") and
 * falls back to live — safe, but a silent loss of the tier for every unbound
 * request.
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

export interface ServeBinding {
   /** The Malloy source name to rebind (`source: <sourceName> is ...`). */
   sourceName: string;
   /**
    * What the source this table was built for IS: `persist` for one the modeler
    * annotated, `preaggregate` for a rollup the publisher synthesized from
    * `#@ preaggregate` measures. Absent means `persist`, which is what an entry
    * written before the field existed means.
    *
    * Load-bearing rather than decorative, because the two are shaped differently
    * and a rollup handled as an ordinary binding fails in both directions. Its
    * `sourceName` names no source in any model file, so the author-model lookups
    * that give an ordinary binding its refinements and its public column set find
    * nothing; and its stored columns are partial aggregates that no source
    * publicly exposes, so narrowing them against an author source would strip
    * exactly the columns its measures read. See {@link rollupServeBindings}.
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
   /**
    * Optional freshness window + fallback, carried through verbatim so the serve
    * path can gate this binding per query the SAME way the colocated
    * serve does — a stale binding whose fallback is `live`/`fail` is dropped from
    * the serve shape and served live; `stale_ok` (or un-gated) keeps serving the
    * materialized table. Placement (`storage=`) is orthogonal to freshness.
    */
   freshnessWindowSeconds?: number;
   freshnessFallback?: "live" | "stale_ok" | "fail";
}

/**
 * For each persist source in a build plan, the sources that materialize into its
 * table — the `aliasesBySourceName` argument {@link deriveServeBindings} takes.
 *
 * Grouped BY content address, because that is what decides which sources really
 * share a table, then keyed BY name, because a name is the only identifier a
 * manifest entry carries that means the same thing whoever built it (an
 * instructed build stamps the caller's `sourceEntityId` on its entry).
 *
 * A name declared at more than ONE address is dropped from aliasing entirely.
 * Source names are not unique in a package — two models may each declare `daily`,
 * which is why the wire plan is keyed by sourceID — so such a name cannot be
 * resolved to a table by name at all, and picking one by map order would
 * eventually bind the same name to two different tables. Two `source: daily`
 * declarations then land in one serve shape, which fails to compile and takes the
 * storage tier out for EVERY model in the package (bindings are pushed
 * package-wide), silently: base-only is the tier the ladder trusts without a
 * probe, so the failure surfaces per query rather than at shape build where the
 * tier-drop metric would see it. Dropping the ambiguous name costs that one source
 * its routing and keeps everything else correct.
 *
 * The source that OWNS a name still binds it — see the builder rule in
 * {@link deriveServeBindings}. Only aliasing is withheld.
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
 * Derive the publisher's self-maintained serve bindings from a build's manifest
 * entries — the standalone half of the injectable binding seam (a host/control
 * plane can supply {@link ServeBinding}s directly instead). Only entries that
 * were materialized into a storage destination (carrying `storageDestinationName`
 * and a captured `schema`) produce a binding; colocated entries do
 * not (they serve through the same-connection manifest, not the transform).
 *
 * The virtual handle is the source's build-posture **content identity**
 * (`sourceEntityId`): identity-scoped so two imports of the same source dedup to
 * one shared virtual table, and stable across generations — the generation lives
 * in the mapped table path (`physicalTableName`), not the handle. This is the one
 * hard cross-producer contract: whoever supplies the binding (this function, or
 * a host) must key the handle the same way the build did.
 *
 * An entry can serve MORE THAN ONE source. `#@ persist` is inherited and `extend`
 * does not change a source's materialization SQL, so a base and its extension
 * share a content address and therefore one entry and one table — the extension
 * correctly gets no table of its own, but it still has to READ the base's. An
 * entry names only the source that BUILT it, so `aliasesBySourceName` supplies the
 * rest, and every one of them is bound to the same virtual handle. That is the
 * handle's purpose: it is identity-scoped, so several sources resolving to one
 * virtual table is the design rather than a collision.
 *
 * Without it exactly one alias routes and the others silently serve live, chosen
 * by whichever source happened to build the table.
 *
 * Keyed by source NAME, deliberately, not by `entry.sourceEntityId`. That field
 * carries the identity the BUILDER stamped, which on an instructed build is the
 * caller's — `executeInstructedBuild` treats an instruction's `sourceEntityId` as
 * opaque precisely so a host may derive it any way it likes — while the alias
 * grouping has to be computed from the publisher's own content addresses. Keying
 * on the entry's id would agree with the group only while the host happened to
 * hash exactly as the publisher does, and would silently degrade to one-alias
 * routing the moment it did not. A name is the one identifier both sides mean the
 * same thing by.
 */
export function deriveServeBindings(
   entries: Record<string, ManifestEntry>,
   aliasesBySourceName: Record<string, string[]>,
): ServeBinding[] {
   const bindings: ServeBinding[] = [];
   // Every name that OWNS a table in this manifest. An alias never claims one:
   // the owner is the source whose SQL produced that table, and a name bound
   // twice — once as its owner, once as someone else's alias — puts two
   // `source: <name>` declarations in one serve shape.
   const builders = new Set(
      Object.values(entries)
         .map((entry) => entry.sourceName)
         .filter((name): name is string => !!name),
   );
   for (const entry of Object.values(entries)) {
      // Need the source name to rebind it, plus a storage destination + table.
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
      // The builder's own name first, so it wins any ordering downstream; the
      // aliases follow. Deduplicated because the builder's name is normally in
      // the address group too.
      const names = [
         entry.sourceName,
         ...(aliasesBySourceName[entry.sourceName] ?? []).filter(
            (name) => !builders.has(name),
         ),
      ].filter((name, i, all) => all.indexOf(name) === i);
      for (const sourceName of names) {
         bindings.push({
            sourceName,
            // Carried rather than inferred: a manifest travels without its build
            // plan, so this is the only thing that says a table belongs to a
            // source that appears in no model file.
            origin:
               entry.origin === "preaggregate" ? "preaggregate" : "persist",
            destinationName: entry.storageDestinationName,
            virtualHandle: entry.sourceEntityId,
            // Qualify the table with the destination catalog (the attach alias) so
            // the serve reads `<store>.<table>` — the build wrote it there, and an
            // unqualified name would resolve against the serve session's default
            // catalog, not the attached store.
            tablePath: `${entry.storageDestinationName}.${entry.physicalTableName}`,
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
source: ${sourceName} is ${binding.destinationName}.virtual('${binding.virtualHandle}')::${shapeTypeName}
`;
   return { modelText, shapeTypeName };
}

/**
 * One `given:` declaration line, with the author's default when it has one.
 *
 * The default is emitted as its already-rendered source literal rather than
 * re-printed from the parsed AST, so a string keeps its quoting and a filter
 * keeps its `f''` form without this having to know one type from another.
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
   // Re-declare the source's refinements on the virtual base so queries that use
   // them are computed from the stored tables at serve time (the wrapper) rather
   // than falling back to live. Emission order matters for resolution: joins
   // first (a dimension/measure/view may reference a joined field), then
   // dimensions/measures, then the source's own `where:` clauses (which may
   // reference either), then views (a view may reference any of them).
   // Everything here references the shape's columns, a sibling virtual source, or
   // an earlier refinement; anything it references that the shape lacks makes the
   // serve shape fail to compile, which safely falls back.
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
 * One line per filter — see {@link FilterRefinement} for why they are never
 * combined with `and`.
 *
 * Shared by the virtual rebind of a materialized source and the lift of a
 * derived one so the two cannot drift into different orders: a change that
 * resolved in one and not the other would show up as an unexplained fallback on
 * whichever was not the case under test.
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
/**
 * The two source sets that explain a serve-shape fallback.
 *
 * The compiler names the symbol it could not resolve, which is a symptom shared
 * by both reasons a source can be missing from the shape: it carries no
 * `#@ persist`, or the freshness gate withheld it. "Reference to undefined
 * object" reads identically either way, so the sets are reported next to it.
 *
 * A name the query wants that appears in NEITHER is not materialized at all.
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
 * Without this a query naming such a source cannot compile against the shape and
 * is served live — correct answers, no tier — which is the whole cost of the
 * arrangement the partitioned design recommends for a caller term that does not
 * belong in an artifact (a term over a joined source's own filter). The term
 * lives one level above the persisted sources, where it was never in an artifact
 * to begin with, so serving it needs the entry point on the shape.
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
    * A query-derived source (`X is base -> { … }`), carried as the author's
    * declaration verbatim rather than as refinements over its base: a query's
    * output is a new relation, so nothing it declares is inherited from the base
    * and there is nothing to subtract. When set, `refinements` is empty.
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
export interface DerivedLiftContext {
   /** The model's `contents`, by source name. */
   contents: Record<string, DerivedSourceDef>;
   /** sourceID -> author source name. */
   sourceNameById: Map<string, string>;
   /**
    * Names already on the shape: the materialized sources it rebinds, FRESH
    * ones only. These are the bases a lift may extend.
    */
   shapeSourceNames: ReadonlySet<string>;
   /**
    * Every source with a serve binding of its own, fresh or not. A candidate is
    * excluded against this rather than against {@link shapeSourceNames}, and the
    * two differ by exactly the sources whose bindings were withheld.
    *
    * Carrying one of those would serve its base's artifact under its name while
    * reporting `servedFrom: storage` — which is the outcome a `freshnessFallback`
    * of `live` or `fail` exists to prevent. A source with no binding at all is
    * still a candidate; it is the withheld ones that must fall back.
    */
   boundSourceNames?: ReadonlySet<string>;
   liftText: (location: SourceLocation) => string | undefined;
   /**
    * Carry a persist source that EXTENDS another as a lift over its base. By
    * design `#@ persist` is inherited and `extend` does not change the SQL, so
    * such an extension is its base's table plus what it adds; a lift over the
    * base's rebound source, re-declaring the additions, is exactly its meaning.
    * Off by default, which is what the serve path wants: there the extension is
    * bound as an ALIAS of its base's entry, with that entry's freshness gate, and
    * a lift beside the alias would be two declarations of one name. The chained
    * build binds no alias for an extension, so it is the lift or nothing.
    */
   carryPersistExtensions?: boolean;
}

/** The subset of a compiled source definition this reads. */
export interface DerivedSourceDef {
   type?: unknown;
   /** The sourceID of the source this one extends, when it extends one. */
   extends?: unknown;
   /**
    * A query-derived source's query. Malloy does not set `extends` on one
    * (`mkQuerySourceDef` drops it, deliberately), so `structRef` is the only
    * link to the source it reads.
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
 * Read from the source's OWN block notes, never from the `persistent` flag. A
 * source extending a persisted one inherits its `#@ persist` and so is
 * `persistent: true`; writing `#@ -persist` makes it `false` — but so does
 * simply never having inherited one, and the two mean different things here.
 * Only the annotation says the AUTHOR asked for this.
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
 * Both halves are load-bearing, and the second is the one that is not obvious.
 * A source that extends another INHERITS every one of its fields — its
 * dimensions, measures, views and joins are all present again on the extending
 * source's own field list, and its `where:` clauses are a prefix of the
 * extending source's `filterList`. Re-emitting any of them on top of a base that
 * already declares them is `Cannot redefine`, which fails the whole shape and
 * takes storage serving for every source in the model with it. So each kind is
 * subtracted against the base: fields by name, filters as a prefix by their
 * code.
 *
 * Selection is a fixpoint rather than one pass, so a chain (a source derived
 * from a derived source) is carried whole, and append order is emission order —
 * a base is always written before anything extending it.
 *
 * It fails closed twice over, and neither is theoretical:
 *
 *  - A source is carried only when its base is ALREADY on the shape. A base that
 *    is not materialized, or whose binding the freshness gate withheld, leaves
 *    the source off, and a query naming it falls back live.
 *  - A source that declares a join this cannot carry is left off ENTIRELY rather
 *    than emitted without it. Dropping a join silently is safe on a materialized
 *    source, where an unreferenced join is pruned from the build and a referenced
 *    one fails to compile; here it is not, because the source's own `where:` may
 *    read the alias, and a shape that does not compile costs every source in the
 *    model its tier rather than just this one. Any join not carried — a
 *    non-materialized target, an inline target with no sourceID, an access-
 *    restricted one, or a declaration whose text could not be recovered —
 *    refuses the lift.
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
         !(ctx.boundSourceNames ?? ctx.shapeSourceNames).has(name) &&
         // A source that is itself a build target is never a lift candidate,
         // however it came to be one — `persistent` is true for a plain
         // extension of a persisted source, which inherits the annotation.
         //
         // `shapeSourceNames` cannot stand in for this. It is the set of bindings
         // that are present AND FRESH, so a persist target whose binding was
         // withheld — refused, failed, never run, or stale past its window — is
         // absent from it and would otherwise be lifted over its base. That
         // serves the base's artifact under the derived source's name while
         // reporting `servedFrom: storage`, which is precisely what a
         // `freshnessFallback` of `live` or `fail` exists to prevent.
         (def?.persistent !== true ||
            (ctx.carryPersistExtensions === true &&
               typeof def.extends === "string")) &&
         // `#@ -persist` is documented as recomputing the query INSTEAD of using
         // the pre-built table, and `opt-out-persist-recomputes` pins that
         // reading. Carrying such a source here would serve it from the stored
         // table, which is the opposite of what its author asked for.
         //
         // This does NOT leave the lift with nothing to carry. Persistence is
         // inherited through `extend` (Malloy's `src/doc/persist/api.md`), and an
         // inheriting source is documented as reading the persisted table — which
         // is what a lift over its base does. `#@ -persist` is the annotation that
         // opts out of exactly that, so it is the one excluded here.
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
         // A source whose joins cannot all be carried is refused for good, not
         // retried: nothing later in the fixpoint can make a join target
         // materialized.
         pending[i] = undefined as unknown as (typeof pending)[number];
         progressed = true;
         if (!lift) continue;
         lifts.push(lift);
         available.add(name);
      }
   }
   return lifts;
}

/** One candidate, or undefined when it cannot be carried safely. */
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
 * The source an inline parenthesized extension is built on, for a declaration
 * of the form `h is (base extend { … }) -> { … }`. The compiler embeds the
 * parenthesized source as the `structRef` itself: a definition with `extends`
 * and its own join fields but no `sourceID`, since nothing names it. Its text
 * is part of the declaration's, so it is carried with the declaration; what
 * the lift needs from it is what it reads — its base, and its joins (see
 * {@link inlineExtendJoins}).
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
 * The sourceID a `structRef` names. The compiled model carries it either as the
 * reference itself or as the referenced definition embedded whole, which then
 * holds its own `sourceID`. An inline source has neither and yields undefined.
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
 * The private-fact / public-wrapper idiom is the case: `#@ persist` sits on the
 * fact, and queries name a wrapper whose query reads it. Malloy gives such a
 * wrapper no identity of its own (no `extends`, not `persistent`), so it is never
 * a build target and has no binding; without this it is an undefined name on the
 * shape and every query naming it serves live.
 *
 * Carried only when everything the declaration names is already on the shape,
 * decided before emission: the base; every source a pipeline stage reads or
 * joins; and every join the extend block declares. That is what keeps a lift
 * that could not compile off the shape — a declaration reaching a warehouse
 * table or an unmaterialized source is left off, and a query naming it falls
 * back live, rather than failing the rung and costing every source in the model
 * its views.
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
 * Every source a query pipeline names — a stage's `structRef`, and the
 * `sourceID` of anything it joins. A reference with no in-model identity (an
 * inline source) is reported as the empty string, which maps to no name and so
 * refuses the lift. A named source is not descended into: what it reads is its
 * own binding's concern, and its embedded definition names sources (its own
 * base, a warehouse table) that are rightly absent from the shape.
 */
function referencedSourceIds(pipeline: unknown): string[] {
   const out: string[] = [];
   // Too deep to read is a reference that cannot be vouched for, so it refuses
   // this lift — never an exception, which would escape lift selection and cost
   // every other lift in the model with it.
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
 * Matched by `code` rather than by identity, and only as a PREFIX: a source's
 * inherited filters arrive ahead of its own, so anything after the first
 * mismatch is the source's own even if it happens to repeat the base's text.
 * Re-applying an inherited filter would in fact be harmless for a deterministic
 * predicate — the terms are ANDed — but it would put a term in the shape text
 * that the base beside it already carries, which is a thing a reader has to
 * work out rather than read.
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

export function buildServeShapeModelForBindings(
   bindings: ServeBinding[],
   /**
    * Pre-aggregation groups, each re-exposing one base source name over its
    * rollup members. Their members are NOT in `bindings`: a rollup is bound under
    * a synthesized name nothing queries, and it reaches the shape only through
    * its group.
    */
   rollupGroups: RollupShapeGroup[] = [],
   /**
    * The author model's given surface, declared verbatim on the shape.
    *
    * The WHOLE surface, not the subset the re-emitted filters reference. Two
    * reasons, and the second is the one that matters. A given the shape declares
    * and nothing references is inert — Malloy substitutes only where a name is
    * read — so carrying extras costs nothing. And the routed QUERY is compiled
    * against this model too: a query whose own text reads a given the model
    * declares would otherwise fail to compile here and fall back to live, even
    * though the live answer it falls back to is the same query over the same
    * rows. Declaring the surface makes the shape accept exactly what the author's
    * model accepts, which is the property that lets a caller-scoped term live in
    * a non-persisted extension over materialized sources.
    */
   givens: ServeShapeGiven[] = [],
   /**
    * Non-persisted sources built entirely from materialized ones, in dependency
    * order (see {@link liftDerivedSources}). Emitted after the sources they
    * extend, which is what makes them resolve.
    */
   derived: DerivedSourceLift[] = [],
   /**
    * The `##!` flags of the author files whose declarations `derived` carries,
    * verbatim. A lift is the author's text, and text written under
    * `access_modifiers` (an `include { public: … }` wrapper) or any other
    * experiment compiles only under the same flag; without them one such lift
    * fails the shape and every lift with it.
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
   // Rollup groups last. Nothing above can reference a group's base name — a join
   // is emitted only when its target is itself a bound source, and a rollup's base
   // is not one — so no ordering constraint reaches across this boundary.
   //
   // That exclusion is correct rather than merely convenient: joining TO a
   // rollup-backed source would join to pre-aggregated rows, which is not what the
   // author's join means. A query using such a join does not compile against this
   // shape and is served live, which is the right answer.
   const groups = rollupGroups.map(rollupServeShapeFragment).join("\n");
   // Each flag only when the thing it enables is actually emitted, so a package
   // with no rollups and no givens produces byte-identical text to before either
   // existed — an unused experimental flag should not be a difference anyone has
   // to reason about when reading a shape that has none of it in it.
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

/** One base source re-exposed over its rollup members. */
/**
 * The refinement kind that is SEMANTICS rather than an optimization, and so is
 * carried by every tier of the serve-shape ladder.
 *
 * Dropping a join or a view costs the tier for the queries that use it; dropping
 * a source's `where:` answers with rows the source excludes. See
 * {@link buildServeShapeTiers}.
 */
export const NEVER_THINNED: readonly string[] = ["filter"];

/**
 * One rung of the serve-shape escalation ladder: which refinement kinds to keep,
 * and whether pre-aggregation groups are carried.
 */
export interface ServeShapeTier {
   keep: ReadonlySet<string>;
   groups: RollupShapeGroup[];
}

/**
 * The ladder `Model.compileServeShape` walks, richest first.
 *
 * Built here rather than inline so the invariant that matters can be asserted:
 * EVERY tier keeps {@link NEVER_THINNED}. A tier that did not would answer with
 * rows the source excludes, which is the defect the filter refinement exists to
 * close — and a floor tier assembled separately from the thinning ladder is
 * exactly how that reappears.
 */
export function buildServeShapeTiers(
   rollupGroups: RollupShapeGroup[],
): ServeShapeTier[] {
   const always = [...NEVER_THINNED];
   // Richest first; each keeps fewer optional kinds than the last.
   const keepKinds: Array<ReadonlySet<string>> = [
      new Set([...always, "join", "dimension", "measure", "view"]),
      new Set([...always, "join", "dimension", "measure"]),
      new Set([...always, "dimension", "measure"]),
      new Set(always),
   ];
   const hasGroups = rollupGroups.length > 0;
   return [
      // Richest, with groups.
      { keep: keepKinds[0], groups: rollupGroups },
      // Then groups DROPPED while every authored refinement is kept, so a
      // group-caused failure costs the rollups and nothing else.
      ...(hasGroups
         ? [{ keep: keepKinds[0], groups: [] as RollupShapeGroup[] }]
         : []),
      // Then the ordinary thinning ladder, still WITH groups: reaching here means
      // dropping the groups alone did not fix it, so an authored refinement is
      // implicated and the groups may be fine.
      ...keepKinds.slice(1).map((keep) => ({ keep, groups: rollupGroups })),
      // The floor: no optional refinements and no groups. Appended only when
      // there is a group to drop, since without one the last thinning tier is
      // already this shape. It keeps the never-thinned kinds like every tier
      // above it — this floor is the one that historically did not, which let a
      // package carrying any rollup serve a filtered source unfiltered.
      ...(hasGroups
         ? [{ keep: new Set(always), groups: [] as RollupShapeGroup[] }]
         : []),
   ];
}

export interface RollupShapeGroup {
   baseSourceName: string;
   members: ServeBinding[];
}

/**
 * The fragment that re-exposes ONE base source name over its rollups: each member
 * as its own virtual source, then a `compose()` binding the author's name to them.
 *
 * `compose()` even for a single member, deliberately. A one-member composite
 * compiles and routes identically to a direct rebind (pinned in
 * preaggregation_virtual_compose_spike.spec.ts), so treating one grain and several
 * as one code path removes a branch that would otherwise be the only difference
 * between the common case and the general one — and a branch there is exactly
 * where a "works with one grain, silently stops with two" bug would live.
 *
 * The composite is NOT total: there is no base member, because the base lives on
 * the source warehouse and every member of a composite must share a connection.
 * So a query no rollup covers fails to compile against this model and falls back
 * to live, which is the fallback the serve path relies on.
 */
function rollupServeShapeFragment(group: RollupShapeGroup): string {
   const members = group.members.map(serveShapeFragment).join("\n");
   const names = group.members.map((m) => m.sourceName).join(", ");
   return `${members}\nsource: ${group.baseSourceName} is compose(${names})`;
}

/**
 * Assemble the transient BUILD model for a chained `storage=` source — the
 * "stack on the parent" build. Every materialized upstream is rebound
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
 * caller falls back to recompute-from-raw — re-emitting parent
 * refinements to widen coverage is a follow-on.
 *
 * `derived` carries the non-persisted sources between the downstream and its
 * stored parents (see {@link liftDerivedSources}), emitted after the parents and
 * before the downstream so each resolves what it names. A downstream need not
 * read a stored parent directly: `checklists_kept is checklists_all extend {…}`
 * over a stored `site_seasons` reads it through `checklists_all`, and without
 * that intermediate in the model the downstream's definition names an undefined
 * object.
 *
 * `documentFlags` are the `##!` lines of the author files the lifted text came
 * from, emitted verbatim ahead of the flags this model needs itself. A lifted
 * declaration is compiled under the flags its own file enabled — `include {}`
 * needs `access_modifiers` — and a transient model that carried only its own two
 * would refuse exactly the declarations the author's file accepts.
 */
export function buildChainedStorageBuildModel(params: {
   upstreams: ServeBinding[];
   downstreamName: string;
   downstreamDefText: string;
   destinationName: string;
   derived?: DerivedSourceLift[];
   documentFlags?: string[];
   /**
    * The author model's given surface, declared on this model as the serve
    * shape declares it. A carried intermediate's text is the author's verbatim,
    * and a dimension on it that reads `$REGION` compiles only if `REGION` is
    * declared — whether or not the downstream reads that dimension. The build
    * binds no given; the declarations are what lets the text compile.
    */
   givens?: ServeShapeGiven[];
}): string {
   // A rollup is never an upstream — nothing can reference one, its name being
   // synthesized and absent from every model file — so one arriving here means a
   // caller widened its set without deciding to.
   //
   // Asserted rather than filtered, and the distinction matters. Filtering would
   // make a rollup here harmless, which it already is: `deriveServeBindings`
   // attaches no refinements, so the fragments are bare virtual sources nothing
   // references. But that is the SAME assumption that expired on the serve path,
   // where these bindings later acquired refinements — at which point a rollup's
   // merged measures would start entering BUILD models. An assertion fails loudly
   // when the assumption stops holding; a filter would keep the damage silent.
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
   // The author's flags first, this model's own last: Malloy accumulates `##!`
   // lines, so repeating a flag the author also enabled is harmless, and the
   // two this model always needs are stated once wherever the author's stand.
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
export interface ReachedSources {
   /**
    * The persist sources the walk stopped at — the stored tables the source
    * depends on, whether it reads them directly or through intermediates — as
    * the model's name for each and its `sourceID`. Neither is a table: several
    * names share one (`#@ persist` is inherited and `extend` never changes the
    * SQL), and a name is not unique across a package's models, so the caller
    * resolves each to its content address by `sourceID` before asking whether
    * the table is present.
    */
   persisted: { name: string; sourceID: string }[];
   /**
    * Set when some path ends at a source with no stored table behind it — a
    * table or SQL source, or a reference with no in-model identity — which is a
    * read of the source warehouse nothing stored can stand in for.
    */
   raw: boolean;
   /** The sources those paths ended at (`regions`), for the reason text. */
   rawLeaves: string[];
   /** The non-persisted sources that reach them (`daily_regional`). */
   rawVia: string[];
   /**
    * Every named source the walk passed through or stopped at, the root
    * excluded: the downstream's dependency closure, which is exactly the set
    * of sources a build over its stored parents needs declared — and no
    * other. A model declares more than one chain, and a source off this
    * one's path is not this build's concern, however it lifts.
    */
   visited: string[];
}

/**
 * What a source reaches, walking the author model from `name` through the
 * sources it is built from and stopping at each persist source on the way —
 * `isPersisted(name)`, which the caller answers from the build plan.
 *
 * Which names are persist sources is the plan's to say, not the annotation's.
 * `#@ persist` is inherited: a plain `extend` of a persisted source, or a rename
 * of one, is a persist source too and shares the base's table, because neither
 * changes the SQL the table is built from. Nothing on a definition says which
 * of several names "declared" the annotation (the compiler copies the base's
 * notes onto an extension), and the design does not ask: a stored table is a
 * content address, and every name whose SQL hashes to it is that table. So the
 * walk records the NAMES it stopped at, and the caller resolves each to its
 * address group to decide whether the table is present.
 *
 * An extension's own refinements are the one thing of its that is NOT in the
 * table: `daily_regional is daily extend { join_one: r is regions … }` has
 * `daily`'s SQL and `daily`'s table, and the join to `regions` is computed over
 * it at read time. A downstream reading `r.region` therefore reaches `regions`
 * — a warehouse table — even though every persist source on its path is
 * stored. So at a persist source that extends another, the walk still follows
 * the joins the extension adds (those its base does not carry) and nothing
 * else of it; its base's query is the table.
 *
 * A reference carried as the referenced definition embedded whole (an inline
 * `(daily extend { … }) -> …`, a join written in place) is walked as that
 * definition; only a reference with no identity at all, or to a name the model
 * does not hold, reads as raw.
 *
 * The distinction this draws is the one a chained build's refusal turns on. A
 * stored table the walk finds that this build did not materialize or reference
 * is an upstream the orchestrator meant to pin and the build cannot see — the
 * case `strictUpstreams` exists to refuse. A path that reaches raw is a shape
 * the destination alone cannot express, where recomputing from the warehouse is
 * the only build there is. And a source that reaches neither raw nor a missing
 * table, yet cannot be carried over its parents, is a limit of the carrying —
 * which strict must not read as licence to recompute a table it pinned.
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
   // A reference is a sourceID (string), the referenced definition embedded
   // whole (object), or nothing usable.
   // A `sourceID` is `name@<model URL>`; the name alone is what a reason may
   // carry, since the URL is the server's path to the file.
   const nameOf = (sourceID: string): string => sourceID.split("@")[0];
   const follow = (ref: unknown, via: string): void => {
      if (typeof ref === "string") {
         const next = ctx.sourceNameById.get(ref);
         if (next !== undefined) {
            visit(ctx.contents[next], next, false, via);
            return;
         }
         // No definition to descend into, but the plan knows the id as a
         // persist source: a stored table reached through an import the
         // model's own view does not carry. A stop, never raw.
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
         // A join field is the joined definition itself, with its identity on it
         // when the model names it.
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
   // `sourceName` is the model's name for the definition, or undefined for one
   // embedded in place, which no plan fact can be about. `via` is the source
   // whose definition referenced this one — what the reason names as reaching a
   // leaf, since the leaf itself (`regions`) says nothing about which
   // intermediate dragged it in, and what an embedded definition is reported as.
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
      // The root is the source being built: it is persisted by definition, and
      // the question is what IT reaches, so only its descendants can stop the walk.
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
 * Every reference a query pipeline makes — a stage's `structRef`, and anything
 * it joins — as the sourceID when the model names it, else the embedded
 * definition itself, so a caller can walk into an inline source rather than
 * give up on it. A named source is not descended into: its definition is the
 * caller's to look up, and descending here would walk it twice.
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
 * The `##!` lines of an author file — the document flags its declarations
 * compile under. Order is kept and blank lines are dropped.
 */
/**
 * What a bound source adds to the relation its table holds: the joins,
 * dimensions and measures, extend-block filters and views declared on it,
 * which its build SQL leaves out (a persist source's build is the persisted
 * relation alone) and which must be re-declared on its virtual binding to be
 * applied when the table is read. One assembly for the serve shape and the
 * chained build, so a table is read the same way wherever it is read; a
 * binding missing its source's `where:` reads unfiltered rows, silently.
 * `materializedSourceNames` bounds the joins carried: one to a source that is
 * not on the shape is left off, and text that reads it then fails to compile.
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
 * The `##!` flags of every author file a set of lifts carries declarations
 * from, deduplicated in first-seen order — what a model that carries their
 * text must enable to compile it.
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

export function documentFlagLines(text: string): string[] {
   // Block comments first, so a `##!` quoted inside one is not a flag.
   return text
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.startsWith("##!"));
}

/**
 * The compiled-model facts a lift reads, assembled from a compiled `ModelDef`:
 * its `contents` by source name, a sourceID index over them, and readers that
 * recover a declaration's verbatim text, or a whole file, from the author's
 * source by location. One file cache per call, so a package's sources read each
 * file once between them.
 *
 * Shared by the serve path (refinement extraction and the derived-source lift)
 * and the chained build (the intermediates it carries), so both read ONE view
 * of the model.
 */
export function authorModelLiftContext(
   modelDef: unknown,
   readFile: (url: string) => string | undefined,
): {
   contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>;
   sourceNameById: Map<string, string>;
   liftText: (location: SourceLocation) => string | undefined;
   fileText: (url: string) => string | undefined;
} {
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
   // The model's namespace, plus its hidden dependencies: a source an import
   // brought in transitively without placing it in this model's namespace —
   // `import { weekly } from "orders.malloy"` leaves `daily`, which `weekly`
   // is built from, out of `contents` — lives in `sourceRegistry` as a full
   // definition under its declared name (`as`). The walk, the lift and the
   // flag gathering read one view of what this model's sources are built
   // from, so a stored parent reached only through an import is a stop and a
   // carried declaration, not a table the model cannot name. The namespace
   // wins a name clash: it is what the model's own text refers to.
   const contents: Record<string, Def> = {};
   for (const value of Object.values(md?.sourceRegistry ?? {})) {
      const entry = value?.entry;
      if (!entry || entry.type === "source_registry_reference") continue;
      const name = typeof entry.as === "string" ? entry.as : undefined;
      if (!name || name in namespace || name in contents) continue;
      contents[name] = entry;
   }
   Object.assign(contents, namespace);
   // sourceID -> author source name, for the join materialization gate and
   // for resolving what a derived source extends.
   const sourceNameById = new Map<string, string>();
   for (const [name, def] of Object.entries(contents)) {
      if (typeof def?.sourceID === "string") {
         sourceNameById.set(def.sourceID, name);
      }
   }
   // Cache each source file's text (or null when unreadable) across lookups.
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

/**
 * Narrow a binding's captured DESCRIBE schema to the columns the source
 * PUBLICLY exposes, dropping any physical column the source hides.
 *
 * The build's `getSQL` projects the source's underlying columns, so a column
 * hidden at the source level — removed by `except:`, or carrying a non-public
 * access modifier — is still materialized into the table and captured by
 * `DESCRIBE`. Declaring it on the serve shape would make it reachable through
 * the virtual source even though the live path hides it (v0 never does this: it
 * recompiles the original source definition with only the base table swapped, so
 * the source's own visibility always applies). A column is kept only if it names
 * a publicly-visible field of the compiled source; an `except:`-ed column is
 * absent from the field list, and an access-restricted one is caught by
 * {@link isAccessRestricted} — both are dropped. A captured name is matched
 * EXACTLY first and only then case-insensitively, and the surviving column is
 * emitted under the AUTHOR's spelling, because the captured name is in the
 * source warehouse's identifier case rather than the author's. An ambiguous
 * fold — several author fields folding onto one captured column — is dropped
 * rather than guessed. See the body for why each of those three is load-bearing. Dropped columns stay physically
 * in the table but become unreachable through the source: a query that
 * references one fails the shape compile and falls back to live, where the
 * source's visibility rules are enforced (fail-safe). This mirrors the
 * refinement/join/view access filtering — the serve surface must reproduce the
 * source's PUBLIC surface exactly, never widen it.
 */
export function narrowSchemaToPublic(
   schema: { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
): { name: string; type: string }[] {
   // Two lookups, tried in that order, because the two sides of this
   // intersection are in different namespaces: `fields` carries the names the
   // author WROTE, while `schema` carries what DESCRIBE reported for the built
   // table -- and a warehouse that folds unquoted identifiers (Snowflake, Oracle,
   // Redshift and Teradata all upper-fold) reports them in ITS case, not the
   // author's. Matching only exactly intersects to nothing there, and an empty
   // narrowed schema is not a partial failure: the caller drops such a binding
   // entirely, so the source vanishes from the serve shape and every query on it
   // dies at "Reference to undefined object" and falls back live. Silently -- the
   // rows are correct, only the tier is lost -- so it reads as "materialization
   // did nothing" rather than as a bug.
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
         // An exact hit is authoritative and must be tried FIRST. On a
         // case-PRESERVING warehouse the captured name already is the author's,
         // and Malloy permits two fields whose names differ only in case -- a
         // `TitleCase` dimension over a `snake_case` column. Folding first would
         // let the dimension's name win the physical column, so the shape declared
         // the stored column under the dimension's name, the duplicate failed the
         // shape down to base-only, and the query served the raw column in place
         // of the computed one. Wrong rows, `servedFrom: storage`, no warning.
         authorName = c.name;
      } else {
         const candidates = byFoldedName.get(c.name.toLowerCase());
         // Exactly one, or not at all. Several author fields folding onto one
         // captured column cannot be resolved from here, and guessing is the
         // failure above; dropping sends a query that touches it to live, where
         // the author's own names still distinguish them.
         if (candidates?.length === 1) {
            authorName = candidates[0];
         }
      }
      // Absent from the public surface (`except:`-ed or access-restricted) in
      // either namespace: dropped, exactly as before. Neither lookup can invent
      // an author field, so a hidden one stays hidden.
      if (authorName === undefined) {
         continue;
      }
      // Two captured columns can still reach one author field (a folded hit plus
      // an exact one). Declaring the name twice fails the whole shape, so the
      // first in captured order wins and the rest drop.
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
 * EVERY entry is carried, including one that cannot be reproduced on the shape:
 * a filter reaching through a join whose target is not materialized, or one over
 * a column the source hides (the declared `::Shape` is narrowed to the source's
 * PUBLIC columns, so `where: not is_deleted` with `except: is_deleted` names a
 * column the shape does not declare). Such an entry makes the shape fail to
 * compile, which is the required outcome — a dropped filter is not a lost
 * optimization, it is rows the source excludes. See the serve-shape ladder in
 * `Model.compileServeShape`, which keeps this kind at every tier for that
 * reason.
 *
 * The cost is that source's tier and no one else's. The shape is one model text
 * covering every binding, so the failure surfaces model-wide; `compileServeShape`
 * answers it by probing each binding alone, withholding the ones whose filters do
 * not reproduce, and re-entering the ladder with the rest — so a sibling keeps
 * its joins and views rather than being frozen at the shape that failed.
 *
 * A filter referencing a given is the main thing that reaches here. The gate
 * admits an extend-block `where:` over a given — the build leaves it out, so the
 * artifact holds every caller's rows and this re-emission is what puts the term
 * back per caller. It compiles because the shape declares the model's givens.
 * `#(partition)` and `#(authorize)` are still refused outright.
 *
 * `filterList` accumulates through `extend`, so a source's own entries already
 * carry every filter it inherits from the source it extends.
 *
 * A pre-aggregation ROLLUP member carries no filter refinement, and that
 * asymmetry is not an oversight: a rollup is a `-> { }` derived source, so
 * READING its base applies the base's filter and the rollup's own build SQL bakes
 * it in. A base binding rebinds the stored relation directly and so must
 * re-declare the filter; a rollup already has it.
 *
 * Fail-closed on a malformed entry: an entry whose `code` is not a string
 * yields a filter that cannot compile, so the binding is withheld rather than
 * silently under-filtered.
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
 * Emitted in place of a filter whose expression text could not be read. Not
 * valid Malloy, deliberately: it fails the shape compile, which withholds the
 * binding and serves live. The alternative — dropping the entry — would serve
 * the unfiltered relation.
 */
export const UNREPRODUCIBLE_FILTER = "__unreproducible_filter__";

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
         // `all(…)` / `exclude(…)` over a measure: a measure like any other
         // on the relation it is declared over, which a virtual base is. A
         // view on the same source commonly reads one, and a view is carried
         // verbatim — leaving the measure off makes that view fail to compile.
         f.expressionType === "ungrouped_aggregate"
      ) {
         out.push({ kind: "measure", name: f.name, code: f.code });
      }
      // analytic / calculation and non-atomic fields (joins, turtles have no
      // `code`) are skipped → those queries fall back.
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
 * Assemble the per-call `virtualMap` (`destinationName -> handle -> canonical
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

/**
 * The bindings left once every binding with a caller-scoped join the serve shape
 * cannot reproduce is withheld, to a fixpoint; and the names withheld.
 *
 * A join to a given-scoped source reproduces the caller's scope only through
 * that source's own binding (see `joinedSourceRefusal`). It cannot be re-emitted
 * when the target is not bound — stale past its window, never built, refused —
 * nor when it is bound on a different destination, since one query cannot join
 * across two connections; nor when the join is access-restricted, since
 * restricted joins are never re-emitted while a public field may still read
 * through them. Either way every
 * field reading through the join fails the shape compile. Left in, that failure
 * is answered by the serve-shape ladder, which thins EVERY binding in the model:
 * one stale grant table would cost each sibling source its joins, dimensions and
 * measures. Withholding the joining binding instead confines the cost to the
 * source that needs the join, which serves live.
 *
 * To a fixpoint because withholding one binding removes it as a target: a source
 * with a caller-scoped join to the withheld one is itself withheld next.
 *
 * Only caller-scoped joins. A join to an ordinary unmaterialized source keeps its
 * existing treatment — skipped at extraction, with the ladder answering whatever
 * reads through it.
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
