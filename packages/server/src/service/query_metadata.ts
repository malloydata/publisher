// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Per-query metadata: the publisher's side of Malloy's
 * `RunSQLOptions.queryMetadata` — a flat bag of string properties each connector
 * attaches to the statement it issues (Snowflake per-statement `QUERY_TAG`,
 * BigQuery per-job labels, a leading SQL comment on the rest). It describes the
 * query for the warehouse's own reporting — cost attribution, workload
 * classification, tracing — and never affects results or data identity.
 *
 * Two jobs live here:
 *
 *  1. **Resolve.** One bag per query, merged least-specific-first: connection
 *     default → model-side resolved value → request override → server context.
 *     Context is applied last because it describes what the server is actually
 *     doing (which package, which class of work) and a caller must not be able
 *     to overwrite that attribution.
 *
 *  2. **Keep the bag legal.** Malloy validates at dispatch and THROWS on a bag
 *     that violates its contract, so an assembled bag that is one character too
 *     long would fail a customer query. {@link mergeQueryMetadata} therefore
 *     never throws: it sanitizes, truncates and drops, metering every drop.
 *     Declaration boundaries — a publish, a connection update, an API request —
 *     use {@link queryMetadataViolations} instead and fail fast with a message.
 *     Config LOAD is the exception: a connection default that violates the
 *     contract warns, because a tag must never be the reason an environment
 *     refuses to come up.
 */

import type { QueryMetadata } from "@malloydata/malloy";
import * as crypto from "node:crypto";
import { getQueryMetadataMode } from "../config";
import { logger } from "../logger";
import {
   recordQueryMetadataApplied,
   recordQueryMetadataDropped,
} from "../query_metadata_metrics";

export type { QueryMetadata };

/**
 * What class of work issued the query. Attribution's primary axis: a warehouse
 * bill answers "how much of this is interactive traffic vs builds vs indexing"
 * only if every statement says which it is.
 */
export const QUERY_CLASSES = [
   "interactive",
   "materialize",
   "index",
   "ops",
] as const;
export type QueryClass = (typeof QUERY_CLASSES)[number];

/**
 * Malloy's contract for a bag, mirrored (`@malloydata/malloy` exports the
 * `QueryMetadata` type but not its validator):
 *
 *   - property names: ASCII alphanumerics and underscore, <=128 chars
 *   - property values: printable ASCII except `"`, <=256 chars
 *   - at most 20 properties
 *
 * A bag that satisfies this is renderable by every connector, including into
 * the `-- NAME="value"` comment form.
 *
 * Pinned to @malloydata/malloy 0.0.426 (`packages/malloy/src/query_metadata.ts`).
 */
export const MAX_PROPERTY_NAME_LENGTH = 128;
export const MAX_PROPERTY_VALUE_LENGTH = 256;
export const MAX_PROPERTIES = 20;
const PROPERTY_NAME_RE = /^[A-Za-z0-9_]+$/;
/**
 * Snowflake's `QUERY_TAG` holds the whole bag as one JSON string capped at 2000
 * chars, and db-snowflake clamps by slicing mid-JSON, leaving a tag that is
 * unqueryable in `QUERY_HISTORY`. So the budget is enforced here, on the
 * serialized size, by dropping whole properties.
 */
const MAX_SERIALIZED_LENGTH = 2000;

/** Printable ASCII (0x20-0x7e) except `"`, which would break the comment form. */
function isRenderableValue(value: string): boolean {
   for (let i = 0; i < value.length; i++) {
      const code = value.charCodeAt(i);
      if (code < 0x20 || code > 0x7e || code === 0x22) return false;
   }
   return true;
}

/**
 * The ways `raw` violates the contract, empty when it conforms. For boundaries
 * where a human is present: a publish, a connection update, an API request. The
 * runtime path clamps instead (see {@link mergeQueryMetadata}).
 */
export function queryMetadataViolations(raw: unknown): string[] {
   if (raw === undefined || raw === null) return [];
   if (typeof raw !== "object" || Array.isArray(raw)) {
      return ["queryMetadata must be an object of string properties"];
   }
   const problems: string[] = [];
   const entries = Object.entries(raw as Record<string, unknown>);
   if (entries.length > MAX_PROPERTIES) {
      problems.push(
         `queryMetadata declares ${entries.length} properties; at most ${MAX_PROPERTIES} are allowed`,
      );
   }
   for (const [name, value] of entries) {
      if (!PROPERTY_NAME_RE.test(name)) {
         problems.push(
            `queryMetadata property name '${name}' must be ASCII alphanumerics and underscore`,
         );
      } else if (name.length > MAX_PROPERTY_NAME_LENGTH) {
         problems.push(
            `queryMetadata property name '${name}' exceeds ${MAX_PROPERTY_NAME_LENGTH} characters`,
         );
      }
      if (typeof value !== "string") {
         problems.push(
            `queryMetadata property '${name}' must be a string (got ${typeof value})`,
         );
         continue;
      }
      if (!isRenderableValue(value)) {
         problems.push(
            `queryMetadata property '${name}' has a value outside printable ASCII, or containing '"'`,
         );
      }
      if (value.length > MAX_PROPERTY_VALUE_LENGTH) {
         problems.push(
            `queryMetadata property '${name}' value exceeds ${MAX_PROPERTY_VALUE_LENGTH} characters`,
         );
      }
   }
   return problems;
}

/**
 * Advisory notes about a bag that satisfies the contract but still will not do
 * what it says, so it is reported at declaration rather than discovered in a
 * warehouse console: BigQuery's label grammar needs a name starting with a
 * letter, and db-bigquery drops one it cannot make valid while other
 * connectors keep it.
 */
export function queryMetadataAdvisoryWarnings(meta: QueryMetadata): string[] {
   const warnings: string[] = [];
   for (const name of Object.keys(meta)) {
      if (!/^[A-Za-z]/.test(name)) {
         warnings.push(
            `queryMetadata property '${name}' does not start with a letter, so BigQuery drops it (other backends keep it); rename it to start with a letter`,
         );
      }
   }
   return warnings;
}

/**
 * Declared properties whose names the server owns, and which
 * {@link mergeQueryMetadata} therefore drops from a declaration.
 *
 * Reported at declaration because otherwise a block naming `model` publishes
 * clean and vanishes on every statement, visible only as a counter tick.
 */
export function queryMetadataReservedWarnings(meta: QueryMetadata): string[] {
   return Object.keys(meta)
      .filter((name) => RESERVED_NAMES.has(name))
      .map(
         (name) =>
            `queryMetadata property '${name}' is a name the server sets itself, so it is dropped rather than attached; rename it (for example '${name}_label')`,
      );
}

/**
 * The budget warning for a declaration of `declared` properties, or undefined
 * when it fits.
 *
 * Takes a count because a connection's default and enforced maps share the
 * budget; summing them is the caller's job. The server's context lands on top
 * of everything declared, so a declaration that fills the contract would
 * otherwise lose properties on every statement, visible only as a metric.
 */
export function queryMetadataBudgetWarning(
   declared: number,
): string | undefined {
   const authorBudget = MAX_PROPERTIES - RESERVED_CONTEXT_PROPERTIES;
   if (declared <= authorBudget) return undefined;
   return (
      `queryMetadata declares ${declared} properties; the server adds up to ` +
      `${RESERVED_CONTEXT_PROPERTIES} of its own to every statement, so a bag over ` +
      `${authorBudget} loses its least specific properties at query time`
   );
}

/**
 * The server's own description of the query, injected on every statement so
 * attribution needs no modeling work from the author.
 *
 * Neutral by design: a platform that wants its own dimensions (tenant,
 * deployment, cost centre) supplies them as a connection default or a
 * per-request override, and the publisher does not invent them.
 *
 * Every field except the correlation id is stable across repeats of a query.
 * The correlation id is per call, so on a backend with no native tag mechanism
 * it makes the statement text unique and bypasses an exact-text result cache:
 * the deliberate cost of being able to find one query again.
 */
export interface QueryContext {
   /** What issued the query. Absent only where the class is genuinely unknown. */
   queryClass?: QueryClass;
   environment?: string;
   package?: string;
   /**
    * Published version id, when the query runs against a versioned package.
    *
    * Not yet reachable: the only supplier is a query request's `versionId`,
    * which the route rejects with 501. It keeps its slot in
    * {@link CONTEXT_SHED_ORDER}.
    */
   version?: string;
   /** Package-relative model path, for query paths. */
   model?: string;
   /** Persist source or index dimension, for build paths. */
   source?: string;
   /** What started a build: `publish`, `on_demand`, `scheduler`. */
   trigger?: string;
   /**
    * Groups every statement of one build. The publisher mints it (the
    * materialization id) unless the caller supplies its own.
    */
   runId?: string;
   /**
    * Identifies this single query, minted by the request boundary that returns
    * it to the caller (see {@link mintCorrelationId}). Set only where a response
    * field carries it: an unread id costs the result cache for nothing.
    */
   correlationId?: string;
}

/**
 * A fresh correlation id for one query. Minted by the publisher rather than
 * accepted from the caller because it is the platform's join key: a caller-owned
 * value can be omitted, reused across calls, or collide with another caller's,
 * and then a response has nothing meaningful to hand back. A caller that wants
 * its own request id in the bag declares it under its own property name.
 */
export function mintCorrelationId(): string {
   // The imported module, not the `crypto` global, which needs Node 19+.
   return crypto.randomUUID();
}

/**
 * The order context properties are given up in when even the context alone will
 * not fit: least load-bearing first. `class` and `query_id` are last because
 * they are the two that make the rest worth reading — the workload split, and
 * the id a response handed a caller as its join key.
 */
export const CONTEXT_SHED_ORDER = [
   "version",
   "model",
   "source",
   "trigger",
   "environment",
   "package",
   "run_id",
   "query_id",
   "class",
];

/**
 * How many properties the context can occupy, so a declaration boundary can warn
 * about an author budget that will not survive the merge.
 */
export const RESERVED_CONTEXT_PROPERTIES = CONTEXT_SHED_ORDER.length;

/** The context as bag properties, dropping every absent field. */
export function queryContextProperties(context: QueryContext): QueryMetadata {
   const out: QueryMetadata = {};
   const put = (name: string, value: string | undefined) => {
      if (value !== undefined && value !== "") out[name] = value;
   };
   put("class", context.queryClass);
   put("environment", context.environment);
   put("package", context.package);
   put("version", context.version);
   put("model", context.model);
   put("source", context.source);
   put("trigger", context.trigger);
   put("run_id", context.runId);
   put("query_id", context.correlationId);
   return out;
}

/** One layer of a resolved bag, least specific first. */
export interface QueryMetadataLayers {
   /** The executing connection's default (least specific). */
   connection?: QueryMetadata | null;
   /** The value the build plan resolved for this materialization unit. */
   model?: QueryMetadata | null;
   /** The caller's per-request override. */
   request?: QueryMetadata | null;
   /**
    * Properties the connection declares as the deployment's own, which no
    * declaration can overwrite and which outlive every author property when the
    * bag has to shrink.
    *
    * A multi-tenant host needs this for the properties it bills by; otherwise
    * the tenant label is forgeable (`#@ persist queryMetadata.tenant=…`
    * outranks a connection default) and shed first under budget.
    *
    * The operator/author trust split is the DEPLOYMENT's: nothing here
    * authenticates a connection write.
    */
   enforced?: QueryMetadata | null;
   /** The server's own context, which wins over every other layer. */
   context?: QueryContext;
}

/** How a property was lost, for the drop metric and the caller's log. */
export type QueryMetadataDropReason =
   | "invalid_name"
   | "invalid_value"
   | "reserved_name"
   | "property_cap"
   | "serialized_cap";

/**
 * Index of the author-declared layer in the merge order below — the layer whose
 * properties come from a package manifest, a model file or a source annotation.
 */
const DECLARED_LAYER_INDEX = 1;

/** Context names, which an author may not declare. See the merge loop. */
const RESERVED_NAMES = new Set(CONTEXT_SHED_ORDER);

export interface ResolvedQueryMetadata {
   /** The bag to hand to Malloy; undefined when there is nothing to attach. */
   metadata?: QueryMetadata;
   /** Properties that did not survive, most useful in a warning log. */
   drops: { name: string; reason: QueryMetadataDropReason }[];
}

/** Truncate a value to the contract and strip anything unrenderable. */
function sanitizeValue(value: string): string {
   let out = "";
   for (
      let i = 0;
      i < value.length && out.length < MAX_PROPERTY_VALUE_LENGTH;
      i++
   ) {
      const code = value.charCodeAt(i);
      out += code < 0x20 || code > 0x7e || code === 0x22 ? "_" : value[i];
   }
   return out;
}

/**
 * Merge the layers into one bag Malloy will accept, and report what was lost.
 *
 * Never throws — metadata must not be the reason a query or a build fails —
 * with one precondition: `getQueryMetadataMode()` rejects an unrecognized
 * `PUBLISHER_QUERY_METADATA`, and it is the boot probe in `server.ts` that
 * turns that into a startup failure rather than a per-query one. A host that
 * embeds this module without that probe owes itself the same check.
 *
 * Precedence is most-specific-wins per property across the author layers
 * (connection < model < request), then the connection's ENFORCED properties,
 * then the server's context — so neither a declaration nor a caller can
 * overwrite what the deployment or the server says about the query.
 *
 * Shedding runs in the same order, from the bottom: the 20-property cap and
 * Snowflake's 2000-char serialized tag take author properties first (losing a
 * caller's `team` label degrades attribution), then enforced properties, and
 * only then context (losing `class` or `query_id` breaks it).
 *
 * Returns no metadata at all when `PUBLISHER_QUERY_METADATA=off`, which is the
 * escape hatch for a deployment that does not want the publisher touching the
 * statements it sends.
 */
export function mergeQueryMetadata(
   layers: QueryMetadataLayers,
): ResolvedQueryMetadata {
   if (getQueryMetadataMode() === "off") return { drops: [] };

   const drops: { name: string; reason: QueryMetadataDropReason }[] = [];
   const contextProperties = queryContextProperties(layers.context ?? {});

   // Each property remembers the layer whose value SURVIVED, so the shed order
   // below matches the precedence that produced the bag.
   // Null-prototype: `__proto__` satisfies the name rule, and an object
   // literal's prototype setter would silently drop it.
   const merged: QueryMetadata = Object.create(null) as QueryMetadata;
   const winningLayer = new Map<string, number>();
   const orderedLayers = [
      layers.connection,
      layers.model,
      layers.request,
      layers.enforced,
   ];
   orderedLayers.forEach((layer, index) => {
      for (const [name, value] of Object.entries(layer ?? {})) {
         if (
            !PROPERTY_NAME_RE.test(name) ||
            name.length > MAX_PROPERTY_NAME_LENGTH
         ) {
            drops.push({ name, reason: "invalid_name" });
            continue;
         }
         // A DECLARED property may not take a context name. Context overwrites
         // only names it has a value for, and a served query has no `source`,
         // so a model's `queryMetadata.source="orders_daily"` would make an
         // interactive statement look like a build of that source. Scoped to
         // the declared layer; the connection and request layers are not
         // restricted (a compatibility decision).
         if (index === DECLARED_LAYER_INDEX && RESERVED_NAMES.has(name)) {
            drops.push({ name, reason: "reserved_name" });
            continue;
         }
         if (typeof value !== "string") {
            drops.push({ name, reason: "invalid_value" });
            continue;
         }
         winningLayer.set(name, index);
         merged[name] = sanitizeValue(value);
      }
   });
   for (const [name, value] of Object.entries(contextProperties)) {
      merged[name] = sanitizeValue(value);
   }

   // Shedding runs least-specific-first, the same order that decides which
   // value wins; enforced properties go last because the deployment bills by
   // them.
   const shedOrder = [...winningLayer.entries()]
      // hasOwnProperty, not `in`: `in` finds `__proto__` on every object,
      // which would make a declared `__proto__` unsheddable.
      .filter(
         ([name]) =>
            !Object.prototype.hasOwnProperty.call(contextProperties, name),
      )
      .sort((a, b) => a[1] - b[1])
      .map(([name]) => name);
   const shed = (reason: QueryMetadataDropReason) => {
      const name = shedOrder.shift() as string;
      delete merged[name];
      drops.push({ name, reason });
   };
   while (Object.keys(merged).length > MAX_PROPERTIES && shedOrder.length > 0) {
      shed("property_cap");
   }
   while (
      JSON.stringify(merged).length > MAX_SERIALIZED_LENGTH &&
      shedOrder.length > 0
   ) {
      shed("serialized_cap");
   }
   // Context alone over budget is a server bug, but a thrown query is worse
   // than a partial tag, so shed context too, in CONTEXT_SHED_ORDER.
   const contextShedOrder = CONTEXT_SHED_ORDER.filter(
      (name) => name in contextProperties,
   );
   while (
      (Object.keys(merged).length > MAX_PROPERTIES ||
         JSON.stringify(merged).length > MAX_SERIALIZED_LENGTH) &&
      contextShedOrder.length > 0
   ) {
      const name = contextShedOrder.shift() as string;
      const overCount = Object.keys(merged).length > MAX_PROPERTIES;
      delete merged[name];
      drops.push({
         name,
         reason: overCount ? "property_cap" : "serialized_cap",
      });
   }

   for (const drop of drops) recordQueryMetadataDropped(drop.reason);
   if (Object.keys(merged).length === 0) return { drops };
   recordQueryMetadataApplied(layers.context?.queryClass ?? "unknown");
   return { metadata: merged, drops };
}

/**
 * Read a caller-supplied bag, rejecting rather than clamping: a request or a
 * connection update has a human behind it, so a bad property should come back
 * as a message instead of silently not doing what it says.
 *
 * @throws {Error} listing every violation, for the caller to map to a 400.
 */
export function parseSuppliedQueryMetadata(
   raw: unknown,
): QueryMetadata | undefined {
   if (raw === undefined || raw === null) return undefined;
   const problems = queryMetadataViolations(raw);
   if (problems.length > 0) throw new Error(problems.join("; "));
   const meta = raw as QueryMetadata;
   return Object.keys(meta).length > 0 ? meta : undefined;
}

/** Read a `queryClass`, rejecting an unrecognized value. @throws {Error} */
export function parseQueryClass(raw: unknown): QueryClass | undefined {
   if (raw === undefined || raw === null) return undefined;
   if (
      typeof raw === "string" &&
      (QUERY_CLASSES as readonly string[]).includes(raw)
   ) {
      return raw as QueryClass;
   }
   throw new Error(
      `queryClass must be one of ${QUERY_CLASSES.join(" | ")} (got ${JSON.stringify(raw)})`,
   );
}

/**
 * What a request boundary contributes to a model query's per-query metadata: the
 * caller's own properties and class, the environment the query runs in, and a
 * reader for the executing connection's default (the controller owns the
 * environment, so it supplies the lookup rather than the model reaching for it).
 */
export interface ModelQueryMetadataInput {
   request?: QueryMetadata;
   queryClass?: QueryClass;
   environment?: string;
   version?: string;
   /**
    * The id this query is correlated by, minted by the boundary that returns it
    * (see `mintCorrelationId`). Omitted by callers with no response field to
    * carry it, which is why it is not minted here.
    */
   correlationId?: string;
   /**
    * The executing connection's two metadata layers: its overridable default and
    * the properties the deployment enforces. Supplied by the controller, which
    * owns the environment; the model only knows the connection by name.
    */
   connectionMetadata?: (connectionName: string) => {
      default?: QueryMetadata | null;
      enforced?: QueryMetadata | null;
   } | null;
   /**
    * Whether a statement on this connection is metered: a storage destination,
    * or a connection an orchestrator marked `managed`. Supplied by the
    * controller with the layers above; absent means nothing is metered.
    */
   meteredConnection?: (connectionName: string) => boolean;
   /**
    * The package's declared bag — the least specific author-declared layer.
    * Supplied by the controller, which owns the package; the model knows only
    * its own file and its package's NAME.
    */
   packageDeclaration?: QueryMetadata | null;
}

/**
 * The {@link ModelQueryMetadataInput} a query boundary (REST or MCP) hands the
 * model: the caller's own bag and class, the environment, a fresh correlation
 * id, the package's declared layer, and a reader for the executing
 * connection's default and enforced layers. Built the same way at every
 * boundary, because a connection's enforced properties describe the
 * deployment rather than the protocol a query arrived over.
 */
export function buildQueryMetadataInput(args: {
   environmentName: string;
   environment: {
      getApiConnection(connectionName: string): {
         queryMetadata?: QueryMetadata;
         queryMetadataEnforced?: QueryMetadata;
         managed?: boolean;
      };
      /** Throws for a name that is not a configured storage destination. */
      getStorageDestination?(destinationName: string): unknown;
   };
   pkg: { getDeclaredQueryMetadata(): QueryMetadata | null };
   request?: QueryMetadata;
   queryClass?: QueryClass;
   version?: string;
   /** Debug message logged when a connection's layers cannot be read. */
   noLayersLogMessage: string;
}): ModelQueryMetadataInput {
   const { environment } = args;
   return {
      request: args.request,
      queryClass: args.queryClass,
      environment: args.environmentName,
      version: args.version,
      correlationId: mintCorrelationId(),
      packageDeclaration: args.pkg.getDeclaredQueryMetadata(),
      connectionMetadata: (connectionName) => {
         try {
            const connection = environment.getApiConnection(connectionName);
            return {
               default: connection.queryMetadata,
               enforced: connection.queryMetadataEnforced,
            };
         } catch (error) {
            // Fail open (a tag must not fail a query), but log it: this drop
            // has no metric and loses the ENFORCED layer.
            logger.debug(args.noLayersLogMessage, { connectionName, error });
            return null;
         }
      },
      meteredConnection: (connectionName) => {
         try {
            if (environment.getApiConnection(connectionName).managed === true) {
               return true;
            }
         } catch {
            // Not a connection; a served stored source runs on its destination.
         }
         try {
            environment.getStorageDestination?.(connectionName);
            return environment.getStorageDestination !== undefined;
         } catch {
            return false;
         }
      },
   };
}
