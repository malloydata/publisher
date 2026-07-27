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
 *     Declaration boundaries (publish, connection update, API request) use
 *     {@link queryMetadataViolations} instead and fail fast with a message.
 */

import type { QueryMetadata } from "@malloydata/malloy";
import { getQueryMetadataMode } from "../config";
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
 * Kept in one place with the upstream pin below so a future core change is a
 * one-line update here rather than a scattered hunt. A bag that satisfies this
 * is renderable by every connector, including into the `-- NAME="value"` comment
 * form.
 *
 * Pinned to @malloydata/malloy 0.0.426 (`packages/malloy/src/query_metadata.ts`).
 */
export const MAX_PROPERTY_NAME_LENGTH = 128;
export const MAX_PROPERTY_VALUE_LENGTH = 256;
export const MAX_PROPERTIES = 20;
const PROPERTY_NAME_RE = /^[A-Za-z0-9_]+$/;
/**
 * Snowflake's `QUERY_TAG` holds the whole bag as one JSON string capped at 2000
 * chars, and db-snowflake clamps by slicing — which truncates mid-JSON and
 * leaves an unparseable tag, i.e. silently unqueryable in `QUERY_HISTORY`,
 * which is the entire point of tagging. So the budget is enforced here, on the
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
 * Advisory notes about a conforming bag: things no backend rejects but one
 * quietly discards, so a declaration that will not do what it says is reported
 * rather than left to be discovered in a warehouse console.
 *
 * BigQuery's label grammar needs a key that starts with a lowercase letter after
 * transformation; db-bigquery drops a key it cannot make valid (a leading digit
 * or underscore) while every other connector keeps it. Same declaration, present
 * on Snowflake, absent on BigQuery.
 */
export function queryMetadataPortabilityWarnings(
   meta: QueryMetadata,
): string[] {
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
 * The server's own description of the query, injected on every statement so
 * attribution needs no modeling work from the author.
 *
 * Deliberately neutral and deliberately STABLE: every field is a property of
 * the unit of work, not of the individual call, so repeating the same query
 * produces a byte-identical bag and the comment-carrying dialects keep whatever
 * result caching they do. A platform that wants its own dimensions (tenant,
 * deployment, request id) supplies them as a connection default or per-request
 * override; the publisher does not invent them.
 */
export interface QueryContext {
   /** What issued the query. Absent only where the class is genuinely unknown. */
   queryClass?: QueryClass;
   environment?: string;
   package?: string;
   /** Published version id, when the query runs against a versioned package. */
   version?: string;
   /** Package-relative model path, for query paths. */
   model?: string;
   /** Persist source or index dimension, for build paths. */
   source?: string;
   /** What started a build: `publish`, `on_demand`, `scheduler`. */
   trigger?: string;
   /** The caller's id for a build run, echoed so a build's statements group. */
   runId?: string;
}

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
   /** The server's own context, which wins over every author layer. */
   context?: QueryContext;
}

/** How a property was lost, for the drop metric and the caller's log. */
export type QueryMetadataDropReason =
   | "invalid_name"
   | "invalid_value"
   | "property_cap"
   | "serialized_cap";

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
 * Never throws: metadata must not be the reason a query or a build fails.
 *
 * Precedence is most-specific-wins per property (connection < model < request),
 * with server context applied last so a caller cannot overwrite the server's
 * own attribution. When the bag has to shrink — the 20-property cap, or
 * Snowflake's 2000-char serialized tag — author properties go before context
 * properties: losing a caller's `team` label degrades attribution, losing
 * `class` or `package` breaks it.
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

   // Author layers first, least specific to most, then context on top.
   const merged: QueryMetadata = {};
   const authorNames: string[] = [];
   for (const layer of [layers.connection, layers.model, layers.request]) {
      for (const [name, value] of Object.entries(layer ?? {})) {
         if (
            !PROPERTY_NAME_RE.test(name) ||
            name.length > MAX_PROPERTY_NAME_LENGTH
         ) {
            drops.push({ name, reason: "invalid_name" });
            continue;
         }
         if (typeof value !== "string") {
            drops.push({ name, reason: "invalid_value" });
            continue;
         }
         if (!(name in merged)) authorNames.push(name);
         merged[name] = sanitizeValue(value);
      }
   }
   for (const [name, value] of Object.entries(contextProperties)) {
      merged[name] = sanitizeValue(value);
   }

   // Author properties are dropped newest-declared-last so the shrink is
   // deterministic, and only ever down to the context set.
   const dropOrder = authorNames.filter((name) => !(name in contextProperties));
   while (Object.keys(merged).length > MAX_PROPERTIES && dropOrder.length > 0) {
      const name = dropOrder.pop() as string;
      delete merged[name];
      drops.push({ name, reason: "property_cap" });
   }
   while (
      JSON.stringify(merged).length > MAX_SERIALIZED_LENGTH &&
      dropOrder.length > 0
   ) {
      const name = dropOrder.pop() as string;
      delete merged[name];
      drops.push({ name, reason: "serialized_cap" });
   }
   // Context alone over either budget is a server bug, not a caller's — but a
   // thrown query would be worse than a partial tag, so shed context too.
   const contextOrder = Object.keys(contextProperties).reverse();
   while (
      (Object.keys(merged).length > MAX_PROPERTIES ||
         JSON.stringify(merged).length > MAX_SERIALIZED_LENGTH) &&
      contextOrder.length > 0
   ) {
      const name = contextOrder.pop() as string;
      if (!(name in merged)) continue;
      delete merged[name];
      drops.push({
         name,
         reason:
            Object.keys(merged).length >= MAX_PROPERTIES
               ? "property_cap"
               : "serialized_cap",
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
