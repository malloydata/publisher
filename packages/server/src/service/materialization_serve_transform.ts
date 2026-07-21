import {
   InMemoryURLReader,
   type LookupConnection,
   type Connection as MalloyConnection,
   Runtime,
} from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../errors";
import { logger } from "../logger";
import type { ManifestEntry } from "../storage/DatabaseInterface";
import { quoteTablePath } from "./quoting";

/**
 * The per-source serve pointer the virtual-source serve transform consumes. Two
 * origins, one shape (the "injectable binding seam"): the publisher self-derives
 * it from a build's manifest entry (standalone), or a host (control plane)
 * supplies it. `tablePath` is the LOGICAL, unquoted physical table path; `schema`
 * is the AUTHORITATIVE DuckDB column schema captured post-build (raw DuckDB type
 * strings), which the transform declares verbatim — see {@link buildServeShapeModel}.
 */
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
         tablePath: entry.physicalTableName,
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
   return (
      `type: ${shapeTypeName} is {\n${fields}\n}\n` +
      `source: ${binding.sourceName} is ${binding.connectionName}.virtual('${binding.virtualHandle}')::${shapeTypeName}`
   );
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
 * captured columns — the leaf/simple-semantic-layer case. A source whose queries
 * rely on measures/dims/joins defined on it in the author's model is not
 * reproduced here; the serve path compiles the caller's query against this model
 * and, if it does not compile (a refinement this shape lacks), falls back to
 * serving live. Widening this to preserve rich extend blocks is the
 * base-swap-preserve-refinements follow-on.
 */
export function buildServeShapeModelForBindings(bindings: ServeBinding[]): {
   modelText: string;
} {
   const fragments = bindings.map(serveShapeFragment).join("\n");
   return {
      modelText: `##! experimental.virtual_source\n${fragments}\n`,
   };
}

/**
 * Assemble the per-call `virtualMap` (`connectionName -> handle -> canonical
 * table path`) core resolves a virtual source through. The table path is quoted
 * canonical for DuckDB — core validates every entry is canonical SQL for some
 * dialect and then pastes it verbatim (it does not quote it), so an unquoted or
 * mis-cased path is a run-time error. Multiple bindings on one connection fold
 * into that connection's inner map.
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
      inner.set(b.virtualHandle, quoteTablePath(b.tablePath, "duckdb"));
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
