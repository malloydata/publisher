// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Read admission's collation port answered by lookups: the same rules
 * `gate_collation.ts` applies, judged from what load, bind and build
 * recorded, with no catalog read and no compile of its own.
 *
 * - The source side reads `source_capability.ts`, which every bind reads
 *   for the model's runtime: each source's string fields by path, each
 *   connection's default collation (Postgres), instance settings (DuckDB)
 *   or catalog (BigQuery). What the query itself adds is judged from its
 *   compiled IR alone: its paths, a `COLLATE` or a table in its own text.
 * - The stored side reads each table's column contract (the snapshot's
 *   `contracts`), which its build recorded and every bind holds its table
 *   to: a column whose collation was altered is unbound at the next bind
 *   (`collation_changed`), not refused on the next answer.
 *
 * What was not recorded is unknown and refuses: the answer is then not
 * kept, and the query answers live.
 */

import type { ModelDef, QueryMaterializer } from "@malloydata/malloy";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { queryTableReads } from "../../governance/ir/source_reads";
import type { Snapshot } from "../domain/binding";
import {
   BINARY_COLLATION,
   type Collation,
   postgresColumnCollation,
} from "../domain/collation";
import type { ColumnContract } from "../domain/column_contract";
import { collationReaderOf } from "./collation";
import { placeTable, sqlProjects, TABLE_TYPES } from "./collation/bigquery";
import { fieldPathsIn } from "./gate_paths";
import { namedEntryOf, stringPaths } from "./gate_collation";
import type {
   CollationProbe,
   GateReads,
   ProbedTable,
} from "./gate_reads_types";
import {
   type ConnectionCapability,
   fieldKey,
   type FieldCapability,
   irStrings,
   type RecordedBigQuery,
   type StringCapabilities,
   stringCapabilitiesOf,
} from "./source_capability";

/** The entry point of a compiled query, as the lookups read it. */
interface Entry {
   modelDef: ModelDef;
   prepared: Awaited<ReturnType<typeof readPreparedQuery>>;
   struct: { dialect?: unknown; connection?: unknown; filterList?: unknown };
   entryName: string | undefined;
   dialect: string;
   connectionName: string | undefined;
   capabilities: StringCapabilities | undefined;
}

async function entryOf(runnable: QueryMaterializer): Promise<Entry | string> {
   let prepared: Entry["prepared"];
   try {
      prepared = await readPreparedQuery(runnable);
   } catch {
      return "no author compile";
   }
   const modelDef = prepared.modelDef as ModelDef | undefined;
   if (!modelDef) return "no author compile";
   const struct = (resolveStructRefSource(
      prepared.structRef as never,
      modelDef as never,
   ) ?? {}) as Entry["struct"];
   const runtime = (runnable as unknown as { runtime?: object }).runtime;
   return {
      modelDef,
      prepared,
      struct,
      entryName: namedEntryOf(prepared.structRef, struct, modelDef),
      dialect: typeof struct.dialect === "string" ? struct.dialect : "",
      connectionName:
         typeof struct.connection === "string" ? struct.connection : undefined,
      capabilities: stringCapabilitiesOf(runtime),
   };
}

/** Whether nothing the reads compare is a string: no collation matters. */
function comparesNoString(reads: GateReads | undefined): boolean {
   return reads !== undefined && !reads.unresolved && reads.comparesNoString;
}

/** What a comparison needs of a collation: equality any deterministic one. */
function needOf(reads: GateReads | undefined): readonly Collation[] {
   return reads?.equalityOnly === true
      ? ["binary", "deterministic"]
      : ["binary"];
}

/** A string in the query's own IR (its author SQL, a caller's text) names a `COLLATE`. */
function namesCollate(entry: Entry): boolean {
   return irStrings([entry.struct, entry.prepared.pipeline]).some((s) =>
      /\bcollate\b/i.test(s),
   );
}

/** A recorded field, or unknown. */
function fieldOf(
   entry: Entry,
   path: readonly string[],
): FieldCapability | undefined {
   return entry.entryName === undefined
      ? undefined
      : entry.capabilities?.sources
           .get(entry.entryName)
           ?.fields.get(fieldKey(path));
}

function connectionOf(entry: Entry): ConnectionCapability | undefined {
   return entry.connectionName === undefined
      ? undefined
      : entry.capabilities?.connections.get(entry.connectionName);
}

/**
 * What a BigQuery query's strings compare under, from the catalog its
 * connection's bind recorded, by `gate_collation.ts`'s `bigquerySource`
 * rules: unknown for a `COLLATE` in its text, a table the record did not
 * read, a view or author SQL over a project holding a collated column.
 */
function bigqueryLookup(entry: Entry):
   | { known: false }
   | {
        known: true;
        collated: ReadonlyMap<string, ReadonlySet<string>>;
        catalog: RecordedBigQuery;
     } {
   const unknown = { known: false } as const;
   const catalog = connectionOf(entry)?.catalog;
   if (!catalog || catalog.projects.length === 0) return unknown;
   if (namesCollate(entry)) return unknown;
   const tables = queryTableReads(
      entry.prepared.compositeResolvedSourceDef ?? entry.struct,
      entry.prepared.pipeline,
   );
   const clean = (projects: Iterable<string>) => {
      for (const p of projects) {
         const d = catalog.collated.get(p);
         if (d === undefined || d.size > 0) return false;
      }
      return true;
   };
   if (tables === undefined) {
      const projects = new Set([
         ...catalog.projects,
         ...sqlProjects(
            irStrings([entry.struct, entry.prepared.pipeline]).join("\n"),
         ),
      ]);
      return clean(projects)
         ? { known: true, collated: new Map(), catalog }
         : unknown;
   }
   const types = catalog.types;
   if (types === undefined) return unknown;
   const collated = new Map<string, Set<string>>();
   for (const t of tables) {
      if (
         entry.connectionName !== undefined &&
         t.connectionName !== entry.connectionName
      ) {
         return unknown;
      }
      const p = placeTable(t.table, catalog.projects);
      if (p === undefined) return unknown;
      if (types !== "no_region") {
         const type = p.projects
            .map((project) =>
               types.get(`${project}.${p.datasetName}.${p.name}`),
            )
            .find((x) => x !== undefined);
         if (type === undefined) return unknown;
         if (!TABLE_TYPES.has(type)) {
            if (!clean(p.projects)) return unknown;
            continue;
         }
         let inCollated = false;
         for (const project of p.projects) {
            const d = catalog.collated.get(project);
            if (d === undefined) return unknown;
            if (d.has(p.datasetName)) inCollated = true;
         }
         if (!inCollated) continue;
      }
      const table = catalog.datasets.get(p.dataset)?.get(p.name);
      if (table === undefined || !TABLE_TYPES.has(table.type)) return unknown;
      for (const [path, c] of table.collated) {
         let set = collated.get(path);
         if (!set) {
            set = new Set();
            collated.set(path, set);
         }
         set.add(c);
      }
   }
   return { known: true, collated, catalog };
}

/** The source side, looked up (`stringGateCollationRefusal`'s rules). */
export async function lookupSourceRefusal(args: {
   authorRunnable: QueryMaterializer;
   reads: GateReads | undefined;
}): Promise<string | undefined> {
   const { reads } = args;
   if (comparesNoString(reads)) return undefined;
   if (reads === undefined || reads.unresolved) return "gate reads unread";
   const need = needOf(reads);
   const entry = await entryOf(args.authorRunnable);
   if (typeof entry === "string") return entry;
   const reader = collationReaderOf(entry.dialect);
   if (reader === undefined) return "a dialect whose collation is not read";
   if (entry.capabilities === undefined) {
      return "no string capability recorded for this model";
   }
   if (reader.scope === "dataset") {
      const source = bigqueryLookup(entry);
      return source.known && source.collated.size === 0
         ? undefined
         : "a query that reads a string under a collation";
   }
   if (reader.scope === "instance") {
      const conn = connectionOf(entry);
      return conn?.noDefaultCollation === true &&
         conn.noCollateDefinitions === true &&
         !namesCollate(entry)
         ? undefined
         : "an instance that compares strings under a collation";
   }
   if (entry.entryName === undefined) return "no named entry point";
   if (!reads.entryPathsComplete) return "a scope from elsewhere";
   if (!reads.comparesNoString && reads.comparesOnlyColumns !== true) {
      const fallback = connectionOf(entry)?.defaultCollation ?? "unknown";
      if (!need.includes(fallback)) {
         return `a comparison under a ${fallback} default collation`;
      }
   }
   const paths = stringPaths(entry.struct, reads.entryPaths);
   if (paths === undefined) return "an unreadable gate path";
   for (const path of paths) {
      const collation = fieldOf(entry, path)?.collation ?? "unknown";
      if (!need.includes(collation)) {
         return `\`${path.join(".")}\` compares under a ${collation} collation`;
      }
   }
   for (const group of reads.sameCollation ?? []) {
      const ids = new Set(group.map((p) => fieldOf(entry, p)?.identity));
      if (ids.has(undefined) || ids.size > 1) {
         return `one value combines columns under different collations (${group.map((p) => p.join(".")).join(", ")})`;
      }
   }
   return undefined;
}

/** The contract `snapshot` holds for the table `t`, when it holds one. */
function contractOf(
   t: ProbedTable,
   snapshot: Snapshot | undefined,
): ColumnContract | undefined {
   return t.virtualHandle === undefined
      ? undefined
      : snapshot?.contracts?.get(t.virtualHandle);
}

/** A stored side's judgement, and why it is unknown when it is. */
type Judged = { collation: Collation; why?: string };

const UNKNOWN = (why: string): Judged => ({ collation: "unknown", why });
const BINARY: Judged = { collation: "binary" };

/** Every string column the contract records is stored byte-wise. */
function storedBinary(contract: ColumnContract | undefined): Judged {
   if (!contract) return UNKNOWN("no contract recorded for the table");
   // The holding engine's default, where the build recorded it.
   if (
      contract.defaultCollation !== undefined &&
      contract.defaultCollation !== BINARY_COLLATION
   ) {
      return UNKNOWN("a default collation recorded for the table");
   }
   const collated = contract.columns.find(
      (c) => c.collation !== null && c.collation !== BINARY_COLLATION,
   );
   return collated === undefined
      ? BINARY
      : UNKNOWN(`\`${collated.name}\` stored under ${collated.collation}`);
}

/** A BigQuery database table against its source (`bigqueryDatabaseTableCollation`). */
function bigqueryStored(
   entry: Entry,
   contract: ColumnContract | undefined,
): Collation {
   const source = bigqueryLookup(entry);
   if (!source.known || !contract) return "unknown";

   const stored = new Map<string, string>();
   for (const c of contract.columns) {
      if (c.collation === null || c.collation === BINARY_COLLATION) continue;
      if (!c.collation.startsWith("bigquery:")) return "unknown";
      stored.set(c.name, c.collation.slice("bigquery:".length));
   }
   for (const [path, collations] of source.collated) {
      if (collations.size !== 1 || !collations.has(stored.get(path) ?? "")) {
         return "unknown";
      }
   }
   for (const [path, c] of stored) {
      if (!source.collated.get(path)?.has(c)) return "unknown";
   }
   return "binary";
}

/** One table's stored side, looked up (`storedSide`'s rules). */
function storedSide(
   t: ProbedTable,
   entry: Entry,
   snapshot: Snapshot | undefined,
): Judged {
   const contract = contractOf(t, snapshot);
   if (t.tier === "storage") return storedBinary(contract);
   const reader = collationReaderOf(entry.dialect);
   if (reader === undefined) return BINARY;
   if (reader.scope === "dataset") {
      const c = bigqueryStored(entry, contract);
      return c === "binary" ? BINARY : UNKNOWN("a BigQuery table unmatched");
   }
   // DuckDB: the table's definition, under the instance's default.
   const name = t.destinationName ?? entry.connectionName ?? "";
   const conn = entry.capabilities?.connections.get(name);
   if (conn?.noDefaultCollation !== true) {
      return UNKNOWN(
         conn === undefined
            ? `no capability recorded for connection ${name}`
            : "a warehouse default collation",
      );
   }
   return storedBinary(contract);
}

/**
 * Database tables in a per-column dialect, against their source, looked up
 * (`databaseCollationRefusal`'s rules): each compared source path and each
 * stored column the reads name hold one and the same collation.
 */
function databaseRefusal(
   tables: readonly ProbedTable[],
   entry: Entry,
   reads: GateReads | undefined,
   snapshot: Snapshot | undefined,
): string | undefined {
   if (reads === undefined || reads.unresolved) return "gate reads unread";
   if (!reads.entryPathsComplete) return "a scope from elsewhere";
   if (entry.entryName === undefined) return "no named entry point";
   const source = connectionOf(entry);
   if (source?.defaultIdentity === undefined) {
      return "the source's default unread";
   }
   const held = new Map<string, string | null>();
   for (const t of tables) {
      if (t.tablePath === undefined) return "a table with no path";
      const storedName = t.destinationName ?? entry.connectionName;
      if (storedName !== entry.connectionName) {
         const stored = entry.capabilities?.connections.get(storedName ?? "");
         if (stored?.defaultIdentity !== source.defaultIdentity) {
            return "a stored table's connection has another default collation";
         }
      }
      const contract = contractOf(t, snapshot);
      if (!contract) return "a table with no contract";
      for (const c of contract.columns) held.set(c.name, c.collation);
   }
   const live = new Set((reads.livePaths ?? []).map((p) => p.join("\u0000")));
   const kept = reads.entryPaths
      .map((path, i) => ({ path, names: reads.columns[i] ?? [] }))
      .filter(
         ({ path, names }) =>
            !live.has(path.join("\u0000")) || names.some((n) => held.has(n)),
      );
   const paths = stringPaths(
      entry.struct,
      kept.map((k) => k.path),
   );
   if (paths === undefined) return "an unreadable gate path";
   const compared = new Set<string>();
   for (const path of paths) {
      const field = fieldOf(entry, path);
      if (field?.identity === undefined) return `\`${path.join(".")}\` unread`;
      // As a contract records a column: byte-wise ones alike.
      compared.add(postgresColumnCollation(field.identity, field.collation));
   }
   const ownWhere = new Set(fieldPathsIn(entry.struct.filterList).flat());
   const rollupRead = tables.some(
      (t) => (t as { kind?: unknown }).kind === "rollup",
   );
   let storedStrings = 0;
   const columns = live.size === 0 ? reads.columns : kept.map((k) => k.names);
   for (const names of columns) {
      const found = names.filter((n) => held.has(n));
      if (found.length === 0) {
         if (rollupRead && names.some((n) => ownWhere.has(n))) continue;
         return "a scope column no stored table holds";
      }
      for (const n of found) {
         const id = held.get(n);
         if (id === null || id === undefined) continue;
         storedStrings++;
         compared.add(id);
      }
   }
   if (storedStrings > 0 && paths.length === 0) {
      return "a stored string column with no compared source path";
   }
   return compared.size <= 1 ? undefined : "a column under another collation";
}

/** The stored side, looked up (`storedTableCollationRefusal`'s rules). */
export async function lookupStoredRefusal(args: {
   tables: readonly ProbedTable[];
   authorRunnable: QueryMaterializer;
   reads: GateReads | undefined;
   bind?: object;
}): Promise<string | undefined> {
   if (args.tables.length === 0) return undefined;
   if (comparesNoString(args.reads)) return undefined;
   const need = needOf(args.reads);
   const snapshot = args.bind as Snapshot | undefined;
   const entry = await entryOf(args.authorRunnable);
   // A source that cannot be read judges its tables as an unread dialect.
   const resolved: Entry =
      typeof entry === "string"
         ? {
              modelDef: {} as ModelDef,
              prepared: {} as Entry["prepared"],
              struct: {},
              entryName: undefined,
              dialect: "",
              connectionName: undefined,
              capabilities: undefined,
           }
         : entry;
   const perColumn = collationReaderOf(resolved.dialect)?.scope === "column";
   let sameNeeded = false;
   for (const t of args.tables) {
      if (perColumn && t.tier === "database") {
         sameNeeded = true;
         continue;
      }
      const { collation, why } = storedSide(t, resolved, snapshot);
      if (!need.includes(collation)) {
         return `a stored table compares strings under a ${collation} collation${why ? ` (${why})` : ""}`;
      }
   }
   if (!sameNeeded) return undefined;
   const why = databaseRefusal(
      args.tables.filter((t) => t.tier === "database"),
      resolved,
      args.reads,
      snapshot,
   );
   return why === undefined
      ? undefined
      : `a stored table does not keep its source's collation: ${why}`;
}

/** The collation port answered by lookups. */
export const lookupProbe: CollationProbe = {
   sourceRefusal: lookupSourceRefusal,
   storedRefusal: lookupStoredRefusal,
};
