// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The collation reads read admission's records are built from, kept beside
 * the rules they serve (`admission_lookup.ts`, `source_capability.ts`):
 *
 * - {@link probeIO}: the I/O a dialect's collation reader goes through over
 *   a connection, each catalog read admitted past a locked DuckDB
 *   instance's gate;
 * - {@link bigqueryBuildCompareBinary} and the BigQuery catalog read behind
 *   it: whether a `storage=` build's warehouse SQL on BigQuery reads only
 *   strings compared byte-exact;
 * - {@link stringPaths}, {@link namedEntryOf} and {@link holdColumn}: which
 *   compared paths are strings, which model source a query's entry point is
 *   judged as, and which collations a table read holds a column under.
 *
 * Nothing here runs on a query's path: the source side is read at every
 * bind, the stored side recorded by each build.
 */

import {
   type Connection,
   type ModelDef,
   type PersistSource,
} from "@malloydata/malloy";
import { runAsInternalStatement } from "../../duckdb_lockdown";
import { sourceReads } from "../../governance/ir/source_reads";
import { CATALOG_ROW_LIMIT, type ProbeIO } from "./collation/probe_io";
import {
   BigQueryCatalog,
   type PlacedTable,
   placeTable,
   sqlProjects,
   TABLE_TYPES,
} from "./collation/bigquery";
import { fieldPathsIn } from "./gate_paths";
import { entryGates } from "./gate_reads";

/**
 * Whether a `storage=` build's warehouse SQL on BigQuery reads only strings
 * compared byte-exact: `sql`, the build of `source`, reading the tables its
 * definition reads (unknown: every project the SQL names is read instead),
 * judged by the catalog reads an answer's check makes
 * ({@link bigqueryCollatedPaths}), complete or not at all. False whenever a
 * read fails or is incomplete: the build's column contract then records its
 * strings compared `none`, which fails closed.
 */
export async function bigqueryBuildCompareBinary(args: {
   connection: Connection;
   source: PersistSource;
   sql: string;
}): Promise<boolean> {
   try {
      const read = await bigqueryCollatedPaths({
         reads: new Map(),
         io: probeIO(args.connection),
         connectionName: args.source.connectionName,
         sql: args.sql,
         tables: sourceReads(args.source),
      });
      return read.known && read.collated.size === 0;
   } catch {
      return false;
   }
}

/**
 * The collated column paths of the tables BigQuery `sql` reads, from the
 * catalog through `io`, or unknown when they cannot be read whole.
 */
async function bigqueryCollatedPaths(args: {
   reads: Map<string, Promise<unknown>>;
   io: ProbeIO;
   connectionName: string | undefined;
   sql: string;
   tables: ReturnType<typeof sourceReads>;
}): Promise<
   | { known: false }
   | {
        known: true;
        collated: ReadonlyMap<string, ReadonlySet<string>>;
        catalog: BigQueryCatalog;
     }
> {
   const unknown = { known: false } as const;
   const { sql, tables, connectionName } = args;
   try {
      if (/\bcollate\b/i.test(sql)) return unknown;
      const catalog = new BigQueryCatalog(args.reads, args.io, connectionName);
      const { job } = catalog;
      if (job.projects.length === 0) return unknown;
      const placed: PlacedTable[] = [];
      for (const t of tables ?? []) {
         // A table on another connection is not this query's to place.
         if (
            connectionName !== undefined &&
            t.connectionName !== connectionName
         ) {
            return unknown;
         }
         const p = placeTable(t.table, job.projects);
         if (p === undefined) return unknown;
         placed.push(p);
      }
      // Whether every project in `projects` is read and holds no collated
      // column: what author SQL and a view are served under.
      const clean = async (projects: Iterable<string>) => {
         for (const project of projects) {
            const d = await catalog.collatedDatasets(project);
            if (d === undefined || d.size > 0) return false;
         }
         return true;
      };
      if (tables === undefined) {
         const projects = new Set([...job.projects, ...sqlProjects(sql)]);
         return (await clean(projects))
            ? { known: true, collated: new Map(), catalog }
            : unknown;
      }
      const types = await catalog.tableTypes(placed);
      // A failed or denied read answers live; only a project with no region
      // to read is read dataset by dataset.
      if (types === undefined) return unknown;
      const collated = new Map<string, Set<string>>();
      for (const t of placed) {
         if (types !== "no_region") {
            // Coverage, by table: the project-wide read must see it.
            const type = t.projects
               .map((p) => types.get(`${p}.${t.datasetName}.${t.name}`))
               .find((x) => x !== undefined);
            if (type === undefined) return unknown;
            if (!TABLE_TYPES.has(type)) {
               if (!(await clean(t.projects))) return unknown;
               continue;
            }
            let inCollated = false;
            for (const p of t.projects) {
               const d = await catalog.collatedDatasets(p);
               if (d === undefined) return unknown;
               if (d.has(t.datasetName)) inCollated = true;
            }
            if (!inCollated) continue;
         }
         const datasetTable = (await catalog.datasetTables(t.dataset)).get(
            t.name,
         );
         // Coverage, by table: the dataset's read must see it, as a table.
         if (
            datasetTable === undefined ||
            !TABLE_TYPES.has(datasetTable.type)
         ) {
            return unknown;
         }
         for (const [path, c] of datasetTable.collated) {
            let set = collated.get(path);
            if (!set) {
               set = new Set();
               collated.set(path, set);
            }
            set.add(c);
         }
      }
      return { known: true, collated, catalog };
   } catch {
      return unknown;
   }
}

/**
 * The entry-level paths that read a string field, or undefined when a path
 * cannot be followed (fail closed). A path through a join is followed into
 * the joined source's fields. A computed field compares the fields its
 * expression reads, so each of those must be a path here too (the walk
 * lists them), and a field of a query-shaped source, whose stage output may
 * be computed from fields no path names, fails closed unless it is a string.
 */
export function stringPaths(
   struct: unknown,
   paths: readonly (readonly string[])[],
): string[][] | undefined {
   const out: string[][] = [];
   const seen = new Set<string>();
   for (const path of paths) {
      let at: unknown = struct;
      let owner: unknown = struct;
      let field: { type?: unknown; e?: unknown } | undefined;
      for (const name of path) {
         owner = at;
         field = (
            ((at as { fields?: unknown[] } | undefined)?.fields ?? []) as {
               name?: unknown;
               as?: unknown;
               type?: unknown;
               e?: unknown;
            }[]
         ).find((f) => String(f.as ?? f.name) === name);
         if (!field) return undefined;
         at = field;
      }
      if (field?.type !== "string") {
         if ((owner as { query?: unknown } | undefined)?.query !== undefined) {
            return undefined;
         }
         if (field?.e !== undefined) {
            const prefix = path.slice(0, -1);
            const listed = new Set(paths.map((p) => p.join(".")));
            const reads = fieldPathsIn(field.e);
            if (
               reads.length === 0 ||
               !reads.every((p) => listed.has([...prefix, ...p].join(".")))
            ) {
               return undefined;
            }
         }
         continue;
      }
      const key = path.join(".");
      if (seen.has(key)) continue;
      seen.add(key);
      out.push([...path]);
   }
   return out;
}

type Rows = { rows: Record<string, unknown>[] };

/**
 * Publisher's own catalog read over `connection`, admitted past a locked
 * DuckDB instance's gate (duckdb_lockdown.ts), at the connection's default
 * page unless `options` asks for more rows.
 */
async function catalogRead(
   connection: Connection,
   sql: string,
   options?: { rowLimit: number },
): Promise<Rows & { totalRows?: unknown }> {
   return (await runAsInternalStatement(sql, () =>
      connection.runSQL(sql, options),
   )) as unknown as Rows & { totalRows?: unknown };
}

/**
 * The I/O the collation reads go through over `connection`: its catalog
 * reads, each admitted past a locked DuckDB instance's gate
 * (duckdb_lockdown.ts), and, for BigQuery, what its driver holds (the
 * client's `datasets.list`, its projects and location), read duck-typed
 * here and nowhere else.
 */
export function probeIO(connection: Connection): ProbeIO {
   return {
      dialectName: connection.dialectName,
      async rows(sql) {
         return (await catalogRead(connection, sql)).rows ?? [];
      },
      async completeRows(sql) {
         const result = await catalogRead(connection, sql, {
            rowLimit: CATALOG_ROW_LIMIT,
         });
         const found = result.rows ?? [];
         if (
            found.length >= CATALOG_ROW_LIMIT ||
            result.totalRows === undefined ||
            result.totalRows === null ||
            Number(result.totalRows) !== found.length
         ) {
            throw new Error("a catalog read that may be incomplete");
         }
         return found;
      },
      async datasetLocations(project) {
         const client = (
            connection as unknown as {
               bigQuery?: {
                  getDatasets(options: {
                     projectId: string;
                     autoPaginate: boolean;
                  }): Promise<[{ metadata?: { location?: unknown } }[]]>;
               };
            }
         ).bigQuery;
         if (!client || typeof client.getDatasets !== "function") {
            throw new Error("no BigQuery client");
         }
         const [datasets] = await client.getDatasets({
            projectId: project,
            autoPaginate: true,
         });
         const locations = new Set<string>();
         for (const d of datasets) {
            const l = d.metadata?.location;
            if (typeof l !== "string" || l === "")
               throw new Error("no location");
            locations.add(l.toLowerCase());
         }
         return [...locations];
      },
      jobContext() {
         const c = connection as unknown as {
            projectId?: unknown;
            billingProjectId?: unknown;
            location?: unknown;
         };
         // The client's `{{projectId}}` placeholder stands for a project not
         // resolved yet: no project to read.
         const projects = [c.projectId, c.billingProjectId].filter(
            (p): p is string =>
               typeof p === "string" && p !== "" && !p.includes("{{"),
         );
         return {
            projects: [...new Set(projects)],
            location:
               typeof c.location === "string" && c.location !== ""
                  ? c.location.toLowerCase()
                  : undefined,
         };
      },
   };
}

/**
 * The model source a compiled query's entry point is judged as: its own name,
 * or, for an inline `extend` (`run: t extend { dimension: z is 1 } -> …`),
 * the name of the source it extends when the extension adds no `where:` and
 * no gate of its own; else undefined.
 */
export function namedEntryOf(
   structRef: unknown,
   struct: unknown,
   modelDef: ModelDef,
): string | undefined {
   if (typeof structRef === "string") return structRef;
   type Shape = {
      as?: unknown;
      extends?: unknown;
      sourceID?: unknown;
      type?: unknown;
      name?: unknown;
      connection?: unknown;
      filterList?: unknown;
      fields?: unknown;
   };
   const s = struct as Shape | undefined;
   if (typeof s?.as !== "string") return undefined;
   const base = (modelDef as unknown as { contents?: Record<string, Shape> })
      .contents?.[s.as];
   if (base === undefined) return undefined;
   // The same source: where Malloy records what it extends, that; and the
   // same root (type, table or SQL, connection), with every one of the
   // source's fields kept as it is (an extension only adds fields).
   if (s.extends !== undefined && s.extends !== base.sourceID) return undefined;
   if (
      s.type !== base.type ||
      s.name !== base.name ||
      s.connection !== base.connection
   ) {
      return undefined;
   }
   const fields = new Set(
      ((s.fields ?? []) as unknown[]).map((f) => JSON.stringify(f)),
   );
   if (
      !((base.fields ?? []) as unknown[]).every((f) =>
         fields.has(JSON.stringify(f)),
      )
   ) {
      return undefined;
   }
   const same = (a: unknown, b: unknown) =>
      JSON.stringify(a ?? []) === JSON.stringify(b ?? []);
   if (!same(base.filterList, s.filterList)) return undefined;
   // The walk's fail-closed deny reads as unequal.
   const gates = (x: unknown) => {
      const entries = entryGates(x, modelDef);
      return entries === "unreadable"
         ? undefined
         : entries.map((e) => [e.route, [...e.exprs]]);
   };
   const own = gates(struct);
   if (own === undefined || !same(gates(base), own)) return undefined;
   return s.as;
}

/** Record that a table read holds `name` under `collation`. */
export function holdColumn(
   held: Map<string, Set<string | null>>,
   name: string,
   collation: string | null,
): void {
   let set = held.get(name);
   if (!set) {
      set = new Set();
      held.set(name, set);
   }
   set.add(collation);
}
