// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a collation read goes through: the I/O of one connection, as
 * `gate_collation.ts` builds it (`probeIO`), and the answer every read
 * gives. A reader in this directory sends SQL and reads rows through it and
 * never holds a connection, so the serve layer's SQL and connection lookups
 * stay in `gate_collation.ts`.
 */

/**
 * How a string column compares: byte-exact under every operator (`binary`),
 * byte-exact for equality only (`deterministic`), or not known to be either
 * (`unknown`, which fails closed).
 */
export type Collation = "binary" | "deterministic" | "unknown";

/** One connection's I/O, as the collation reads use it. */
export interface ProbeIO {
   /** The connection's Malloy dialect, which quotes a name it reads. */
   readonly dialectName: string;
   /**
    * Rows of Publisher's own catalog read, admitted past a locked DuckDB
    * instance's gate (which refuses settings reads to caller and author SQL).
    */
   rows(sql: string): Promise<Record<string, unknown>[]>;
   /**
    * BigQuery: the locations of project `project`'s datasets, through the
    * client the connection holds (`datasets.list`, a metadata call, which
    * lists only the datasets the caller may get). Throws when the
    * connection holds no client, or a dataset names no location.
    */
   datasetLocations(project: string): Promise<string[]>;
   /**
    * BigQuery: what the connection runs jobs as, its projects (none when a
    * project is not resolved yet) and its location, lower-cased.
    */
   jobContext(): { projects: string[]; location: string | undefined };
}

/** One dialect's collation reads, each through a {@link ProbeIO}. */
export interface CollationReader {
   /**
    * How a query's strings are read: `instance`, once over the whole
    * instance and the query's SQL; `column`, per string column the gates and
    * scopes compare. (BigQuery's reader is `dataset`: from the catalog of
    * every table the query reads.) It agrees with the dialect's
    * `stringCollation` fact (`domain/dialect.ts`), which a spec checks.
    */
   readonly scope: "instance" | "column";
   /**
    * What the strings of a source compare under, read from SQL that
    * selects them (as `probe_value`, through the author's compile).
    */
   sourceColumns(io: ProbeIO, sql: string): Promise<Collation>;
   /** What the strings of a stored table compare under, by its path. */
   storedTable(io: ProbeIO, tablePath: string): Promise<Collation>;
}
