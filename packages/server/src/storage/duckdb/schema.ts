// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../../logger";
import { DuckDBConnection } from "./DuckDBConnection";

export async function initializeSchema(
   db: DuckDBConnection,
   force: boolean = false,
): Promise<void> {
   const initialized = await db.isInitialized();

   if (force) {
      logger.info(
         "Reinitializing database schema dropping and recreating all tables",
      );
      await dropAllTables(db);
   } else if (!initialized) {
      // TODO: Remove this during projects cleanup
      // If a pre-rename `projects` schema is on disk, the new
      // CREATE TABLE IF NOT EXISTS pass below would silently leave child
      // tables on the old `project_id` column and the first query against
      // `environment_id` would crash. Drop the legacy tables (with a loud
      // warning) so the fresh schema can be created cleanly. This is
      // destructive: operators upgrading should re-create their environments
      // and packages via the API after the upgrade.
      await dropLegacyProjectSchema(db);
      logger.info("Creating database schema for the first time...");
   }
   // Re-keying `incremental_ledger` has to happen on an ALREADY-initialized
   // database — that is the only case where the old key is on disk — so it runs
   // outside the branch above rather than beside the legacy-projects drop.
   await dropPackageKeyedIncrementalLedger(db);
   await dropStoreBlindIncrementalLedger(db);

   // Always fall through to the CREATE TABLE IF NOT EXISTS pass below.
   // The statements are idempotent and let an already-initialized DB pick
   // up new tables added in later builds (e.g., the `themes` table for
   // the in-app Theme Editor) without forcing operators to run --init
   // and lose their existing environments / packages / materializations.
   await createDeclaredTables(db);

   // A CREATE TABLE IF NOT EXISTS is a no-op against an existing table no matter
   // how its COLUMNS differ, so the pass above carries a new table but never a
   // new column. Reconcile those separately, and only where the drift can exist:
   // nothing is on disk to have drifted before this build created it.
   //
   // `--init` is NOT exempt. It looks like it should be, since dropAllTables
   // recreates everything from scratch — but that function's table list is a
   // second, hand-maintained declaration of the table set, exactly the kind of
   // parallel list this reconcile exists to avoid depending on. A table added to
   // the DDL and forgotten there survives the drop, no-ops through CREATE TABLE
   // IF NOT EXISTS, and would keep its drift through the one command the warning
   // below tells operators to run. On a correctly recreated store the reconcile
   // finds nothing, so the cost of not exempting it is one in-memory DDL run.
   if (initialized) {
      await reconcileDeclaredColumns(db);
   }

   // After the reconcile, so an index over a newly added column can be created
   // in the same boot that adds it.
   await createDeclaredIndexes(db);
}

/**
 * The declared schema: every table this build expects `publisher.db` to hold.
 *
 * These statements are the single source of truth for the shape of the store —
 * `reconcileDeclaredColumns` derives the expected columns by running this same
 * function against a scratch database rather than from a second, hand-maintained
 * list, so there is no parallel declaration to drift out of sync with the DDL.
 */
async function createDeclaredTables(db: DuckDBConnection): Promise<void> {
   // Environments table
   await db.run(`
    CREATE TABLE IF NOT EXISTS environments (
      id VARCHAR PRIMARY KEY,
      name VARCHAR NOT NULL UNIQUE,
      path VARCHAR NOT NULL,
      description VARCHAR,
      metadata JSON,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL
    )
  `);

   // Packages table
   await db.run(`
    CREATE TABLE IF NOT EXISTS packages (
      id VARCHAR PRIMARY KEY,
      environment_id VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      description VARCHAR,
      manifest_path VARCHAR NOT NULL,
      metadata JSON,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id),
      UNIQUE (environment_id, name)
    )
  `);

   // Connections table
   await db.run(`
    CREATE TABLE IF NOT EXISTS connections (
      id VARCHAR PRIMARY KEY,
      environment_id VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      type VARCHAR NOT NULL,
      config JSON NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id),
      UNIQUE (environment_id, name)
    )
  `);

   // Storage destinations table.
   //
   // Warehouses materialization builds write to and materialized queries are
   // served from. A separate table from `connections`, mirroring the two
   // separate in-memory lists: a destination is not a connection, and the two
   // namespaces are independent, so `UNIQUE (environment_id, name)` here says
   // nothing about a connection of the same name.
   await db.run(`
    CREATE TABLE IF NOT EXISTS storage_destinations (
      id VARCHAR PRIMARY KEY,
      environment_id VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      type VARCHAR NOT NULL,
      config JSON NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id),
      UNIQUE (environment_id, name)
    )
  `);

   // Materializations table.
   //
   // `active_key` enforces at-most-one active (non-terminal) materialization
   // per (environment, package) at the DB layer. It is set to
   // `{environment_id}|{package_name}` while the row is active and cleared
   // to NULL on transition to any terminal state. A unique index on
   // `active_key` (see below) makes the insert-then-check race impossible —
   // a second concurrent create fails with a constraint violation, which the
   // service layer translates to `MaterializationConflictError`.
   // `manifest` is a JSON blob holding the build output returned inline on the
   // resource.
   await db.run(`
    CREATE TABLE IF NOT EXISTS materializations (
      id VARCHAR PRIMARY KEY,
      environment_id VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      status VARCHAR NOT NULL,
      active_key VARCHAR,
      started_at TIMESTAMP,
      completed_at TIMESTAMP,
      error TEXT,
      metadata JSON,
      manifest JSON,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id)
    )
  `);

   // Incremental ledger.
   //
   // One row per TABLE that refreshes incrementally, holding the durable
   // `covered_through` boundary: the exclusive upper end of the watermark range
   // that table is known to contain. A delta run reads it as the range's start,
   // and advances it only after the DML commits, which is what makes a crash
   // between the two a repeat rather than a gap.
   //
   // Keyed on (environment, connection, physical table) because that is what the
   // boundary is a fact ABOUT. It was keyed on (environment, package, source
   // address) and that was wrong in a way only an orchestrated host could see: a
   // host that serves several versions of one package presents a version-
   // qualified package name (Credible sends `<package>|<versionId>`, which is how
   // one worker holds several versions at once), so a table shared across those
   // versions got a fresh ledger row on the first refresh after every publish. No
   // row means no boundary, and no boundary means a full re-seed — of a table
   // whose whole purpose is not being rebuilt. Worse, the re-seed lands on the
   // serving table's own name, so it goes DROP + RENAME over a live table instead
   // of the atomic cutover a fresh generation gets.
   //
   // The declaration columns — the watermark, the merge keys, the strategy, and
   // the source's content address — are not bookkeeping. Each is recorded so the
   // read can REFUSE (ledgerLineageMismatch): a change to any of them means the
   // boundary was measured under rules that no longer apply, so the build path
   // re-seeds rather than applying a delta whose semantics no longer match the
   // table. The address needs comparing because it stopped being the key, and a
   // table's name need not change when its definition does — a standalone
   // publisher's names are `#@ persist name=` or the source name, with no content
   // token. The watermark and merge keys need comparing because changing them
   // changes the DML WITHOUT moving the address at all (proven in
   // incremental_compiler_contract.spec.ts), so not even the address would be
   // sufficient on its own.
   //
   // No claim/lease column: the single writer is already guaranteed by the
   // `active_key` unique index on `materializations` (at most one active run per
   // environment+package), so a second claim here would only add a way to
   // disagree with it.
   await db.run(`
    CREATE TABLE IF NOT EXISTS incremental_ledger (
      environment_id VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      source_entity_id VARCHAR NOT NULL,
      covered_through_value VARCHAR NOT NULL,
      covered_through_type VARCHAR NOT NULL,
      watermark_dimension VARCHAR NOT NULL,
      merge_key_dimensions JSON NOT NULL,
      derived_strategy VARCHAR NOT NULL,
      physical_table_name VARCHAR NOT NULL,
      connection_name VARCHAR NOT NULL,
      -- The storage destination the table lives in, or '' when it lives in
      -- connection_name's own warehouse (the colocated default). Part of the
      -- table's IDENTITY, not a description of it: a destination and a connection
      -- are separate namespaces that may share a name, and a source's physical
      -- name does not change when it moves between them, so without this a
      -- boundary measured on a stored table is indistinguishable from one
      -- measured on a colocated table of the same name.
      --
      -- In the KEY, because two such tables coexist legitimately. A source
      -- persisted colocated and another persisted into a destination under the
      -- same name are two different tables and neither is a misconfiguration —
      -- the within-package collision check keys on the destination too, so it
      -- correctly says nothing about the pair. Sharing one row between them makes
      -- both seed forever: each read matches on the store, finds nothing, and
      -- overwrites the other's row on the way out. Nothing reports that; it just
      -- never advances.
      --
      -- '' rather than NULL, and that is what makes it keyable. A NULL in the key
      -- defeats ON CONFLICT (NULL never equals NULL, so two colocated rows both
      -- insert) and forces every read to compare with IS NOT DISTINCT FROM. The
      -- sentinel keeps the key a plain equality on every path.
      storage_destination_name VARCHAR NOT NULL DEFAULT '',
      advanced_by_materialization_id VARCHAR,
      advanced_at TIMESTAMP NOT NULL,
      created_at TIMESTAMP NOT NULL,
      PRIMARY KEY (
        environment_id, connection_name, storage_destination_name,
        physical_table_name)
    )
  `);

   // Themes table.
   //
   // Singleton storage for the instance-wide theme edited by the in-app
   // Theme Editor. At most one row (id = "default"); we use INSERT ON
   // CONFLICT to keep the table effectively a key/value cell.
   //
   // publisher.config.json's optional `theme` block is a BOOT SEED only —
   // on first run, if this table is empty, ThemeStore writes the seed
   // here. Subsequent reads come from this table so the editor's saves
   // win over the file.
   await db.run(`
    CREATE TABLE IF NOT EXISTS themes (
      id VARCHAR PRIMARY KEY,
      payload JSON NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL
    )
  `);

   await createEntityEmbeddingsTable(db);
}

async function createDeclaredIndexes(db: DuckDBConnection): Promise<void> {
   // Create indexes for better query performance
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_packages_environment_id ON packages(environment_id)",
   );
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_connections_environment_id ON connections(environment_id)",
   );
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_storage_destinations_environment_id ON storage_destinations(environment_id)",
   );
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_materializations_environment_package ON materializations(environment_id, package_name)",
   );
   await db.run(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_materializations_active_key ON materializations(active_key)",
   );
   // Not a key prefix any more, and so load-bearing rather than merely helpful:
   // deleting a package's rows is now a scan of a non-key column.
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_incremental_ledger_environment_package ON incremental_ledger(environment_id, package_name)",
   );
}

export interface ColumnShape {
   table_name: string;
   column_name: string;
   data_type: string;
   is_nullable: boolean;
   /** SQL expression, as the catalog renders it (e.g. `CAST('t' AS BOOLEAN)`). */
   column_default: string | null;
}

export interface ConstraintRow {
   table_name: string;
   constraint_type: string;
   constraint_column_names: string[];
}

// `database_name = current_database()` is load-bearing, not tidiness: filtering
// on the schema alone also matches DuckDB's own `system.main` catalog (141 of the
// 146 rows on an empty database) and anything in `temp.main`. Those cancel out
// across a diff of two DuckDBs, but the keys here are `table.column`, so a
// same-named table in a second attached database would collide last-wins.
const COLUMN_SHAPE_QUERY = `
   SELECT table_name, column_name, data_type, is_nullable, column_default
   FROM duckdb_columns()
   WHERE schema_name = 'main' AND database_name = current_database()
`;

const CONSTRAINT_QUERY = `
   SELECT table_name, constraint_type, constraint_column_names
   FROM duckdb_constraints()
   WHERE schema_name = 'main' AND database_name = current_database()
`;

function columnKey(column: ColumnShape): string {
   return `${column.table_name}.${column.column_name}`;
}

/**
 * Add columns this build declares that an existing `publisher.db` does not have.
 *
 * The expected shape is not written down anywhere: it is read back out of a
 * throwaway in-memory database that `createDeclaredTables` has just been run
 * against. A hand-maintained column list would be a second declaration of the
 * schema, and a column added to the DDL but forgotten there would reproduce this
 * bug exactly; executing the DDL makes the statements their own specification.
 * The disk is the other half of the comparison, so no schema-version marker is
 * needed and none is kept.
 *
 * Strictly additive, and only for columns carrying no constraint, which is a
 * boundary DuckDB enforces rather than one this code chooses: `ALTER TABLE ...
 * ADD COLUMN` rejects a column carrying any constraint ("Adding columns with
 * constraints not yet supported") — NOT NULL, PRIMARY KEY, UNIQUE, CHECK and
 * FOREIGN KEY alike, with or without a DEFAULT. A bare DEFAULT is accepted and
 * backfills existing rows, so a declared default IS carried across; a constraint
 * never can be. Constraints are read from `duckdb_constraints()` rather than
 * inferred from `is_nullable`, because a UNIQUE or CHECK column reports as
 * nullable and would otherwise be added as a bare column: the store would then
 * hold the right column under the wrong rules, and two servers on the same build
 * would enforce differently depending on how their store was created.
 *
 * Everything else the diff finds is reported, not acted on:
 *
 * - A missing constrained column cannot be added at all. Adding an unconstrained
 *   stand-in would leave the store permanently disagreeing with its own
 *   declaration, so it warns and leaves the write to fail, which is at least
 *   traceable to a named column at boot.
 * - A column already present whose type, nullability, default or constraints
 *   have changed is not an ALTER we can infer the intent of.
 *
 *   Where the hand-written step goes when one is needed:
 *   `dropPackageKeyedIncrementalLedger` below is exactly that shape — a
 *   targeted pre-pass, keyed off what it finds on disk rather than a version
 *   marker, running before the CREATE pass so the tables are recreated
 *   correctly. Copy it rather than rediscovering the pattern; the warning's
 *   other suggestion, `--init`, is destructive and takes environments,
 *   packages and materializations with it.
 * - A column or table on disk that this build no longer declares is left alone.
 *   Dropping is a decision, not a reconciliation, and the relics are inert:
 *   `materializations.build_plan` (added and removed within four days in June
 *   2026) and the `build_manifests` table both sit on stores in the wild,
 *   unreferenced by any query.
 *
 * A failure here is logged and swallowed. The server can serve an environment it
 * cannot materialize into, and refusing to boot over that would turn a partial
 * outage into a total one.
 */
async function reconcileDeclaredColumns(db: DuckDBConnection): Promise<void> {
   let mirror: DuckDBConnection | null = null;
   try {
      mirror = new DuckDBConnection(":memory:");
      await mirror.initialize();
      await createDeclaredTables(mirror);

      const declared = await mirror.all<ColumnShape>(COLUMN_SHAPE_QUERY);
      const onDisk = await db.all<ColumnShape>(COLUMN_SHAPE_QUERY);

      const plan = planColumnReconcile(
         declared,
         onDisk,
         constrainedColumnsOf(
            await mirror.all<ConstraintRow>(CONSTRAINT_QUERY),
         ),
         constrainedColumnsOf(await db.all<ConstraintRow>(CONSTRAINT_QUERY)),
      );

      const added: string[] = [];
      const failed: string[] = [];
      for (const add of plan.adds) {
         // Per column, so one column DuckDB refuses cannot abandon the rest of
         // the reconcile — and so the summary below still reports what did land.
         try {
            await db.run(add.sql);
            added.push(add.description);
         } catch (err) {
            // First line only: DuckDBConnection.run appends the whole failing
            // statement after a newline, which would turn a scannable boot
            // warning into a multi-line SQL dump.
            const message = (
               err instanceof Error ? err.message : String(err)
            ).split("\n")[0];
            failed.push(`${add.description} (${message})`);
         }
      }

      const needsMigration = [...plan.needsMigration, ...failed];
      if (added.length > 0) {
         logger.info(
            `publisher.db: added ${added.length} column(s) this build declares that the store predates: ${added.join(", ")}`,
         );
      }
      if (needsMigration.length > 0) {
         logger.warn(
            "publisher.db does not match the schema this build declares, and the difference " +
               "cannot be reconciled automatically. Writes naming these columns will fail until " +
               `the store is migrated by hand or recreated with --init: ${needsMigration.join(", ")}`,
         );
      }
      if (plan.undeclared.length > 0) {
         logger.debug(
            `publisher.db holds ${plan.undeclared.length} column(s)/table(s) this build no longer declares; left in place: ${plan.undeclared.join(", ")}`,
         );
      }
   } catch (err) {
      logger.error(
         "Failed to reconcile publisher.db against the schema this build declares. " +
            "The server will continue; if the store predates a column, writes naming it will fail.",
         err,
      );
   } finally {
      try {
         await mirror?.close();
      } catch {
         // A scratch in-memory database that will not close is not worth failing
         // a boot over.
      }
   }
}

/**
 * The constraint kinds worth comparing between two stores, sorted so the
 * comparison is order-insensitive. NOT NULL is dropped because `is_nullable`
 * already carries it, and reporting both would name one difference twice.
 */
function enforcedKinds(kinds: string[] | undefined): string[] {
   return (kinds ?? []).filter((kind) => kind !== "NOT NULL").sort();
}

/** Every column named by any constraint, mapped to the constraint kinds naming it. */
export function constrainedColumnsOf(
   constraints: ConstraintRow[],
): Map<string, string[]> {
   const byColumn = new Map<string, string[]>();
   for (const constraint of constraints) {
      for (const column of constraint.constraint_column_names ?? []) {
         const key = `${constraint.table_name}.${column}`;
         const kinds = byColumn.get(key) ?? [];
         if (!kinds.includes(constraint.constraint_type)) {
            kinds.push(constraint.constraint_type);
         }
         byColumn.set(key, kinds);
      }
   }
   return byColumn;
}

/**
 * Decide what to do about each difference between the declared shape and the
 * store, without touching either.
 *
 * Split out from the IO so the cases that matter can be tested against synthetic
 * shapes: today's declared schema has no defaulted or UNIQUE column, so no test
 * driving the real DDL could reach the branches that handle them, and those are
 * exactly the branches a future column will land in.
 *
 * Exported for tests.
 */
export function planColumnReconcile(
   declared: ColumnShape[],
   onDisk: ColumnShape[],
   constrainedColumns: Map<string, string[]>,
   diskConstrainedColumns: Map<string, string[]>,
): {
   adds: Array<{ sql: string; description: string }>;
   needsMigration: string[];
   undeclared: string[];
} {
   const declaredTables = new Set(declared.map((c) => c.table_name));
   const declaredColumns = new Set(declared.map(columnKey));
   const diskTables = new Set(onDisk.map((c) => c.table_name));
   const diskColumns = new Map(onDisk.map((c) => [columnKey(c), c]));

   const adds: Array<{ sql: string; description: string }> = [];
   const needsMigration: string[] = [];

   for (const column of declared) {
      // A table absent from disk was just created by the pass above, at the
      // declared shape; only tables that predate this build can drift.
      if (!diskTables.has(column.table_name)) {
         continue;
      }
      const key = columnKey(column);
      const existing = diskColumns.get(key);

      if (!existing) {
         const kinds = constrainedColumns.get(key);
         if (kinds && kinds.length > 0) {
            needsMigration.push(
               `${key} (declared ${kinds.join(" + ")}; DuckDB cannot add a constrained column)`,
            );
            continue;
         }
         const withDefault = column.column_default
            ? ` DEFAULT ${column.column_default}`
            : "";
         // Deliberately NOT `IF NOT EXISTS`. The catalog read above is what
         // establishes the column is absent, so the guard adds no safety — and
         // on the DuckDB this pins (1.5.0–1.5.3, duckdb/duckdb#23209) `ADD
         // COLUMN IF NOT EXISTS ... DEFAULT` against a column that DOES exist
         // re-applies the default to every row instead of doing nothing,
         // silently destroying the column's data. It hits fixed-width types —
         // measured here on BOOLEAN and TIMESTAMP, while VARCHAR, INTEGER and
         // JSON survive — and TIMESTAMP is most of this schema's non-VARCHAR
         // columns. Without the guard, a disagreement between the plan and the
         // catalog raises into the per-column catch and is reported, which is
         // the outcome this whole path is built around.
         adds.push({
            sql: `ALTER TABLE "${column.table_name}" ADD COLUMN "${column.column_name}" ${column.data_type}${withDefault}`,
            description: `${key} ${column.data_type}${withDefault}`,
         });
         continue;
      }

      // Present, but not as declared. Reported per difference rather than as a
      // bare "differs", because which of the three moved decides what the
      // hand-written step has to do.
      const differences: string[] = [];
      if (existing.data_type !== column.data_type) {
         differences.push(
            `type on disk ${existing.data_type}, declared ${column.data_type}`,
         );
      }
      if (existing.is_nullable !== column.is_nullable) {
         differences.push(
            `nullability on disk ${existing.is_nullable ? "NULL" : "NOT NULL"}, declared ${column.is_nullable ? "NULL" : "NOT NULL"}`,
         );
      }
      if (
         (existing.column_default ?? null) !== (column.column_default ?? null)
      ) {
         differences.push(
            `default on disk ${existing.column_default ?? "none"}, declared ${column.column_default ?? "none"}`,
         );
      }
      // A constraint added to a table that already exists is as invisible to
      // CREATE TABLE IF NOT EXISTS as a column is, and the consequence is worse
      // than a missing column: the store keeps accepting rows a fresh store
      // rejects, so two servers on one build enforce different rules with
      // nothing failing. It has happened here once already — `incremental_ledger`
      // was re-keyed, and needed the hand-written detector below to catch it.
      // NOT NULL is excluded because `is_nullable` above says it better.
      const declaredKinds = enforcedKinds(constrainedColumns.get(key));
      const diskKinds = enforcedKinds(diskConstrainedColumns.get(key));
      if (declaredKinds.join(",") !== diskKinds.join(",")) {
         differences.push(
            `constraints on disk ${diskKinds.join(" + ") || "none"}, declared ${declaredKinds.join(" + ") || "none"}`,
         );
      }
      if (differences.length > 0) {
         needsMigration.push(`${key} (${differences.join("; ")})`);
      }
   }

   const undeclared = [
      ...onDisk
         .filter(
            (c) =>
               declaredTables.has(c.table_name) &&
               !declaredColumns.has(columnKey(c)),
         )
         .map(columnKey),
      ...[...diskTables]
         .filter((t) => !declaredTables.has(t))
         .map((t) => `${t} (whole table)`),
   ];

   return { adds, needsMigration, undeclared };
}

/**
 * Drop an `incremental_ledger` still keyed on (environment, package, source
 * address), so the pass above recreates it keyed on the table.
 *
 * DuckDB cannot re-key a table in place and this schema has no migration
 * framework, so the choice is drop-and-recreate or leave the old key on disk
 * silently. Dropping is safe here in a way it would not be for any other table:
 * the ledger is a CACHE of a boundary the publisher can always re-derive, and its
 * miss path is a seed — the same fallback every unprovable fact takes. Losing it
 * costs one full rebuild per incremental source, which is exactly what the old key
 * was already costing on every publish.
 */
async function dropPackageKeyedIncrementalLedger(
   db: DuckDBConnection,
): Promise<void> {
   const present = await db.all<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='incremental_ledger'",
   );
   if (!present || present.length === 0) {
      return;
   }
   // `pk` is true for every column of a composite primary key, so the old key is
   // identifiable by package_name being part of it. Reading the key rather than a
   // schema version means this is self-limiting: once no database has the old key,
   // it never fires again.
   const columns = await db.all<{ name: string; pk: boolean }>(
      "PRAGMA table_info('incremental_ledger')",
   );
   const keyedOnPackage = columns.some(
      (column) => column.name === "package_name" && Boolean(column.pk),
   );
   if (!keyedOnPackage) {
      return;
   }
   logger.info(
      "Re-keying the incremental ledger onto (environment, connection, table); " +
         "recorded covered_through boundaries are discarded, so each incremental " +
         "source rebuilds in full once and then resumes advancing by delta",
   );
   await db.run("DROP TABLE IF EXISTS incremental_ledger");
}

/**
 * Drop an `incremental_ledger` whose primary key predates the storage destination.
 *
 * Same shape, same reasoning and the same cost as
 * {@link dropPackageKeyedIncrementalLedger} below: the key is read rather than a
 * schema version, so this is self-limiting — once no database carries the old key
 * it never fires again — and the boundaries go with the table, which costs one
 * full rebuild per incremental source and then resumes advancing.
 *
 * A migration that preserved the rows is possible (rename, recreate, copy with ''
 * for the new column) and was deliberately not taken: it buys one avoided rebuild
 * of a feature that is not yet widely released, and pays for it with a
 * partial-failure state in schema initialization — a half-copied ledger and a
 * stranded `_old` table — which is a worse thing to own than a rebuild.
 */
async function dropStoreBlindIncrementalLedger(
   db: DuckDBConnection,
): Promise<void> {
   const present = await db.all<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='incremental_ledger'",
   );
   if (!present || present.length === 0) {
      return;
   }
   const columns = await db.all<{ name: string; pk: boolean }>(
      "PRAGMA table_info('incremental_ledger')",
   );
   // Nothing to do for a table this build created; the old key is identifiable by
   // the destination being absent from it — either the column does not exist at
   // all, or it exists as the nullable non-key form that shipped before this.
   const destinationInKey = columns.some(
      (column) =>
         column.name === "storage_destination_name" && Boolean(column.pk),
   );
   if (destinationInKey) {
      return;
   }
   logger.info(
      "Re-keying the incremental ledger onto (environment, connection, store, " +
         "table); recorded covered_through boundaries are discarded, so each " +
         "incremental source rebuilds in full once and then resumes advancing by " +
         "delta",
   );
   await db.run("DROP TABLE IF EXISTS incremental_ledger");
}

/**
 * Vector cache for semantic `malloy_getContext` retrieval (see
 * mcp/tools/embedding_index.ts). One row per discoverable model entity;
 * the primary key mirrors the tool's in-memory dedup key. Rows are
 * content-addressed: the index sync compares `content_hash` (over the
 * embedded text) plus `embedding_model` and re-embeds on mismatch, while
 * a dimensionality change is detected at query time by the stale-row
 * heal, so staleness always self-corrects and wiping the table only
 * ever costs re-embedding. `embedding` is a LIST column searched with
 * list_cosine_similarity; at the entity counts a single Publisher
 * serves, a brute-force scan is faster and simpler than a vector-index
 * extension.
 */
export async function createEntityEmbeddingsTable(
   db: DuckDBConnection,
): Promise<void> {
   await db.run(`
    CREATE TABLE IF NOT EXISTS entity_embeddings (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      entity_kind VARCHAR NOT NULL,
      entity_source VARCHAR NOT NULL,
      entity_name VARCHAR NOT NULL,
      model_path VARCHAR NOT NULL,
      content_hash VARCHAR NOT NULL,
      embedding_model VARCHAR NOT NULL,
      dims INTEGER NOT NULL,
      embedding FLOAT[] NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      PRIMARY KEY (environment_name, package_name, entity_kind, entity_source, entity_name)
    )
  `);
}

// TODO: Remove this during projects cleanup
// Tables in the pre-rename schema, listed children-first so DROP order
// satisfies foreign-key dependencies on the legacy `projects` table.
const LEGACY_TABLES_DROP_ORDER = [
   "build_manifests",
   "materializations",
   "packages",
   "connections",
   "projects",
] as const;

async function dropLegacyProjectSchema(db: DuckDBConnection): Promise<void> {
   const legacy = await db.all<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='projects'",
   );
   if (!legacy || legacy.length === 0) {
      return;
   }

   logger.warn(
      "Detected legacy 'projects' schema. Dropping legacy tables; existing environments/packages/connections/materializations data will be lost. Re-create them via the API after upgrade.",
   );

   for (const table of LEGACY_TABLES_DROP_ORDER) {
      try {
         await db.run(`DROP TABLE IF EXISTS ${table}`);
      } catch (err) {
         logger.warn(`Failed to drop legacy table ${table}:`, err);
      }
   }
}

async function dropAllTables(db: DuckDBConnection): Promise<void> {
   const tables = [
      "build_manifests",
      "incremental_ledger",
      "materializations",
      "packages",
      "connections",
      "storage_destinations",
      "environments",
      "themes",
      "entity_embeddings",
   ];

   logger.info("Dropping tables:", tables.join(", "));

   for (const table of tables) {
      try {
         await db.run(`DROP TABLE IF EXISTS ${table} `);
         logger.info(`Dropped table: ${table}`);
      } catch (err) {
         logger.warn(` Warning: Could not drop table ${table}:`, err);
      }
   }
}
