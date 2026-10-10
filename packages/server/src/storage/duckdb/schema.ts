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
      // A pre-rename `projects` schema would leave child tables on the old
      // `project_id` column (CREATE TABLE IF NOT EXISTS does not alter them),
      // and the first query against `environment_id` would crash. Dropping is
      // destructive: operators re-create environments and packages via the API.
      await dropLegacyProjectSchema(db);
      logger.info("Creating database schema for the first time...");
   }
   // The old ledger key only exists on an already-initialized database, so
   // these run outside the branch above.
   await dropPackageKeyedIncrementalLedger(db);
   await dropStoreBlindIncrementalLedger(db);

   // Idempotent, so an existing store picks up tables added in later builds
   // without operators running --init and losing their data.
   await createDeclaredTables(db);

   // CREATE TABLE IF NOT EXISTS never adds a column to an existing table, so
   // columns are reconciled separately wherever a table could predate this build.
   //
   // `--init` is NOT exempt: dropAllTables' table list is hand-maintained, so a
   // table missing from it survives the drop with its drift intact. On a
   // correctly recreated store the reconcile finds nothing.
   if (initialized) {
      await reconcileDeclaredColumns(db);
   }

   // After the reconcile, so an index over a newly added column can be created
   // in the same boot.
   await createDeclaredIndexes(db);
}

/**
 * The declared schema, and its single source of truth:
 * `reconcileDeclaredColumns` runs this same function against a scratch database
 * to derive the expected columns, so there is no parallel list to drift.
 */
async function createDeclaredTables(db: DuckDBConnection): Promise<void> {
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

   // Storage destinations: warehouses materialization builds write to and serve
   // from. Separate from `connections` because the namespaces are independent;
   // `UNIQUE (environment_id, name)` says nothing about a same-named connection.
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

   // Materializations. `active_key` enforces at most one active (non-terminal)
   // materialization per (environment, package): it is
   // `{environment_id}|{package_name}` while active and NULL once terminal, and
   // its unique index makes a concurrent second create fail with a constraint
   // violation (translated to `MaterializationConflictError`) instead of racing.
   // `manifest` holds the build output returned inline on the resource.
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

   // Incremental ledger: one row per incrementally refreshed TABLE, holding the
   // durable `covered_through` boundary (exclusive upper end of the watermark
   // range the table contains). It advances only after the DML commits, so a
   // crash between the two is a repeat rather than a gap.
   //
   // Keyed on the physical table because that is what the boundary is a fact
   // about. It must not be keyed on package: a host serving several versions of
   // one package uses version-qualified package names (Credible sends
   // `<package>|<versionId>`), so a package key would give a shared table a
   // fresh row, and a full re-seed via DROP + RENAME over the live table, after
   // every publish.
   //
   // The declaration columns (watermark, merge keys, strategy, source content
   // address) let the read REFUSE (ledgerLineageMismatch) a boundary measured
   // under rules that no longer apply, so the build re-seeds instead. Each is
   // needed: a table's name need not change when its definition does, and
   // changing the watermark or merge keys changes the DML without moving the
   // address (see incremental_compiler_contract.spec.ts).
   //
   // No claim/lease column: the `active_key` unique index on `materializations`
   // already guarantees a single writer; a second claim could only disagree.
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

   // Themes: the instance-wide theme edited by the in-app Theme Editor, at
   // most one row (id = "default", written with INSERT ON CONFLICT).
   // publisher.config.json's `theme` block is only a boot seed written when
   // this table is empty, so the editor's saves win over the file.
   await db.run(`
    CREATE TABLE IF NOT EXISTS themes (
      id VARCHAR PRIMARY KEY,
      payload JSON NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL
    )
  `);

   await createEntityEmbeddingsTable(db);
   await createEntityKeyphrasesTable(db);
   await createSourceSummariesTable(db);
   await createCatalogTables(db);
}

async function createDeclaredIndexes(db: DuckDBConnection): Promise<void> {
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
   // Load-bearing: package_name is not a key prefix, so deleting a package's
   // rows would otherwise scan the table.
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

// `database_name = current_database()` is load-bearing: the schema filter alone
// also matches DuckDB's `system.main` and `temp.main` catalogs, and since keys
// are `table.column`, a same-named table in another attached database would
// collide last-wins.
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
 * The expected shape is read back from a throwaway in-memory database that
 * `createDeclaredTables` has just run against, so the DDL is its own
 * specification and no schema-version marker is kept.
 *
 * Strictly additive, and only for unconstrained columns: DuckDB's `ALTER TABLE
 * ... ADD COLUMN` rejects any constraint (NOT NULL, PRIMARY KEY, UNIQUE, CHECK,
 * FOREIGN KEY), though a bare DEFAULT is accepted and backfills. Constraints come
 * from `duckdb_constraints()` rather than `is_nullable`, because a UNIQUE or
 * CHECK column reports as nullable and would otherwise be added bare, leaving
 * two servers on one build enforcing different rules.
 *
 * Everything else the diff finds is reported, not acted on:
 *
 * - A missing constrained column: an unconstrained stand-in would permanently
 *   disagree with the declaration, so it warns and lets writes fail traceably.
 * - A present column whose type, nullability, default or constraints changed:
 *   the intent cannot be inferred. The hand-written fix should copy
 *   `dropPackageKeyedIncrementalLedger`: a targeted pre-pass keyed off what is
 *   on disk, run before the CREATE pass. (`--init` also works but destroys
 *   environments, packages and materializations.)
 * - A column or table no longer declared is left alone; dropping is a decision,
 *   not a reconciliation, and such relics (e.g. `materializations.build_plan`,
 *   `build_manifests`) are inert.
 *
 * A failure here is logged and swallowed: refusing to boot over an environment
 * the server cannot materialize into would turn a partial outage into a total one.
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
         // Per column, so one refused column cannot abandon the rest.
         try {
            await db.run(add.sql);
            added.push(add.description);
         } catch (err) {
            // First line only: DuckDBConnection.run appends the failing
            // statement after a newline.
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
         // Not worth failing a boot over a scratch database.
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
 * Pure so tests can reach the defaulted/UNIQUE branches with synthetic shapes;
 * today's DDL has no such column. Exported for tests.
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
         // Deliberately NOT `IF NOT EXISTS`: the catalog read already proves the
         // column absent, and on DuckDB 1.5.0–1.5.3 (duckdb/duckdb#23209) `ADD
         // COLUMN IF NOT EXISTS ... DEFAULT` on an existing fixed-width column
         // (BOOLEAN, TIMESTAMP) overwrites every row with the default. Without
         // the guard, a plan/catalog disagreement raises and is reported.
         adds.push({
            sql: `ALTER TABLE "${column.table_name}" ADD COLUMN "${column.column_name}" ${column.data_type}${withDefault}`,
            description: `${key} ${column.data_type}${withDefault}`,
         });
         continue;
      }

      // Reported per difference, because which one moved decides what the
      // hand-written migration has to do.
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
      // A constraint change is as invisible to CREATE TABLE IF NOT EXISTS as a
      // new column, and worse: the old store keeps accepting rows a fresh one
      // rejects, silently. NOT NULL is excluded because `is_nullable` covers it.
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

/** A table's columns as `PRAGMA table_info` reports them; [] when it is absent. */
async function existingColumns(
   db: DuckDBConnection,
   table: string,
): Promise<{ name: string; pk: boolean }[]> {
   const present = await db.all<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
      [table],
   );
   if (present.length === 0) {
      return [];
   }
   return db.all<{ name: string; pk: boolean }>(
      `PRAGMA table_info('${table}')`,
   );
}

/**
 * Drop an `incremental_ledger` still keyed on (environment, package, source
 * address), so the CREATE pass recreates it keyed on the table.
 *
 * DuckDB cannot re-key in place and there is no migration framework. Dropping is
 * safe for this table only: the ledger caches a boundary whose miss path is a
 * seed, so losing it costs one full rebuild per incremental source.
 */
async function dropPackageKeyedIncrementalLedger(
   db: DuckDBConnection,
): Promise<void> {
   const columns = await existingColumns(db, "incremental_ledger");
   if (columns.length === 0) {
      return;
   }
   // `pk` is true for every column of a composite key. Reading the key rather
   // than a schema version makes this self-limiting.
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
 * Drop an `incremental_ledger` whose primary key lacks the storage destination.
 * Same reasoning and cost as {@link dropPackageKeyedIncrementalLedger}.
 *
 * A row-preserving migration (rename, recreate, copy) is deliberately not used:
 * it saves one rebuild but risks a half-copied ledger and a stranded `_old`
 * table during schema initialization.
 */
async function dropStoreBlindIncrementalLedger(
   db: DuckDBConnection,
): Promise<void> {
   const columns = await existingColumns(db, "incremental_ledger");
   if (columns.length === 0) {
      return;
   }
   // The old key lacks the destination: either no such column, or the earlier
   // nullable non-key form.
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
 * Drop a pre-`facet` embedding cache so the faceted key can be created. Same
 * pattern as dropPackageKeyedIncrementalLedger; wiping this vector cache only
 * costs re-embedding.
 */
async function dropPreFacetEntityEmbeddings(
   db: DuckDBConnection,
): Promise<void> {
   const columns = await existingColumns(db, "entity_embeddings");
   if (columns.length === 0) {
      return;
   }
   if (columns.some((column) => column.name === "facet")) {
      return;
   }
   logger.info(
      "Re-keying the semantic entity cache onto (…, facet) so an entity's name " +
         "and documentation are embedded separately; each package re-embeds " +
         "once on its next getContext call",
   );
   await db.run("DROP TABLE IF EXISTS entity_embeddings");
}

/**
 * Vector cache for semantic `get_context` retrieval (see
 * mcp/tools/embedding_index.ts). One row per (entity, facet): an entity's
 * name is embedded separately from its documentation, and long docs are
 * split across `doc:N` facets, so a hit is scored on its best-matching
 * facet rather than on one averaged vector. That is what stops a long doc
 * from burying the entity's own name — see embedding_index.entityFacets.
 *
 * Rows are content-addressed per facet (`content_hash` plus
 * `embedding_model`), so editing a doc re-embeds only the doc facets; a
 * dimensionality change is healed at query time, and wiping the table only
 * costs re-embedding. `embedding` is searched by brute-force
 * list_cosine_similarity, which at one Publisher's entity counts beats a
 * vector-index extension.
 */
export async function createEntityEmbeddingsTable(
   db: DuckDBConnection,
): Promise<void> {
   await dropPreFacetEntityEmbeddings(db);
   await db.run(`
    CREATE TABLE IF NOT EXISTS entity_embeddings (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      entity_kind VARCHAR NOT NULL,
      entity_source VARCHAR NOT NULL,
      entity_name VARCHAR NOT NULL,
      facet VARCHAR NOT NULL,
      model_path VARCHAR NOT NULL,
      content_hash VARCHAR NOT NULL,
      embedding_model VARCHAR NOT NULL,
      dims INTEGER NOT NULL,
      embedding FLOAT[] NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      PRIMARY KEY (environment_name, package_name, entity_kind, entity_source, entity_name, facet)
    )
  `);
}

/**
 * LLM-written search phrases for entities (see mcp/tools/keyphrases.ts). One
 * row per entity. A row is reused only while all three of `input_hash` (the
 * fields sent to the model), `prompt_hash` (the instructions in force) and
 * `model` still match, so a keyphrase is regenerated exactly when its inputs,
 * the prompt or the model changed, and an unchanged entity costs no LLM call
 * on a restart, a reload or a republish.
 *
 * `entity_key` is the JSON array [kind, source, name], which cannot collide
 * whatever characters an identifier contains. Losing the table only costs
 * regenerating.
 */
export async function createEntityKeyphrasesTable(
   db: DuckDBConnection,
): Promise<void> {
   await db.run(`
    CREATE TABLE IF NOT EXISTS entity_keyphrases (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      entity_key VARCHAR NOT NULL,
      input_hash VARCHAR NOT NULL,
      prompt_hash VARCHAR NOT NULL,
      model VARCHAR NOT NULL,
      keyphrase VARCHAR NOT NULL,
      created_at TIMESTAMP NOT NULL,
      PRIMARY KEY (environment_name, package_name, entity_key)
    )
  `);
}

/**
 * LLM-written source summaries (see mcp/tools/source_summaries.ts). One row per
 * (model path, source). `input_hash` covers everything the model was shown
 * (the rendered source and fields), the prompt text and the model, so a row is
 * reused only while none of those changed, and an unchanged source costs no LLM
 * call on a restart, a reload or a republish. Losing the table only costs
 * regenerating.
 */
export async function createSourceSummariesTable(
   db: DuckDBConnection,
): Promise<void> {
   await db.run(`
    CREATE TABLE IF NOT EXISTS source_summaries (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      model_path VARCHAR NOT NULL,
      source_name VARCHAR NOT NULL,
      input_hash VARCHAR NOT NULL,
      model VARCHAR NOT NULL,
      summary VARCHAR NOT NULL,
      one_line_summary VARCHAR NOT NULL,
      created_at TIMESTAMP NOT NULL,
      PRIMARY KEY (environment_name, package_name, model_path, source_name)
    )
  `);
}

/**
 * The find, connect, judge engine's catalog (see mcp/context/catalog.ts): one
 * row per node and per edge of a package, rewritten whole when the package
 * loads. The engine searches its in-memory copy; these rows are what an
 * operator reads to see what the catalog holds.
 *
 * Additive: new tables, nothing existing is altered. Losing them costs only a
 * rebuild on the next load.
 */
export async function createCatalogTables(db: DuckDBConnection): Promise<void> {
   await db.run(`
    CREATE TABLE IF NOT EXISTS catalog_nodes (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      node_key VARCHAR NOT NULL,
      entity_id VARCHAR NOT NULL,
      kind VARCHAR NOT NULL,
      source_key VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      model_paths VARCHAR NOT NULL,
      derived_text VARCHAR NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      PRIMARY KEY (environment_name, package_name, node_key)
    )
  `);
   await db.run(`
    CREATE TABLE IF NOT EXISTS catalog_edges (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      kind VARCHAR NOT NULL,
      from_key VARCHAR NOT NULL,
      to_key VARCHAR NOT NULL,
      path VARCHAR NOT NULL,
      PRIMARY KEY (environment_name, package_name, kind, from_key, to_key, path)
    )
  `);
   // Index-time LLM output of the same engine (mcp/context/llm_cache.ts): a
   // doc split into parts, and the questions written for a node. A row is
   // reused only while its input, prompt and model hashes all match.
   for (const table of ["doc_parts", "node_questions"]) {
      await db.run(`
    CREATE TABLE IF NOT EXISTS ${table} (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      item_key VARCHAR NOT NULL,
      input_hash VARCHAR NOT NULL,
      prompt_hash VARCHAR NOT NULL,
      model VARCHAR NOT NULL,
      output VARCHAR NOT NULL,
      created_at TIMESTAMP NOT NULL,
      PRIMARY KEY (environment_name, package_name, item_key)
    )
  `);
   }
   // The engine's embedding rows: one per (node, facet), with the hash of the
   // text that was embedded, so a sync re-embeds only what changed. Kept apart
   // from entity_embeddings, whose sync deletes any row it did not write.
   await db.run(`
    CREATE TABLE IF NOT EXISTS catalog_embeddings (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      node_key VARCHAR NOT NULL,
      facet VARCHAR NOT NULL,
      content_hash VARCHAR NOT NULL,
      embedding_model VARCHAR NOT NULL,
      dims INTEGER NOT NULL,
      embedding FLOAT[] NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      PRIMARY KEY (environment_name, package_name, node_key, facet)
    )
  `);
}

// Tables in the pre-rename `projects` schema, children first so DROP order
// satisfies foreign keys. Removable once no store predates the rename.
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
      "entity_keyphrases",
      "source_summaries",
      "catalog_nodes",
      "catalog_edges",
      "doc_parts",
      "node_questions",
      "catalog_embeddings",
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
