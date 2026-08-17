import { components } from "../api";

/**
 * Generic database interface for storage abstraction
 * This allows switching between DuckDB, PostgreSQL, MySQL, etc.
 */
export interface DatabaseConnection {
   initialize(): Promise<void>;
   close(): Promise<void>;
   isInitialized(): Promise<boolean>;
}

export interface ResourceRepository {
   // Environments
   listEnvironments(): Promise<Environment[]>;
   getEnvironmentById(id: string): Promise<Environment | null>;
   getEnvironmentByName(name: string): Promise<Environment | null>;
   createEnvironment(
      environment: Omit<Environment, "id" | "createdAt" | "updatedAt">,
   ): Promise<Environment>;
   updateEnvironment(
      id: string,
      updates: Partial<Environment>,
   ): Promise<Environment>;
   deleteEnvironment(id: string): Promise<void>;

   // Packages
   listPackages(environmentId: string): Promise<Package[]>;
   getPackageById(id: string): Promise<Package | null>;
   getPackageByName(
      environmentId: string,
      name: string,
   ): Promise<Package | null>;
   createPackage(
      pkg: Omit<Package, "id" | "createdAt" | "updatedAt">,
   ): Promise<Package>;
   updatePackage(id: string, updates: Partial<Package>): Promise<Package>;
   deletePackage(id: string): Promise<void>;

   // Connections
   listConnections(environmentId: string): Promise<Connection[]>;
   getConnectionById(id: string): Promise<Connection | null>;
   getConnectionByName(
      environmentId: string,
      name: string,
   ): Promise<Connection | null>;
   createConnection(
      connection: Omit<Connection, "id" | "createdAt" | "updatedAt">,
   ): Promise<Connection>;
   updateConnection(
      id: string,
      updates: Partial<Connection>,
   ): Promise<Connection>;
   deleteConnection(id: string): Promise<void>;

   // Storage destinations
   listStorageDestinations(
      environmentId: string,
   ): Promise<StorageDestination[]>;
   getStorageDestinationByName(
      environmentId: string,
      name: string,
   ): Promise<StorageDestination | null>;
   upsertStorageDestination(
      destination: Omit<StorageDestination, "id" | "createdAt" | "updatedAt">,
   ): Promise<StorageDestination>;
   deleteStorageDestination(id: string): Promise<void>;
   deleteStorageDestinationsByEnvironmentId(id: string): Promise<void>;

   // Materializations
   listMaterializations(
      environmentId: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]>;
   listMaterializationsByEnvironment(
      environmentId: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]>;
   getLatestScheduledFireAt(
      environmentId: string,
      packageName: string,
   ): Promise<Date | null>;
   getMaterializationById(id: string): Promise<Materialization | null>;
   getActiveMaterialization(
      environmentId: string,
      packageName: string,
   ): Promise<Materialization | null>;
   createMaterialization(
      environmentId: string,
      packageName: string,
      status?: MaterializationStatus,
      metadata?: Record<string, unknown> | null,
   ): Promise<Materialization>;
   updateMaterialization(
      id: string,
      updates: MaterializationUpdate,
   ): Promise<Materialization>;
   deleteMaterialization(id: string): Promise<void>;

   // Incremental ledger
   getIncrementalLedgerEntry(
      environmentId: string,
      connectionName: string,
      physicalTableName: string,
   ): Promise<IncrementalLedgerEntry | null>;
   upsertIncrementalLedgerEntry(
      entry: Omit<IncrementalLedgerEntry, "createdAt" | "advancedAt">,
   ): Promise<IncrementalLedgerEntry>;
   deleteIncrementalLedgerEntry(
      environmentId: string,
      connectionName: string,
      physicalTableName: string,
   ): Promise<void>;
}

export interface Environment {
   id: string;
   name: string;
   path: string;
   description?: string;
   createdAt: Date;
   updatedAt: Date;
   metadata?: Record<string, unknown>;
}

export interface Package {
   id: string;
   environmentId: string;
   name: string;
   description?: string;
   manifestPath: string;
   createdAt: Date;
   updatedAt: Date;
   metadata?: Record<string, unknown>;
}

export interface Connection {
   id: string;
   environmentId: string;
   name: string;
   type: "bigquery" | "postgres" | "duckdb" | "mysql" | "snowflake" | "trino";
   config: Record<string, unknown>;
   createdAt: Date;
   updatedAt: Date;
}

/**
 * A warehouse materialization builds write to and materialized queries are
 * served from. Stored apart from {@link Connection} because it is not one: it is
 * never resolvable by name from a model, and its name lives in its own namespace
 * (so a row here and a connection row may share a name and mean two different
 * warehouses). `type` is a plain string rather than a union because the set of
 * warehouses a destination may be is enforced where destinations are validated,
 * not by the store.
 */
export interface StorageDestination {
   id: string;
   environmentId: string;
   name: string;
   type: string;
   config: Record<string, unknown>;
   createdAt: Date;
   updatedAt: Date;
}

// Wire types for the build protocol, kept in sync with the OpenAPI spec via
// the generated `api.ts`.
export type MaterializationStatus =
   components["schemas"]["MaterializationStatus"];
export type BuildPlan = components["schemas"]["BuildPlan"];
export type BuildManifestResult = components["schemas"]["BuildManifest"];
export type ManifestEntry = components["schemas"]["ManifestEntry"];
export type BuildInstruction = components["schemas"]["BuildInstruction"];
export type ManifestReference = components["schemas"]["ManifestReference"];
export type Realization = components["schemas"]["Realization"];
/**
 * One incremental source's `covered_through` boundary as it travels on the
 * wire: reported on `ManifestEntry.ledger`, and returned verbatim in
 * `BuildInstructions.ledger` by a caller that owns the ledger. The store-row
 * counterpart is {@link IncrementalLedgerEntry}.
 */
export type LedgerEntry = components["schemas"]["LedgerEntry"];

export interface Materialization {
   id: string;
   environmentId: string;
   packageName: string;
   status: MaterializationStatus;
   /** Build output. Null until status = MANIFEST_FILE_READY. */
   manifest: BuildManifestResult | null;
   startedAt: Date | null;
   completedAt: Date | null;
   error: string | null;
   metadata: Record<string, unknown> | null;
   createdAt: Date;
   updatedAt: Date;
}

/**
 * The durable boundary of one incrementally-refreshed source lineage: the
 * EXCLUSIVE upper end of the watermark range its serving table is known to
 * contain. A delta run starts exactly here and advances it only after its DML
 * commits, so a crash in between costs a repeated (idempotent) range rather than
 * a gap.
 *
 * The declarations are recorded, not merely described. A change to `watermark=`
 * or `merge_key=` changes the DML the publisher would issue WITHOUT moving the
 * source's content address, so the address cannot detect it; comparing these
 * against the plan's current declaration is what forces a re-seed instead of a
 * delta computed under one set of rules being applied under another.
 */
export interface IncrementalLedgerEntry {
   environmentId: string;
   /** Whoever last advanced this boundary. Recorded, not part of the key. */
   packageName: string;
   /**
    * The source's content address, as of the build that last advanced this
    * boundary. Recorded, not part of the key — and therefore COMPARED on read
    * (see ledgerLineageMismatch), because a table whose definition changed keeps
    * its name in a standalone publisher and a delta must not be applied across
    * that change.
    */
   sourceEntityId: string;
   /**
    * Canonical scalar text of the boundary (ISO-8601 for temporal types,
    * decimal for numbers, the value itself for strings) — NOT a pre-rendered
    * literal, because a boundary outlives any one statement's spelling of it and
    * has to be COMPARED to the next run's frontier, not just pasted into SQL.
    */
   coveredThroughValue: string;
   /** The watermark's Malloy type, which decides how the value is rendered. */
   coveredThroughType: string;
   /** The `watermark=` dimension the boundary was computed over. */
   watermarkDimension: string;
   /** The `merge_key=` dimensions, in declared order; empty for range replace. */
   mergeKeyDimensions: string[];
   derivedStrategy: IncrementalStrategy;
   /**
    * The table the boundary is a fact about. With `environmentId` and
    * `connectionName` this is the row's KEY, which is what makes a boundary
    * survive a package being presented under a different (version-qualified)
    * name between refreshes.
    */
   physicalTableName: string;
   connectionName: string;
   /** Audit: which run last advanced this boundary, and when. */
   advancedByMaterializationId: string | null;
   advancedAt: Date;
   createdAt: Date;
}

/** How a delta advances the serving table. */
export type IncrementalStrategy = "merge" | "range_replace";

/**
 * Whether a manifest entry records a source that failed to build.
 *
 * A failed entry still carries the identifying fields it was instructed with,
 * including `physicalTableName`, so presence of a table name is not evidence a
 * table exists. Anything that resolves an entry to a real table -- serve
 * bindings, reuse decisions, seeds for a downstream build -- has to exclude
 * these, or it will name a table that was never created.
 */
export function isFailedEntry(entry: Pick<ManifestEntry, "error">): boolean {
   return Boolean(entry?.error);
}

export interface MaterializationUpdate {
   status?: MaterializationStatus;
   manifest?: BuildManifestResult | null;
   startedAt?: Date;
   completedAt?: Date;
   error?: string | null;
   metadata?: Record<string, unknown> | null;
}

/**
 * Malloy-facing build manifest: maps a sourceEntityId to the physical table backing
 * that persist source. This is the shape the Malloy runtime consumes when
 * (re)loading models so persist references resolve to materialized tables.
 * Distinct from {@link BuildManifestResult} (the wire build output, which also
 * carries caller bookkeeping like materializedTableId and rowCount).
 */
export interface BuildManifestEntry {
   tableName: string;
}

export interface BuildManifest {
   entries: Record<string, BuildManifestEntry>;
   strict?: boolean;
}

/**
 * A wire manifest entry carrying the control-plane freshness fields alongside
 * the physical `tableName`. This is what the publisher retains at bind so the
 * serve path can re-evaluate `age vs window` per query (see
 * {@link ../service/freshness}). A `{ tableName }`-only value is structurally
 * assignable here (the freshness fields are optional), so callers that only
 * know a table name — e.g. the post-build auto-load — bind un-gated entries.
 *
 *  - `dataAsOf`   the artifact's data-as-of instant (snapshot start); the age
 *                 anchor. `age = now - dataAsOf`. Absent ⇒ never stale.
 *  - `freshnessWindowSeconds`  the version's effective window. Absent ⇒ un-gated.
 *  - `freshnessFallback`       per-version read-time policy when stale.
 */
export interface FreshnessAwareManifestEntry {
   tableName: string;
   /**
    * The connection the physical table lives on, carried from the wire
    * manifest so the bind step can quote `tableName` for that connection's
    * dialect (see Package.quoteBoundTableNames). Absent on entries whose
    * producer didn't record it; those bind verbatim.
    */
   connectionName?: string;
   dataAsOf?: string;
   freshnessWindowSeconds?: number;
   freshnessFallback?: "live" | "stale_ok" | "fail";
}

/** Full (unfiltered) manifest map keyed by sourceEntityId. */
export type FreshnessManifest = Record<string, FreshnessAwareManifestEntry>;
