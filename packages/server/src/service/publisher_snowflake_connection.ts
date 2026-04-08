import { SnowflakeConnection } from "@malloydata/db-snowflake";
import {
   SnowflakeDialect,
   makeDigest,
   mkFieldDef,
   sqlKey,
   type SQLSourceDef,
   type SQLSourceRequest,
   type StructDef,
   type TableSourceDef,
} from "@malloydata/malloy";

type SnowflakeConnectionOptions = NonNullable<
   ConstructorParameters<typeof SnowflakeConnection>[1]
>;

/** Pinned to @malloydata/db-snowflake@0.0.370 private field layout. */
type SnowflakeInternals = {
   executor: { batch: (sql: string) => Promise<Record<string, unknown>[]> };
   connOptions: { database?: string };
};

/** Unquoted Snowflake identifier segment (database / schema / table). */
const SAFE_SNOWFLAKE_IDENT = /^[A-Za-z_][A-Za-z0-9_$]*$/;

/** SQL single-quoted literal for dynamic values in Snowflake (not identifiers). */
export function snowflakeSqlStringLiteral(value: string): string {
   return `'${value.replace(/'/g, "''")}'`;
}

/**
 * Parse `db.schema.table`, or `schema.table` when `defaultDatabase` is set.
 * Returns undefined for quoted paths, wrong arity, or unsafe segments.
 */
export function parseSnowflakeDbSchemaTable(
   tablePath: string,
   defaultDatabase?: string | null,
): { database: string; schema: string; table: string } | undefined {
   const parts = tablePath.split(".").filter((p) => p.length > 0);
   let database: string;
   let schema: string;
   let table: string;
   if (parts.length === 3) {
      [database, schema, table] = parts;
   } else if (parts.length === 2 && defaultDatabase) {
      database = defaultDatabase;
      [schema, table] = parts;
   } else {
      return undefined;
   }
   if (![database, schema, table].every((p) => SAFE_SNOWFLAKE_IDENT.test(p))) {
      return undefined;
   }
   return { database, schema, table };
}

type SnowflakeTableLocation = {
   database: string;
   schema: string;
   table: string;
};

function rowString(row: Record<string, unknown>, candidates: string[]): string {
   for (const key of candidates) {
      const direct =
         row[key] ?? row[key.toUpperCase()] ?? row[key.toLowerCase()];
      if (direct != null && String(direct).trim() !== "") {
         return String(direct).trim();
      }
   }
   return "";
}

/**
 * Publisher Snowflake driver: skips VARIANT / ARRAY / OBJECT row sampling; prefers
 * `DATABASE.INFORMATION_SCHEMA.COLUMNS` (including session-qualified temp views)
 * instead of `DESCRIBE TABLE` when resolution succeeds.
 */
export class PublisherSnowflakeConnection extends SnowflakeConnection {
   private readonly publisherDialect = new SnowflakeDialect();
   private sessionLocationPromise?: Promise<
      { database: string; schema: string } | undefined
   >;

   constructor(name: string, options?: SnowflakeConnectionOptions) {
      super(name, options);
      const i = this.internals();
      if (typeof i.executor?.batch !== "function") {
         throw new Error(
            "PublisherSnowflakeConnection: internals cast broken — executor.batch " +
               "is not a function. Upstream @malloydata/db-snowflake may have changed (pinned: 0.0.370).",
         );
      }
   }

   override async fetchTableSchema(
      tableKey: string,
      tablePath: string,
   ): Promise<TableSourceDef> {
      const structDef: TableSourceDef = {
         type: "table",
         dialect: "snowflake",
         name: tableKey,
         tablePath,
         connection: this.name,
         fields: [],
      };
      await this.describeWithoutVariantSampling(tablePath, structDef);
      return structDef;
   }

   override async fetchSelectSchema(
      sqlRef: SQLSourceRequest,
   ): Promise<SQLSourceDef> {
      const structDef: SQLSourceDef = {
         type: "sql_select",
         ...sqlRef,
         dialect: this.dialectName,
         fields: [],
         name: sqlKey(sqlRef.connection, sqlRef.selectStr),
      };
      const tempTableName = this.makeTempViewName(sqlRef.selectStr);
      await this.runSQL(
         `CREATE OR REPLACE TEMP VIEW ${tempTableName} AS (${sqlRef.selectStr});`,
      );
      await this.describeWithoutVariantSampling(tempTableName, structDef);
      return structDef;
   }

   private internals(): SnowflakeInternals {
      return this as unknown as SnowflakeInternals;
   }

   private makeTempViewName(sqlCommand: string): string {
      const hash = makeDigest(sqlCommand);
      return `tt${hash.slice(0, this.publisherDialect.maxIdentifierLength - 2)}`;
   }

   /**
    * Resolve where to read `INFORMATION_SCHEMA.COLUMNS` for this object name.
    * Caches the session database/schema query to avoid repeated round-trips.
    */
   private async resolveTableLocation(
      tablePath: string,
   ): Promise<SnowflakeTableLocation | undefined> {
      const { connOptions } = this.internals();
      const defaultDb = connOptions.database ?? null;

      const qualified = parseSnowflakeDbSchemaTable(tablePath, defaultDb);
      if (qualified) {
         return qualified;
      }

      const trimmed = tablePath.trim();
      if (!trimmed || !SAFE_SNOWFLAKE_IDENT.test(trimmed)) {
         return undefined;
      }

      const session = await this.getSessionLocation();
      let database = session?.database ?? "";
      const schema = session?.schema ?? "";
      if (!database && defaultDb) {
         database = defaultDb;
      }
      if (!database || !schema) {
         return undefined;
      }
      if (
         !SAFE_SNOWFLAKE_IDENT.test(database) ||
         !SAFE_SNOWFLAKE_IDENT.test(schema)
      ) {
         return undefined;
      }
      return { database, schema, table: trimmed };
   }

   private getSessionLocation(): Promise<
      { database: string; schema: string } | undefined
   > {
      if (!this.sessionLocationPromise) {
         this.sessionLocationPromise = (async () => {
            const { executor } = this.internals();
            const sessionRows = await executor.batch(
               "SELECT CURRENT_DATABASE() AS DB_NAME, CURRENT_SCHEMA() AS SCHEMA_NAME",
            );
            const sessionRow = (sessionRows[0] ?? {}) as Record<
               string,
               unknown
            >;
            const database = rowString(sessionRow, ["DB_NAME", "db_name"]);
            const schema = rowString(sessionRow, [
               "SCHEMA_NAME",
               "schema_name",
            ]);
            if (!database || !schema) return undefined;
            return { database, schema };
         })();
      }
      return this.sessionLocationPromise;
   }

   /**
    * VARIANT / ARRAY / OBJECT → Malloy `string`. No flatten/sample query.
    * Uses INFORMATION_SCHEMA when we can resolve catalog/schema/table; only then falls
    * back to `DESCRIBE TABLE` (e.g. quoted or exotic identifiers).
    */
   private async describeWithoutVariantSampling(
      tablePath: string,
      structDef: StructDef,
   ): Promise<void> {
      const loc = await this.resolveTableLocation(tablePath);

      const { executor } = this.internals();
      const rows = loc
         ? await this.columnsFromInformationSchema(executor, loc)
         : await executor.batch(`DESCRIBE TABLE ${tablePath}`);

      if (loc && rows.length === 0) {
         throw new Error(
            `No columns found for '${tablePath}' in INFORMATION_SCHEMA. ` +
               `Table may not exist or session lacks access.`,
         );
      }

      for (const row of rows) {
         const name = String(
            row["COLUMN_NAME"] ?? row["column_name"] ?? row["name"] ?? "",
         );
         if (!name) {
            continue;
         }

         const fullType = loc
            ? informationSchemaRowToDialectTypeString(row)
            : String(row["type"] ?? row["TYPE"] ?? "").toLowerCase();
         const baseType = fullType.split("(")[0].toLowerCase();

         if (["variant", "array", "object"].includes(baseType)) {
            structDef.fields.push(mkFieldDef({ type: "string" }, name));
         } else {
            const typeForMapping = [
               "number",
               "numeric",
               "decimal",
               "dec",
            ].includes(baseType)
               ? fullType
               : baseType;
            const malloyType =
               this.publisherDialect.sqlTypeToMalloyType(typeForMapping);
            structDef.fields.push(mkFieldDef(malloyType, name));
         }
      }
   }

   private async columnsFromInformationSchema(
      executor: SnowflakeInternals["executor"],
      loc: SnowflakeTableLocation,
   ): Promise<Record<string, unknown>[]> {
      const catalog = loc.database.toUpperCase();
      const schemaUpper = loc.schema.toUpperCase();
      const tableUpper = loc.table.toUpperCase();
      const sql = `
SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
FROM ${loc.database}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = ${snowflakeSqlStringLiteral(catalog)}
  AND TABLE_SCHEMA = ${snowflakeSqlStringLiteral(schemaUpper)}
  AND TABLE_NAME = ${snowflakeSqlStringLiteral(tableUpper)}
ORDER BY ORDINAL_POSITION`.trim();
      return executor.batch(sql);
   }
}

/** Type string for {@link SnowflakeDialect.sqlTypeToMalloyType} (incl. NUMBER scale). */
export function informationSchemaRowToDialectTypeString(
   row: Record<string, unknown>,
): string {
   const dtRaw = row["DATA_TYPE"] ?? row["data_type"];
   const dt = String(dtRaw ?? "")
      .trim()
      .toLowerCase();
   if (!dt) {
      return "varchar";
   }

   const precRaw = row["NUMERIC_PRECISION"] ?? row["numeric_precision"];
   const scaleRaw = row["NUMERIC_SCALE"] ?? row["numeric_scale"];
   const prec = optionalNumeric(precRaw);
   const scale = optionalNumeric(scaleRaw);

   const numericFamily = ["number", "numeric", "decimal", "dec"];
   const base = dt.split("(")[0]!.toLowerCase();
   if (
      numericFamily.includes(base) &&
      Number.isFinite(prec) &&
      Number.isFinite(scale)
   ) {
      return `${base}(${prec},${scale})`;
   }
   if (numericFamily.includes(base) && Number.isFinite(prec)) {
      return `${base}(${prec})`;
   }
   return dt;
}

function optionalNumeric(value: unknown): number {
   if (value == null || value === "") {
      return NaN;
   }
   const n = Number(value as string | number | bigint);
   return Number.isFinite(n) ? n : NaN;
}
