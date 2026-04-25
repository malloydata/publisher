/**
 * Minimal identifier-quoting surface. Every `Dialect` in `@malloydata/malloy`
 * implements this; we accept the duck type so tests can inject a fake without
 * instantiating a full dialect.
 */
export interface Quoter {
   quoteTablePath(seg: string): string;
}

/**
 * Quote a potentially schema-qualified table path (e.g. "schema.table")
 * by quoting each segment individually with the dialect's quoteTablePath.
 */
export function quoteTablePath(path: string, dialect: Quoter): string {
   return path
      .split(".")
      .map((seg) => dialect.quoteTablePath(seg))
      .join(".");
}

/**
 * Split a possibly schema-qualified table name into its schema prefix
 * (including the trailing dot) and the bare table name.
 *
 * Examples:
 *   "my_schema.my_table" -> { schemaPrefix: "my_schema.", bareName: "my_table" }
 *   "my_table"           -> { schemaPrefix: "", bareName: "my_table" }
 */
export function splitTablePath(tableName: string): {
   schemaPrefix: string;
   bareName: string;
} {
   const lastDot = tableName.lastIndexOf(".");
   if (lastDot >= 0) {
      return {
         schemaPrefix: tableName.substring(0, lastDot + 1),
         bareName: tableName.substring(lastDot + 1),
      };
   }
   return { schemaPrefix: "", bareName: tableName };
}
