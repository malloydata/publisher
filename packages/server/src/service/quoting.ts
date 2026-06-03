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
