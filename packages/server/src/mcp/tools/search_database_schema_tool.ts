import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { ConnectionController } from "../../controller/connection.controller";
import { EnvironmentStore } from "../../service/environment_store";
import { schemaEmbeddingEnabled } from "../../config";
import { getEmbeddingProvider } from "../../service/embedding_provider";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import { logger } from "../../logger";
import { RankingMode, SchemaTableEntity, rankTables } from "./schema_index";

/** Tables returned when the caller does not ask for a specific number. */
const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 100;

/**
 * Columns included per table when listing or searching a schema. A wide fact
 * table would otherwise dominate the response. Ask for a single `tableName` to
 * get all of its columns.
 */
const MAX_COLUMNS_PER_TABLE = 80;

/**
 * Length cap on the identifier-ish string arguments.
 *
 * These are echoed back into human-readable warning and error prose that an
 * agent reads as server-authored instruction, so an unbounded argument is an
 * unbounded injection surface into that prose. No real environment, connection,
 * package, schema or table name approaches this.
 */
const MAX_ARG_CHARS = 256;

const searchDatabaseSchemaShape = {
   environmentName: z
      .string()
      .max(MAX_ARG_CHARS)
      .optional()
      .describe(
         "Environment to look in. Omit to list the environments and their connections.",
      ),
   connectionName: z
      .string()
      .max(MAX_ARG_CHARS)
      .optional()
      .describe(
         "Connection to introspect. Omit to list the connections in the environment.",
      ),
   packageName: z
      .string()
      .max(MAX_ARG_CHARS)
      .optional()
      .describe(
         'Required only for the per-package "duckdb" sandbox connection, which exists once per package.',
      ),
   schemaName: z
      .string()
      .max(MAX_ARG_CHARS)
      .optional()
      .describe(
         'Schema (or dataset/database) to list tables from. Omit to list the connection\'s schemas and use one of those names verbatim: DuckDB qualifies them as "catalog.schema" (for example "memory.main"), so a bare "main" is rejected.',
      ),
   tableName: z
      .string()
      .max(MAX_ARG_CHARS)
      .optional()
      .describe(
         "A single table to return in full, with every column. Requires schemaName.",
      ),
   searchQuery: z
      .string()
      .max(500)
      .optional()
      .describe(
         'Plain-English description of the data you are looking for, e.g. "customer orders and shipping addresses". Requires schemaName. Omit to list the schema\'s tables in order.',
      ),
   limit: z
      .number()
      .int()
      .positive()
      .max(MAX_LIMIT)
      .optional()
      .describe(`Maximum tables to return. Default ${DEFAULT_LIMIT}.`),
   offset: z
      .number()
      .int()
      .min(0)
      .optional()
      .describe(
         "Tables to skip, for paging a long table listing. Pass back the nextOffset from a previous response. Ignored when searchQuery is set, because ranked results cannot be paged.",
      ),
};
type SearchDatabaseSchemaParams = z.infer<
   z.ZodObject<typeof searchDatabaseSchemaShape>
>;

/**
 * The `source:` line an agent can paste to start modelling this table.
 *
 * This is the seam between "search a warehouse's schema" and "build a model
 * from it": the response carries the exact syntax rather than leaving the agent
 * to assemble a connection name, a dotted path, and quoting rules that differ
 * per dialect. `resource` keeps its qualification exactly as introspection
 * produced it; only the quoting around it is this function's business.
 *
 * Escaping matters here because the agent is told this line is ready to use, so
 * this function's output is what actually gets run. Malloy escapes string
 * literals with a BACKSLASH, not by doubling the quote as SQL does, so a
 * warehouse path like `s3://bucket/o'brien/orders.parquet` needs `\'`. Getting
 * that wrong produces a parse error the agent was promised would not happen.
 */
function escapeMalloyString(value: string): string {
   return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/**
 * Quote an identifier, escaping backslashes and any backtick it contains.
 *
 * ALWAYS quoted, never bare. A table named `table` or `source` is a Malloy
 * reserved word, and emitting it bare produces a line that does not compile
 * from a field the description calls "the ready-to-use source line". Quoting
 * only when the name looks unusual left exactly those cases broken. Verified
 * against the compiler: a backticked alias always compiles.
 *
 * Backslash FIRST, for the same reason as escapeMalloyString: escaping the
 * backtick first would then double the backslash that escape introduced. And a
 * name ending in a backslash, left unescaped, would consume the closing
 * backtick and swallow the rest of the line.
 */
function malloyIdentifier(name: string): string {
   return `\`${name.replace(/\\/g, "\\\\").replace(/`/g, "\\`")}\``;
}

export function malloySourceSnippet(
   connectionName: string,
   resource: string,
   tableName: string,
): string {
   const alias = malloyIdentifier(tableName);
   const connection = malloyIdentifier(connectionName);
   return `source: ${alias} is ${connection}.table('${escapeMalloyString(resource)}') extend { }`;
}

/**
 * DuckDB's file-path grammar for a table path, from its dialect in
 * @malloydata/malloy. Deliberately NOT a guess: a space, an apostrophe or a
 * parenthesis is outside it, which is why an S3 key containing one produced a
 * line that did not compile.
 */
const DUCKDB_FILE_PATH = /^[A-Za-z0-9._~:/?#@!$&*+,=%-]+$/;

/**
 * The strictest bare-identifier rule across the dialects we serve, which is
 * Malloy's base `tablePathBareIdentRegex`. Individual dialects widen it (MySQL
 * allows a leading digit, BigQuery allows a hyphen), so requiring the base is
 * conservative everywhere and wrong nowhere.
 */
const STRICT_BARE_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Whether a table path can be pasted into `table('...')` as-is.
 *
 * Conservative by construction. Malloy validates this per dialect, with seven
 * distinct identifier rules plus DuckDB's separate file-path grammar, and it
 * does not export `getDialect` through any public entry point, so this cannot
 * delegate to the authority. Rather than approximate ten grammars with one
 * regex (an earlier attempt did exactly that and still emitted lines that did
 * not compile), this accepts only what every dialect accepts, and the caller
 * omits `malloySource` and says why for anything else.
 *
 * The two shapes: a dotted path, where every segment must satisfy the strictest
 * bare-identifier rule; or a file/URL path, which only DuckDB-family
 * connections ever produce, checked against DuckDB's own file-path charset.
 *
 * A false negative here costs a convenience field plus a warning explaining it.
 * A false positive costs an agent a line that does not run, from a field the
 * description calls ready to paste. The asymmetry is the whole design.
 */
export function isPastableTablePath(resource: string): boolean {
   if (!resource) return false;
   if (resource.includes("/")) return DUCKDB_FILE_PATH.test(resource);
   // A bare data-file name has no slash, so it would otherwise take the dotted
   // branch and pass: `orders.parquet` looks like schema `orders`, table
   // `parquet`, and that is exactly how DuckDB resolves it, because its
   // validator tries the identifier grammar before the file grammar. There is
   // no way to write this one as a bare path, so it is not pastable. The
   // single-file Azure describe is the producer (db_utils describeRemoteFile
   // returns the basename), and bareTableName already knew about this shape.
   if (DATA_FILE_EXTENSION.test(resource)) return false;
   return resource.split(".").every((seg) => STRICT_BARE_IDENT.test(seg));
}

/**
 * Whether this table gets a `malloySource`.
 *
 * ONE predicate, shared by the field and by the warning that explains its
 * absence. They were separate, and the field was omitted for two reasons while
 * the warning counted only one, so a table with no usable name lost the field
 * in silence: the precise thing the omission exists to avoid.
 */
export function canPasteSource(entity: {
   tableName: string;
   resource: string;
}): boolean {
   return Boolean(entity.tableName) && isPastableTablePath(entity.resource);
}

/** Data-file suffixes DuckDB addresses directly, as whole file paths. */
const DATA_FILE_EXTENSION = /\.(parquet|csv|tsv|json|jsonl|ndjson|xlsx)$/i;

/**
 * The human-facing name of a table.
 *
 * Two resource shapes reach here. Most dialects produce a dotted path, where
 * the table is the last segment (`sales.orders` -> `orders`). DuckDB over
 * cloud or local files produces a file path instead
 * (`s3://bucket/data/customers.parquet`), where splitting on the last dot would
 * name every table after its file extension. Path form is detected first.
 *
 * Exported so a spec can pin both shapes.
 */
export function bareTableName(resource: string): string {
   const lastSlash = resource.lastIndexOf("/");
   if (lastSlash >= 0) {
      return resource.slice(lastSlash + 1).replace(DATA_FILE_EXTENSION, "");
   }
   if (DATA_FILE_EXTENSION.test(resource)) {
      return resource.replace(DATA_FILE_EXTENSION, "");
   }
   return resource.slice(resource.lastIndexOf(".") + 1);
}

/** The per-package DuckDB sandbox's reserved name. */
const SANDBOX_CONNECTION = "duckdb";

interface ResponseConnection {
   name: string;
   type: string;
   /** Present only on the per-package sandbox, which needs a packageName. */
   scope?: "package";
   packages?: string[];
}

/**
 * The connections an agent can actually introspect in this environment.
 *
 * Environment-level connections come from config. The per-package `duckdb`
 * sandbox does not: `duckdb` is a reserved name that ConnectionController
 * synthesizes on demand for whichever package is named, so it appears in no
 * listing. Left out, the drill-down dead-ends at "no connections" on exactly
 * the setup the bundled examples ship with, which is the first thing a new user
 * runs. It is reported with the packages it can be used against.
 *
 * An ApiConnection carries the full config, including BigQuery service-account
 * JSON and Postgres passwords, so this projects down to name and type and the
 * object itself never reaches a response.
 */
async function listConnectionsFor(environment: {
   listApiConnections: () => { name?: string; type?: string }[];
   listPackages: () => Promise<{ name?: string }[]>;
}): Promise<ResponseConnection[]> {
   const connections: ResponseConnection[] = environment
      .listApiConnections()
      .map((c) => ({ name: c.name ?? "", type: c.type ?? "" }));

   let packages: string[] = [];
   try {
      packages = (await environment.listPackages())
         .map((p) => p.name)
         .filter((n): n is string => Boolean(n));
   } catch {
      // A package listing failure must not hide the environment's real
      // connections; the sandbox entry is simply omitted.
      packages = [];
   }
   if (packages.length > 0) {
      connections.push({
         name: SANDBOX_CONNECTION,
         type: "duckdb",
         scope: "package",
         packages,
      });
   }
   return connections;
}

interface ResponseTable {
   connectionName: string;
   schemaName: string;
   tableName: string;
   tablePath: string;
   malloySource?: string;
   columns: { name: string; type?: string }[];
   columnCount: number;
   score?: number;
}

function toResponseTable(
   entity: SchemaTableEntity,
   options: { maxColumns: number; score?: number },
): ResponseTable {
   return {
      connectionName: entity.connectionName,
      schemaName: entity.schemaName,
      tableName: entity.tableName,
      tablePath: entity.resource,
      // A directory-shaped URL with no blobs yields no usable name; emitting
      // `source: `` is ...` would be a line that cannot compile, from a field
      // documented as ready to paste.
      ...(canPasteSource(entity)
         ? {
              malloySource: malloySourceSnippet(
                 entity.connectionName,
                 entity.resource,
                 entity.tableName,
              ),
           }
         : {}),
      columns: entity.columns.slice(0, options.maxColumns),
      columnCount: entity.columns.length,
      ...(options.score !== undefined ? { score: options.score } : {}),
   };
}

const SEARCH_DATABASE_SCHEMA_DESCRIPTION = `Find the tables in a database connection, by plain-English description. Use it to model a database you have not modelled yet, or to check schema, table and column names. To search an existing model, use malloy_getContext.

## Drill down, one level at a time
Supply what you know, omit the rest. No arguments lists the environments and their connections; + connectionName lists its schemas; + schemaName lists its tables (up to ${MAX_COLUMNS_PER_TABLE} columns each; add searchQuery to rank them); + tableName returns that one table with every column.

## Contract rules
- Use connectionName, tablePath and column names exactly as returned.
- A connection with scope "package" (the "duckdb" sandbox) is per package: pass packageName too, from those it lists.\n- Schemas marked isHidden are system schemas; your tables are in the others.
- malloySource is the ready-to-use \`source:\` line; its identifiers are already quoted, so paste it as-is.
- Names and types only: no row value is returned. For a column's values, run malloy_executeQuery against a model using this connection: \`run: c.table('s.t') -> { group_by: col }\`.
- No tables for a searchQuery means nothing matched, not an empty schema. Broaden it, or list without one.
- An empty schema may still hold data: DuckDB over CSV or Parquet addresses files by path, registering none.
- Read warnings: they name anything omitted or ignored.

## Response
JSON: tables (connectionName, schemaName, tableName, tablePath, malloySource, columns, columnCount, and score on a search), plus totalAvailable and returned. A search adds matched and ranking; a listing adds nextOffset when more remain, to pass back as offset.

## Worked example
Start with no arguments and follow what it names. For connection "warehouse", schema "sales":
{ "environmentName": "examples", "connectionName": "warehouse", "schemaName": "sales", "searchQuery": "customer orders" }
Then paste that table's malloySource verbatim, e.g. source: \`orders\` is \`warehouse\`.table('sales.orders') extend { }`;

/**
 * Registers malloy_searchDatabaseSchema: natural-language search over a
 * configured connection's schema.
 *
 * Ranking is lexical (lunr/BM25) unless the operator has set BOTH
 * EMBEDDING_API_KEY and EMBEDDING_INDEX_CONNECTION_SCHEMA, because embedding a
 * schema sends the customer's table and column names to a third-party provider.
 * See config.schemaEmbeddingEnabled for why that is a second switch rather than
 * riding on the existing key.
 */
export function registerSearchDatabaseSchemaTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   const connectionController = new ConnectionController(environmentStore);

   /**
    * Shed load before querying the warehouse, the way getModelForQuery does for
    * the other MCP tools.
    *
    * Every tier that reaches the warehouse calls this, not just the ranked one:
    * introspection can pull 100k rows, and tier 5 waives the column cap and can
    * fan out one DESCRIBE per file, so it is the most expensive of the three.
    * Without it the memory governor cannot see any of this traffic.
    */
   /**
    * The warning for tables whose path could not be pasted, or [] if all could.
    *
    * A helper rather than an inline push because it was previously built only
    * in the listing branch, so the single-table tier omitted `malloySource`
    * with no explanation at all: the same silence the omission exists to avoid.
    */
   const unpastableWarning = (entities: SchemaTableEntity[]): string[] => {
      const n = entities.filter((e) => !isPastableTablePath(e.resource)).length;
      if (n === 0) return [];
      return [
         `${n} table(s) have a path this server will not vouch for across every dialect it serves, so malloySource is omitted for them rather than risk a line that does not compile. Build it from tablePath: on your own dialect the path may work as-is, or may need the offending segment quoted. Double quotes on DuckDB, Postgres, Snowflake and Trino; backticks on MySQL, BigQuery and Databricks.`,
      ];
   };

   const assertCanAdmit = async (environmentName: string) => {
      (
         await environmentStore.getEnvironment(environmentName, false)
      ).assertCanAdmitQuery();
   };

   mcpServer.tool(
      "malloy_searchDatabaseSchema",
      SEARCH_DATABASE_SCHEMA_DESCRIPTION,
      searchDatabaseSchemaShape,
      async (params: SearchDatabaseSchemaParams) => {
         const {
            environmentName,
            connectionName,
            packageName,
            schemaName,
            tableName,
            searchQuery,
            limit,
            offset,
         } = params;
         const max = limit ?? DEFAULT_LIMIT;
         const skip = offset ?? 0;
         const uri = buildMalloyUri(
            {
               environment: environmentName,
               package: packageName,
            },
            "search-database-schema",
         );

         logger.info("[MCP Tool searchDatabaseSchema] Searching schema", {
            environmentName,
            connectionName,
            packageName,
            schemaName,
            tableName,
            searchQuery,
            limit,
            offset,
         });

         try {
            // Tier 1: no environment -> what environments exist, and what
            // Each tier is reached by what is MISSING, so an argument below the
            // tier that was reached does nothing. Silently, the response looks
            // like a successful answer to the question actually asked: ask for
            // tableName without schemaName and you get the schema list back,
            // from which an agent reasonably concludes the table is not there.
            // Naming what was ignored makes each of those self-describing.
            const ignored: string[] = [];
            const noteIgnored = (
               entries: readonly (readonly [string, unknown])[],
            ) => {
               for (const [name, value] of entries) {
                  if (value !== undefined && value !== "") ignored.push(name);
               }
            };
            // Paging arguments only mean something on a table listing, so they
            // are ignored by every other tier and by a ranked search.
            const pagingArgs = [
               ["limit", limit],
               ["offset", offset],
            ] as const;
            if (!environmentName) {
               noteIgnored([
                  ["connectionName", connectionName],
                  ["packageName", packageName],
                  ["schemaName", schemaName],
                  ["tableName", tableName],
                  ["searchQuery", searchQuery],
                  ...pagingArgs,
               ]);
            } else if (!connectionName) {
               noteIgnored([
                  ["packageName", packageName],
                  ["schemaName", schemaName],
                  ["tableName", tableName],
                  ["searchQuery", searchQuery],
                  ...pagingArgs,
               ]);
            } else if (!schemaName) {
               noteIgnored([
                  ["tableName", tableName],
                  ["searchQuery", searchQuery],
                  ...pagingArgs,
               ]);
            } else if (tableName) {
               // Tier 5 wins over tier 4; say so rather than dropping the rest.
               noteIgnored([["searchQuery", searchQuery], ...pagingArgs]);
            } else if (searchQuery !== undefined) {
               // Ranked results cannot be paged, so offset does nothing here.
               // Without this the tool silently returns the same top hits for
               // every offset, and an agent paging a ranked result loops.
               noteIgnored([["offset", offset]]);
            }
            // packageName only selects anything for the per-package sandbox.
            // On any other connection the controller drops it, so an agent that
            // passed it believes it scoped the search to that package and did
            // not. This is the last ignored argument that went unreported.
            if (connectionName && connectionName !== SANDBOX_CONNECTION) {
               noteIgnored([["packageName", packageName]]);
            }
            const ignoredWarning =
               ignored.length > 0
                  ? [
                       `Ignored ${ignored.join(", ")}. Each level needs the one above it: environmentName, then connectionName (with packageName for the per-package "duckdb" sandbox), then schemaName, then either tableName or searchQuery. limit and offset apply only to a plain table listing, and offset does nothing on a ranked search because ranked results cannot be paged.`,
                    ]
                  : [];

            // Tier 1: no environment -> what environments exist, and what
            // connections each has, so an agent with no prior knowledge starts here.
            if (!environmentName) {
               const environments = await environmentStore.listEnvironments();
               const results = await Promise.all(
                  environments.map(async (env) => {
                     const name = env.name ?? "";
                     let connections: ResponseConnection[] = [];
                     try {
                        const environment =
                           await environmentStore.getEnvironment(name, false);
                        connections = await listConnectionsFor(environment);
                     } catch (error) {
                        logger.debug(
                           "[MCP Tool searchDatabaseSchema] Could not list connections",
                           {
                              environmentName: name,
                              error:
                                 error instanceof Error
                                    ? error.message
                                    : String(error),
                           },
                        );
                     }
                     return { name, connections };
                  }),
               );
               return jsonResource(uri, {
                  environments: results,
                  ...(ignoredWarning.length > 0
                     ? { warnings: ignoredWarning }
                     : {}),
               });
            }

            // Tier 2: environment only -> its connections.
            if (!connectionName) {
               const environment = await environmentStore.getEnvironment(
                  environmentName,
                  false,
               );
               const connections = await listConnectionsFor(environment);
               return jsonResource(uri, {
                  environmentName,
                  connections,
                  ...(ignoredWarning.length > 0
                     ? { warnings: ignoredWarning }
                     : {}),
               });
            }

            // Tier 3: connection only -> its schemas. Shed load first: this
            // queries the warehouse too, and on DuckDB it can list cloud
            // directories.
            if (!schemaName) {
               await assertCanAdmit(environmentName);
               const schemas = await connectionController.listSchemas(
                  environmentName,
                  connectionName,
                  packageName,
               );
               return jsonResource(uri, {
                  environmentName,
                  connectionName,
                  schemas: schemas.map((s) => ({
                     name: s.name ?? "",
                     isDefault: s.isDefault ?? false,
                     isHidden: s.isHidden ?? false,
                  })),
                  ...(ignoredWarning.length > 0
                     ? { warnings: ignoredWarning }
                     : {}),
               });
            }

            // Tier 5 (checked before 4): one named table, in full. The most
            // expensive tier, since it waives the column cap and can fan out one
            // DESCRIBE per file on the cloud branches.
            if (tableName) {
               await assertCanAdmit(environmentName);
               const tables = await connectionController.listTables(
                  environmentName,
                  connectionName,
                  schemaName,
                  [tableName],
                  packageName,
               );
               if (tables.length === 0) {
                  return jsonToolError(
                     uri,
                     {
                        message: `Table "${tableName}" not found in schema "${schemaName}" of connection "${connectionName}".`,
                        suggestions: [
                           `List the schema's tables by calling this tool with schemaName "${schemaName}" and no tableName.`,
                           "Check the table name's spelling and case; some warehouses are case-sensitive.",
                        ],
                     },
                     { tables: [] },
                  );
               }
               // Several dialects ignore the tableNames filter entirely: the
               // DuckDB cloud and Azure branches list a whole directory. Without
               // this, asking for one table on a 200-file prefix returns all 200,
               // each with every column, because the branch below waives the
               // column cap on the assumption that one table was requested.
               const matching = tables.filter(
                  (t) => bareTableName(t.resource ?? "") === tableName,
               );
               const entities = (matching.length > 0 ? matching : tables).map(
                  (t) => toEntity(t, connectionName, schemaName),
               );
               if (matching.length === 0) {
                  return jsonToolError(
                     uri,
                     {
                        message: `Table "${tableName}" not found in schema "${schemaName}" of connection "${connectionName}".`,
                        suggestions: [
                           `This schema lists ${tables.length} table(s). Call this tool with schemaName "${schemaName}" and no tableName to see them.`,
                           "Check the table name's spelling and case; some warehouses are case-sensitive.",
                        ],
                     },
                     { tables: [] },
                  );
               }
               return jsonResource(uri, {
                  environmentName,
                  connectionName,
                  schemaName,
                  // No column cap here: asking for one table is how you ask for
                  // all of its columns.
                  tables: entities.map((e) =>
                     toResponseTable(e, {
                        maxColumns: Number.MAX_SAFE_INTEGER,
                     }),
                  ),
                  totalAvailable: entities.length,
                  returned: entities.length,
                  ...(() => {
                     const w = [
                        ...ignoredWarning,
                        ...unpastableWarning(entities),
                     ];
                     return w.length > 0 ? { warnings: w } : {};
                  })(),
               });
            }

            // Tier 4: a schema's tables, listed or ranked.
            await assertCanAdmit(environmentName);
            const allTables = await connectionController.listTables(
               environmentName,
               connectionName,
               schemaName,
               undefined,
               packageName,
            );
            const entities = allTables.map((t) =>
               toEntity(t, connectionName, schemaName),
            );
            const warnings: string[] = [...ignoredWarning];

            // A schema with nothing registered in it is not necessarily an
            // empty database. A DuckDB connection over local or cloud data
            // files addresses those files by path, so information_schema is
            // legitimately empty while the data is right there. Returning a
            // bare [] reads as "no data", which sends an agent down the wrong
            // path; say which of the two it is.
            if (entities.length === 0) {
               warnings.push(
                  `No tables are registered in schema "${schemaName}". If this connection reads data files directly (DuckDB over CSV or Parquet), those files are not listed in a schema; reference them by path instead, for example ${connectionName}.table('data/orders.parquet'). Otherwise check the schema name against the schema list for this connection.`,
               );
            }

            let page: { entity: SchemaTableEntity; score?: number }[];
            let ranking: RankingMode | undefined;
            let nextOffset: number | undefined;
            let matched: number | undefined;

            if (searchQuery !== undefined) {
               const provider = resolveProvider();
               const ranked = await rankTables({
                  tables: entities,
                  query: searchQuery,
                  limit: max,
                  provider,
                  // packageName is in the key ONLY for the per-package
                  // sandbox, because that is the only connection for which it
                  // changes what gets introspected: two packages each present
                  // a "duckdb" connection with a "memory.main" schema holding
                  // entirely different tables. For any other connection the
                  // controller ignores packageName, so including it would let
                  // a caller mint unlimited distinct keys for one schema, each
                  // costing a full re-embed and a retained entry.
                  cacheKey: [
                     environmentName,
                     connectionName === SANDBOX_CONNECTION
                        ? (packageName ?? "")
                        : "",
                     connectionName,
                     schemaName,
                  ].join("\x00"),
               });
               // An empty query ran no ranking at all, so reporting a mode
               // would contradict the warning that says none ran.
               ranking = ranked.emptyQuery ? undefined : ranked.ranking;
               matched = ranked.emptyQuery ? undefined : ranked.matched;
               page = ranked.hits.map((hit) => ({
                  entity: hit,
                  score: hit.score,
               }));
               // Only when there was something to match against; an empty
               // schema already explained itself above.
               if (ranked.emptyQuery) {
                  // Not "nothing matched": nothing was asked. Telling an agent
                  // to broaden a query that was never run sends it in circles.
                  warnings.push(
                     `searchQuery carried no searchable content, so no ranking was run. Provide words to search for, or omit searchQuery to list the schema's tables.`,
                  );
               } else if (ranked.hits.length === 0 && entities.length > 0) {
                  warnings.push(
                     `No table in "${schemaName}" matched "${searchQuery}". List the schema without a searchQuery to see everything in it.`,
                  );
               }
               // Ranked results cannot be paged, so a cut here is invisible
               // unless it is stated: totalAvailable counts the whole schema,
               // not the matches, and an agent reading "20 of 500" concludes
               // nothing else matched and stops.
               if (ranked.matched > page.length) {
                  // Worded per mode, because the two count different things and
                  // one of them is nearly meaningless on its own. Lexical
                  // ranking is an OR over terms, so every table sharing one
                  // common column name (`customer_id`, `created_at`) counts as
                  // a match: on a real warehouse that number is routinely the
                  // whole schema, and telling an agent to raise `limit` on the
                  // strength of it just fills the response with noise. Narrow
                  // first is the advice that actually helps.
                  warnings.push(
                     ranked.ranking === "lexical"
                        ? `${ranked.matched} tables share at least one term with "${searchQuery}"; the top ${page.length} by relevance are shown. Term-matching counts loosely, so a large number here is normal and does not mean that many tables are relevant. Ranked results cannot be paged: make searchQuery more specific, or raise limit (max ${MAX_LIMIT}) if you want more of this ranking.`
                        : `${ranked.matched} tables scored above the relevance floor for "${searchQuery}"; the top ${page.length} are shown. Ranked results cannot be paged: make searchQuery more specific, or raise limit (max ${MAX_LIMIT}).`,
                  );
               }
            } else {
               page = entities
                  .slice(skip, skip + max)
                  .map((entity) => ({ entity }));
               if (skip + max < entities.length) {
                  nextOffset = skip + max;
                  warnings.push(
                     `Showing ${page.length} of ${entities.length} tables. Pass offset ${nextOffset} for the next page, or add a searchQuery to narrow.`,
                  );
               } else if (page.length === 0 && entities.length > 0) {
                  // Distinguishes "you paged past the end" from "this schema
                  // is empty", which otherwise return the identical shape.
                  warnings.push(
                     `offset ${skip} is past the end of this schema, which has ${entities.length} tables. Use an offset below ${entities.length}, or omit it to start from the beginning.`,
                  );
               }
            }

            warnings.push(
               ...unpastableWarning(page.map(({ entity }) => entity)),
            );

            const capped = page.filter(
               ({ entity }) => entity.columns.length > MAX_COLUMNS_PER_TABLE,
            ).length;
            if (capped > 0) {
               warnings.push(
                  `${capped} table(s) have more than ${MAX_COLUMNS_PER_TABLE} columns; only the first ${MAX_COLUMNS_PER_TABLE} are shown. Pass tableName to see all columns of one table.`,
               );
            }

            return jsonResource(uri, {
               environmentName,
               connectionName,
               schemaName,
               tables: page.map(({ entity, score }) =>
                  toResponseTable(entity, {
                     maxColumns: MAX_COLUMNS_PER_TABLE,
                     score,
                  }),
               ),
               totalAvailable: entities.length,
               returned: page.length,
               ...(matched !== undefined ? { matched } : {}),
               ...(nextOffset !== undefined ? { nextOffset } : {}),
               ...(ranking ? { ranking } : {}),
               ...(warnings.length > 0 ? { warnings } : {}),
            });
         } catch (error) {
            const identifier = [environmentName, connectionName, schemaName]
               .filter(Boolean)
               .join("/");
            return jsonToolError(
               uri,
               classifyToolError("searchDatabaseSchema", identifier, error),
               { tables: [] },
            );
         }
      },
   );
}

/**
 * The embedding provider to rank with, or null to stay lexical.
 *
 * Null unless the operator set BOTH switches.
 *
 * BOTH lookups sit inside the try, because both throw on a malformed value:
 * getEmbeddingProvider on a bad companion variable, and schemaEmbeddingEnabled
 * on anything that is not a boolean (parseBoolEnv rejects rather than coercing).
 * With that second call outside, `EMBEDDING_INDEX_CONNECTION_SCHEMA=maybe` made
 * every search return an error instead of ranking lexically, which is the
 * opposite of what this comment and the docs promise.
 */
function resolveProvider() {
   try {
      if (!schemaEmbeddingEnabled()) return null;
      return getEmbeddingProvider();
   } catch (error) {
      logger.warn(
         "[MCP Tool searchDatabaseSchema] Embedding config invalid; ranking lexically",
         { error: error instanceof Error ? error.message : String(error) },
      );
      return null;
   }
}

/** Map an introspected ApiTable onto the entity the ranking layer indexes. */
function toEntity(
   table: { resource?: string; columns?: { name?: string; type?: string }[] },
   connectionName: string,
   schemaName: string,
): SchemaTableEntity {
   const resource = table.resource ?? "";
   return {
      connectionName,
      schemaName,
      tableName: bareTableName(resource),
      resource,
      columns: (table.columns ?? []).map((c) => ({
         name: c.name ?? "",
         ...(c.type ? { type: c.type } : {}),
      })),
   };
}
