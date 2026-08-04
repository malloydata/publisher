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

const searchDatabaseSchemaShape = {
   environmentName: z
      .string()
      .optional()
      .describe(
         "Environment to look in. Omit to list the environments and their connections.",
      ),
   connectionName: z
      .string()
      .optional()
      .describe(
         "Connection to introspect. Omit to list the connections in the environment.",
      ),
   packageName: z
      .string()
      .optional()
      .describe(
         'Required only for the per-package "duckdb" sandbox connection, which exists once per package.',
      ),
   schemaName: z
      .string()
      .optional()
      .describe(
         'Schema (or dataset/database) to list tables from. Omit to list the connection\'s schemas. Postgres and DuckDB usually use "main" or "public".',
      ),
   tableName: z
      .string()
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

/** A bare identifier Malloy accepts unquoted; anything else needs backticks. */
const BARE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

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
 * Backslash FIRST, for the same reason as escapeMalloyString: escaping the
 * backtick first would then double the backslash that escape introduced. And a
 * name ending in a backslash, left unescaped, would consume the closing
 * backtick and swallow the rest of the line.
 */
function malloyIdentifier(name: string): string {
   if (BARE_IDENTIFIER.test(name)) return name;
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
   malloySource: string;
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
      malloySource: malloySourceSnippet(
         entity.connectionName,
         entity.resource,
         entity.tableName,
      ),
      columns: entity.columns.slice(0, options.maxColumns),
      columnCount: entity.columns.length,
      ...(options.score !== undefined ? { score: options.score } : {}),
   };
}

const SEARCH_DATABASE_SCHEMA_DESCRIPTION = `Find the tables in a database connection, by plain-English description. Use it to model a database you have not modelled yet, or to confirm schema, table and column names before writing a source. To search an existing model, use malloy_getContext.

## Drill down, one level at a time
Supply what you know and omit the rest. No arguments lists the environments and their connections; + connectionName lists that connection's schemas; + schemaName lists its tables (up to ${MAX_COLUMNS_PER_TABLE} columns each, and add searchQuery to rank them); + tableName returns that one table with every column.

## Contract rules
- Use connectionName, tablePath and column names exactly as returned. Do not reformat or re-case.
- A connection with scope "package" (the "duckdb" sandbox) exists once per package: pass packageName too, from those listed on it.
- malloySource is the ready-to-use \`source:\` line. Rename the source if the table name is a Malloy keyword.
- Names and types only. It never reads rows, so it cannot say what a column contains; use malloy_executeQuery with \`select: distinct\` for that.
- No tables for a searchQuery means nothing matched, not that the schema is empty. Broaden it, or list without one.
- An empty schema is not always an empty database: DuckDB over CSV or Parquet addresses files by path, registering none.
- Read warnings: they name anything omitted, paged or ignored.

## Response
JSON: tables (connectionName, schemaName, tableName, tablePath, malloySource, columns, columnCount), plus totalAvailable and returned. A search adds matched (hits before limit) and ranking; a listing adds nextOffset when more remain, to pass back as offset.

## Worked example
Start with no arguments and follow the connections it names. For connection "warehouse", schema "sales":
{ "environmentName": "examples", "connectionName": "warehouse", "schemaName": "sales", "searchQuery": "customer orders" }
Then paste that table's malloySource, e.g. source: orders is warehouse.table('sales.orders') extend { }`;

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
            if (!environmentName) {
               for (const [name, value] of [
                  ["connectionName", connectionName],
                  ["packageName", packageName],
                  ["schemaName", schemaName],
                  ["tableName", tableName],
                  ["searchQuery", searchQuery],
               ] as const) {
                  if (value) ignored.push(name);
               }
            } else if (!connectionName) {
               for (const [name, value] of [
                  ["schemaName", schemaName],
                  ["tableName", tableName],
                  ["searchQuery", searchQuery],
               ] as const) {
                  if (value) ignored.push(name);
               }
            } else if (!schemaName) {
               for (const [name, value] of [
                  ["tableName", tableName],
                  ["searchQuery", searchQuery],
               ] as const) {
                  if (value) ignored.push(name);
               }
            } else if (tableName && searchQuery) {
               // Tier 5 wins over tier 4; say so rather than dropping the query.
               ignored.push("searchQuery");
            }
            const ignoredWarning =
               ignored.length > 0
                  ? [
                       `Ignored ${ignored.join(", ")}: each level of this tool needs the one above it, so supply them in order (environmentName, connectionName, schemaName, then tableName or searchQuery).`,
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

            // Tier 3: connection only -> its schemas.
            if (!schemaName) {
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

            // Tier 5 (checked before 4): one named table, in full.
            if (tableName) {
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
               const entities = tables.map((t) =>
                  toEntity(t, connectionName, schemaName),
               );
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
                  ...(ignoredWarning.length > 0
                     ? { warnings: ignoredWarning }
                     : {}),
               });
            }

            // Tier 4: a schema's tables, listed or ranked.
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

            if (searchQuery) {
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
               ranking = ranked.ranking;
               matched = ranked.matched;
               page = ranked.hits.map((hit) => ({
                  entity: hit,
                  score: hit.score,
               }));
               // Only when there was something to match against; an empty
               // schema already explained itself above.
               if (ranked.hits.length === 0 && entities.length > 0) {
                  warnings.push(
                     `No table in "${schemaName}" matched "${searchQuery}". List the schema without a searchQuery to see everything in it.`,
                  );
               }
               // Ranked results cannot be paged, so a cut here is invisible
               // unless it is stated: totalAvailable counts the whole schema,
               // not the matches, and an agent reading "20 of 500" concludes
               // nothing else matched and stops.
               if (ranked.matched > page.length) {
                  warnings.push(
                     `${ranked.matched} tables matched "${searchQuery}" but only the top ${page.length} are shown, and ranked results cannot be paged. Raise limit (max ${MAX_LIMIT}) or use a more specific searchQuery.`,
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
