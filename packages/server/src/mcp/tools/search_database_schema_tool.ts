// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
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
 * Length cap on the identifier-ish string arguments. They are echoed into
 * warning and error prose an agent reads as server-authored, so an unbounded
 * argument is an unbounded injection surface.
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
 * Escape a Malloy string literal. Malloy escapes with a backslash, not by
 * doubling the quote as SQL does, so `o'brien` needs `\'`.
 */
function escapeMalloyString(value: string): string {
   return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/**
 * Quote an identifier, always: a bare reserved word (`table`, `source`) does
 * not compile. Backslash is escaped first so the backtick's escape is not
 * doubled, and a trailing backslash cannot swallow the closing backtick.
 */
function malloyIdentifier(name: string): string {
   return `\`${name.replace(/\\/g, "\\\\").replace(/`/g, "\\`")}\``;
}

/**
 * The ready-to-paste `source:` line for a table. `resource` keeps its
 * qualification exactly as introspection produced it; only quoting is added.
 */
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
 * DuckDB's file-path grammar for a table path, copied from its dialect in
 * @malloydata/malloy (a space, apostrophe or parenthesis is outside it).
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
 * Conservative by construction: Malloy validates per dialect and does not
 * export `getDialect`, so this accepts only what every dialect accepts (a
 * dotted path of strict bare identifiers, or a DuckDB file/URL path) and the
 * caller omits `malloySource` with a warning otherwise. A false negative costs
 * a convenience field; a false positive costs a line that does not run.
 */
export function isPastableTablePath(resource: string): boolean {
   if (!resource) return false;
   if (resource.includes("/")) return DUCKDB_FILE_PATH.test(resource);
   // A bare data-file name (`orders.parquet`, from describeRemoteFile) would
   // pass the dotted check, but DuckDB reads it as schema `orders`, table
   // `parquet`, so it cannot be written as a bare path.
   if (DATA_FILE_EXTENSION.test(resource)) return false;
   return resource.split(".").every((seg) => STRICT_BARE_IDENT.test(seg));
}

/**
 * Whether this table gets a `malloySource`. Shared by the field and the
 * warning explaining its absence, so the two cannot disagree.
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
 * The human-facing name of a table: the last segment of a dotted path
 * (`sales.orders` -> `orders`), or the file name without extension for a
 * DuckDB file path, where splitting on the last dot would yield the extension.
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
 * The connections an agent can introspect in this environment: the configured
 * ones, plus the per-package `duckdb` sandbox (synthesized on demand, so in no
 * listing) with the packages it can be used against.
 *
 * Projected to name and type: an ApiConnection carries credentials
 * (service-account JSON, passwords) that must never reach a response.
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

const SEARCH_DATABASE_SCHEMA_DESCRIPTION = `Find the tables in a database connection, by plain-English description. Use it to model a database you have not modelled yet, or to check schema, table and column names. To search an existing model, use get_context.

## Drill down, one level at a time
Supply what you know, omit the rest. No arguments lists the environments and their connections; + connectionName lists its schemas; + schemaName lists its tables (up to ${MAX_COLUMNS_PER_TABLE} columns each; add searchQuery to rank them); + tableName returns that one table with every column.

## Contract rules
- Use connectionName, tablePath and column names exactly as returned.
- A connection with scope "package" (the "duckdb" sandbox) is per package: pass packageName too, from those it lists.\n- Schemas marked isHidden are system schemas; your tables are in the others.
- malloySource is the ready-to-use \`source:\` line; its identifiers are already quoted, so paste it as-is.
- Names and types only: no row value is returned. For a column's values, run execute_query against a model using this connection: \`run: c.table('s.t') -> { group_by: col }\`.
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
 * Registers search_database_schema: natural-language search over a
 * configured connection's schema.
 *
 * Ranking is lexical (lunr/BM25) unless both EMBEDDING_API_KEY and
 * EMBEDDING_INDEX_CONNECTION_SCHEMA are set, because embedding sends table and
 * column names to a third party (see config.schemaEmbeddingEnabled).
 */
export function registerSearchDatabaseSchemaTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   const deps: TierDeps = {
      environmentStore,
      connectionController: new ConnectionController(environmentStore),
   };

   mcpServer.tool(
      "search_database_schema",
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
         const uri = buildMalloyUri(
            {
               environment: environmentName,
               package: packageName,
            },
            "search-database-schema",
         );

         logger.debug("[MCP Tool searchDatabaseSchema] Searching schema", {
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
            const tier = resolveTier(params);
            const warnings = ignoredArgsWarning(params, tier.tier);
            switch (tier.tier) {
               case "environments":
                  return await listEnvironmentsTier(deps, uri, warnings);
               case "connections":
                  return await listConnectionsTier(deps, uri, tier, warnings);
               case "schemas":
                  return await listSchemasTier(
                     deps,
                     uri,
                     tier,
                     packageName,
                     warnings,
                  );
               case "table":
                  return await describeTableTier(
                     deps,
                     uri,
                     tier,
                     packageName,
                     warnings,
                  );
               case "tables":
                  return await listTablesTier(
                     deps,
                     uri,
                     tier,
                     params,
                     warnings,
                  );
            }
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

/** What the tiers that reach a store or a warehouse need. */
interface TierDeps {
   environmentStore: EnvironmentStore;
   connectionController: ConnectionController;
}

/**
 * Which level of the drill-down a call reaches. Each tier is reached by the
 * first argument that is MISSING, so the arguments a tier needs are known to be
 * present once it is chosen.
 */
type Tier =
   | { tier: "environments" }
   | { tier: "connections"; environmentName: string }
   | { tier: "schemas"; environmentName: string; connectionName: string }
   | {
        tier: "table";
        environmentName: string;
        connectionName: string;
        schemaName: string;
        tableName: string;
     }
   | {
        tier: "tables";
        environmentName: string;
        connectionName: string;
        schemaName: string;
     };

function resolveTier(params: SearchDatabaseSchemaParams): Tier {
   const { environmentName, connectionName, schemaName, tableName } = params;
   if (!environmentName) return { tier: "environments" };
   if (!connectionName) return { tier: "connections", environmentName };
   if (!schemaName) {
      return { tier: "schemas", environmentName, connectionName };
   }
   // Tier 5 (one named table) is checked before tier 4 and wins over it.
   if (tableName) {
      return {
         tier: "table",
         environmentName,
         connectionName,
         schemaName,
         tableName,
      };
   }
   return { tier: "tables", environmentName, connectionName, schemaName };
}

/**
 * The warning naming the arguments the reached tier does not read, or [].
 * Without it, tableName without schemaName silently returns the schema list
 * and reads as "the table is not there".
 */
function ignoredArgsWarning(
   params: SearchDatabaseSchemaParams,
   tier: Tier["tier"],
): string[] {
   const {
      connectionName,
      packageName,
      schemaName,
      tableName,
      searchQuery,
      limit,
      offset,
   } = params;
   const ignored: string[] = [];
   const noteIgnored = (entries: readonly (readonly [string, unknown])[]) => {
      for (const [name, value] of entries) {
         // Deduped: two rules can name the same argument (e.g. packageName
         // via the tier rule and the sandbox rule below).
         if (value !== undefined && value !== "" && !ignored.includes(name)) {
            ignored.push(name);
         }
      }
   };
   // Paging arguments only mean something on a table listing, so they
   // are ignored by every other tier and by a ranked search.
   const pagingArgs = [
      ["limit", limit],
      ["offset", offset],
   ] as const;
   switch (tier) {
      case "environments":
         noteIgnored([
            ["connectionName", connectionName],
            ["packageName", packageName],
            ["schemaName", schemaName],
            ["tableName", tableName],
            ["searchQuery", searchQuery],
            ...pagingArgs,
         ]);
         break;
      case "connections":
         noteIgnored([
            ["packageName", packageName],
            ["schemaName", schemaName],
            ["tableName", tableName],
            ["searchQuery", searchQuery],
            ...pagingArgs,
         ]);
         break;
      case "schemas":
         noteIgnored([
            ["tableName", tableName],
            ["searchQuery", searchQuery],
            ...pagingArgs,
         ]);
         break;
      case "table":
         // Tier 5 wins over tier 4; say so rather than dropping the rest.
         noteIgnored([["searchQuery", searchQuery], ...pagingArgs]);
         break;
      case "tables":
         // Ranked results cannot be paged; unwarned, an agent paging them
         // gets the same top hits forever.
         if (searchQuery !== undefined) noteIgnored([["offset", offset]]);
         break;
   }
   // packageName only selects anything for the per-package sandbox; on any
   // other connection the controller drops it.
   if (connectionName && connectionName !== SANDBOX_CONNECTION) {
      noteIgnored([["packageName", packageName]]);
   }
   return ignored.length > 0
      ? [
           `Ignored ${ignored.join(", ")}. Each level needs the one above it: environmentName, then connectionName (with packageName for the per-package "duckdb" sandbox), then schemaName, then either tableName or searchQuery. limit and offset apply only to a plain table listing, and offset does nothing on a ranked search because ranked results cannot be paged.`,
        ]
      : [];
}

/** Spread into a payload: `warnings` when there are any, nothing otherwise. */
function withWarnings(warnings: string[]): { warnings?: string[] } {
   return warnings.length > 0 ? { warnings } : {};
}

/**
 * Shed load before querying the warehouse, as getModelForQuery does for the
 * other MCP tools. Every warehouse-reaching tier calls this: introspection can
 * pull 100k rows, and the memory governor otherwise cannot see this traffic.
 */
async function assertCanAdmit(
   environmentStore: EnvironmentStore,
   environmentName: string,
): Promise<void> {
   (
      await environmentStore.getEnvironment(environmentName, false)
   ).assertCanAdmitQuery();
}

/**
 * The warning for tables with no `malloySource`, or [] if all have one. Shared
 * by both table tiers so neither omits the field unexplained.
 */
function unpastableWarning(entities: SchemaTableEntity[]): string[] {
   // canPasteSource, not isPastableTablePath: it covers both reasons the field
   // is withheld.
   const n = entities.filter((e) => !canPasteSource(e)).length;
   if (n === 0) return [];
   return [
      `${n} table(s) have a path this server will not vouch for across every dialect it serves, so malloySource is omitted for them rather than risk a line that does not compile. Build it from tablePath: on your own dialect the path may work as-is, or may need the offending segment quoted. Double quotes on DuckDB, Postgres, Snowflake, Trino and SQL Server; backticks on MySQL, BigQuery and Databricks.`,
   ];
}

/** The not-found error for a named table, with a first suggestion that fits the case. */
function tableNotFound(
   uri: string,
   tier: Extract<Tier, { tier: "table" }>,
   listSuggestion: string,
) {
   return jsonToolError(
      uri,
      {
         message: `Table "${tier.tableName}" not found in schema "${tier.schemaName}" of connection "${tier.connectionName}".`,
         suggestions: [
            listSuggestion,
            "Check the table name's spelling and case; some warehouses are case-sensitive.",
         ],
      },
      { tables: [] },
   );
}

/**
 * Tier 1: no environment -> what environments exist, and what connections
 * each has, so an agent with no prior knowledge starts here.
 */
async function listEnvironmentsTier(
   { environmentStore }: TierDeps,
   uri: string,
   warnings: string[],
) {
   const environments = await environmentStore.listEnvironments();
   const results = await Promise.all(
      environments.map(async (env) => {
         const name = env.name ?? "";
         let connections: ResponseConnection[] = [];
         try {
            const environment = await environmentStore.getEnvironment(
               name,
               false,
            );
            connections = await listConnectionsFor(environment);
         } catch (error) {
            logger.debug(
               "[MCP Tool searchDatabaseSchema] Could not list connections",
               {
                  environmentName: name,
                  error: error instanceof Error ? error.message : String(error),
               },
            );
         }
         return { name, connections };
      }),
   );
   return jsonResource(uri, {
      environments: results,
      ...withWarnings(warnings),
   });
}

/** Tier 2: environment only -> its connections. */
async function listConnectionsTier(
   { environmentStore }: TierDeps,
   uri: string,
   { environmentName }: Extract<Tier, { tier: "connections" }>,
   warnings: string[],
) {
   const environment = await environmentStore.getEnvironment(
      environmentName,
      false,
   );
   const connections = await listConnectionsFor(environment);
   return jsonResource(uri, {
      environmentName,
      connections,
      ...withWarnings(warnings),
   });
}

/**
 * Tier 3: connection only -> its schemas. Shed load first: this queries the
 * warehouse too, and on DuckDB it can list cloud directories.
 */
async function listSchemasTier(
   { environmentStore, connectionController }: TierDeps,
   uri: string,
   { environmentName, connectionName }: Extract<Tier, { tier: "schemas" }>,
   packageName: string | undefined,
   warnings: string[],
) {
   await assertCanAdmit(environmentStore, environmentName);
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
      ...withWarnings(warnings),
   });
}

/**
 * Tier 5 (checked before 4): one named table, in full. The most expensive
 * tier, since it waives the column cap and can fan out one DESCRIBE per file
 * on the cloud branches.
 */
async function describeTableTier(
   { environmentStore, connectionController }: TierDeps,
   uri: string,
   tier: Extract<Tier, { tier: "table" }>,
   packageName: string | undefined,
   warnings: string[],
) {
   const { environmentName, connectionName, schemaName, tableName } = tier;
   await assertCanAdmit(environmentStore, environmentName);
   const tables = await connectionController.listTables(
      environmentName,
      connectionName,
      schemaName,
      [tableName],
      packageName,
   );
   if (tables.length === 0) {
      return tableNotFound(
         uri,
         tier,
         `List the schema's tables by calling this tool with schemaName "${schemaName}" and no tableName.`,
      );
   }
   // Some branches (DuckDB cloud, Azure) ignore the tableNames filter and
   // list a whole directory; filter here, since this tier waives the column
   // cap on the assumption that one table was returned.
   const exact = tables.filter(
      (t) => bareTableName(t.resource ?? "") === tableName,
   );
   // Case-insensitive only when nothing matched exactly, so `Orders` and
   // `orders` side by side still resolve to the one asked for.
   const matching =
      exact.length > 0
         ? exact
         : tables.filter(
              (t) =>
                 bareTableName(t.resource ?? "").toLowerCase() ===
                 tableName.toLowerCase(),
           );
   if (matching.length === 0) {
      return tableNotFound(
         uri,
         tier,
         `This schema lists ${tables.length} table(s). Call this tool with schemaName "${schemaName}" and no tableName to see them.`,
      );
   }
   const entities = matching.map((t) =>
      toEntity(t, connectionName, schemaName),
   );
   return jsonResource(uri, {
      environmentName,
      connectionName,
      schemaName,
      // No column cap: asking for one table is how you get all its columns.
      tables: entities.map((e) =>
         toResponseTable(e, {
            maxColumns: Number.MAX_SAFE_INTEGER,
         }),
      ),
      totalAvailable: entities.length,
      returned: entities.length,
      ...withWarnings([...warnings, ...unpastableWarning(entities)]),
   });
}

/** Tier 4: a schema's tables, listed or ranked. */
async function listTablesTier(
   { environmentStore, connectionController }: TierDeps,
   uri: string,
   {
      environmentName,
      connectionName,
      schemaName,
   }: Extract<Tier, { tier: "tables" }>,
   params: SearchDatabaseSchemaParams,
   ignoredWarning: string[],
) {
   const { packageName, searchQuery, limit, offset } = params;
   const max = limit ?? DEFAULT_LIMIT;
   const skip = offset ?? 0;
   await assertCanAdmit(environmentStore, environmentName);
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

   // An empty schema is not necessarily an empty database: DuckDB over data
   // files addresses them by path, registering none. Say so.
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
         // packageName is in the key only for the per-package sandbox, the
         // one connection where it changes what is introspected. Elsewhere it
         // would let a caller mint unlimited keys, each a full re-embed.
         cacheKey: [
            environmentName,
            connectionName === SANDBOX_CONNECTION ? (packageName ?? "") : "",
            connectionName,
            schemaName,
         ].join("\x00"),
      });
      // An empty query ran no ranking, so no mode is reported.
      ranking = ranked.emptyQuery ? undefined : ranked.ranking;
      matched = ranked.emptyQuery ? undefined : ranked.matched;
      page = ranked.hits.map((hit) => ({
         entity: hit,
         score: hit.score,
      }));
      if (ranked.emptyQuery) {
         // Not "nothing matched": nothing was asked.
         warnings.push(
            `searchQuery carried no searchable content, so no ranking was run. Provide words to search for, or omit searchQuery to list the schema's tables.`,
         );
      } else if (ranked.hits.length === 0 && entities.length > 0) {
         warnings.push(
            `No table in "${schemaName}" matched "${searchQuery}". List the schema without a searchQuery to see everything in it.`,
         );
      }
      // Ranked results cannot be paged, so a cut must be stated
      // (totalAvailable counts the whole schema, not the matches).
      if (ranked.matched > page.length) {
         // Worded per mode: a lexical match count is an OR over terms and
         // routinely the whole schema, so narrowing is the useful advice.
         warnings.push(
            ranked.ranking === "lexical"
               ? `${ranked.matched} tables share at least one term with "${searchQuery}"; the top ${page.length} by relevance are shown. Term-matching counts loosely, so a large number here is normal and does not mean that many tables are relevant. Ranked results cannot be paged: make searchQuery more specific, or raise limit (max ${MAX_LIMIT}) if you want more of this ranking.`
               : `${ranked.matched} tables scored above the relevance floor for "${searchQuery}"; the top ${page.length} are shown. Ranked results cannot be paged: make searchQuery more specific, or raise limit (max ${MAX_LIMIT}).`,
         );
      }
   } else {
      page = entities.slice(skip, skip + max).map((entity) => ({ entity }));
      if (skip + max < entities.length) {
         nextOffset = skip + max;
         warnings.push(
            `Showing ${page.length} of ${entities.length} tables. Pass offset ${nextOffset} for the next page, or add a searchQuery to narrow.`,
         );
      } else if (page.length === 0 && entities.length > 0) {
         // Distinguishes "paged past the end" from "empty schema".
         warnings.push(
            `offset ${skip} is past the end of this schema, which has ${entities.length} tables. Use an offset below ${entities.length}, or omit it to start from the beginning.`,
         );
      }
   }

   warnings.push(...unpastableWarning(page.map(({ entity }) => entity)));

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
      ...withWarnings(warnings),
   });
}

/**
 * The embedding provider to rank with, or null to stay lexical (unless both
 * switches are set). Both lookups sit inside the try because both throw on a
 * malformed value, which must degrade to lexical ranking, not an error.
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
