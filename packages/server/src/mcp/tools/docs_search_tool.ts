import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import lunr from "lunr";
import { EnvironmentStore } from "../../service/environment_store";
import { buildMalloyUri } from "../handler_utils";
import { logger } from "../../logger";
import rawIndex from "./docs_search/malloy_docs_index.json";

interface DocEntry {
   title: string;
   path: string;
   url: string;
   excerpt: string;
}

const asset = rawIndex as unknown as {
   docCount: number;
   index: object;
   store: Record<string, DocEntry>;
};

// Load the prebuilt lunr index once at module init. The index is a build-time asset
// (see docs_search/build_docs_index.ts), so there is no runtime network dependency.
const docsIndex = lunr.Index.load(asset.index);

const searchDocsShape = {
   query: z
      .string()
      .max(500)
      .describe(
         'Keywords describing what you need, e.g. "window functions", "bar chart", "pivot".',
      ),
   limit: z
      .number()
      .int()
      .positive()
      .max(50)
      .optional()
      .describe("Maximum number of results to return. Default 8, max 50."),
};
type SearchDocsParams = z.infer<z.ZodObject<typeof searchDocsShape>>;

// lunr treats several characters as query operators; strip them so a plain-English
// query never throws and is matched as an OR of its terms.
export function sanitize(query: string): string {
   return query.replace(/[~^:*+\-"]/g, " ").trim();
}

/**
 * Search the bundled docs index and return up to `max` matches. Pure over the
 * module-level index asset, so it is unit-testable without a server. An empty or
 * operator-only query returns []; a lunr parse failure is logged and returns [].
 */
export function searchDocsIndex(
   query: string,
   max: number,
): Array<{ title: string; url: string; excerpt: string }> {
   const sanitized = sanitize(query);
   if (!sanitized) return [];

   let hits: lunr.Index.Result[] = [];
   try {
      hits = docsIndex.search(sanitized);
   } catch (error) {
      logger.warn("[MCP Tool searchDocs] lunr search failed", {
         query,
         error: error instanceof Error ? error.message : String(error),
      });
      return [];
   }

   // Guard against an index/store mismatch (a stale or corrupt asset): skip any
   // hit whose ref is missing from the store rather than throwing.
   return hits
      .map((hit) => asset.store[hit.ref])
      .filter((entry): entry is DocEntry => entry !== undefined)
      .slice(0, max)
      .map((entry) => ({
         title: entry.title,
         url: entry.url,
         excerpt: entry.excerpt,
      }));
}

const SEARCH_DOCS_DESCRIPTION = `Search the Malloy documentation by keyword and return the most relevant doc pages, each with a short excerpt and a link. Use this to look up Malloy language or rendering syntax instead of guessing.

## When to use
- Before writing unfamiliar Malloy syntax (window functions, autobin, dialect-specific functions, rendering tags) or when a query fails with a syntax error you do not recognize.
- Do NOT use it to look up field or source names in a model; use malloy_getContext for that.

## Parameters
- query (required): keywords describing what you need.
- limit (optional): maximum results to return; default 8.

## Response
A JSON array of matches, each with title, url (a docs.malloydata.dev link), and a short excerpt, ordered by relevance. Empty array if nothing matches; broaden the keywords and retry.

## Contract rules
- These are documentation pages, not model entities. Do not treat a doc title as a field or source name.
- The excerpt is only a hint; open the url for the full detail.

## Worked example
{ "query": "window functions lag" }`;

/**
 * Registers the malloy_searchDocs MCP tool: lexical (lunr/BM25) search over a bundled
 * index of the Malloy documentation.
 */
export function registerDocsSearchTool(
   mcpServer: McpServer,
   _environmentStore: EnvironmentStore,
): void {
   mcpServer.tool(
      "malloy_searchDocs",
      SEARCH_DOCS_DESCRIPTION,
      searchDocsShape,
      async (params: SearchDocsParams) => {
         const { query, limit } = params;
         const max = limit ?? 8;
         logger.info("[MCP Tool searchDocs] Searching docs", { query, limit });

         const results = searchDocsIndex(query, max);

         return {
            content: [
               {
                  type: "resource" as const,
                  resource: {
                     type: "application/json",
                     uri: buildMalloyUri({}, "docs-search"),
                     text: JSON.stringify(results),
                  },
               },
            ],
         };
      },
   );
}
