// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A report, drawn inside the conversation: the MCP Apps extension
// (io.modelcontextprotocol/ui). Three tools and one resource.
//
// - `list_reports` names a package's reports, so an agent can pick one.
// - `show_report` opens one. Its `_meta.ui.resourceUri` tells a host that
//   supports MCP Apps to load the panel resource and hand it this call's
//   result; a host that does not just shows the text.
// - `report_api` is the panel's only way back to the server. It is app-only
//   (`visibility: ["app"]`): the host proxies it for the panel, and does not
//   offer it to the model. It answers the four reads the SDK's `Report` makes,
//   through the same controllers the REST routes call, so gates, givens, the
//   query slot and the access record are the REST path's, with the MCP
//   request's caller.
// - `ui://publisher/report.html` is a few hundred bytes of HTML whose script
//   and stylesheet load from the REST origin (`mcp.appOrigin`). Inlining the
//   bundle would make every resource read several megabytes.

import type { GivenValue } from "@malloydata/malloy";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import path from "node:path";
import { z } from "zod/v3";
import { QueryController } from "../../controller/query.controller";
import { ReportController } from "../../controller/report.controller";
import {
   BadRequestError,
   DashboardNotFoundError,
   internalErrorToHttpError,
} from "../../errors";
import { tryAcquireQuerySlot } from "../../query_concurrency";
import { accessIdentityOf } from "../../service/access_record";
import { EnvironmentStore } from "../../service/environment_store";
import { ThemeStore } from "../../service/theme_store";
import { settings } from "../../settings";
import { SERVER_VERSION } from "../../version";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import type { RequestExtra } from "../request_extra";
import { jsonResource, jsonToolError } from "../tool_response";

/** The MCP Apps resource MIME type (`RESOURCE_MIME_TYPE` in `@modelcontextprotocol/ext-apps`). */
export const MCP_APP_MIME_TYPE = "text/html;profile=mcp-app";

/** The panel every `show_report` result is drawn in. One resource for every report: which report is in the tool result. */
export const REPORT_PANEL_URI = "ui://publisher/report.html";

/** What `report_api` answers: the reads `Report` makes, and nothing else. */
export const REPORT_API_OPERATIONS = [
   "status",
   "listReports",
   "getReport",
   "query",
] as const;
export type ReportApiOperation = (typeof REPORT_API_OPERATIONS)[number];

/**
 * The REST origin the panel loads from, as `scheme://host[:port]`. A value
 * with a path or a trailing slash is cut to its origin, so the CSP entry and
 * the script URL always agree; a value that is not a URL fails loudly here
 * rather than as a blank panel.
 */
export function reportAppOrigin(
   raw: string = settings.get("mcp.appOrigin") ??
      `http://localhost:${settings.get("port")}`,
): string {
   let url: URL;
   try {
      url = new URL(raw);
   } catch {
      throw new Error(
         `Invalid MCP_APP_ORIGIN "${raw}": expected an origin such as http://localhost:4000 or https://publisher.example.com.`,
      );
   }
   if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new Error(
         `Invalid MCP_APP_ORIGIN "${raw}": expected an http or https origin.`,
      );
   }
   return url.origin;
}

const escapeAttribute = (value: string) =>
   value
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");

/** The panel document. Static apart from the origin and the version that busts the cache. */
export function reportPanelHtml(origin: string, version = SERVER_VERSION) {
   const v = encodeURIComponent(version);
   const base = escapeAttribute(`${origin}/mcp-app`);
   return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Report</title>
<link rel="stylesheet" href="${base}/report.css?v=${v}">
</head>
<body>
<div id="root"></div>
<script src="${base}/report.js?v=${v}"></script>
</body>
</html>
`;
}

/** The Console address of a report: `/<env>/<pkg>/sales/q3` for `sales/q3.malloy`, as the SDK's `reportRoute`. */
export function consoleReportUrl(
   origin: string,
   environmentName: string,
   packageName: string,
   reportPath: string,
   givens?: Record<string, string>,
): string {
   const route = [
      environmentName,
      packageName,
      ...reportPath.replace(/\.malloy$/, "").split("/"),
   ]
      .map(encodeURIComponent)
      .join("/");
   const query = new URLSearchParams(givens ?? {}).toString();
   return `${origin}/${route}${query ? `?${query}` : ""}`;
}

const packageShape = {
   environmentName: z
      .string()
      .describe("The environment, as list_packages names it."),
   packageName: z.string().describe("The package, as list_packages names it."),
};

const showReportShape = {
   ...packageShape,
   path: z
      .string()
      .describe(
         "The report's path in the package, including `.malloy`, exactly as list_reports gives it: `sales/q3.malloy`.",
      ),
   givens: z
      .record(z.string())
      .optional()
      .describe(
         'Starting filter values, by given name, as the report\'s filter controls write them (`{"CATEGORY": "Outerwear"}`). They beat the report\'s own starting values. Omit to open it as its author left it.',
      ),
};

const reportApiShape = {
   operation: z
      .enum(REPORT_API_OPERATIONS)
      .describe("Which read: status, listReports, getReport or query."),
   environmentName: z.string().optional(),
   packageName: z.string().optional(),
   path: z
      .string()
      .optional()
      .describe("getReport: the report's path. query: the model's path."),
   versionId: z.string().optional(),
   body: z
      .object({
         query: z.string().optional(),
         queryName: z.string().optional(),
         sourceName: z.string().optional(),
         givens: z.record(z.unknown()).optional(),
         compactJson: z.boolean().optional(),
         filterParams: z
            .record(z.union([z.string(), z.array(z.string())]))
            .optional(),
         queryMetadata: z.unknown().optional(),
         queryClass: z.unknown().optional(),
      })
      .optional()
      .describe("query: the body POST …/models/{path}/query takes."),
};

type ReportApiArgs = {
   operation: ReportApiOperation;
   environmentName?: string;
   packageName?: string;
   path?: string;
   versionId?: string;
   body?: {
      query?: string;
      queryName?: string;
      sourceName?: string;
      givens?: Record<string, unknown>;
      compactJson?: boolean;
      filterParams?: Record<string, string | string[]>;
      queryMetadata?: unknown;
      queryClass?: unknown;
   };
};

/** What one `report_api` call answered: the REST status and body the same request would have got. */
export interface ReportApiAnswer {
   status: number;
   data: unknown;
}

function required(
   value: string | undefined,
   name: string,
   operation: string,
): string {
   if (typeof value !== "string" || value.length === 0) {
      throw new BadRequestError(`report_api ${operation} needs ${name}.`);
   }
   return value;
}

/**
 * Answer one panel read as the REST route would: the same controller call,
 * and on a failure the same status and JSON body `internalErrorToHttpError`
 * gives the route. Exported for tests.
 */
export async function answerReportApi(
   args: ReportApiArgs,
   deps: {
      environmentStore: EnvironmentStore;
      reportController: ReportController;
      queryController: QueryController;
      themeStore: () => ThemeStore;
   },
   extra?: RequestExtra,
): Promise<ReportApiAnswer> {
   const { operation } = args;
   try {
      switch (operation) {
         case "status": {
            // Only what the SDK's provider reads from /status: the theme, the
            // model page and frozenConfig. Not the environments: the panel
            // has no use for them, and get_status is where an agent reads them.
            const status = await deps.environmentStore.getStatus({});
            const theme = deps.environmentStore.storageManager.isInitialized()
               ? await deps.themeStore().get()
               : undefined;
            const s = status as Record<string, unknown>;
            return {
               status: 200,
               data: {
                  frozenConfig: s.frozenConfig,
                  modelPage: s.modelPage,
                  theme: theme ?? {},
               },
            };
         }
         case "listReports":
            return {
               status: 200,
               data: await deps.reportController.listReports(
                  required(args.environmentName, "environmentName", operation),
                  required(args.packageName, "packageName", operation),
                  args.versionId,
               ),
            };
         case "getReport":
            return {
               status: 200,
               data: await deps.reportController.getReport(
                  required(args.environmentName, "environmentName", operation),
                  required(args.packageName, "packageName", operation),
                  required(args.path, "path", operation),
                  args.versionId,
               ),
            };
         case "query": {
            const environmentName = required(
               args.environmentName,
               "environmentName",
               operation,
            );
            const packageName = required(
               args.packageName,
               "packageName",
               operation,
            );
            const modelPath = required(args.path, "path", operation);
            const body = args.body ?? {};
            // The REST route's slot pool, so a panel full of tiles cannot get
            // around PUBLISHER_MAX_CONCURRENT_QUERIES.
            const slot = tryAcquireQuerySlot("mcp:reportPanel");
            try {
               return {
                  status: 200,
                  data: await deps.queryController.getQuery(
                     environmentName,
                     packageName,
                     modelPath,
                     body.sourceName as string,
                     body.queryName as string,
                     body.query as string,
                     body.compactJson === true,
                     body.filterParams,
                     undefined,
                     body.givens as Record<string, GivenValue> | undefined,
                     {
                        queryMetadata: body.queryMetadata,
                        queryClass: body.queryClass,
                        versionId: args.versionId,
                     },
                     // MCP sends no #(authorize) bypass, ever.
                     false,
                     false,
                     extra?.signal,
                     accessIdentityOf(extra?.requestInfo?.headers),
                  ),
               };
            } finally {
               slot.release();
            }
         }
      }
   } catch (error) {
      const { status, json } = internalErrorToHttpError(error as Error);
      return { status, data: json };
   }
}

const LIST_REPORTS_DESCRIPTION = `List the reports in one package: each one's path (its identity, for show_report), layout (grid, a dashboard; linear, a story read top to bottom), title and description. A report is a .malloy file with an ## artifact tag, wherever it lives in the package. Takes the environment and package names list_packages gives.`;

const SHOW_REPORT_DESCRIPTION = `Show a report to the user: its tiles, charts and filter controls, drawn in a panel in the conversation by a host that supports MCP Apps (Claude Desktop and claude.ai among them), and runnable there. Use it when the user wants to see a dashboard or report, not to answer a question yourself: for numbers, run execute_query. Takes the report's path from list_reports and, optionally, starting filter values. The text result says what the report holds and gives its address in the Publisher Console, which works in any client, including one that draws no panel.`;

const REPORT_API_DESCRIPTION = `Used by the show_report panel to load the report it shows and run its tiles. Not for agents: use execute_query.`;

/**
 * Register the report panel resource, `list_reports`, `show_report` and the
 * app-only `report_api`.
 */
export function registerReportAppTools(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   const reportController = new ReportController(environmentStore);
   const queryController = new QueryController(environmentStore);
   let themeStore: ThemeStore | undefined;
   const deps = {
      environmentStore,
      reportController,
      queryController,
      themeStore: () =>
         (themeStore ??= new ThemeStore(
            environmentStore.storageManager,
            path.resolve(process.cwd(), settings.get("root")),
         )),
   };

   mcpServer.registerResource(
      "report-panel",
      REPORT_PANEL_URI,
      {
         title: "Publisher report",
         description:
            "The panel show_report draws a report in. Loads its script from the Publisher server.",
         mimeType: MCP_APP_MIME_TYPE,
      },
      async () => {
         const origin = reportAppOrigin();
         return {
            contents: [
               {
                  uri: REPORT_PANEL_URI,
                  mimeType: MCP_APP_MIME_TYPE,
                  text: reportPanelHtml(origin),
                  _meta: {
                     ui: {
                        // The script, stylesheet and fonts come from the REST
                        // origin; nothing is fetched: data goes through the host.
                        csp: { resourceDomains: [origin], connectDomains: [] },
                        prefersBorder: false,
                     },
                  },
               },
            ],
         };
      },
   );

   mcpServer.tool(
      "list_reports",
      LIST_REPORTS_DESCRIPTION,
      packageShape,
      async ({ environmentName, packageName }) => {
         const uri = buildMalloyUri(
            { environment: environmentName, package: packageName },
            "reports",
         );
         try {
            const reports = await reportController.listReports(
               environmentName,
               packageName,
            );
            return jsonResource(
               uri,
               {
                  reports: reports.map((r) => ({
                     path: r.path,
                     layout: r.layout,
                     title: r.title,
                     ...(r.description ? { description: r.description } : {}),
                     ...(r.error ? { error: r.error } : {}),
                  })),
               },
               { space: 2 },
            );
         } catch (error) {
            return jsonToolError(
               "error://listReports",
               classifyToolError(
                  "listReports",
                  `${environmentName}/${packageName}`,
                  error,
               ),
            );
         }
      },
   );

   mcpServer.registerTool(
      "show_report",
      {
         title: "Show a report",
         description: SHOW_REPORT_DESCRIPTION,
         inputSchema: showReportShape,
         annotations: { readOnlyHint: true, openWorldHint: false },
         _meta: {
            ui: { resourceUri: REPORT_PANEL_URI },
            // The key hosts read before `ui.resourceUri` existed.
            "ui/resourceUri": REPORT_PANEL_URI,
         },
      },
      async ({ environmentName, packageName, path: reportPath, givens }) => {
         try {
            const report = await reportController.getReport(
               environmentName,
               packageName,
               reportPath,
            );
            const url = consoleReportUrl(
               reportAppOrigin(),
               environmentName,
               packageName,
               report.path,
               givens,
            );
            const tiles = report.tiles ?? [];
            const queryTiles = tiles.filter((t) => t.kind !== "text").length;
            const filters = (report.givens ?? [])
               .map((g) => g.label ?? g.name)
               .filter(Boolean);
            const lines = [
               `Showing "${report.title}" (${report.layout === "linear" ? "a linear report" : "a grid report"}, ${queryTiles} ${queryTiles === 1 ? "query tile" : "query tiles"}${tiles.length > queryTiles ? `, ${tiles.length - queryTiles} text` : ""}).`,
               filters.length > 0
                  ? `Its filters: ${filters.join(", ")}.`
                  : "It has no filters.",
               report.error ? `It has a problem: ${report.error}` : undefined,
               `Open it in Publisher: ${url}`,
            ].filter(Boolean);
            return {
               content: [{ type: "text" as const, text: lines.join("\n") }],
               structuredContent: {
                  environmentName,
                  packageName,
                  path: report.path,
                  title: report.title,
                  layout: report.layout,
                  givens: givens ?? {},
                  url,
               },
            };
         } catch (error) {
            if (error instanceof DashboardNotFoundError) {
               return jsonToolError("error://showReport/notFound", {
                  message: error.message,
                  suggestions: [
                     `Call list_reports for ${environmentName}/${packageName}: it gives each report's path, including \`.malloy\` and any directory, which show_report takes verbatim.`,
                  ],
               });
            }
            return jsonToolError(
               "error://showReport",
               classifyToolError(
                  "showReport",
                  `${environmentName}/${packageName}/${reportPath}`,
                  error,
               ),
            );
         }
      },
   );

   mcpServer.registerTool(
      "report_api",
      {
         title: "Report panel API",
         description: REPORT_API_DESCRIPTION,
         inputSchema: reportApiShape,
         annotations: { readOnlyHint: true, openWorldHint: false },
         _meta: {
            ui: { resourceUri: REPORT_PANEL_URI, visibility: ["app"] },
         },
      },
      async (args, extra?: RequestExtra) => {
         const answer = await answerReportApi(
            args as ReportApiArgs,
            deps,
            extra,
         );
         return {
            isError: answer.status >= 400,
            // Hosts pass `structuredContent` to the panel; the text is for a
            // client that shows tool results.
            content: [
               {
                  type: "text" as const,
                  text: `${args.operation}: ${answer.status}`,
               },
            ],
            structuredContent: answer as unknown as Record<string, unknown>,
         };
      },
   );
}
