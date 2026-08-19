import fs from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { logger } from "../logger";

/**
 * MCP Apps support: the interactive HTML widgets that render a tool result inline
 * in a chat client which implements the spec.
 *
 * Three things have to line up, and all three are gated on the widget bundle
 * being on disk. A client told that a tool has a widget, which then cannot read
 * it, renders a broken card; advertising nothing is strictly better than
 * advertising something absent.
 *
 *   1. The server declares the `io.modelcontextprotocol/ui` extension in its
 *      initialize response, so the host knows to look for widgets at all.
 *   2. The widget HTML is served as a `ui://` resource with the MCP App MIME
 *      type.
 *   3. The tool carries `_meta.ui.resourceUri` naming that resource, which is
 *      what associates a result with the widget that draws it. That part lives
 *      with the tool, in execute_query_tool.ts, and reads `uiToolMeta` below.
 *
 * Everything the widget needs arrives over postMessage from the host, so it makes
 * no network request of its own and the built HTML has no external script, style,
 * font, or image reference. Two consequences worth stating, because both remove
 * work a reader might expect to find here: no `_meta.ui.csp` is emitted, since
 * there are no origins for a host to allowlist; and the server needs no notion of
 * its own public URL, which it could not reliably have anyway (an arbitrary local
 * port, Docker, `npx`, or behind a proxy that terminates elsewhere).
 */

/** Per the MCP Apps spec: the MIME type marking a resource as a widget. */
export const MCP_APP_MIME_TYPE = "text/html;profile=mcp-app";

/** Per the MCP Apps spec: the capability key a UI-capable server declares. */
export const UI_EXTENSION_NAME = "io.modelcontextprotocol/ui";

/** The widget that renders a malloy_executeQuery result. */
export const EXECUTE_QUERY_UI_URI = "ui://execute-query/app.html";

/** That widget's filename inside the widget directory. */
export const EXECUTE_QUERY_WIDGET_FILE = "execute-query.html";

/**
 * Locates the built widget directory.
 *
 * Mirrors `resolveWorkerScript` in package_load/package_load_pool.ts, and for the
 * same reason: this module is bundled INTO dist/server.mjs, so `import.meta.url`
 * resolves to the bundle rather than to this source file. The file extension is
 * what distinguishes the two cases.
 *
 * - Bundled as `dist/server.mjs`: the widgets sit beside it in `dist/mcp-apps/`,
 *   put there by the copy step in packages/server/build.ts.
 * - From source as `src/mcp/ui_resources.ts`: read the widget package's own build
 *   output, so `bun run start:dev` serves widgets too.
 */
export function resolveWidgetDir(): string {
   const thisFile = fileURLToPath(import.meta.url);
   const thisDir = dirname(thisFile);
   if (thisFile.endsWith(".mjs") || thisFile.endsWith(".js")) {
      return join(thisDir, "mcp-apps");
   }
   return join(thisDir, "..", "..", "..", "mcp-apps", "dist");
}

/**
 * Whether a widget file exists, keyed by full path.
 *
 * EXISTENCE is cached; the CONTENT is not. A fresh McpServer is built for every
 * MCP POST (see initializeMcpServer), and registration has to decide whether to
 * advertise a widget on each one, so that decision must not cost a
 * multi-megabyte read. Serving it must not cost a permanently resident copy
 * either: the bundle is several megabytes, a `resources/read` is rare because
 * hosts fetch a widget once per instance, and holding it for the life of the
 * process would be paying memory forever for an occasional read. Credible's
 * server reads its widget from disk on every read for the same reason.
 *
 * A cached `false` keeps a missing bundle from being re-stat-ed on every request.
 * Both callers below consult this, which is what stops `uiToolMeta` and
 * `registerUiResources` from disagreeing about whether a widget exists.
 *
 * Reading on demand also means rebuilding a widget takes effect without
 * restarting the server, as long as it existed when the server first looked.
 */
let widgetExistsCache = new Map<string, boolean>();

function widgetPath(widgetDir: string, file: string): string {
   return join(widgetDir, file);
}

function widgetExists(widgetDir: string, file: string): boolean {
   const path = widgetPath(widgetDir, file);
   const cached = widgetExistsCache.get(path);
   if (cached !== undefined) return cached;

   let exists = false;
   try {
      exists = fs.statSync(path).isFile();
   } catch (error) {
      // Absent is ordinary when running from source without having built the
      // widget package, so it is a debug line rather than a fault. Any other
      // failure is a warning, because the widget will silently not appear.
      const code = (error as NodeJS.ErrnoException).code;
      if (code === "ENOENT") {
         logger.debug("[MCP Apps] No widget bundle; widgets disabled", {
            path,
         });
      } else {
         logger.warn("[MCP Apps] Could not stat widget bundle", { path, code });
      }
   }
   widgetExistsCache.set(path, exists);
   return exists;
}

/** Clears the per-process widget existence cache. For tests. */
export function resetWidgetCache(): void {
   widgetExistsCache = new Map();
}

/** Whether the execute-query widget is available to serve. */
export function hasExecuteQueryWidget(
   widgetDir: string = resolveWidgetDir(),
): boolean {
   return widgetExists(widgetDir, EXECUTE_QUERY_WIDGET_FILE);
}

/**
 * The `_meta` that associates a tool with the widget rendering its result.
 *
 * Returns undefined when the widget is not built, so the tool is registered
 * without it rather than pointing at a resource that does not exist.
 *
 * Two keys carry the same URI: `ui.resourceUri` is the MCP Apps spec's, and
 * `openai/outputTemplate` is the equivalent ChatGPT reads. A server emitting only
 * one is invisible to the other host, so Credible's tools emit both and so do
 * these.
 */
export function uiToolMeta(
   resourceUri: string,
   widgetDir: string = resolveWidgetDir(),
): Record<string, unknown> | undefined {
   if (!hasExecuteQueryWidget(widgetDir)) return undefined;
   return {
      ui: { resourceUri },
      "openai/outputTemplate": resourceUri,
   };
}

/**
 * Declares the UI extension and registers the widget resources.
 *
 * Must be called before the server is connected to a transport:
 * `registerCapabilities` throws afterwards.
 *
 * Returns the URIs registered, so a caller (or a spec) can distinguish "widgets
 * served" from "no bundle present" without reaching into the server.
 */
export function registerUiResources(
   mcpServer: McpServer,
   widgetDir: string = resolveWidgetDir(),
): string[] {
   if (!widgetExists(widgetDir, EXECUTE_QUERY_WIDGET_FILE)) {
      return [];
   }
   const path = widgetPath(widgetDir, EXECUTE_QUERY_WIDGET_FILE);

   // The SDK's ServerCapabilities schema is a passthrough object, so this
   // specified-but-not-yet-typed key survives validation and reaches the wire.
   // The cast is only because the SDK has no field for it.
   mcpServer.server.registerCapabilities({
      extensions: {
         [UI_EXTENSION_NAME]: { mimeTypes: [MCP_APP_MIME_TYPE] },
      },
   } as Parameters<typeof mcpServer.server.registerCapabilities>[0]);

   mcpServer.registerResource(
      "execute-query-ui",
      EXECUTE_QUERY_UI_URI,
      {
         title: "Malloy query result",
         description:
            "Renders a malloy_executeQuery result with the Malloy renderer: charts, tables and dashboards as the query's annotation tags describe them.",
         mimeType: MCP_APP_MIME_TYPE,
      },
      // Read on demand rather than closed over: see widgetExistsCache above.
      //
      // The catch is not decoration. This endpoint is unauthenticated, and a raw
      // fs error carries the absolute path, which on a developer machine means
      // the OS user name and on a deployment means the install layout. Node's
      // message is logged where an operator can act on it and replaced with one
      // that says what to do, so a failed read stays a failed read rather than
      // becoming disclosure. Throwing at all is deliberate: serving an empty
      // document instead would render as a blank card explaining nothing.
      () => {
         let html: string;
         try {
            html = fs.readFileSync(path, "utf8");
         } catch (error) {
            logger.error("[MCP Apps] Failed to read widget bundle", {
               path,
               code: (error as NodeJS.ErrnoException).code,
            });
            throw new McpError(
               ErrorCode.InternalError,
               "The MCP Apps widget bundle could not be read. Rebuild it with `bun run build:mcp-apps`.",
            );
         }
         return {
            contents: [
               {
                  uri: EXECUTE_QUERY_UI_URI,
                  mimeType: MCP_APP_MIME_TYPE,
                  text: html,
               },
            ],
         };
      },
   );

   return [EXECUTE_QUERY_UI_URI];
}
