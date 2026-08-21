// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
   EXECUTE_QUERY_UI_URI,
   EXECUTE_QUERY_WIDGET_FILE,
   MCP_APP_MIME_TYPE,
   hasExecuteQueryWidget,
   registerUiResources,
   resetWidgetCache,
   resolveWidgetDir,
   uiToolMeta,
} from "./ui_resources";

const WIDGET_HTML = "<!doctype html><title>widget</title><div id=root></div>";

function newServer(): McpServer {
   return new McpServer({ name: "test", version: "0.0.0" });
}

/**
 * The capability declaration itself is asserted in ui_resources.protocol.spec.ts
 * instead of here, in both the present and absent cases. Reading it off an
 * unconnected server needs the SDK's private `getCapabilities`; over the
 * in-memory transport the client's `getServerCapabilities()` is public, and it
 * proves the extra key survives the wire rather than merely surviving our own
 * call.
 */
describe("ui_resources", () => {
   let widgetDir: string;

   beforeEach(() => {
      resetWidgetCache();
      widgetDir = fs.mkdtempSync(join(os.tmpdir(), "publisher-widget-"));
   });

   afterEach(() => {
      resetWidgetCache();
      fs.rmSync(widgetDir, { recursive: true, force: true });
   });

   function writeWidget(contents = WIDGET_HTML) {
      fs.writeFileSync(join(widgetDir, EXECUTE_QUERY_WIDGET_FILE), contents);
   }

   describe("when the widget bundle is absent", () => {
      it("reports no widget", () => {
         expect(hasExecuteQueryWidget(widgetDir)).toBe(false);
      });

      it("registers nothing", () => {
         expect(registerUiResources(newServer(), widgetDir)).toEqual([]);
      });

      it("gives the tool no widget metadata", () => {
         // So malloy_executeQuery registers exactly as it did before this
         // feature, and every client sees today's behaviour.
         expect(uiToolMeta(EXECUTE_QUERY_UI_URI, widgetDir)).toBeUndefined();
      });
   });

   describe("when the widget bundle is present", () => {
      beforeEach(() => writeWidget());

      it("reports the widget", () => {
         expect(hasExecuteQueryWidget(widgetDir)).toBe(true);
      });

      it("registers the execute-query resource", () => {
         expect(registerUiResources(newServer(), widgetDir)).toEqual([
            EXECUTE_QUERY_UI_URI,
         ]);
      });

      it("serves the widget HTML with the MCP App MIME type", async () => {
         const server = newServer();
         registerUiResources(server, widgetDir);
         const result = await readResource(server, EXECUTE_QUERY_UI_URI);
         expect(result.contents).toHaveLength(1);
         expect(result.contents[0].mimeType).toBe(MCP_APP_MIME_TYPE);
         expect(result.contents[0].text).toBe(WIDGET_HTML);
         expect(result.contents[0].uri).toBe(EXECUTE_QUERY_UI_URI);
      });

      it("emits both the spec and the ChatGPT widget-association keys", () => {
         // A server emitting only one of these is invisible to the other host.
         expect(uiToolMeta(EXECUTE_QUERY_UI_URI, widgetDir)).toEqual({
            ui: { resourceUri: EXECUTE_QUERY_UI_URI },
            "openai/outputTemplate": EXECUTE_QUERY_UI_URI,
         });
      });

      it("emits no CSP metadata, because the widget loads nothing external", () => {
         // If the bundle ever stops being self-contained, it needs CSP
         // resourceDomains and this assertion should fail rather than the widget
         // silently rendering blank in a host that blocks the fetch.
         const meta = uiToolMeta(EXECUTE_QUERY_UI_URI, widgetDir) as {
            ui: Record<string, unknown>;
         };
         expect(meta.ui.csp).toBeUndefined();
      });

      it("caches EXISTENCE, so the per-request path costs one stat at most", () => {
         // Registration runs on every MCP POST and has to decide whether to
         // advertise a widget, so that decision must not re-stat each time.
         expect(hasExecuteQueryWidget(widgetDir)).toBe(true);
         fs.rmSync(join(widgetDir, EXECUTE_QUERY_WIDGET_FILE));
         expect(hasExecuteQueryWidget(widgetDir)).toBe(true);
         resetWidgetCache();
         expect(hasExecuteQueryWidget(widgetDir)).toBe(false);
      });

      it("caches absence too, so a missing bundle is not re-stat-ed", () => {
         resetWidgetCache();
         fs.rmSync(join(widgetDir, EXECUTE_QUERY_WIDGET_FILE));
         expect(hasExecuteQueryWidget(widgetDir)).toBe(false);
         writeWidget();
         expect(hasExecuteQueryWidget(widgetDir)).toBe(false);
      });

      it("does NOT cache the content, so it holds no resident copy", async () => {
         // The bundle is several megabytes and a read is rare, so keeping it in
         // memory for the life of the process would pay that cost forever for an
         // occasional request. Rewriting the file between two reads must show
         // through; if it does not, the content is being held somewhere.
         const server = newServer();
         registerUiResources(server, widgetDir);
         const first = await readResource(server, EXECUTE_QUERY_UI_URI);
         expect(first.contents[0].text).toBe(WIDGET_HTML);

         const rebuilt = "<!doctype html><title>rebuilt</title>";
         writeWidget(rebuilt);
         const second = await readResource(server, EXECUTE_QUERY_UI_URI);
         expect(second.contents[0].text).toBe(rebuilt);
      });

      it("errors on the one request rather than serving a blank document", async () => {
         // The bundle deleted under a running server. An error names the problem;
         // an empty document would render as a blank card explaining nothing.
         const server = newServer();
         registerUiResources(server, widgetDir);
         fs.rmSync(join(widgetDir, EXECUTE_QUERY_WIDGET_FILE));
         await expect(
            readResource(server, EXECUTE_QUERY_UI_URI),
         ).rejects.toThrow(/could not be read/i);
      });

      it("does not put the filesystem path in the error a client sees", async () => {
         // This endpoint is unauthenticated. Node's own ENOENT message carries the
         // absolute path, which leaks the OS user name on a developer machine and
         // the install layout on a deployment, so the message is replaced rather
         // than passed through. The path still reaches the server log.
         const server = newServer();
         registerUiResources(server, widgetDir);
         fs.rmSync(join(widgetDir, EXECUTE_QUERY_WIDGET_FILE));
         const message = await readResource(server, EXECUTE_QUERY_UI_URI).then(
            () => "resolved, but it should have thrown",
            (error: unknown) => (error as Error).message,
         );
         expect(message).not.toContain(widgetDir);
         expect(message).not.toContain(EXECUTE_QUERY_WIDGET_FILE);
         expect(message).not.toMatch(/ENOENT|no such file/i);
         expect(message).toMatch(/build:mcp-apps/);
      });
   });

   describe("resolveWidgetDir", () => {
      it("resolves the widget package's build output when running from source", () => {
         // Under `bun test` this file runs from source, so the from-source branch
         // is the one exercised: packages/mcp-apps/dist, the widget package's own
         // build output. The bundled branch (dist/mcp-apps, beside server.mjs) is
         // covered by actually booting a built server.
         //
         // Built with join() against this spec's own directory rather than matched
         // against a slash-separated literal. resolveWidgetDir returns a PLATFORM
         // path, so a regex containing "/" passes on macOS and Linux and fails on
         // Windows, where join() separates with backslashes. Loosening the regex
         // would hide that rather than express it. This spec sits beside
         // ui_resources.ts, so the expected value is the same walk the function
         // makes, which also pins the number of parent steps: the old assertion
         // only looked at the tail and would have accepted any depth.
         const moduleDir = dirname(fileURLToPath(import.meta.url));
         expect(resolveWidgetDir()).toBe(
            join(moduleDir, "..", "..", "..", "mcp-apps", "dist"),
         );
      });
   });
});

/** Invokes a registered resource's read callback through the server. */
async function readResource(
   server: McpServer,
   uri: string,
): Promise<{
   contents: { uri: string; mimeType?: string; text?: string }[];
}> {
   const registered = (
      server as unknown as {
         _registeredResources: Record<
            string,
            {
               readCallback: (
                  url: URL,
                  extra: unknown,
               ) => Promise<{
                  contents: {
                     uri: string;
                     mimeType?: string;
                     text?: string;
                  }[];
               }>;
            }
         >;
      }
   )._registeredResources[uri];
   expect(registered).toBeDefined();
   return registered.readCallback(new URL(uri), {});
}
