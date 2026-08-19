import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import { join } from "path";
import { z } from "zod";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
   EXECUTE_QUERY_UI_URI,
   EXECUTE_QUERY_WIDGET_FILE,
   MCP_APP_MIME_TYPE,
   UI_EXTENSION_NAME,
   registerUiResources,
   resetWidgetCache,
} from "./ui_resources";

/**
 * MCP Apps support over the real MCP protocol, using the SDK's in-memory
 * transport.
 *
 * Everything the widget feature relies on is a claim about the SDK, and every one
 * of those claims is checked here on the wire rather than assumed:
 *
 *   - a non-standard `extensions` capability survives ServerCapabilities
 *     validation and reaches the client (the schema is a passthrough object);
 *   - `ui://` is an acceptable resource URI scheme;
 *   - a resource can carry a non-JSON, non-text-plain MIME type per content item;
 *   - `registerTool`'s `_meta` reaches `tools/list`, which is the only channel
 *     that associates a tool with its widget.
 *
 * The pinned SDK is 1.18.1, which predates typed support for the UI extension, so
 * if a later bump changed any of the above the feature would silently stop
 * working: a host would see no widget and simply render text. These fail loudly
 * instead.
 *
 * The widget bundle is written into a temp directory rather than read from
 * packages/mcp-apps/dist, so this spec passes on a fresh clone that has not built
 * the widget package.
 */
const WIDGET_HTML =
   "<!doctype html><title>Malloy Query Result</title><div id=root></div>";

describe("MCP Apps over the MCP protocol (in-memory)", () => {
   let client: Client;
   let widgetDir: string;

   beforeAll(async () => {
      resetWidgetCache();
      widgetDir = fs.mkdtempSync(join(os.tmpdir(), "publisher-widget-proto-"));
      fs.writeFileSync(join(widgetDir, EXECUTE_QUERY_WIDGET_FILE), WIDGET_HTML);

      const server = new McpServer({ name: "ui-proto-test", version: "0.0.0" });
      registerUiResources(server, widgetDir);

      // A stand-in for malloy_executeQuery, carrying the same `_meta` shape it
      // does. Registered here rather than using the real tool so this spec needs
      // no EnvironmentStore, and so what it proves is precisely the SDK's
      // forwarding of `_meta`; that the real tool passes the right `_meta` is
      // covered in ui_resources.spec.ts.
      server.registerTool(
         "widget_tool",
         {
            description: "A tool whose result is rendered by a widget.",
            inputSchema: { q: z.string() },
            _meta: {
               ui: { resourceUri: EXECUTE_QUERY_UI_URI },
               "openai/outputTemplate": EXECUTE_QUERY_UI_URI,
            },
         },
         async () => ({ content: [{ type: "text" as const, text: "ok" }] }),
      );

      const [clientTransport, serverTransport] =
         InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);
      client = new Client({ name: "ui-proto-client", version: "0.0.0" });
      await client.connect(clientTransport);
   });

   afterAll(() => {
      resetWidgetCache();
      fs.rmSync(widgetDir, { recursive: true, force: true });
   });

   it("declares the UI extension in the initialize response", () => {
      // The SDK has no typed field for `extensions`; this proves the passthrough
      // schema really does carry it to the client, which is what tells a host to
      // look for widgets at all.
      const capabilities = client.getServerCapabilities() as unknown as Record<
         string,
         unknown
      >;
      expect(capabilities.extensions).toEqual({
         [UI_EXTENSION_NAME]: { mimeTypes: [MCP_APP_MIME_TYPE] },
      });
   });

   it("declares NO UI extension when the widget bundle is absent", async () => {
      // The graceful-absence path, on the wire. Declaring the extension with no
      // resource to read is worse than staying silent: the host goes looking for
      // a widget that is not there and shows a broken card. Note the server also
      // declares no `resources` capability at all in this state, which is exactly
      // how it behaved before this feature existed.
      resetWidgetCache();
      const emptyDir = fs.mkdtempSync(
         join(os.tmpdir(), "publisher-widget-none-"),
      );
      try {
         const server = new McpServer({ name: "no-ui", version: "0.0.0" });
         expect(registerUiResources(server, emptyDir)).toEqual([]);
         const [ct, st] = InMemoryTransport.createLinkedPair();
         await server.connect(st);
         const bare = new Client({ name: "no-ui-client", version: "0.0.0" });
         await bare.connect(ct);
         const capabilities = bare.getServerCapabilities() as unknown as Record<
            string,
            unknown
         >;
         expect(capabilities.extensions).toBeUndefined();
         expect(capabilities.resources).toBeUndefined();
         await bare.close();
      } finally {
         fs.rmSync(emptyDir, { recursive: true, force: true });
         resetWidgetCache();
      }
   });

   it("lists the widget as a ui:// resource", async () => {
      const { resources } = await client.listResources();
      const widget = resources.find((r) => r.uri === EXECUTE_QUERY_UI_URI);
      expect(widget).toBeDefined();
      expect(widget?.mimeType).toBe(MCP_APP_MIME_TYPE);
   });

   it("serves the widget HTML with the MCP App MIME type", async () => {
      const result = await client.readResource({ uri: EXECUTE_QUERY_UI_URI });
      expect(result.contents).toHaveLength(1);
      expect(result.contents[0].mimeType).toBe(MCP_APP_MIME_TYPE);
      expect(result.contents[0].text).toBe(WIDGET_HTML);
   });

   it("forwards a tool's _meta to tools/list", async () => {
      // The association between a tool result and the widget that renders it.
      // Without this the widget is served but never used.
      const { tools } = await client.listTools();
      const tool = tools.find((t) => t.name === "widget_tool");
      expect(tool?._meta).toEqual({
         ui: { resourceUri: EXECUTE_QUERY_UI_URI },
         "openai/outputTemplate": EXECUTE_QUERY_UI_URI,
      });
   });
});
