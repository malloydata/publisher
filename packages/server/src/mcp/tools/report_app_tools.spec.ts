// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import type { QueryController } from "../../controller/query.controller";
import { ReportController } from "../../controller/report.controller";
import { DashboardNotFoundError } from "../../errors";
import type { EnvironmentStore } from "../../service/environment_store";
import type { ThemeStore } from "../../service/theme_store";
import { mcpServicesFor } from "../../test_helpers/mcp_services";
import { initializeMcpServer } from "../server";
import {
   answerReportApi,
   consoleReportUrl,
   MCP_APP_MIME_TYPE,
   REPORT_PANEL_URI,
   reportAppOrigin,
   reportPanelHtml,
} from "./report_app_tools";

const MANIFEST = {
   resource:
      "/environments/examples/packages/storefront/reports/sales/q3.malloy",
   packageName: "storefront",
   path: "sales/q3.malloy",
   name: "q3",
   layout: "grid",
   title: "Q3 sales",
   editable: true,
   tiles: [
      { kind: "query", query: "orders -> by_month" },
      { kind: "query", query: "orders -> by_state" },
      { kind: "text", name: "intro", markdown: "# Hi" },
   ],
   givens: [
      { name: "REGION", label: "Region" },
      { name: "SINCE", label: undefined },
   ],
};

function stubStore(): EnvironmentStore {
   const pkg = {
      getReport: async (p: string) =>
         p === MANIFEST.path ? MANIFEST : undefined,
      listReports: async () => [
         {
            path: MANIFEST.path,
            layout: "grid",
            title: "Q3 sales",
            name: "q3",
            editable: true,
            description: "Quarterly",
         },
         {
            path: "story.malloy",
            layout: "linear",
            title: "Story",
            name: "story",
            editable: true,
         },
         {
            path: "notebooks/old.malloynb",
            layout: "linear",
            title: "Old",
            name: "old",
            editable: false,
         },
      ],
   };
   return {
      getEnvironment: async (name: string) => {
         if (name !== "examples")
            throw new Error(`Environment not found: ${name}`);
         return {
            getPackage: async (p: string) => {
               if (p !== "storefront")
                  throw new Error(`Package not found: ${p}`);
               return pkg;
            },
         };
      },
      getStatus: async () => ({
         operationalState: "serving",
         frozenConfig: false,
         modelPage: "viewer",
         environments: [{ name: "secret-env" }],
      }),
      storageManager: { isInitialized: () => false },
   } as unknown as EnvironmentStore;
}

describe("report panel tools over the MCP protocol", () => {
   let client: Client;

   beforeAll(async () => {
      const server = initializeMcpServer(mcpServicesFor(stubStore()));
      const [clientTransport, serverTransport] =
         InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);
      client = new Client({ name: "report-app-test", version: "0.0.0" });
      await client.connect(clientTransport);
   });

   it("links show_report to the panel, and hides report_api from the model", async () => {
      const { tools } = await client.listTools();
      const byName = new Map(tools.map((t) => [t.name, t]));
      expect(byName.has("list_reports")).toBe(true);
      const show = byName.get("show_report");
      expect(show?._meta?.ui).toEqual({ resourceUri: REPORT_PANEL_URI });
      expect(show?._meta?.["ui/resourceUri"]).toBe(REPORT_PANEL_URI);
      const api = byName.get("report_api");
      expect(api?._meta?.ui).toEqual({
         resourceUri: REPORT_PANEL_URI,
         visibility: ["app"],
      });
   });

   it("serves the panel as an MCP App resource that loads from the REST origin", async () => {
      const { resources } = await client.listResources();
      const listed = resources.find((r) => r.uri === REPORT_PANEL_URI);
      expect(listed?.mimeType).toBe(MCP_APP_MIME_TYPE);

      const read = await client.readResource({ uri: REPORT_PANEL_URI });
      const content = read.contents[0] as unknown as {
         text: string;
         mimeType: string;
         _meta: {
            ui: {
               csp: { resourceDomains: string[]; connectDomains: string[] };
            };
         };
      };
      expect(content.mimeType).toBe(MCP_APP_MIME_TYPE);
      const origin = reportAppOrigin();
      expect(content._meta.ui.csp.resourceDomains).toEqual([origin]);
      expect(content._meta.ui.csp.connectDomains).toEqual([]);
      expect(content.text).toContain(
         `<script src="${origin}/mcp-app/report.js?v=`,
      );
      expect(content.text).toContain(`href="${origin}/mcp-app/report.css?v=`);
      // A shell, not the bundle.
      expect(content.text.length).toBeLessThan(2000);
   });

   it("lists a package's reports by path, leaving out the notebooks the panel cannot run", async () => {
      const result = await client.callTool({
         name: "list_reports",
         arguments: { environmentName: "examples", packageName: "storefront" },
      });
      expect(result.isError).toBe(false);
      const block = (result.content as { resource?: { text: string } }[])[0];
      const payload = JSON.parse(block.resource!.text);
      expect(payload.reports).toEqual([
         {
            path: "sales/q3.malloy",
            layout: "grid",
            title: "Q3 sales",
            description: "Quarterly",
         },
         { path: "story.malloy", layout: "linear", title: "Story" },
      ]);
   });

   it("shows a report: says what it holds, and hands the panel which one", async () => {
      const result = await client.callTool({
         name: "show_report",
         arguments: {
            environmentName: "examples",
            packageName: "storefront",
            path: "sales/q3.malloy",
            givens: { REGION: "West" },
         },
      });
      expect(result.isError).toBeFalsy();
      const text = (result.content as { text: string }[])[0].text;
      expect(text).toContain(
         'Showing "Q3 sales" (a grid report, 2 query tiles, 1 text).',
      );
      expect(text).toContain("Its filters: Region, SINCE.");
      const url = `${reportAppOrigin()}/examples/storefront/sales/q3?REGION=West`;
      expect(text).toContain(`Open it in Publisher: ${url}`);
      expect(result.structuredContent).toEqual({
         environmentName: "examples",
         packageName: "storefront",
         path: "sales/q3.malloy",
         title: "Q3 sales",
         layout: "grid",
         givens: { REGION: "West" },
         url,
      });
   });

   it("refuses a report the package does not have", async () => {
      const result = await client.callTool({
         name: "show_report",
         arguments: {
            environmentName: "examples",
            packageName: "storefront",
            path: "nope.malloy",
         },
      });
      expect(result.isError).toBe(true);
      const text = (result.content as { type: string; text?: string }[])
         .filter((c) => c.type === "text")
         .map((c) => c.text)
         .join("\n");
      expect(text).toContain("nope.malloy does not exist");
      expect(text).toContain("Call list_reports for examples/storefront");
      expect(text).not.toContain("Try the request again later");
   });

   it("refuses a .malloynb notebook and points at the Console", async () => {
      const result = await client.callTool({
         name: "show_report",
         arguments: {
            environmentName: "examples",
            packageName: "storefront",
            path: "notebooks/old.malloynb",
         },
      });
      expect(result.isError).toBe(true);
      const text = (result.content as { type: string; text?: string }[])
         .filter((c) => c.type === "text")
         .map((c) => c.text)
         .join("\n");
      expect(text).toContain(
         "notebooks/old.malloynb is a .malloynb notebook, which the report panel does not run.",
      );
      expect(text).toContain(
         `${reportAppOrigin()}/examples/storefront/notebooks/old.malloynb`,
      );
   });

   it("answers the panel's reads through report_api with the REST status", async () => {
      const ok = await client.callTool({
         name: "report_api",
         arguments: {
            operation: "getReport",
            environmentName: "examples",
            packageName: "storefront",
            path: "sales/q3.malloy",
         },
      });
      expect(ok.isError).toBe(false);
      expect(ok.structuredContent).toEqual({ status: 200, data: MANIFEST });

      const missing = await client.callTool({
         name: "report_api",
         arguments: {
            operation: "getReport",
            environmentName: "examples",
            packageName: "storefront",
            path: "nope.malloy",
         },
      });
      expect(missing.isError).toBe(true);
      expect((missing.structuredContent as { status: number }).status).toBe(
         404,
      );
   });

   it("does not take an operation it was not built for", async () => {
      const result = await client.callTool({
         name: "report_api",
         arguments: { operation: "putReport" },
      });
      expect(result.isError).toBe(true);
   });
});

describe("answerReportApi", () => {
   const store = stubStore();
   const unusedTheme = {
      get: () => {
         throw new Error("theme store not expected");
      },
   } as unknown as ThemeStore;

   it("status carries only what the provider reads, never the environments", async () => {
      const answer = await answerReportApi(
         { operation: "status" },
         {
            environmentStore: store,
            reportController: new ReportController(store),
            queryController: {} as QueryController,
            themeStore: unusedTheme,
         },
      );
      expect(answer).toEqual({
         status: 200,
         data: { frozenConfig: false, modelPage: "viewer", theme: {} },
      });
   });

   it("a missing argument is a 400, not a throw", async () => {
      const answer = await answerReportApi(
         { operation: "getReport", environmentName: "examples" },
         {
            environmentStore: store,
            reportController: new ReportController(store),
            queryController: {} as QueryController,
            themeStore: unusedTheme,
         },
      );
      expect(answer.status).toBe(400);
   });

   it("runs a query as the REST route does: the caller from the headers, no bypass, a slot from the pool", async () => {
      const calls: unknown[][] = [];
      const queryController = {
         getQuery: async (...args: unknown[]) => {
            calls.push(args);
            return { result: "{}" };
         },
      } as unknown as QueryController;
      const signal = new AbortController().signal;
      const answer = await answerReportApi(
         {
            operation: "query",
            environmentName: "examples",
            packageName: "storefront",
            path: "sales/q3.malloy",
            versionId: "1.2.0",
            body: {
               query: "run: orders -> by_month",
               givens: { REGION: "West" },
            },
         },
         {
            environmentStore: store,
            reportController: new ReportController(store),
            queryController,
            themeStore: unusedTheme,
         },
         {
            signal,
            requestInfo: {
               headers: { "x-publisher-caller": "ana@example.com" },
            },
         },
      );
      expect(answer).toEqual({ status: 200, data: { result: "{}" } });
      const [request, options] = calls[0] as [
         Record<string, unknown>,
         { querySlotLabel?: string },
      ];
      expect(request).toMatchObject({
         environmentName: "examples",
         packageName: "storefront",
         modelPath: "sales/q3.malloy",
         sourceName: undefined,
         queryName: undefined,
         query: "run: orders -> by_month",
         compactJson: false,
         givens: { REGION: "West" },
         bypassAuthorize: false,
         clientSignal: signal,
      });
      expect(String(request.versionId)).toContain("1.2.0");
      // Taken inside the access record, so a refusal at the cap is recorded.
      expect(options).toEqual({ querySlotLabel: "mcp:reportPanel" });
   });

   it("runs a query on the version the body pins, and refuses two different ones", async () => {
      const versions: unknown[] = [];
      const queryController = {
         getQuery: async (request: { versionId: unknown }) => {
            versions.push(request.versionId);
            return { result: "{}" };
         },
      } as unknown as QueryController;
      const deps = {
         environmentStore: store,
         reportController: new ReportController(store),
         queryController,
         themeStore: unusedTheme,
      };
      const base = {
         operation: "query" as const,
         environmentName: "examples",
         packageName: "storefront",
         path: "sales/q3.malloy",
      };
      const pinned = await answerReportApi(
         { ...base, body: { queryName: "q", versionId: "1.2.0" } },
         deps,
      );
      expect(pinned.status).toBe(200);
      expect(String(versions[0])).toContain("1.2.0");

      const conflict = await answerReportApi(
         {
            ...base,
            versionId: "1.1.0",
            body: { queryName: "q", versionId: "1.2.0" },
         },
         deps,
      );
      expect(conflict.status).toBe(400);
      expect(versions).toHaveLength(1);
   });

   it("a refused query comes back as its REST status and body", async () => {
      const queryController = {
         getQuery: async () => {
            throw new DashboardNotFoundError("no such model");
         },
      } as unknown as QueryController;
      const answer = await answerReportApi(
         {
            operation: "query",
            environmentName: "examples",
            packageName: "storefront",
            path: "x.malloy",
            body: { queryName: "q" },
         },
         {
            environmentStore: store,
            reportController: new ReportController(store),
            queryController,
            themeStore: unusedTheme,
         },
      );
      expect(answer.status).toBe(404);
   });
});

describe("panel helpers", () => {
   it("cuts an origin to scheme, host and port, and refuses what is not one", () => {
      expect(reportAppOrigin("https://pub.example.com/some/path/")).toBe(
         "https://pub.example.com",
      );
      expect(reportAppOrigin("http://localhost:4000")).toBe(
         "http://localhost:4000",
      );
      expect(() => reportAppOrigin("localhost:4000")).toThrow(/MCP_APP_ORIGIN/);
      expect(() => reportAppOrigin("ftp://x")).toThrow(/http or https/);
   });

   it("escapes the origin into the panel's attributes", () => {
      const html = reportPanelHtml('http://a"b', "1.0.0");
      expect(html).toContain('src="http://a&quot;b/mcp-app/report.js?v=1.0.0"');
   });

   it("builds the Console route of a report, every segment encoded", () => {
      expect(
         consoleReportUrl("http://h", "my env", "p", "a b/c#d.malloy", {
            X: "1 2",
         }),
      ).toBe("http://h/my%20env/p/a%20b/c%23d?X=1+2");
   });
});
