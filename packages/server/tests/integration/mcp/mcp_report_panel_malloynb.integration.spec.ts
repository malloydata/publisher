// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A `.malloynb` shown through the MCP report tools, on a curated package:
 * `list_reports` lists it, `show_report` opens it, and the panel's
 * `report_api` runs its cells as the REST query route does, the surface and
 * `#(authorize)` included, because both call the same controller. MCP
 * `execute_query` still refuses the notebook's path.
 */

import { rm } from "fs/promises";
import path from "path";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { malloynbCellQuery } from "../../../../sdk/src/components/Report/malloynb/cellKind";
import { initializeMcpServer } from "../../../src/mcp/server";
import { bootMainServer } from "../../harness/main_server";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

const ENV = "mcp-report-panel-malloynb-env";
/** The fixture as published, behind its `index.malloy` surface. */
const PKG = "malloynb-curated";
/** The same files with no `index.malloy`: every source is on the surface. */
const OPEN = "malloynb-open";

interface Cell {
   type: "markdown" | "code";
   text: string;
}

type Outcome = { rows: unknown[][] } | { refused: number; message?: string };

/** An answer as rows of plain values, or the refusal's status and message. */
const outcomeOf = (
   status: number,
   body: { result?: string; message?: string },
): Outcome => {
   if (status !== 200 || body.result === undefined)
      return { refused: status, message: body.message };
   const parsed = JSON.parse(body.result) as {
      data?: {
         array_value?: {
            record_value?: { number_value?: number; string_value?: string }[];
         }[];
      };
   };
   return {
      rows: (parsed.data?.array_value ?? []).map((row) =>
         (row.record_value ?? []).map(
            (cell) => cell.number_value ?? cell.string_value ?? null,
         ),
      ),
   };
};

describe(".malloynb through the MCP report tools (E2E)", () => {
   let env: PackageEnv;
   let client: Client;

   beforeAll(async () => {
      env = await startPackageEnv(
         ENV,
         {
            [PKG]: fixture("malloynb-parity"),
            [OPEN]: fixture("malloynb-parity"),
         },
         {
            prepare: async (dir, pkg) => {
               if (pkg === OPEN) await rm(path.join(dir, "index.malloy"));
            },
         },
      );
      // The process's own services, the ones the REST routes call.
      const { services } = await bootMainServer();
      const server = initializeMcpServer(services);
      const [clientTransport, serverTransport] =
         InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);
      client = new Client({ name: "malloynb-panel-test", version: "0.0.0" });
      await client.connect(clientTransport);
   }, 300_000);

   afterAll(async () => {
      await client?.close();
      await env?.stop();
   });

   /** The `run:` the SDK's `Report` sends for cell `i` of `nb`. */
   const cellQuery = async (nb: string, i: number): Promise<string> => {
      const report = await env.getJson<{
         notebook?: { notebookCells?: Cell[] };
      }>(env.api(`/reports/notebooks/${nb}`));
      const query = malloynbCellQuery(
         report.notebook?.notebookCells?.[i] ?? {},
      );
      if (query === undefined) throw new Error(`${nb} cell ${i} has no run:`);
      return query;
   };

   /** Cell `i` of `nb` over REST and through `report_api`, as outcomes. */
   const both = async (nb: string, i: number, givens: object = {}) => {
      const query = await cellQuery(nb, i);
      const res = await env.query(`notebooks/${nb}`, { query, givens });
      const rest = outcomeOf(res.status, await res.json());
      const result = await client.callTool({
         name: "report_api",
         arguments: {
            operation: "query",
            environmentName: ENV,
            packageName: PKG,
            path: `notebooks/${nb}`,
            body: { query, givens },
         },
      });
      const answer = result.structuredContent as {
         status: number;
         data: { result?: string; message?: string };
      };
      expect(result.isError).toBe(answer.status >= 400);
      return { rest, panel: outcomeOf(answer.status, answer.data) };
   };

   it("list_reports lists a .malloynb, linear", async () => {
      const result = await client.callTool({
         name: "list_reports",
         arguments: { environmentName: ENV, packageName: PKG },
      });
      expect(result.isError).toBe(false);
      const block = (result.content as { resource?: { text: string } }[])[0];
      const reports = (
         JSON.parse(block.resource!.text) as {
            reports: { path: string; layout: string }[];
         }
      ).reports;
      expect(reports).toContainEqual(
         expect.objectContaining({
            path: "notebooks/surface.malloynb",
            layout: "linear",
         }),
      );
   });

   it("show_report opens a .malloynb in the panel", async () => {
      const result = await client.callTool({
         name: "show_report",
         arguments: {
            environmentName: ENV,
            packageName: PKG,
            path: "notebooks/authorize.malloynb",
         },
      });
      expect(result.isError).toBeFalsy();
      expect(result.structuredContent).toMatchObject({
         environmentName: ENV,
         packageName: PKG,
         path: "notebooks/authorize.malloynb",
         layout: "linear",
      });
   });

   it("report_api reads a .malloynb's cells as GET …/reports/{path} does", async () => {
      const result = await client.callTool({
         name: "report_api",
         arguments: {
            operation: "getReport",
            environmentName: ENV,
            packageName: PKG,
            path: "notebooks/surface.malloynb",
         },
      });
      expect(result.isError).toBe(false);
      const rest = await env.getJson(
         env.api("/reports/notebooks/surface.malloynb"),
      );
      expect((result.structuredContent as { data: unknown }).data).toEqual(
         rest,
      );
   });

   it("report_api runs a published source's cell as REST does", async () => {
      const { rest, panel } = await both("surface.malloynb", 2);
      expect(rest).toEqual({ rows: [[6]] });
      expect(panel).toEqual(rest);
   });

   it("report_api refuses a cell over a hidden source with REST's 404", async () => {
      const { rest, panel } = await both("surface.malloynb", 3);
      expect(rest).toMatchObject({ refused: 404 });
      expect(panel).toEqual(rest);
   });

   it("report_api applies #(authorize) as REST does: 403 without the role, rows with it", async () => {
      const denied = await both("authorize.malloynb", 1, {
         ROLES: ["viewer"],
      });
      expect(denied.rest).toEqual({
         refused: 403,
         message: 'Access denied for source "locked".',
      });
      expect(denied.panel).toEqual(denied.rest);
      const allowed = await both("authorize.malloynb", 1, {
         ROLES: ["admin"],
      });
      expect(allowed.rest).toEqual({ rows: [[6]] });
      expect(allowed.panel).toEqual(allowed.rest);
   });

   it("execute_query still refuses a .malloynb's path, with a surface or without one", async () => {
      for (const pkg of [PKG, OPEN]) {
         const result = await client.callTool({
            name: "execute_query",
            arguments: {
               environmentName: ENV,
               packageName: pkg,
               modelPath: "notebooks/surface.malloynb",
               query: "run: orders -> { aggregate: order_count }",
            },
         });
         expect(result.isError).toBe(true);
         expect(JSON.stringify(result.content)).toContain(
            `Resource not found: model 'notebooks/surface.malloynb' in package '${pkg}' for environment '${ENV}'`,
         );
      }
   });
});
