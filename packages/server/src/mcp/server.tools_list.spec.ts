// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { initializeMcpServer } from "./server";
import type { EnvironmentStore } from "../service/environment_store";

/**
 * Pins the full tools/list and prompts/list output byte for byte: tool names,
 * descriptions and input schemas are the contract an agent was written against.
 *
 * Regenerate after an INTENDED change, then review the golden's diff:
 *
 *    UPDATE_GOLDEN=1 bun test src/mcp/server.tools_list.spec.ts
 */
const GOLDEN_PATH = path.join(__dirname, "testdata", "tools_list.golden.json");

describe("MCP tools/list and prompts/list", () => {
   let listing: string;

   beforeAll(async () => {
      const server = initializeMcpServer({} as unknown as EnvironmentStore);
      const [clientTransport, serverTransport] =
         InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);
      const client = new Client({ name: "tools-list-pin", version: "0.0.0" });
      await client.connect(clientTransport);
      const { tools } = await client.listTools();
      const { prompts } = await client.listPrompts();
      listing = `${JSON.stringify({ tools, prompts }, null, 2)}\n`;
      await client.close();
   });

   it("matches the committed golden", () => {
      if (process.env.UPDATE_GOLDEN === "1") {
         fs.mkdirSync(path.dirname(GOLDEN_PATH), { recursive: true });
         fs.writeFileSync(GOLDEN_PATH, listing);
         return;
      }
      // A Windows checkout may convert the golden's line endings to CRLF.
      const golden = fs
         .readFileSync(GOLDEN_PATH, "utf8")
         .replace(/\r\n/g, "\n");
      // Structural first, so a failure names the field that moved.
      expect(JSON.parse(listing)).toEqual(JSON.parse(golden));
      expect(listing).toBe(golden);
   });
});
