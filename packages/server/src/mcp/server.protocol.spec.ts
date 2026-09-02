// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { initializeMcpServer } from "./server";
import { RELOAD_FAILURE_IS_SAFE } from "./tools/reload_package_tool";
import type { EnvironmentStore } from "../service/environment_store";

/**
 * End-to-end coverage of the unified MCP server over the real MCP protocol,
 * using the SDK's in-memory transport (no HTTP, no network, no DuckDB).
 * Exercises tool registration (including the agent retrieval tools that now
 * live on the same server), the dual-channel skill prompts, a real tool call
 * (searchDocs over the bundled index), and the getContext error path.
 */
describe("MCP server over the MCP protocol (in-memory)", () => {
   let client: Client;

   beforeAll(async () => {
      // searchDocs does not touch the store, and the tools' error paths only
      // need getEnvironment to reject. The one exception is `compiles-badly`,
      // which resolves to a compileSource returning an error diagnostic: that
      // is malloy_compile's own isError path (a bad Malloy snippet rather than
      // a thrown error), and it is only reachable with an environment that
      // exists.
      const stubStore = {
         getEnvironment: async (name: string) => {
            if (name === "compiles-badly") {
               return {
                  compileSource: async () => ({
                     problems: [
                        {
                           severity: "error",
                           message: "'nope' is not defined",
                           code: "field-not-found",
                           at: {
                              range: {
                                 start: { line: 2, character: 3 },
                                 end: { line: 2, character: 7 },
                              },
                           },
                        },
                     ],
                  }),
               };
            }
            throw new Error(`Environment not found: ${name}`);
         },
      } as unknown as EnvironmentStore;

      const server = initializeMcpServer(stubStore);
      const [clientTransport, serverTransport] =
         InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);
      client = new Client({ name: "mcp-protocol-test", version: "0.0.0" });
      await client.connect(clientTransport);
   });

   it("exposes the agent retrieval tools alongside the core tools", async () => {
      const { tools } = await client.listTools();
      const names = new Set(tools.map((t) => t.name));
      expect(names.has("malloy_getContext")).toBe(true);
      expect(names.has("malloy_searchDocs")).toBe(true);
      expect(names.has("malloy_executeQuery")).toBe(true);
      expect(names.has("malloy_compile")).toBe(true);
      expect(names.has("malloy_reloadPackage")).toBe(true);
      expect(names.has("malloy_searchDatabaseSchema")).toBe(true);
      expect(names.has("malloy_getStatus")).toBe(true);
   });

   /**
    * Tool descriptions are truncated by some clients. `malloy_getContext`'s was
    * observed arriving cut off mid-sentence at 2271 characters, and what a tail
    * cut removes is whatever the description put last.
    *
    * Two defenses, and the ordering one matters more. No single number is
    * portable, since the cap belongs to the client and is not published; this
    * budget only stops silent regrowth past the one length we have watched fail.
    * The real rule is that contract rules come BEFORE the reference material, so
    * a cut costs an agent the worked example rather than the invariants it
    * cannot self-correct without. See docs/agent-skills/tool-description-template.md.
    */
   it("keeps every tool description under the truncation budget", async () => {
      // Raised from 2000 once malloy_getContext's contract outgrew it, after
      // restructuring rather than extending the description. 2150 still sits
      // ~120 chars below 2271, the only length ever observed truncating, so
      // the margin is smaller but not gone. Raise it again only after the
      // same exercise: cut reference material first, and keep the contract
      // rules ahead of it (see the ordering test below, which is the defense
      // that actually matters).
      const BUDGET = 2150;
      const { tools } = await client.listTools();
      const oversized = tools
         .filter((t) => (t.description ?? "").length > BUDGET)
         .map((t) => `${t.name} (${t.description?.length})`);
      expect(oversized).toEqual([]);
   });

   it("puts contract rules ahead of the reference sections", async () => {
      // The section a tail-truncating client drops must never be the invariants.
      const { tools } = await client.listTools();
      for (const tool of tools) {
         const description = tool.description ?? "";
         const contract = description.indexOf("## Contract rules");
         if (contract === -1) continue;
         for (const later of ["## Response", "## Worked example"]) {
            const index = description.indexOf(later);
            if (index !== -1) {
               expect(contract).toBeLessThan(index);
            }
         }
      }
   });

   it("states the getContext response contract the payload actually ships", async () => {
      // Fields have shipped before the description explained them, which is
      // the expensive half: an agent cannot act on `sources` or on an empty
      // `joins` list it was never told about. Pin the contract terms to the
      // description so a new response field cannot land silently.
      const { tools } = await client.listTools();
      const description =
         tools.find((t) => t.name === "malloy_getContext")?.description ?? "";
      // snake_case throughout since the response converged on the hosted
      // retrieval API's shape: one shape, one parser, one mental model across
      // a local server and a hosted one.
      for (const term of [
         "entity_type",
         "sources",
         "source_info",
         "resource_id",
         "entities",
         "joins",
         "sourceName",
         "relationship",
         "also_in",
         "ranking",
         "total_available",
         "below_cutoff_count",
         "total_entities",
         "retrieval_reason",
         "warnings",
         "data_type",
         "one_line_summary",
         // The two an agent cannot recover from a failed query: a gated
         // source denies rather than erroring usefully, and a given it
         // never supplies silently takes the model's default.
         "givens",
         "authorize",
         // Not agent-actionable — an agent writes Malloy from name and
         // sourceName, never from a composite key — but pinned on the same
         // rule anyway, since a harness that cannot identify an entity
         // cannot score retrieval at all.
         "entity_id",
      ]) {
         expect(description).toContain(term);
      }
   });

   it("exposes the skill set as dual-channel prompts", async () => {
      const { prompts } = await client.listPrompts();
      expect(prompts.length).toBeGreaterThanOrEqual(24);
      expect(prompts.some((p) => p.name === "malloy-analysis")).toBe(true);
   });

   it("delivers orientation instructions to the connecting client", () => {
      const instructions = client.getInstructions();
      expect(instructions).toBeDefined();
      expect(instructions).toContain("malloy_getContext");
      // Pin the shared fragment, not just that instructions exist. The whole
      // point of exporting it is that this surface and the tool description
      // cannot drift, and they have drifted twice. Without this, deleting the
      // interpolation leaves the suite green and the drift comes back.
      expect(instructions).toContain(RELOAD_FAILURE_IS_SAFE);
      // The two working rules that keep an agent from trusting a stale or
      // silently empty model (see F1/F7 in the QA field notes): reload after
      // every edit, and never read an empty getContext as an empty package.
      expect(instructions).toContain(
         "After every model edit, call malloy_reloadPackage before querying",
      );
      expect(instructions).toContain("malloy_getStatus");
   });

   it("malloy_searchDocs returns relevant docs over the protocol", async () => {
      const res = await client.callTool({
         name: "malloy_searchDocs",
         arguments: { query: "window functions" },
      });
      const content = res.content as Array<{ resource?: { text?: string } }>;
      const results = JSON.parse(
         content?.[0]?.resource?.text ?? "[]",
      ) as Array<{
         url: string;
      }>;
      expect(results.length).toBeGreaterThan(0);
      expect(results[0].url.length).toBeGreaterThan(0);
   });

   /**
    * Assert the tool's OWN error payload, not just isError. The SDK sets
    * isError for any uncaught throw, so `expect(res.isError).toBe(true)` passes
    * even with the tool's catch deleted entirely. Only these tools emit a
    * resource block carrying structured JSON; a raw throw yields the SDK's text
    * content instead, so this shape is what actually pins the catch.
    */
   function expectToolErrorPayload(result: unknown): {
      error: string;
      suggestions: string[];
   } {
      const res = result as {
         isError?: boolean;
         content?: Array<{
            type?: string;
            text?: string;
            resource?: { text?: string; mimeType?: string };
         }>;
      };
      expect(res.isError).toBe(true);
      expect(res.content?.[0]?.type).toBe("resource");
      expect(res.content?.[0]?.resource?.mimeType).toBe("application/json");
      const payload = JSON.parse(res.content?.[0]?.resource?.text ?? "{}");
      expect(typeof payload.error).toBe("string");
      expect(Array.isArray(payload.suggestions)).toBe(true);

      // The structured payload alone is invisible to a client that renders
      // only text blocks on an error, which is how a real diagnostic ends up
      // reported as a bare "Unknown error". Every error must also say it in
      // plain text.
      const textBlock = res.content?.find((b) => b.type === "text");
      expect(textBlock?.text).toContain(payload.error);

      return payload;
   }

   it("malloy_getContext returns its own error payload over the protocol", async () => {
      const res = await client.callTool({
         name: "malloy_getContext",
         arguments: {
            environmentName: "nope",
            packageName: "nope",
            query: "x",
         },
      });
      expect(expectToolErrorPayload(res).error).toContain("nope");
      // getContext answers with `results` at every tier, so an error keeps the
      // key rather than making callers branch on success first.
      const payload = JSON.parse(
         (res.content as Array<{ resource?: { text?: string } }>)[0]?.resource
            ?.text ?? "{}",
      );
      expect(payload.results).toEqual([]);
   });

   it("malloy_compile returns its own error payload over the protocol", async () => {
      const res = await client.callTool({
         name: "malloy_compile",
         arguments: {
            environmentName: "nope",
            packageName: "nope",
            modelPath: "x.malloy",
            source: "run: x -> { aggregate: c is count() }",
         },
      });
      expect(expectToolErrorPayload(res).error).toContain("nope");
   });

   it("malloy_compile states a compile diagnostic in text over the protocol", async () => {
      // The diagnostics path keeps its {status, diagnostics} payload rather
      // than the {error, suggestions} of the thrown-error path, so it cannot go
      // through expectToolErrorPayload. What it shares is the invariant that
      // matters: an isError result is never resource-only. Asserted over the
      // real transport because a client that renders only text is exactly the
      // one this was invisible to.
      const res = await client.callTool({
         name: "malloy_compile",
         arguments: {
            environmentName: "compiles-badly",
            packageName: "p",
            modelPath: "m.malloy",
            source: "run: nope -> { aggregate: c is count() }",
         },
      });
      const content = res.content as Array<{
         type?: string;
         text?: string;
         resource?: { text?: string; mimeType?: string };
      }>;
      expect(res.isError).toBe(true);
      expect(content[0]?.type).toBe("resource");
      expect(content[0]?.resource?.mimeType).toBe("application/json");
      expect(JSON.parse(content[0]?.resource?.text ?? "{}").status).toBe(
         "error",
      );
      const textBlock = content.find((b) => b.type === "text");
      expect(textBlock?.text).toContain("'nope' is not defined");
      expect(textBlock?.text).toContain("field-not-found");
   });

   it("malloy_reloadPackage returns its own error payload over the protocol", async () => {
      const res = await client.callTool({
         name: "malloy_reloadPackage",
         arguments: { environmentName: "nope", packageName: "nope" },
      });
      expect(expectToolErrorPayload(res).error).toContain("nope");
   });

   it("a skill prompt returns its body", async () => {
      const res = await client.getPrompt({ name: "malloy-analysis" });
      const content = res.messages[0].content as { text?: string };
      expect((content.text ?? "").length).toBeGreaterThan(200);
   });
});
