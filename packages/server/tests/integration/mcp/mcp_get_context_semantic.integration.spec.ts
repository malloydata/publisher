import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import {
   Notification,
   Request,
   Result,
} from "@modelcontextprotocol/sdk/types.js";
import http from "http";
import { AddressInfo } from "net";
import {
   McpE2ETestEnvironment,
   cleanupE2ETestEnvironment,
   setupE2ETestEnvironment,
} from "../../harness/mcp_test_setup";

/**
 * End-to-end coverage of semantic malloy_getContext retrieval: the real
 * server, the real DuckDB vector cache, and a deterministic in-test
 * embedding endpoint (letter-bigram bag-of-words vectors, so related
 * phrases genuinely land near each other, no network, no key).
 */

const ENVIRONMENT_NAME = "examples";
const PACKAGE_NAME = "storefront";
const DIMS = 64;

/** Deterministic unit vector from letter bigrams. */
function bigramVector(text: string): number[] {
   const v = new Array<number>(DIMS).fill(0);
   const s = ` ${text.toLowerCase().replace(/[^a-z0-9]+/g, " ")} `;
   for (let i = 0; i < s.length - 1; i++) {
      v[(s.charCodeAt(i) * 31 + s.charCodeAt(i + 1)) % DIMS] += 1;
   }
   const norm = Math.sqrt(v.reduce((acc, x) => acc + x * x, 0)) || 1;
   return v.map((x) => x / norm);
}

let env: McpE2ETestEnvironment | null = null;
let mcpClient: Client<Request, Notification, Result>;
let embeddingStub: http.Server;
let stubFailing = false;
let stubRequests = 0;

const savedEnv: Record<string, string | undefined> = {};
const ENV_KEYS = [
   "EMBEDDING_API_KEY",
   "EMBEDDING_API_BASE",
   "EMBEDDING_MODEL",
   "EMBEDDING_DIMENSIONS",
];

interface GetContextPayload {
   retrieval?: string;
   results?: Array<{
      kind: string;
      name: string;
      source?: string;
      score?: number;
   }>;
   error?: string;
}

async function callGetContext(
   args: Record<string, unknown>,
): Promise<GetContextPayload> {
   const result = (await mcpClient.callTool({
      name: "malloy_getContext",
      arguments: args,
   })) as {
      content: Array<{ type: string; resource?: { text?: string } }>;
   };
   const text = result.content?.[0]?.resource?.text;
   if (!text) throw new Error("malloy_getContext returned no resource text");
   return JSON.parse(text) as GetContextPayload;
}

describe.serial("MCP getContext semantic retrieval (E2E Integration)", () => {
   beforeAll(async () => {
      // A local OpenAI-compatible /embeddings endpoint with deterministic
      // vectors. Bound to 127.0.0.1:0; the real port comes from the OS.
      embeddingStub = http.createServer((req, res) => {
         let body = "";
         req.on("data", (chunk) => (body += chunk));
         req.on("end", () => {
            stubRequests++;
            if (stubFailing) {
               res.writeHead(500).end("stub down");
               return;
            }
            const { input } = JSON.parse(body) as { input: string[] };
            res.writeHead(200, { "Content-Type": "application/json" }).end(
               JSON.stringify({
                  data: input.map((text, index) => ({
                     index,
                     embedding: bigramVector(text),
                  })),
               }),
            );
         });
      });
      await new Promise<void>((resolve) =>
         embeddingStub.listen(0, "127.0.0.1", resolve),
      );
      const port = (embeddingStub.address() as AddressInfo).port;

      // Set the embedding env vars BEFORE the server harness dynamically
      // imports src/server; config is read per call, but this keeps the
      // whole server lifetime consistently configured.
      for (const key of ENV_KEYS) {
         savedEnv[key] = process.env[key];
      }
      process.env.EMBEDDING_API_KEY = "integration-test-key";
      process.env.EMBEDDING_API_BASE = `http://127.0.0.1:${port}/v1`;
      delete process.env.EMBEDDING_MODEL;
      delete process.env.EMBEDDING_DIMENSIONS;

      env = await setupE2ETestEnvironment();
      mcpClient = env.mcpClient;
   }, 200000);

   afterAll(async () => {
      await cleanupE2ETestEnvironment(env);
      env = null;
      // Restore the embedding env so later suites in this process run
      // unconfigured (the provider getter re-reads env per call).
      for (const key of ENV_KEYS) {
         if (savedEnv[key] === undefined) delete process.env[key];
         else process.env[key] = savedEnv[key];
      }
      await new Promise<void>((resolve, reject) =>
         embeddingStub.close((err) => (err ? reject(err) : resolve())),
      );
   });

   it(
      "answers lexically while indexing, then flips to semantic with scores",
      async () => {
         const first = await callGetContext({
            environmentName: ENVIRONMENT_NAME,
            packageName: PACKAGE_NAME,
            query: "total sales revenue",
         });
         // Configured server: the marker is always present on tier 4.
         expect(["lexical", "semantic"]).toContain(first.retrieval);

         let payload = first;
         for (let i = 0; i < 60 && payload.retrieval !== "semantic"; i++) {
            await new Promise((resolve) => setTimeout(resolve, 500));
            payload = await callGetContext({
               environmentName: ENVIRONMENT_NAME,
               packageName: PACKAGE_NAME,
               query: "total sales revenue",
            });
         }
         expect(payload.retrieval).toBe("semantic");
         expect(stubRequests).toBeGreaterThan(0);

         const results = payload.results ?? [];
         expect(results.length).toBeGreaterThan(0);
         for (const r of results) {
            expect(typeof r.score).toBe("number");
         }
         // The bigram embedding puts "total sales revenue" on top of the
         // total_sales measure ("total sales" once humanized).
         expect(results.some((r) => r.name === "total_sales")).toBe(true);
      },
      { timeout: 60000 },
   );

   it(
      "keeps the semantic index warm across calls (no re-embed per call)",
      async () => {
         const before = stubRequests;
         const payload = await callGetContext({
            environmentName: ENVIRONMENT_NAME,
            packageName: PACKAGE_NAME,
            query: "orders by month",
         });
         expect(payload.retrieval).toBe("semantic");
         // Exactly one stub call: the query embedding. No bulk re-sync.
         expect(stubRequests).toBe(before + 1);
      },
      { timeout: 30000 },
   );

   it(
      "narrows semantic retrieval with sourceName",
      async () => {
         const payload = await callGetContext({
            environmentName: ENVIRONMENT_NAME,
            packageName: PACKAGE_NAME,
            query: "total sales revenue",
            sourceName: "order_items",
         });
         expect(payload.retrieval).toBe("semantic");
         for (const r of payload.results ?? []) {
            expect(r.source).toBe("order_items");
         }
      },
      { timeout: 30000 },
   );

   // Keep this test LAST: the induced failure starts the provider
   // cool-down, which short-circuits the semantic path for its window.
   it(
      "falls back to lexical, marked, when the embedding endpoint fails",
      async () => {
         stubFailing = true;
         try {
            const payload = await callGetContext({
               environmentName: ENVIRONMENT_NAME,
               packageName: PACKAGE_NAME,
               query: "top selling products",
            });
            expect(payload.retrieval).toBe("lexical");
            const results = payload.results ?? [];
            expect(results.length).toBeGreaterThan(0);
            for (const r of results) {
               expect(r.score).toBeUndefined();
            }
         } finally {
            stubFailing = false;
         }
      },
      { timeout: 30000 },
   );
});
