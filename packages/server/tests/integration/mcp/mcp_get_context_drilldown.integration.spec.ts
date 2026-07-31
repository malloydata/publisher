/// <reference types="bun-types" />

/**
 * The `sourceName`-without-`query` drill-down, against a real compiled model.
 *
 * This is the tier the unit tests cover with a hand-written model stand-in, and
 * a stand-in is exactly what let the original bug ship: the filter returned
 * only the source row, and no test — unit or integration — ever asked a real
 * `getSourceInfos()` what a drill-down returns. The oracle here is the compiled
 * `storefront` package, so the entity kinds and the `source` back-pointer are
 * whatever Malloy actually produces rather than whatever a mock asserts.
 *
 * Field names are deliberately not enumerated: the parquet columns behind
 * `products` are free to change. What is pinned is the contract — the source
 * leads, every row belongs to the named source, declared entities show up with
 * their docs, and a neighbouring source contributes nothing.
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type {
   Notification,
   Request,
   Result,
} from "@modelcontextprotocol/sdk/types.js";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   cleanupE2ETestEnvironment,
   McpE2ETestEnvironment,
   setupE2ETestEnvironment,
} from "../../harness/mcp_test_setup";

const ENVIRONMENT_NAME = "examples";
const PACKAGE_NAME = "storefront";

interface Entity {
   kind: string;
   name: string;
   source?: string;
   doc?: string;
}

describe.serial("malloy_getContext drill-down (E2E, real model)", () => {
   let env: McpE2ETestEnvironment | null = null;
   let mcpClient: Client<Request, Notification, Result>;

   beforeAll(async () => {
      env = await setupE2ETestEnvironment();
      mcpClient = env.mcpClient;
   });

   afterAll(async () => {
      await cleanupE2ETestEnvironment(env);
      env = null;
   });

   const getContext = async (
      args: Record<string, unknown>,
   ): Promise<{ results: Entity[] }> => {
      const result = (await mcpClient.callTool({
         name: "malloy_getContext",
         arguments: { environmentName: ENVIRONMENT_NAME, ...args },
      })) as { content: Array<{ resource?: { text?: string } }> };
      const text = result.content?.[0]?.resource?.text;
      if (!text) throw new Error("malloy_getContext returned no resource text");
      return JSON.parse(text) as { results: Entity[] };
   };

   it("returns the named source's own entities, not just the source row", async () => {
      const { results } = await getContext({
         packageName: PACKAGE_NAME,
         sourceName: "products",
      });

      // The regression this pins: one row back meant an agent following the
      // documented drill-down could never learn a source's fields.
      expect(results.length).toBeGreaterThan(1);

      expect(results[0].kind).toBe("source");
      expect(results[0].name).toBe("products");
      expect(results[0].doc).toContain("Catalog of products");

      // Every row belongs to the source that was asked for.
      expect(results.every((e) => e.source === "products")).toBe(true);

      // The declared measure comes back, carrying the #(doc) from the model —
      // proof this reached real annotations and not a fixture's.
      const measure = results.find((e) => e.name === "product_count");
      expect(measure?.kind).toBe("measure");
      expect(measure?.doc).toBe("Distinct products");

      // A neighbouring source's entities stay out.
      expect(results.some((e) => e.name === "customer_count")).toBe(false);
   });

   it("surfaces a source's views", async () => {
      const { results } = await getContext({
         packageName: PACKAGE_NAME,
         sourceName: "order_items",
      });
      const views = results.filter((e) => e.kind === "view").map((e) => e.name);
      // Declared in storefront.malloy on order_items.
      expect(views).toContain("top_products");
      expect(views).toContain("business_overview");
   });

   it("without sourceName the same package still lists sources only", async () => {
      const { results } = await getContext({ packageName: PACKAGE_NAME });
      expect(results.length).toBeGreaterThan(0);
      expect(results.every((e) => e.kind === "source")).toBe(true);
      expect(results.map((e) => e.name)).toContain("products");
   });

   it("an unknown sourceName returns nothing rather than everything", async () => {
      const { results } = await getContext({
         packageName: PACKAGE_NAME,
         sourceName: "not_a_source",
      });
      expect(results).toEqual([]);
   });
});
