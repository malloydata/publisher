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
 * `products` are free to change. What is pinned is the contract — the drill-down
 * comes back as that source's own card, its declared entities show up with their
 * docs, and a neighbouring source contributes nothing.
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
   entity_type: string;
   name: string;
   description?: string;
}

interface SourceCard {
   source_info: {
      resource_id: { source: string };
      docs?: string;
      joins: unknown[];
   };
   entities?: Entity[];
}

describe.serial("get_context drill-down (E2E, real model)", () => {
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
   ): Promise<{ sources: SourceCard[]; total_available?: number }> => {
      const result = (await mcpClient.callTool({
         name: "get_context",
         arguments: { environmentName: ENVIRONMENT_NAME, ...args },
      })) as { content: Array<{ resource?: { text?: string } }> };
      const text = result.content?.[0]?.resource?.text;
      if (!text) throw new Error("get_context returned no resource text");
      return JSON.parse(text) as {
         sources: SourceCard[];
         total_available?: number;
      };
   };

   it("returns the named source's own entities, not just the source row", async () => {
      const { sources } = await getContext({
         packageName: PACKAGE_NAME,
         sourceName: "products",
      });

      // One card, for the source that was asked for.
      expect(sources).toHaveLength(1);
      expect(sources[0].source_info.resource_id.source).toBe("products");
      expect(sources[0].source_info.docs).toContain("Catalog of products");

      // The regression this pins: an empty card meant an agent following the
      // documented drill-down could never learn a source's fields.
      const entities = sources[0].entities ?? [];
      expect(entities.length).toBeGreaterThan(0);

      // The declared measure comes back, carrying the #(doc) from the model —
      // proof this reached real annotations and not a fixture's.
      const measure = entities.find((e) => e.name === "product_count");
      expect(measure?.entity_type).toBe("measure");
      expect(measure?.description).toBe("Distinct products");

      // A neighbouring source's entities stay out.
      expect(entities.some((e) => e.name === "customer_count")).toBe(false);
   });

   it("surfaces a source's views", async () => {
      const { sources } = await getContext({
         packageName: PACKAGE_NAME,
         sourceName: "order_items",
      });
      const views = (sources[0].entities ?? [])
         .filter((e) => e.entity_type === "view")
         .map((e) => e.name);
      // Declared in storefront.malloy on order_items.
      expect(views).toContain("top_products");
      expect(views).toContain("business_overview");
   });

   it("without sourceName the same package still lists sources only", async () => {
      const { sources } = await getContext({ packageName: PACKAGE_NAME });
      expect(sources.length).toBeGreaterThan(0);
      // An overview names sources; nothing nests under them.
      expect(sources.every((c) => c.entities === undefined)).toBe(true);
      expect(sources.map((c) => c.source_info.resource_id.source)).toContain(
         "products",
      );
   });

   it("an unknown sourceName returns nothing rather than everything", async () => {
      const { sources, total_available } = await getContext({
         packageName: PACKAGE_NAME,
         sourceName: "not_a_source",
      });
      expect(sources).toEqual([]);
      expect(total_available).toBe(0);
   });
});
