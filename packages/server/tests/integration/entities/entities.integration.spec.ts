// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's `entities` ride the single-package responses a host reads after
 * a load (the publish, a get, a reload) and never a listing. Over HTTP against
 * the real Express app and the real worker-pool load, because the contract is
 * that the load's own compile answers it. A package whose `#(index)` or
 * required `#(filter)` the index cannot honor is refused with a 400.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { components } from "../../../src/api";
import { startRestE2E } from "../../harness/rest_e2e";

type ModelEntities = components["schemas"]["ModelEntities"];

const ENV_NAME = "entities-env";

const ROWS = `duckdb.sql("select * from (values (1, 'web', 'us'), (2, 'store', 'eu')) as t(id, channel, region)")`;

const ORDERS = `##! experimental.givens

#(secure)
given: ROLES :: string[]

#(doc) Orders in every channel.
#(filter) name=channel dimension=channel type=equal required
source: orders is ${ROWS} extend {
  #(index)
  dimension: region_label is concat('r-', region)
  measure: order_count is count()
}
`;

const HELPER = `source: helper is ${ROWS}
`;

describe("entities on the package responses", () => {
   let env: Awaited<ReturnType<typeof startRestE2E>> | undefined;
   let baseUrl = "";
   let tempDir = "";

   function writePackage(
      name: string,
      files: Record<string, string>,
      manifest: Record<string, unknown> = {},
   ): string {
      const dir = path.join(tempDir, name);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name, ...manifest }),
      );
      for (const [file, text] of Object.entries(files)) {
         fs.writeFileSync(path.join(dir, file), text);
      }
      return dir;
   }

   async function publish(
      name: string,
      location: string,
   ): Promise<{ status: number; json: Record<string, unknown> }> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name, location }),
         },
      );
      return {
         status: res.status,
         json: (await res.json()) as Record<string, unknown>,
      };
   }

   async function get(path_: string): Promise<unknown> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}${path_}`,
      );
      expect(res.status).toBe(200);
      return res.json();
   }

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "entities-"));
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);
   }, 180_000);

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("the publish, get and reload responses carry every model's entities", async () => {
      // `helper.malloy` is off the surface and still reported.
      const location = writePackage(
         "shop",
         { "orders.malloy": ORDERS, "helper.malloy": HELPER },
         { explores: ["orders.malloy"] },
      );
      const created = await publish("shop", location);
      expect(created.status).toBe(200);
      const entities = created.json.entities as ModelEntities[];
      expect(entities.map((m) => m.modelPath)).toEqual([
         "helper.malloy",
         "orders.malloy",
      ]);

      const orders = entities.find((m) => m.modelPath === "orders.malloy")!;
      expect(orders.error).toBeUndefined();
      expect(orders.givens?.map((g) => [g.name, g.type, g.secure])).toEqual([
         ["ROLES", "string[]", true],
      ]);
      const source = orders.sources!.find((s) => s.name === "orders")!;
      expect(source.joinOnly).toBe(false);
      expect(source.queryable).toBe(true);
      expect(source.doc).toBe("Orders in every channel.");
      expect(source.filters?.map((f) => [f.name, f.required])).toEqual([
         ["channel", true],
      ]);
      const regionLabel = orders.fields!.find(
         (f) => f.name === "region_label",
      )!;
      expect(regionLabel.kind).toBe("dimension");
      expect(regionLabel.source).toBe("orders");
      expect(regionLabel.indexFingerprint?.sqlHash).toMatch(/^[0-9a-f]{64}$/);
      expect(regionLabel.indexFingerprint?.requiredFilterDimensions).toEqual([
         "channel",
      ]);
      expect(orders.fields!.find((f) => f.name === "order_count")!.kind).toBe(
         "measure",
      );
      expect(orders.sourceSchemas!.map((s) => s.name)).toEqual(["orders"]);

      // Off the surface: reported, but not queryable.
      const helper = entities.find((m) => m.modelPath === "helper.malloy")!;
      expect(helper.sources!.find((s) => s.name === "helper")!.queryable).toBe(
         false,
      );

      expect(
         ((await get("/packages/shop")) as Record<string, unknown>).entities,
      ).toEqual(entities);
      expect(
         ((await get("/packages/shop?reload=true")) as Record<string, unknown>)
            .entities,
      ).toEqual(entities);
   }, 120_000);

   it("a listing never carries them", async () => {
      const listed = (await get("/packages")) as Record<string, unknown>[];
      expect(listed.length).toBeGreaterThan(0);
      for (const entry of listed) {
         expect(entry.entities).toBeUndefined();
      }
   }, 60_000);

   it("a package with an #(index) on a measure is refused with a 400 naming it", async () => {
      const location = writePackage("bad_index", {
         "bad.malloy": `source: bad is ${ROWS} extend {
  #(index)
  measure: n is count()
}
`,
      });
      const created = await publish("bad_index", location);
      expect(created.status).toBe(400);
      expect(JSON.stringify(created.json)).toContain("is not a dimension");
   }, 120_000);

   it("a package with two required filters on one dimension is refused with a 400", async () => {
      const location = writePackage("bad_filter", {
         "bad.malloy": `#(filter) name=a dimension=channel type=equal required
#(filter) name=b dimension=channel type=equal required
source: bad is ${ROWS}
`,
      });
      const created = await publish("bad_filter", location);
      expect(created.status).toBe(400);
      expect(JSON.stringify(created.json)).toContain(
         "multiple required filters",
      );
   }, 120_000);
});
